#!/usr/bin/env python3
"""
DATABASE LOADER - CARGA DE DATOS A BASE DE DATOS
================================================
Carga datos procesados al data warehouse
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from typing import Dict, Any
from pathlib import Path
import os
import logging


class DatabaseLoader:
    """Loader de datos a PostgreSQL"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def load_table(
        self, file_path: Path, table_name: str, strategy: str = "truncate_and_load"
    ) -> int:
        """
        Carga un archivo parquet a una tabla

        Args:
            file_path: Ruta al archivo parquet
            table_name: Nombre de la tabla
            strategy: Estrategia de carga

        Returns:
            Número de registros cargados
        """
        # Leer archivo
        df = pd.read_parquet(file_path)
        
        # Skip si no hay datos
        if len(df) == 0:
            self.logger.info(f"⏭️  {table_name} vacía, skipping...")
            return 0

        # Conectar a base de datos
        conn = self._get_dw_connection()

        try:
            if strategy == "truncate_and_load":
                self._truncate_and_load(conn, table_name, df)
            elif strategy == "incremental":
                self._incremental_load(conn, table_name, df)
            elif strategy == "upsert":
                self._upsert_load(conn, table_name, df)
            else:
                raise ValueError(f"Estrategia desconocida: {strategy}")

            conn.commit()
            return len(df)

        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def _truncate_and_load(self, conn, table_name: str, df: pd.DataFrame):
        """Trunca tabla y carga datos"""
        cursor = conn.cursor()

        try:
            # Terminar cualquier transacción bloqueante en la tabla
            cursor.execute(f"SET statement_timeout = '5s'")
            try:
                cursor.execute(f"""
                    SELECT pg_terminate_backend(pid) 
                    FROM pg_stat_activity 
                    WHERE datname = current_database() 
                    AND pid <> pg_backend_pid()
                    AND state = 'idle in transaction'
                    AND query_start < NOW() - INTERVAL '30 seconds'
                """)
            except:
                pass
            
            # Usar DELETE en lugar de TRUNCATE para evitar locks exclusivos
            cursor.execute(f"SET statement_timeout = '30s'")
            cursor.execute(f"DELETE FROM {table_name}")
            cursor.execute(f"SET statement_timeout = '30min'")
            
            # Insertar datos
            if len(df) > 0:
                # Obtener columnas de la tabla desde la base de datos
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' 
                    AND table_schema = 'public'
                    AND column_name NOT IN ('created_at', 'updated_at')
                    ORDER BY ordinal_position
                """)
                
                db_columns = [row[0] for row in cursor.fetchall()]
                
                # Mapear columnas del DF a columnas de la BD
                column_mapping = {}
                for db_col in db_columns:
                    if db_col in df.columns:
                        # Coincidencia exacta - SIEMPRE preferir esto
                        column_mapping[db_col] = db_col
                    elif db_col.endswith('_id'):
                        # Buscar columna extendida SOLO si no existe coincidencia exacta
                        # Ej: cuenta_id en BD busca cuenta_contable_id, centro_costo_id
                        base_name = db_col[:-3]  # Quitar '_id'
                        matching_cols = [c for c in df.columns 
                                        if c != db_col  # No auto-mapear
                                        and base_name in c 
                                        and c.endswith('_id')
                                        and len(c) > len(db_col)]  # Solo columnas MÁS largas (más específicas)
                        if matching_cols:
                            # Preferir la columna más específica
                            column_mapping[db_col] = max(matching_cols, key=len)
                
                print(f"MAPPING {table_name}: {list(column_mapping.items())[:5]}")
                
                if not column_mapping:
                    self.logger.warning(f"No hay columnas coincidentes para {table_name}")
                    cursor.close()
                    return
                
                # Crear DataFrame con columnas renombradas
                df_to_load = df[[column_mapping[db_col] for db_col in column_mapping.keys()]].copy()
                df_to_load.columns = list(column_mapping.keys())  # Renombrar a nombres de BD
                
                # Convertir valores numpy a Python nativos
                def convert_value(val):
                    if pd.isna(val):
                        return None
                    if hasattr(val, 'item'):  # numpy types tienen .item()
                        return val.item()
                    return val
                
                values = [tuple(convert_value(v) for v in row) for row in df_to_load.values]

                # Usar los nombres de columnas de BD (ya mapeados en df_to_load)
                insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(df_to_load.columns)})
                    VALUES %s
                """

                # Cargar en batches con commits intermedios para mejor rendimiento
                batch_size = 10000
                total_loaded = 0
                
                for i in range(0, len(values), batch_size):
                    batch = values[i:i + batch_size]
                    execute_values(cursor, insert_query, batch, page_size=1000)
                    conn.commit()
                    total_loaded += len(batch)
                    
                self.logger.debug(f"Cargados {total_loaded} registros en {table_name}")
            
            # Si es dimensión, resetear secuencia DESPUÉS de la inserción
            print(f"POST-INSERT: {table_name}, dim={table_name.startswith('dim_')}, len={len(df)}")
            
            if table_name.startswith('dim_') and len(df) > 0:
                print(f"RESET SEQ: {table_name}")
                try:
                    # Obtener la columna PK de la tabla en BD
                    cursor.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = '{table_name}' 
                        AND table_schema = 'public'
                        AND column_name LIKE '%_id'
                        AND ordinal_position = 1
                        LIMIT 1
                    """)
                    result = cursor.fetchone()
                    print(f"PK: {result}")
                    
                    if result:
                        db_id_col = result[0]
                        df_id_col = next((col for col in df.columns if col.endswith('_id')), None)
                        print(f"DF col: {df_id_col}")
                        
                        if df_id_col:
                            max_id = df[df_id_col].max()
                            print(f"Max: {max_id}")
                            if pd.notna(max_id) and max_id > 0:
                                seq_name = f"{table_name}_{db_id_col}_seq"
                                cursor.execute(f"SELECT setval('{seq_name}', {int(max_id)}, true)")
                                conn.commit()
                                print(f"🔄 {seq_name} → {int(max_id)}")
                                self.logger.info(f"🔄 Secuencia {seq_name} → {int(max_id)}")
                except Exception as e:
                    print(f"ERROR SEQ: {e}")
                    self.logger.warning(f"⚠️ Error reseteando secuencia de {table_name}: {e}")
        
        finally:
            cursor.close()

    def _incremental_load(self, conn, table_name: str, df: pd.DataFrame):
        """Carga incremental (solo nuevos registros)"""
        # TODO: Implementar lógica incremental
        self._truncate_and_load(conn, table_name, df)

    def _upsert_load(self, conn, table_name: str, df: pd.DataFrame):
        """Upsert (actualiza o inserta)"""
        # TODO: Implementar lógica upsert
        self._truncate_and_load(conn, table_name, df)

    def _get_dw_connection(self):
        """Obtiene conexión al data warehouse"""
        conn = psycopg2.connect(
            host=os.getenv("DW_DB_HOST"),
            port=int(os.getenv("DW_DB_PORT")),
            dbname=os.getenv("DW_DB_NAME"),
            user=os.getenv("DW_DB_USER"),
            password=os.getenv("DW_DB_PASS"),
            connect_timeout=120,
            options="-c statement_timeout=1800000"  # 30 minutos timeout
        )
        # Asegurar autocommit para evitar transacciones idle
        conn.set_session(autocommit=False)
        return conn
