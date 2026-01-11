#!/usr/bin/env python3
"""
SIMPLE DATABASE LOADER - Cargador simple de datos a PostgreSQL
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class SimpleDatabaseLoader:
    """Loader simple de datos a PostgreSQL"""
    
    def __init__(self):
        self.conn_params = {
            'host': os.getenv('DW_DB_HOST'),
            'port': int(os.getenv('DW_DB_PORT')),
            'dbname': os.getenv('DW_DB_NAME'),
            'user': os.getenv('DW_DB_USER'),
            'password': os.getenv('DW_DB_PASS'),
            'connect_timeout': 120,
            'options': '-c statement_timeout=1800000'
        }
    
    def load_to_database(self, file_path: str, table_name: str) -> int:
        """Cargar archivo parquet a tabla PostgreSQL"""
        
        # Leer archivo
        df = pd.read_parquet(file_path)
        
        if df.empty:
            logger.warning(f"⚠️ Archivo vacío: {file_path}")
            return 0
        
        # Conexión
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()
        
        try:
            # Obtener columnas de la tabla destino
            cursor.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            
            db_columns = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Filtrar columnas que existen en ambos (df y tabla)
            common_columns = [col for col in df.columns if col in db_columns]
            
            if not common_columns:
                logger.error(f"❌ No hay columnas en común entre parquet y tabla {table_name}")
                return 0
            
            # Seleccionar solo columnas comunes
            df_to_load = df[common_columns].copy()
            
            # Convertir tipos de datos
            for col in common_columns:
                if db_columns[col].startswith('timestamp'):
                    df_to_load[col] = pd.to_datetime(df_to_load[col], errors='coerce')
                elif db_columns[col] == 'date':
                    df_to_load[col] = pd.to_datetime(df_to_load[col], errors='coerce').dt.date
                elif db_columns[col].startswith('boolean'):
                    df_to_load[col] = df_to_load[col].fillna(False).astype(bool)
            
            # Reemplazar NaN con None
            df_to_load = df_to_load.where(pd.notna(df_to_load), None)
            
            # Convertir tipos numpy a tipos nativos de Python para evitar errores en psycopg2
            import numpy as np
            
            def convert_value(val):
                """Convertir valor numpy a tipo nativo Python"""
                if val is None or pd.isna(val):
                    return None
                if isinstance(val, (np.integer, np.int64, np.int32)):
                    return int(val)
                if isinstance(val, (np.floating, np.float64, np.float32)):
                    return float(val)
                if isinstance(val, np.bool_):
                    return bool(val)
                return val
            
            # Truncar tabla
            logger.info(f"🗑️  Truncando tabla {table_name}...")
            cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
            
            # Preparar datos para inserción
            columns = ', '.join(common_columns)
            placeholders = ', '.join(['%s'] * len(common_columns))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # Insertar datos en lotes
            batch_size = 1000
            total_rows = len(df_to_load)
            rows_inserted = 0
            
            for i in range(0, total_rows, batch_size):
                batch = df_to_load.iloc[i:i+batch_size]
                values = [tuple(convert_value(val) for val in row) for row in batch.values]
                
                cursor.executemany(insert_query, values)
                rows_inserted += len(values)
                
                if rows_inserted % 5000 == 0:
                    logger.info(f"   ↳ {rows_inserted:,} / {total_rows:,} registros insertados...")
            
            conn.commit()
            logger.info(f"✅ {table_name}: {rows_inserted:,} registros cargados exitosamente")
            
            return rows_inserted
            
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error cargando {table_name}: {str(e)}", exc_info=True)
            return 0
            
        finally:
            cursor.close()
            conn.close()
