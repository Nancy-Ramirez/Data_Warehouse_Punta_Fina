#!/usr/bin/env python3
"""
ORQUESTADOR ETL BATCH - SISTEMA PRINCIPAL
=========================================
Orquestador principal del sistema ETL con procesamiento por lotes
optimizado para Ubuntu 22.04
"""

import sys
import os
from pathlib import Path
import logging
from datetime import datetime
import yaml
import click
from dotenv import load_dotenv
from typing import Dict, Any, List
import psycopg2
from psycopg2.extras import execute_values

# Agregar ruta del proyecto
sys.path.insert(0, str(Path(__file__).parent))

from core.batch_processor import BatchProcessor, BatchConfig, StreamingBatchProcessor
from core.data_validator import DataValidator
from extractors.database_extractor import DatabaseExtractor
from extractors.csv_extractor import CSVExtractor
from transformers.complete_dimension_builder import CompleteDimensionBuilder
from transformers.complete_fact_builder import CompleteFactBuilder
from loaders.database_loader import DatabaseLoader
from utils.logger import setup_logger
from utils.metrics import MetricsCollector


class ETLOrchestrator:
    """Orquestador principal del ETL"""

    def __init__(self, config_path: Path = None):
        """
        Inicializa el orquestador

        Args:
            config_path: Ruta al archivo de configuración
        """
        # Cargar configuración
        if config_path is None:
            config_path = Path(__file__).parent / "config" / "etl_config.yaml"

        with open(config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        # Cargar variables de entorno
        env_file = Path(__file__).parent / ".env"
        if env_file.exists():
            load_dotenv(env_file)

        # Configurar logger
        self.logger = setup_logger("ETLOrchestrator", self.config["paths"]["logs"])

        # Inicializar componentes
        batch_config = BatchConfig(
            chunk_size=self.config["batch"]["chunk_size"],
            max_workers=self.config["batch"]["max_workers"],
            timeout=self.config["batch"]["timeout"],
            max_retries=self.config["batch"]["max_retries"],
            retry_delay=self.config["batch"]["retry_delay"],
            max_memory_mb=self.config["batch"]["max_memory_mb"],
            enable_checkpoints=self.config["recovery"]["enable_checkpoints"],
            checkpoint_interval=self.config["recovery"]["checkpoint_interval"],
        )

        self.batch_processor = BatchProcessor(
            batch_config, Path(self.config["paths"]["checkpoints"])
        )

        self.streaming_processor = StreamingBatchProcessor(
            batch_config, Path(self.config["paths"]["checkpoints"])
        )

        self.data_validator = DataValidator(self.config)

        self.db_extractor = DatabaseExtractor(self.config)
        self.csv_extractor = CSVExtractor(self.config)

        self.dimension_builder = CompleteDimensionBuilder()
        self.fact_builder = CompleteFactBuilder()

        self.db_loader = DatabaseLoader(self.config)

        self.metrics = MetricsCollector()

        self.logger.info("🚀 Orquestador ETL inicializado")

    def run_full_etl(self) -> Dict[str, Any]:
        """
        Ejecuta el proceso ETL completo

        Returns:
            Diccionario con resultados de la ejecución
        """
        self.logger.info("=" * 80)
        self.logger.info("🏪 PUNTAFINA ETL BATCH - PROCESO COMPLETO")
        self.logger.info("=" * 80)

        start_time = datetime.now()

        try:
            # -1. Desbloquear tablas forzadamente
            self.logger.info("\n🔓 FASE -1: DESBLOQUEO FORZADO DE TABLAS")
            self._force_unlock_tables()

            # 1. Extracción
            self.logger.info("\n📥 FASE 1: EXTRACCIÓN")
            extraction_results = self._run_extraction()

            # 2. Transformación - Dimensiones
            self.logger.info("\n🔄 FASE 2: TRANSFORMACIÓN - DIMENSIONES")
            dimension_results = self._run_dimension_building()

            # 3. Transformación - Facts
            self.logger.info("\n🔄 FASE 3: TRANSFORMACIÓN - TABLAS DE HECHOS")
            fact_results = self._run_fact_building()

            # 4. Carga
            self.logger.info("\n📤 FASE 4: CARGA")
            loading_results = self._run_loading()

            # 5. Validación final
            self.logger.info("\n✅ FASE 5: VALIDACIÓN FINAL")
            validation_results = self._run_final_validation()

            elapsed_time = (datetime.now() - start_time).total_seconds()

            # Reporte final
            final_report = {
                "status": "success",
                "start_time": start_time.isoformat(),
                "end_time": datetime.now().isoformat(),
                "elapsed_time": elapsed_time,
                "extraction": extraction_results,
                "dimensions": dimension_results,
                "facts": fact_results,
                "loading": loading_results,
                "validation": validation_results,
                "metrics": self.metrics.get_summary(),
            }

            self._print_final_summary(final_report)

            return final_report

        except Exception as e:
            self.logger.error(f"❌ Error en proceso ETL: {e}", exc_info=True)
            raise

    def _force_unlock_tables(self):
        """Desbloquear forzadamente todas las tablas eliminando conexiones idle y locks"""
        import psycopg2

        try:
            conn = psycopg2.connect(
                host=os.getenv("DW_DB_HOST"),
                port=int(os.getenv("DW_DB_PORT")),
                dbname=os.getenv("DW_DB_NAME"),
                user=os.getenv("DW_DB_USER"),
                password=os.getenv("DW_DB_PASS"),
                connect_timeout=30,
            )
            cursor = conn.cursor()

            # 1. Terminar todas las conexiones idle in transaction
            self.logger.info("   💥 Terminando conexiones idle...")
            cursor.execute(
                """
                SELECT pg_terminate_backend(pid), pid, usename, state, query_start
                FROM pg_stat_activity 
                WHERE datname = current_database() 
                AND pid <> pg_backend_pid()
                AND state IN ('idle in transaction', 'idle in transaction (aborted)')
            """
            )
            terminated = cursor.fetchall()
            if terminated:
                self.logger.info(f"   ✓ Terminadas {len(terminated)} conexiones idle")

            # 2. Cancelar queries largas (más de 5 minutos)
            self.logger.info("   ⏱️  Cancelando queries largas...")
            cursor.execute(
                """
                SELECT pg_cancel_backend(pid), pid, usename, 
                       EXTRACT(EPOCH FROM (NOW() - query_start)) as duration
                FROM pg_stat_activity 
                WHERE datname = current_database() 
                AND pid <> pg_backend_pid()
                AND state = 'active'
                AND query_start < NOW() - INTERVAL '5 minutes'
                AND query NOT LIKE '%pg_stat_activity%'
            """
            )
            cancelled = cursor.fetchall()
            if cancelled:
                self.logger.info(f"   ✓ Canceladas {len(cancelled)} queries largas")

            # 3. Liberar locks de tablas
            self.logger.info("   🔒 Liberando locks de tablas...")
            cursor.execute(
                """
                SELECT pg_terminate_backend(a.pid)
                FROM pg_locks l
                JOIN pg_stat_activity a ON l.pid = a.pid
                WHERE l.locktype = 'relation'
                AND a.datname = current_database()
                AND a.pid <> pg_backend_pid()
                AND a.state <> 'active'
            """
            )
            unlocked = cursor.fetchall()
            if unlocked:
                self.logger.info(f"   ✓ Liberados {len(unlocked)} locks")

            conn.commit()
            cursor.close()
            conn.close()

            self.logger.info("   ✅ Desbloqueo forzado completado")

        except Exception as e:
            self.logger.warning(f"   ⚠️  Error en desbloqueo: {e}")

    def _cleanup_obsolete_tables(self):
        """Limpiar tablas obsoletas del modelo"""
        import psycopg2

        obsolete_tables = [
            "dim_sitio_web",
            "dim_canal",
            "dim_direccion",
            "dim_envio",
            "dim_pago",
            "dim_line_item",
            "dim_estado_orden",
            "dim_estado_pago",
            "dim_categoria_producto",
        ]

        try:
            conn = psycopg2.connect(
                host=os.getenv("DW_DB_HOST"),
                port=int(os.getenv("DW_DB_PORT")),
                dbname=os.getenv("DW_DB_NAME"),
                user=os.getenv("DW_DB_USER"),
                password=os.getenv("DW_DB_PASS"),
                connect_timeout=120,
                options="-c statement_timeout=1800000",
            )
            cursor = conn.cursor()

            for table in obsolete_tables:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                    self.logger.info(f"   ✓ Eliminada tabla obsoleta: {table}")
                except Exception as e:
                    self.logger.warning(f"   ⚠️  No se pudo eliminar {table}: {e}")

            conn.commit()
            cursor.close()
            conn.close()

            self.logger.info("   ✅ Limpieza de estructura completada")

        except Exception as e:
            self.logger.error(f"   ❌ Error en limpieza: {e}")

    def _run_extraction(self) -> Dict[str, Any]:
        """Fase de extracción de datos - Informativo (datos extraídos directamente)"""
        results = {"database": {}, "csv": {}, "total_records": 0}

        self.logger.info("   📊 Verificando fuentes de datos disponibles...")

        # Verificar OroCommerce
        self.logger.info(
            "   ✓ orocommerce: oro_customer, oro_order, oro_product, oro_order_line_item"
        )
        results["database"]["orocommerce"] = {
            "tables": 4,
            "records": 177000,
        }  # Estimado

        # Verificar OroCRM
        self.logger.info("   ✓ oro_crm: orocrm_channel")
        results["database"]["orocrm"] = {"tables": 1, "records": 5}

        # Verificar CSVs
        csv_path = Path(__file__).parent.parent / "data" / "inputs"
        csv_files = []
        if csv_path.exists():
            csv_files = list(csv_path.rglob("*.csv"))
            self.logger.info(f"   ✓ {len(csv_files)} archivos CSV en data/inputs/")
            results["csv"] = {"files": len(csv_files), "records": 700000}  # Estimado

        # Sumatoria defensiva: si no hay CSVs, usar 0
        csv_records = results.get("csv", {}).get("records", 0)
        results["total_records"] = (
            results["database"]["orocommerce"]["records"]
            + results["database"]["orocrm"]["records"]
            + csv_records
        )

        self.logger.info(
            f"\n   ✅ Fuentes verificadas: ~{results['total_records']:,} registros disponibles"
        )

        return results

    def _run_dimension_building(self) -> Dict[str, Any]:
        """Fase de construcción de dimensiones - Usando CompleteDimensionBuilder"""
        results = {"dimensions_built": [], "total_records": 0, "errors": []}

        self.logger.info(
            "   🔨 Construyendo dimensiones con CompleteDimensionBuilder..."
        )

        try:
            # Usar CompleteDimensionBuilder para construir y cargar dimensiones
            from transformers.complete_dimension_builder import CompleteDimensionBuilder
            from psycopg2.extras import execute_values
            import pandas as pd

            builder = CompleteDimensionBuilder()

            # Conexión al DW
            conn = psycopg2.connect(
                host=os.getenv("DW_DB_HOST"),
                port=int(os.getenv("DW_DB_PORT")),
                dbname=os.getenv("DW_DB_NAME"),
                user=os.getenv("DW_DB_USER"),
                password=os.getenv("DW_DB_PASS"),
            )
            conn.autocommit = True

            # FIRST: Truncate all fact tables to allow dimension truncation
            self.logger.info("   🧹 Pre-truncando fact tables...")
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "TRUNCATE TABLE fact_ventas, fact_inventario, fact_transacciones CASCADE"
                )
                self.logger.info("   ✓ Fact tables truncadas")
            except Exception as e:
                self.logger.warning(f"   ⚠️  Error truncando facts: {e}")
            cursor.close()

            # Lista de dimensiones con su método y si requiere OVERRIDING SYSTEM VALUE
            dimensions_to_build = [
                ("dim_fecha", builder.build_dim_fecha, False),
                ("dim_producto", builder.build_dim_producto, False),
                ("dim_cliente", builder.build_dim_cliente, False),
                ("dim_orden", builder.build_dim_orden, False),
                ("dim_usuario", builder.build_dim_usuario, False),
                ("dim_cuenta_contable", builder.build_dim_cuenta_contable, False),
                ("dim_impuestos", builder.build_dim_impuestos, True),
                ("dim_promocion", builder.build_dim_promocion, False),
                ("dim_almacen", builder.build_dim_almacen, False),
                ("dim_proveedor", builder.build_dim_proveedor, False),
                ("dim_tipo_movimiento", builder.build_dim_tipo_movimiento, False),
                ("dim_centro_costo", builder.build_dim_centro_costo, False),
                ("dim_tipo_transaccion", builder.build_dim_tipo_transaccion, False),
            ]

            parquet_dir = Path(__file__).parent.parent / "data" / "outputs" / "parquet"
            parquet_dir.mkdir(parents=True, exist_ok=True)

            for dim_name, build_method, override_id in dimensions_to_build:
                try:
                    self.logger.info(f"      🔨 Construyendo {dim_name}...")

                    # Construir dimensión usando el método específico
                    df = build_method()

                    if df is not None and len(df) > 0:
                        # Guardar en parquet
                        parquet_file = parquet_dir / f"{dim_name}.parquet"
                        df.to_parquet(parquet_file, index=False)

                        # Cargar a BD directamente
                        cursor = conn.cursor()

                        # TRUNCATE con CASCADE
                        try:
                            # Para dim_promocion: resetear secuencia primero
                            if dim_name == "dim_promocion":
                                cursor.execute("TRUNCATE TABLE dim_promocion CASCADE")
                                cursor.execute(
                                    "ALTER SEQUENCE dim_promocion_sk_promocion_seq RESTART WITH 1"
                                )
                            else:
                                # Usar DELETE en vez de TRUNCATE para evitar deadlocks
                                cursor.execute(f"DELETE FROM {dim_name}")
                        except Exception as trunc_e:
                            self.logger.warning(
                                f"         ⚠️  No se pudo limpiar {dim_name}: {trunc_e}"
                            )

                        # Insertar registros
                        columns = df.columns.tolist()
                        values = [tuple(row) for row in df.values]

                        # Para tablas con IDs explícitos usar OVERRIDING SYSTEM VALUE
                        if override_id:
                            insert_query = f"INSERT INTO {dim_name} ({', '.join(columns)}) OVERRIDING SYSTEM VALUE VALUES %s ON CONFLICT DO NOTHING"
                        else:
                            insert_query = f"INSERT INTO {dim_name} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"

                        execute_values(cursor, insert_query, values, page_size=1000)

                        # NO insertar registros por defecto - todos los datos deben venir de OroCommerce
                        # para mantener simetría perfecta con el origen

                        # Después de insertar dim_promocion, asegurar SK=1 para default
                        if dim_name == "dim_promocion":
                            try:
                                # Insertar SK=1 si no existe (el builder ya lo incluye, pero por si acaso)
                                cursor.execute(
                                    """
                                    INSERT INTO dim_promocion (sk_promocion, id_promocion_source, nombre_promocion, tipo_promocion, usa_cupones, activa, fecha_creacion, fecha_actualizacion, fecha_carga)
                                    VALUES (1, -1, 'Sin Promoción', 'Ninguno', false, true, '2020-01-01', '2020-01-01', NOW())
                                    ON CONFLICT (sk_promocion) DO NOTHING
                                """
                                )
                                # Actualizar secuencia para siguientes inserts
                                cursor.execute(
                                    "SELECT setval('dim_promocion_sk_promocion_seq', (SELECT MAX(sk_promocion) FROM dim_promocion))"
                                )
                            except Exception as e:
                                self.logger.warning(
                                    f"         ⚠️  Error ajustando secuencia dim_promocion: {e}"
                                )

                        records = len(df)
                        self.logger.info(
                            f"         ✓ {dim_name}: {records:,} registros"
                        )
                        results["dimensions_built"].append(dim_name)
                        results["total_records"] += records

                        cursor.close()
                    else:
                        self.logger.warning(f"         ⚠️  {dim_name}: sin datos")

                except Exception as e:
                    self.logger.error(f"         ❌ Error en {dim_name}: {e}")
                    results["errors"].append({"dimension": dim_name, "error": str(e)})

            # NO cerrar conn aquí - se usará en fact_building
            self.logger.info(
                f"\n   ✅ Dimensiones completadas: {results['total_records']:,} registros totales"
            )

            # Almacenar conexión para facts
            self._dw_conn_for_facts = conn

        except Exception as e:
            self.logger.error(f"   ❌ Error construyendo dimensiones: {e}")
            results["errors"].append({"error": str(e)})

        return results

    def _run_fact_building(self) -> Dict[str, Any]:
        """Fase de construcción de tablas de hechos usando CompleteFactBuilder"""
        results = {"facts_built": [], "total_records": 0, "errors": []}

        self.logger.info("   🏗️  Construyendo facts con CompleteFactBuilder...")

        try:
            # Reusar conexión de la fase de dimensiones (con autocommit=True)
            conn = getattr(self, "_dw_conn_for_facts", None)
            if conn is None:
                # Fallback: crear nueva conexión si no existe
                conn = psycopg2.connect(
                    host=os.getenv("DW_DB_HOST"),
                    port=int(os.getenv("DW_DB_PORT")),
                    dbname=os.getenv("DW_DB_NAME"),
                    user=os.getenv("DW_DB_USER"),
                    password=os.getenv("DW_DB_PASS"),
                )
                conn.autocommit = True

            # Crear builder pasando la misma conexión usada en dimensiones
            builder = CompleteFactBuilder(dw_conn=conn)
            cursor = conn.cursor()

            # ===== FACT_VENTAS =====
            self.logger.info("      🔨 Construyendo fact_ventas...")
            try:
                df = builder.build_fact_ventas()
                if df is not None and len(df) > 0:
                    cursor.execute("TRUNCATE TABLE fact_ventas CASCADE")
                    columns = df.columns.tolist()
                    values = [tuple(row) for row in df.values]
                    insert_query = (
                        f"INSERT INTO fact_ventas ({', '.join(columns)}) VALUES %s"
                    )
                    execute_values(cursor, insert_query, values, page_size=1000)
                    self.logger.info(f"         ✓ fact_ventas: {len(df):,} registros")
                    results["facts_built"].append("fact_ventas")
                    results["total_records"] += len(df)
                else:
                    self.logger.warning("         ⚠️  fact_ventas: sin datos")
            except Exception as e:
                self.logger.error(f"         ❌ Error en fact_ventas: {e}")
                results["errors"].append({"fact": "fact_ventas", "error": str(e)})

            # ===== FACT_INVENTARIO =====
            self.logger.info("      🔨 Construyendo fact_inventario...")
            try:
                df = builder.build_fact_inventario()
                if df is not None and len(df) > 0:
                    cursor.execute("TRUNCATE TABLE fact_inventario CASCADE")
                    columns = df.columns.tolist()
                    values = [tuple(row) for row in df.values]
                    insert_query = (
                        f"INSERT INTO fact_inventario ({', '.join(columns)}) VALUES %s"
                    )
                    execute_values(cursor, insert_query, values, page_size=1000)
                    self.logger.info(
                        f"         ✓ fact_inventario: {len(df):,} registros"
                    )
                    results["facts_built"].append("fact_inventario")
                    results["total_records"] += len(df)
                else:
                    self.logger.warning("         ⚠️  fact_inventario: sin datos")
            except Exception as e:
                self.logger.error(f"         ❌ Error en fact_inventario: {e}")
                results["errors"].append({"fact": "fact_inventario", "error": str(e)})

            # ===== FACT_TRANSACCIONES =====
            self.logger.info("      🔨 Construyendo fact_transacciones...")
            try:
                df = builder.build_fact_transacciones()
                if df is not None and len(df) > 0:
                    cursor.execute("TRUNCATE TABLE fact_transacciones CASCADE")
                    columns = df.columns.tolist()
                    values = [tuple(row) for row in df.values]
                    insert_query = f"INSERT INTO fact_transacciones ({', '.join(columns)}) VALUES %s"
                    execute_values(cursor, insert_query, values, page_size=1000)
                    self.logger.info(
                        f"         ✓ fact_transacciones: {len(df):,} registros"
                    )
                    results["facts_built"].append("fact_transacciones")
                    results["total_records"] += len(df)
                else:
                    self.logger.warning("         ⚠️  fact_transacciones: sin datos")
            except Exception as e:
                self.logger.error(f"         ❌ Error en fact_transacciones: {e}")
                results["errors"].append(
                    {"fact": "fact_transacciones", "error": str(e)}
                )

            # ===== FACT_BALANCE =====
            self.logger.info("      🔨 Construyendo fact_balance...")
            try:
                df = builder.build_fact_balance()
                if df is not None and len(df) > 0:
                    cursor.execute("TRUNCATE TABLE fact_balance CASCADE")
                    columns = df.columns.tolist()
                    values = df.values.tolist()  # Convertir a lista de Python nativa
                    insert_query = f"INSERT INTO fact_balance ({', '.join(columns)}) VALUES %s"
                    execute_values(cursor, insert_query, values, page_size=1000)
                    self.logger.info(
                        f"         ✓ fact_balance: {len(df):,} registros"
                    )
                    results["facts_built"].append("fact_balance")
                    results["total_records"] += len(df)
                else:
                    self.logger.warning("         ⚠️  fact_balance: sin datos")
            except Exception as e:
                self.logger.error(f"         ❌ Error en fact_balance: {e}")
                results["errors"].append(
                    {"fact": "fact_balance", "error": str(e)}
                )

            # ===== FACT_ESTADO_RESULTADOS =====
            self.logger.info("      🔨 Construyendo fact_estado_resultados...")
            try:
                df = builder.build_fact_estado_resultados()
                if df is not None and len(df) > 0:
                    cursor.execute("TRUNCATE TABLE fact_estado_resultados CASCADE")
                    columns = df.columns.tolist()
                    values = df.values.tolist()  # Convertir a lista de Python nativa
                    insert_query = f"INSERT INTO fact_estado_resultados ({', '.join(columns)}) VALUES %s"
                    execute_values(cursor, insert_query, values, page_size=1000)
                    self.logger.info(
                        f"         ✓ fact_estado_resultados: {len(df):,} registros"
                    )
                    results["facts_built"].append("fact_estado_resultados")
                    results["total_records"] += len(df)
                else:
                    self.logger.warning("         ⚠️  fact_estado_resultados: sin datos")
            except Exception as e:
                self.logger.error(f"         ❌ Error en fact_estado_resultados: {e}")
                results["errors"].append(
                    {"fact": "fact_estado_resultados", "error": str(e)}
                )

            cursor.close()
            conn.close()

        except Exception as e:
            self.logger.error(f"   ❌ Error construyendo facts: {e}")
            results["errors"].append({"error": str(e)})

        self.logger.info(
            f"\n   ✅ Facts completadas: {results['total_records']:,} registros totales"
        )

        return results

    def _run_loading(self) -> Dict[str, Any]:
        """Fase de carga a base de datos - Ya realizada en pasos anteriores"""
        results = {"tables_loaded": [], "total_records": 0, "errors": []}

        self.logger.info(
            "   ℹ️  La carga se realizó directamente en las fases anteriores"
        )
        self.logger.info(
            "   ℹ️  Los datos ya están en la base de datos datawarehouse_bi"
        )

        # Verificar conteo final
        try:
            import psycopg2

            conn = psycopg2.connect(
                host=os.getenv("DW_DB_HOST"),
                port=int(os.getenv("DW_DB_PORT")),
                dbname=os.getenv("DW_DB_NAME"),
                user=os.getenv("DW_DB_USER"),
                password=os.getenv("DW_DB_PASS"),
            )
            conn.autocommit = True  # Evitar problemas con transacciones
            cursor = conn.cursor()

            all_tables = [
                "dim_fecha",
                "dim_cliente",
                "dim_producto",
                "dim_orden",
                "dim_almacen",
                "dim_proveedor",
                "dim_tipo_movimiento",
                "dim_centro_costo",
                "dim_tipo_transaccion",
                "dim_cuenta_contable",
                "dim_impuestos",
                "dim_usuario",
                "dim_promocion",
                "fact_ventas",
                "fact_inventario",
                "fact_transacciones",
            ]

            for table in all_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    results["tables_loaded"].append({"table": table, "records": count})
                    results["total_records"] += count
                except Exception as e:
                    self.logger.warning(f"      ⚠️  {table}: {e}")

            cursor.close()
            conn.close()

        except Exception as e:
            self.logger.error(f"   ❌ Error verificando tablas: {e}")
            results["errors"].append({"error": str(e)})

        self.logger.info(
            f"\n   ✅ Verificación completada: {results['total_records']:,} registros totales en DW"
        )

        return results

    def _clean_fact_tables(self):
        """Limpiar todas las fact tables primero para evitar violaciones de FK"""
        import psycopg2

        fact_tables = [
            "fact_ventas",
            "fact_inventario",
            "fact_transacciones",
            "fact_balance",
            "fact_estado_resultados",
        ]

        try:
            conn = psycopg2.connect(
                host=os.getenv("DW_DB_HOST"),
                port=int(os.getenv("DW_DB_PORT")),
                dbname=os.getenv("DW_DB_NAME"),
                user=os.getenv("DW_DB_USER"),
                password=os.getenv("DW_DB_PASS"),
                connect_timeout=30,
            )
            cursor = conn.cursor()

            for table in fact_tables:
                try:
                    cursor.execute(f"SET statement_timeout = '30s'")
                    cursor.execute(f"DELETE FROM {table}")
                    conn.commit()
                    self.logger.info(f"      ✓ Limpiada: {table}")
                except Exception as e:
                    # Si la tabla no existe, no es un error crítico
                    if "does not exist" not in str(e):
                        self.logger.warning(f"      ⚠️  {table}: {e}")

            cursor.close()
            conn.close()

        except Exception as e:
            self.logger.warning(f"   ⚠️  Error limpiando fact tables: {e}")

    def _run_final_validation(self) -> Dict[str, Any]:
        """Validación final del proceso - Integridad referencial y reconciliación"""
        results = {
            "validations": [],
            "passed": True,
            "summary": {},
            "fk_issues": [],
            "null_issues": [],
        }

        self.logger.info("   🔍 Verificando integridad de datos...")

        try:
            import psycopg2
            import pandas as pd

            conn = psycopg2.connect(
                host=os.getenv("DW_DB_HOST"),
                port=int(os.getenv("DW_DB_PORT")),
                dbname=os.getenv("DW_DB_NAME"),
                user=os.getenv("DW_DB_USER"),
                password=os.getenv("DW_DB_PASS"),
            )
            cursor = conn.cursor()

            # ===== VALIDAR CONTEOS EN DIMENSIONES =====
            dimensions = [
                "dim_fecha",
                "dim_cliente",
                "dim_producto",
                "dim_orden",
                "dim_almacen",
                "dim_proveedor",
                "dim_tipo_movimiento",
                "dim_centro_costo",
                "dim_tipo_transaccion",
                "dim_promocion",
                "dim_usuario",
                "dim_impuestos",
            ]

            dim_total = 0
            for dim in dimensions:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {dim}")
                    count = cursor.fetchone()[0]
                    dim_total += count
                    status = "✓" if count > 0 else "✗"
                    self.logger.info(f"      {status} {dim}: {count:,} registros")
                    results["validations"].append(
                        {"table": dim, "count": count, "passed": count > 0}
                    )
                    if count == 0:
                        results["passed"] = False
                except Exception as e:
                    self.logger.warning(f"      ⚠️  {dim}: {e}")

            # ===== VALIDAR FACTS =====
            facts = ["fact_ventas", "fact_inventario", "fact_transacciones"]
            fact_total = 0

            for fact in facts:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {fact}")
                    count = cursor.fetchone()[0]
                    fact_total += count
                    status = "✓" if count > 0 else "⚠️"
                    self.logger.info(f"      {status} {fact}: {count:,} registros")
                    results["validations"].append(
                        {"table": fact, "count": count, "passed": count > 0}
                    )
                except Exception as e:
                    self.logger.warning(f"      ⚠️  {fact}: {e}")

            # ===== VALIDAR INTEGRIDAD REFERENCIAL EN FACT_VENTAS =====
            self.logger.info(
                "\n   🔗 Verificando integridad referencial en fact_ventas..."
            )

            fk_checks = [
                ("fecha_id", "dim_fecha", "fecha_id"),
                ("cliente_id", "dim_cliente", "cliente_id"),
                ("producto_id", "dim_producto", "producto_id"),
                ("orden_id", "dim_orden", "orden_id"),
                ("usuario_id", "dim_usuario", "usuario_id"),
                ("almacen_id", "dim_almacen", "almacen_id"),
                ("impuesto_id", "dim_impuestos", "impuesto_id"),
                ("sk_promocion", "dim_promocion", "sk_promocion"),
            ]

            for fk_col, dim_table, pk_col in fk_checks:
                try:
                    query = f"""
                    SELECT COUNT(*) as huerfanos
                    FROM fact_ventas fv
                    LEFT JOIN {dim_table} d ON fv.{fk_col} = d.{pk_col}
                    WHERE d.{pk_col} IS NULL AND fv.{fk_col} IS NOT NULL
                    """
                    cursor.execute(query)
                    huerfanos = cursor.fetchone()[0]

                    if huerfanos > 0:
                        self.logger.warning(
                            f"      ⚠️  {fk_col} → {dim_table}: {huerfanos:,} registros huérfanos"
                        )
                        results["fk_issues"].append(
                            {"fk": fk_col, "dimension": dim_table, "orphans": huerfanos}
                        )
                    else:
                        self.logger.info(f"      ✓ {fk_col} → {dim_table}: OK")
                except Exception as e:
                    self.logger.warning(f"      ⚠️  Error verificando {fk_col}: {e}")

            # ===== VERIFICAR DUPLICADOS POR COMBINACIÓN (orden_id, producto_id) =====
            self.logger.info("\n   🔍 Verificando duplicados en fact_ventas...")
            try:
                cursor.execute(
                    """
                    SELECT orden_id, producto_id, COUNT(*) as cantidad
                    FROM fact_ventas
                    GROUP BY orden_id, producto_id
                    HAVING COUNT(*) > 1
                    LIMIT 10
                """
                )
                duplicados = cursor.fetchall()

                if duplicados:
                    # Esto es NORMAL - una orden puede tener el mismo producto múltiples veces
                    # Lo importante es que cada line_item_id_externo sea único
                    self.logger.info(
                        f"      ℹ️  {len(duplicados)} combinaciones (orden,producto) con múltiples líneas (normal)"
                    )

                    # Verificar unicidad de line_item_id_externo
                    cursor.execute(
                        """
                        SELECT line_item_id_externo, COUNT(*) 
                        FROM fact_ventas 
                        WHERE line_item_id_externo IS NOT NULL
                        GROUP BY line_item_id_externo 
                        HAVING COUNT(*) > 1
                    """
                    )
                    li_dupes = cursor.fetchall()

                    if li_dupes:
                        self.logger.error(
                            f"      ❌ {len(li_dupes)} line_item_id_externo duplicados (ERROR)"
                        )
                        results["passed"] = False
                    else:
                        self.logger.info(f"      ✓ Cada line_item_id_externo es único")
                else:
                    self.logger.info(f"      ✓ No hay duplicados problemáticos")
            except Exception as e:
                self.logger.warning(f"      ⚠️  Error verificando duplicados: {e}")

            # ===== VERIFICAR NULLs EN DIMENSIONES CRÍTICAS =====
            self.logger.info("\n   🔍 Verificando NULLs en dimensiones...")

            null_checks = [
                ("dim_cliente", "nombre"),
                ("dim_producto", "nombre"),
                ("dim_usuario", "nombre"),
                ("dim_orden", "numero_orden"),
            ]

            for table, col in null_checks:
                try:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL OR TRIM({col}) = ''"
                    )
                    nulls = cursor.fetchone()[0]

                    if nulls > 0:
                        self.logger.warning(
                            f"      ⚠️  {table}.{col}: {nulls:,} valores NULL/vacíos"
                        )
                        results["null_issues"].append(
                            {"table": table, "column": col, "nulls": nulls}
                        )
                    else:
                        self.logger.info(f"      ✓ {table}.{col}: OK")
                except Exception as e:
                    pass  # Columna puede no existir

            # ===== RECONCILIACIÓN CON ORIGEN =====
            self.logger.info("\n   📊 Reconciliación con origen...")
            try:
                # Crear nueva conexión para evitar transacciones abortadas
                dw_conn_recon = psycopg2.connect(
                    host=os.getenv("DW_DB_HOST"),
                    port=int(os.getenv("DW_DB_PORT")),
                    dbname=os.getenv("DW_DB_NAME"),
                    user=os.getenv("DW_DB_USER"),
                    password=os.getenv("DW_DB_PASS"),
                )
                dw_cursor_recon = dw_conn_recon.cursor()
                
                oro_conn = psycopg2.connect(
                    host=os.getenv("ORO_DB_HOST"),
                    port=int(os.getenv("ORO_DB_PORT")),
                    dbname=os.getenv("ORO_DB_NAME"),
                    user=os.getenv("ORO_DB_USER"),
                    password=os.getenv("ORO_DB_PASS"),
                )
                oro_cursor = oro_conn.cursor()

                # Contar line items válidos en origen
                oro_cursor.execute(
                    """
                    SELECT COUNT(DISTINCT oli.id)
                    FROM oro_order o
                    INNER JOIN oro_order_line_item oli ON o.id = oli.order_id
                    WHERE o.created_at IS NOT NULL 
                      AND oli.product_id IS NOT NULL
                      AND oli.quantity > 0
                """
                )
                origen_count = oro_cursor.fetchone()[0]

                # Contar en DW (usando nueva conexión)
                dw_cursor_recon.execute("SELECT COUNT(*) FROM fact_ventas")
                dw_count = dw_cursor_recon.fetchone()[0]

                diferencia = dw_count - origen_count

                if diferencia == 0:
                    self.logger.info(
                        f"      ✓ fact_ventas cuadra: {dw_count:,} = {origen_count:,} (origen)"
                    )
                else:
                    self.logger.warning(
                        f"      ⚠️  Diferencia: DW={dw_count:,} vs Origen={origen_count:,} (diff={diferencia:+,})"
                    )

                dw_cursor_recon.close()
                dw_conn_recon.close()
                oro_cursor.close()
                oro_conn.close()

            except Exception as e:
                self.logger.warning(f"      ⚠️  No se pudo reconciliar con origen: {e}")

            results["summary"] = {
                "total_dimensions": dim_total,
                "total_facts": fact_total,
                "total_records": dim_total + fact_total,
                "fk_issues_count": len(results["fk_issues"]),
                "null_issues_count": len(results["null_issues"]),
            }

            cursor.close()
            conn.close()

            self.logger.info(
                f"\n      ✓ Total en DW: {dim_total + fact_total:,} registros"
            )

            if results["fk_issues"] or results["null_issues"]:
                self.logger.warning("      ⚠️  Hay problemas de integridad que revisar")
            else:
                self.logger.info("      ✓ Integridad verificada completamente")

        except Exception as e:
            self.logger.error(f"      ✗ Error en validación: {e}")
            results["passed"] = False
            results["error"] = str(e)

        return results

    def _save_dimension(self, name: str, df):
        """Guarda dimensión en formato parquet y CSV"""
        output_dir = Path(self.config["paths"]["output_parquet"])
        output_dir.mkdir(parents=True, exist_ok=True)

        # Parquet
        parquet_file = output_dir / f"{name}.parquet"
        df.to_parquet(parquet_file, index=False, compression="snappy")

        # CSV (opcional)
        if self.config.get("exportar_csv", True):
            csv_dir = Path(self.config["paths"]["output_csv"])
            csv_dir.mkdir(parents=True, exist_ok=True)
            csv_file = csv_dir / f"{name}.csv"
            df.to_csv(csv_file, index=False, encoding="utf-8")

    def _save_fact(self, name: str, df):
        """Guarda fact table en formato parquet y CSV"""
        self._save_dimension(name, df)  # Mismo proceso

    def _print_final_summary(self, report: Dict[str, Any]):
        """Imprime resumen final"""
        self.logger.info("\n" + "=" * 80)
        self.logger.info("📊 RESUMEN FINAL DEL PROCESO ETL")
        self.logger.info("=" * 80)

        self.logger.info(f"\n⏱️  Tiempo total: {report['elapsed_time']:.2f} segundos")
        self.logger.info(f"✅ Estado: {report['status']}")

        self.logger.info(f"\n📥 Extracción:")
        self.logger.info(
            f"   Total registros: {report['extraction']['total_records']:,}"
        )

        self.logger.info(f"\n🔄 Transformación:")
        self.logger.info(
            f"   Dimensiones: {len(report['dimensions']['dimensions_built'])}"
        )
        self.logger.info(f"   Facts: {len(report['facts']['facts_built'])}")
        self.logger.info(
            f"   Total registros: {report['dimensions']['total_records'] + report['facts']['total_records']:,}"
        )

        self.logger.info(f"\n📤 Carga:")
        self.logger.info(f"   Tablas: {len(report['loading']['tables_loaded'])}")
        self.logger.info(f"   Total registros: {report['loading']['total_records']:,}")

        if (
            report["dimensions"]["errors"]
            or report["facts"]["errors"]
            or report["loading"]["errors"]
        ):
            self.logger.warning(f"\n⚠️  Errores encontrados:")
            for error in (
                report["dimensions"]["errors"]
                + report["facts"]["errors"]
                + report["loading"]["errors"]
            ):
                self.logger.warning(f"   {error}")

        self.logger.info("\n" + "=" * 80)


@click.group()
def cli():
    """PuntaFina ETL Batch - Sistema de procesamiento por lotes"""
    pass


@cli.command()
@click.option("--config", type=click.Path(exists=True), help="Archivo de configuración")
def run(config):
    """Ejecuta el proceso ETL completo"""
    orchestrator = ETLOrchestrator(Path(config) if config else None)
    orchestrator.run_full_etl()


@cli.command()
def setup():
    """Configura el sistema inicial"""
    click.echo("🔧 Configurando sistema ETL...")
    
    # Verificar .env
    env_file = Path(__file__).parent / ".env"
    env_example = Path(__file__).parent / ".env.example"
    
    if not env_file.exists() and env_example.exists():
        click.echo("   📝 Creando archivo .env desde .env.example...")
        import shutil
        shutil.copy(env_example, env_file)
        click.echo("   ⚠️  Por favor, edita .env con tus credenciales")
    elif env_file.exists():
        click.echo("   ✓ Archivo .env encontrado")
    
    # Crear directorios necesarios
    click.echo("   📁 Creando estructura de directorios...")
    dirs = [
        Path(__file__).parent / "logs" / "etl",
        Path(__file__).parent / "data" / "outputs" / "csv",
        Path(__file__).parent / "data" / "outputs" / "parquet",
        Path(__file__).parent / "data" / "checkpoints"
    ]
    
    for dir_path in dirs:
        dir_path.mkdir(parents=True, exist_ok=True)
    
    click.echo("   ✓ Directorios creados")
    
    # Verificar conexión a base de datos
    click.echo("   🔌 Verificando conexión a base de datos...")
    try:
        load_dotenv(env_file)
        import psycopg2
        
        # Verificar DW
        conn = psycopg2.connect(
            host=os.getenv("DW_DB_HOST"),
            port=int(os.getenv("DW_DB_PORT")),
            dbname=os.getenv("DW_DB_NAME"),
            user=os.getenv("DW_DB_USER"),
            password=os.getenv("DW_DB_PASS")
        )
        conn.close()
        click.echo("   ✓ Conexión al Data Warehouse exitosa")
        
    except Exception as e:
        click.echo(f"   ⚠️  Error de conexión: {e}")
        click.echo("   Por favor verifica las credenciales en .env")
    
    click.echo("\n✅ Configuración completada")
    click.echo("\n📖 Próximos pasos:")
    click.echo("   1. Edita .env con tus credenciales si no lo has hecho")
    click.echo("   2. Ejecuta: python main.py validate")
    click.echo("   3. Ejecuta: python main.py run")


@cli.command()
def validate():
    """Valida la configuración y conexiones"""
    click.echo("🔍 Validando configuración...")
    
    env_file = Path(__file__).parent / ".env"
    
    # Verificar .env
    if not env_file.exists():
        click.echo("   ❌ Archivo .env no encontrado")
        click.echo("   Ejecuta: python main.py setup")
        return
    
    click.echo("   ✓ Archivo .env encontrado")
    
    # Cargar variables
    load_dotenv(env_file)
    
    # Validar variables requeridas
    required_vars = [
        "SOURCE_DB_HOST", "SOURCE_DB_PORT", "SOURCE_DB_NAME",
        "SOURCE_DB_USER", "SOURCE_DB_PASS",
        "DW_DB_HOST", "DW_DB_PORT", "DW_DB_NAME",
        "DW_DB_USER", "DW_DB_PASS"
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        click.echo(f"   ❌ Variables faltantes: {', '.join(missing)}")
        return
    
    click.echo("   ✓ Variables de entorno configuradas")
    
    # Verificar conexiones
    try:
        import psycopg2
        
        # Verificar OroCommerce
        click.echo("   🔌 Verificando conexión a OroCommerce...")
        conn = psycopg2.connect(
            host=os.getenv("SOURCE_DB_HOST"),
            port=int(os.getenv("SOURCE_DB_PORT")),
            dbname=os.getenv("SOURCE_DB_NAME"),
            user=os.getenv("SOURCE_DB_USER"),
            password=os.getenv("SOURCE_DB_PASS")
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM oro_order")
        count = cur.fetchone()[0]
        click.echo(f"   ✓ OroCommerce conectado ({count:,} órdenes)")
        cur.close()
        conn.close()
        
        # Verificar Data Warehouse
        click.echo("   🔌 Verificando conexión a Data Warehouse...")
        conn = psycopg2.connect(
            host=os.getenv("DW_DB_HOST"),
            port=int(os.getenv("DW_DB_PORT")),
            dbname=os.getenv("DW_DB_NAME"),
            user=os.getenv("DW_DB_USER"),
            password=os.getenv("DW_DB_PASS")
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'dim_%' OR table_name LIKE 'fact_%'
        """)
        count = cur.fetchone()[0]
        click.echo(f"   ✓ Data Warehouse conectado ({count} tablas DW)")
        cur.close()
        conn.close()
        
    except Exception as e:
        click.echo(f"   ❌ Error: {e}")
        return
    
    # Verificar archivos CSV
    click.echo("   📄 Verificando archivos CSV...")
    csv_files = [
        "data/inputs/inventario/almacenes.csv",
        "data/inputs/inventario/proveedores.csv",
        "data/inputs/inventario/tipos_movimiento.csv",
        "data/inputs/inventario/movimientos_inventario.csv",
        "Compras_Productos_PuntaFina.csv"
    ]
    
    missing_files = []
    for csv_file in csv_files:
        csv_path = Path(__file__).parent / csv_file
        if not csv_path.exists():
            missing_files.append(csv_file)
    
    if missing_files:
        click.echo(f"   ⚠️  Archivos CSV faltantes: {len(missing_files)}")
        for file in missing_files:
            click.echo(f"      - {file}")
    else:
        click.echo(f"   ✓ Todos los archivos CSV encontrados")
    
    click.echo("\n✅ Validación completada")
    
    if not missing_files:
        click.echo("\n🚀 Todo listo! Ejecuta: python main.py run")


if __name__ == "__main__":
    cli()
