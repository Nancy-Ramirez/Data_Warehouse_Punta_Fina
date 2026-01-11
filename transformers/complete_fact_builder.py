#!/usr/bin/env python3
"""
FACT TRANSFORMERS - Transformadores completos para todas las tablas de hechos
Puebla facts con datos reales desde OroCommerce y CSVs
"""

import pandas as pd
import psycopg2
import os
from datetime import datetime
from typing import Dict, Any
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# ROOT del proyecto
ROOT = Path(__file__).resolve().parent.parent.parent


class CompleteFactBuilder:
    """Constructor completo de todas las tablas de hechos"""

    def __init__(self, dw_conn=None):
        self.oro_conn = self._get_oro_connection()
        # Usar conexión proporcionada o crear una nueva
        self.dw_conn = dw_conn if dw_conn is not None else self._get_dw_connection()
        self._owns_dw_conn = dw_conn is None  # Para saber si debemos cerrarla

    def build(self, fact_name: str, fact_config: Dict[str, Any] = None) -> pd.DataFrame:
        """
        Método genérico para construir cualquier fact table
        Delegación a métodos específicos
        """
        method_name = f"build_{fact_name}"
        if hasattr(self, method_name):
            method = getattr(self, method_name)
            return method()
        else:
            logger.warning(
                f"Fact table {fact_name} no implementada en CompleteFactBuilder"
            )
            return pd.DataFrame()

    def get_schema(self, fact_name: str) -> Dict[str, str]:
        """
        Retorna el esquema de la fact table para el loader
        Este método es requerido por el orchestrator pero no lo usamos
        porque fact_ventas se carga via dblink directamente
        """
        return {}

    def _get_oro_connection(self):
        """Conexión a OroCommerce"""
        return psycopg2.connect(
            host=os.getenv("ORO_DB_HOST"),
            port=int(os.getenv("ORO_DB_PORT")),
            dbname=os.getenv("ORO_DB_NAME"),
            user=os.getenv("ORO_DB_USER"),
            password=os.getenv("ORO_DB_PASS"),
        )

    def _get_dw_connection(self):
        """Conexión al Data Warehouse"""
        return psycopg2.connect(
            host=os.getenv("DW_DB_HOST"),
            port=int(os.getenv("DW_DB_PORT")),
            dbname=os.getenv("DW_DB_NAME"),
            user=os.getenv("DW_DB_USER"),
            password=os.getenv("DW_DB_PASS"),
            connect_timeout=120,
            options="-c statement_timeout=1800000",
        )

    def _resolve_surrogate_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """Resolver Surrogate Keys de dimensiones desde el DW"""

        # Lookup dim_cliente
        query_cliente = "SELECT cliente_id, cliente_externo_id FROM dim_cliente"
        dim_cliente = pd.read_sql_query(query_cliente, self.dw_conn)
        df = df.merge(
            dim_cliente[["cliente_id", "cliente_externo_id"]],
            on="cliente_externo_id",
            how="left",
        )
        df["cliente_id"] = df["cliente_id"].fillna(1).astype(int)

        # Lookup dim_usuario - convertir ambos a string para el merge
        query_usuario = "SELECT usuario_id, usuario_externo_id FROM dim_usuario"
        dim_usuario = pd.read_sql_query(query_usuario, self.dw_conn)
        df["usuario_id_str"] = df["usuario_id"].astype(str)
        dim_usuario["usuario_externo_id_str"] = dim_usuario[
            "usuario_externo_id"
        ].astype(str)

        df = df.merge(
            dim_usuario[["usuario_id", "usuario_externo_id_str"]],
            left_on="usuario_id_str",
            right_on="usuario_externo_id_str",
            how="left",
            suffixes=("_orig", "_sk"),
        )
        df["usuario_id"] = df["usuario_id_sk"].fillna(1).astype(int)
        df = df.drop(
            columns=[
                "usuario_id_str",
                "usuario_externo_id_str",
                "usuario_id_orig",
                "usuario_id_sk",
            ],
            errors="ignore",
        )

        # Lookup dim_producto
        query_producto = "SELECT producto_id, producto_externo_id FROM dim_producto"
        dim_producto = pd.read_sql_query(query_producto, self.dw_conn)
        df["product_id"] = df["product_id"].astype(int)
        dim_producto["producto_externo_id"] = dim_producto[
            "producto_externo_id"
        ].astype(int)

        df = df.merge(
            dim_producto[["producto_id", "producto_externo_id"]],
            left_on="product_id",
            right_on="producto_externo_id",
            how="left",
        )
        df["producto_id"] = df["producto_id"].fillna(1).astype(int)
        df = df.drop(columns=["producto_externo_id", "product_id"], errors="ignore")

        # Lookup dim_direccion
        query_direccion = "SELECT direccion_id, direccion_externo_id FROM dim_direccion"
        dim_direccion = pd.read_sql_query(query_direccion, self.dw_conn)
        df["direccion_id"] = df["direccion_id"].astype(int)
        dim_direccion["direccion_externo_id"] = dim_direccion[
            "direccion_externo_id"
        ].astype(int)

        df = df.merge(
            dim_direccion[["direccion_id", "direccion_externo_id"]],
            left_on="direccion_id",
            right_on="direccion_externo_id",
            how="left",
            suffixes=("_orig", "_sk"),
        )
        df["direccion_id"] = df["direccion_id_sk"].fillna(1).astype(int)
        df = df.drop(
            columns=["direccion_externo_id", "direccion_id_orig", "direccion_id_sk"],
            errors="ignore",
        )

        # Lookup dim_orden
        query_orden = "SELECT orden_id, orden_externo_id FROM dim_orden"
        dim_orden = pd.read_sql_query(query_orden, self.dw_conn)
        df["orden_id"] = df["orden_id"].astype(int)
        dim_orden["orden_externo_id"] = dim_orden["orden_externo_id"].astype(int)

        df = df.merge(
            dim_orden[["orden_id", "orden_externo_id"]],
            left_on="orden_id",
            right_on="orden_externo_id",
            how="left",
            suffixes=("_orig", "_sk"),
        )
        df["orden_id"] = df["orden_id_sk"].fillna(1).astype(int)
        df = df.drop(
            columns=["orden_externo_id", "orden_id_orig", "orden_id_sk"],
            errors="ignore",
        )

        # Lookup dim_line_item
        query_line_item = "SELECT line_item_id, line_item_externo_id FROM dim_line_item"
        dim_line_item = pd.read_sql_query(query_line_item, self.dw_conn)
        df["line_item_id"] = df["line_item_id"].astype(int)
        dim_line_item["line_item_externo_id"] = dim_line_item[
            "line_item_externo_id"
        ].astype(int)

        df = df.merge(
            dim_line_item[["line_item_id", "line_item_externo_id"]],
            left_on="line_item_id",
            right_on="line_item_externo_id",
            how="left",
            suffixes=("_orig", "_sk"),
        )
        df["line_item_id"] = df["line_item_id_sk"].fillna(1).astype(int)
        df = df.drop(
            columns=["line_item_externo_id", "line_item_id_orig", "line_item_id_sk"],
            errors="ignore",
        )

        logger.info(
            f"✓ SKs resueltas: clientes={df['cliente_id'].nunique()}, productos={df['producto_id'].nunique()}, ordenes={df['orden_id'].nunique()}"
        )

        return df

    def build_fact_ventas(self) -> pd.DataFrame:
        """
        Construir fact_ventas desde oro_order + oro_order_line_item
        CON COSTOS REALES desde dim_producto y DESCUENTOS/PROMOCIONES REALES
        """
        logger.info("💰 Construyendo fact_ventas...")

        # Extraer datos de OroCommerce CON DESCUENTOS REALES por línea
        query = """
        SELECT 
            o.created_at::date as fecha,
            o.id as orden_id_externo,
            COALESCE(o.customer_id, 1) as cliente_id_externo,
            COALESCE(o.user_owner_id, 1) as usuario_id_externo,
            oli.product_id as producto_id_externo,
            oli.id as line_item_id_externo,
            CAST(oli.quantity AS NUMERIC(10,2)) as cantidad,
            CAST(oli.value AS NUMERIC(10,2)) as precio_unitario,
            CAST(oli.quantity * oli.value AS NUMERIC(10,2)) as subtotal_bruto,
            CAST(COALESCE(d.amount, 0.0) AS NUMERIC(10,2)) as descuento_total,
            CAST(0.0 AS NUMERIC(10,2)) as envio,
            CAST(COALESCE(o.total_discounts_amount, 0.0) AS NUMERIC(10,2)) as total_descuento_orden,
            pa.source_promotion_id as promocion_id_externo
        FROM oro_order o
        JOIN oro_order_line_item oli ON o.id = oli.order_id
        LEFT JOIN oro_promotion_applied pa ON o.id = pa.order_id
        LEFT JOIN oro_promotion_applied_discount d ON oli.id = d.line_item_id AND pa.id = d.applied_promotion_id
        WHERE o.created_at IS NOT NULL 
          AND oli.product_id IS NOT NULL
          AND oli.quantity > 0
        """

        logger.info("   📥 Extrayendo datos desde OroCommerce...")
        df = pd.read_sql_query(query, self.oro_conn)
        logger.info(f"   ✓ Extraídos {len(df):,} registros")

        if df.empty:
            logger.warning("   ⚠️  No hay datos en oro_order/oro_order_line_item")
            return pd.DataFrame()

        # Calcular subtotal después de descuento
        df["subtotal"] = df["subtotal_bruto"] - df["descuento_total"]

        # Calcular impuesto e total sobre el subtotal con descuento
        df["impuesto"] = df["subtotal"] * 0.13
        df["total"] = df["subtotal"] + df["impuesto"] + df["envio"]

        # Cargar dimensiones en memoria desde la base de datos DW
        logger.info("   🔗 Cargando dimensiones desde DW...")

        dim_fecha = pd.read_sql_query(
            "SELECT fecha_id, fecha FROM dim_fecha", self.dw_conn
        )
        dim_impuestos = pd.read_sql_query(
            "SELECT impuesto_id, codigo FROM dim_impuestos", self.dw_conn
        )
        # Note: dim_promocion uses sk_promocion as PK

        # Para dimensiones que fallaron, usar IDs por defecto
        cliente_id_default = 1
        producto_id_default = 1
        orden_id_default = 1
        usuario_id_default = 1
        line_item_id_default = 1

        logger.info("   🔗 Resolviendo surrogate keys...")

        # Convertir fechas a mismo tipo para merge
        df["fecha"] = pd.to_datetime(df["fecha"])
        dim_fecha["fecha"] = pd.to_datetime(dim_fecha["fecha"])

        # Resolver fecha_id
        df = df.merge(
            dim_fecha[["fecha_id", "fecha"]],
            left_on="fecha",
            right_on="fecha",
            how="left",
        )
        df["fecha_id"] = df["fecha_id"].fillna(1).astype(int)

        # Cargar dim_producto para obtener producto_id y costos
        logger.info("   📦 Cargando dim_producto para costos...")
        try:
            dim_producto = pd.read_sql_query(
                "SELECT producto_id, producto_externo_id, costo_estandar, precio_base FROM dim_producto",
                self.dw_conn,
            )

            # Resolver producto_id usando producto_externo_id
            df = df.merge(
                dim_producto[["producto_id", "producto_externo_id", "costo_estandar"]],
                left_on="producto_id_externo",
                right_on="producto_externo_id",
                how="left",
            )
            df["producto_id"] = df["producto_id"].fillna(1).astype(int)

            # Calcular costo_unitario, costo_total y margen basado en costo_estandar
            df["costo_unitario"] = df["costo_estandar"].fillna(0.0)
            df["costo_total"] = df["costo_unitario"] * df["cantidad"]
            df["margen"] = df["subtotal"] - df["costo_total"]

            logger.info(
                f"   ✓ Productos resueltos: {(df['producto_id'] > 1).sum():,} con ID real"
            )
            logger.info(
                f"   ✓ Costos asignados: {(df['costo_unitario'] > 0).sum():,} registros con costo"
            )

            # Limpiar columnas temporales
            df = df.drop(
                columns=["producto_externo_id", "costo_estandar"], errors="ignore"
            )

        except Exception as e:
            logger.warning(f"   ⚠️  No se pudo cargar dim_producto: {e}")
            df["producto_id"] = 1
            df["costo_unitario"] = 0.0
            df["costo_total"] = 0.0
            df["margen"] = df["subtotal"]

        # Resolver orden_id desde dim_orden usando orden_externo_id
        logger.info("   📋 Resolviendo orden_id desde dim_orden...")
        try:
            dim_orden = pd.read_sql_query(
                "SELECT orden_id, orden_externo_id FROM dim_orden",
                self.dw_conn,
            )
            df = df.merge(
                dim_orden,
                left_on="orden_id_externo",
                right_on="orden_externo_id",
                how="left",
            )
            # Usar el primer orden_id válido como default para los que no matcheen
            first_valid_orden = dim_orden["orden_id"].min() if len(dim_orden) > 0 else 1
            df["orden_id"] = df["orden_id"].fillna(first_valid_orden).astype(int)
            logger.info(
                f"   ✓ Ordenes resueltas: {(df['orden_id'] > 0).sum():,} registros"
            )
            df = df.drop(columns=["orden_externo_id"], errors="ignore")
        except Exception as e:
            logger.warning(f"   ⚠️  No se pudo resolver orden_id: {e}")
            df["orden_id"] = 1

        # Resolver cliente_id desde dim_cliente usando cliente_externo_id
        logger.info("   👥 Resolviendo cliente_id desde dim_cliente...")
        try:
            dim_cliente = pd.read_sql_query(
                "SELECT cliente_id, cliente_externo_id FROM dim_cliente",
                self.dw_conn,
            )
            df = df.merge(
                dim_cliente,
                left_on="cliente_id_externo",
                right_on="cliente_externo_id",
                how="left",
            )
            first_valid_cliente = (
                dim_cliente["cliente_id"].min() if len(dim_cliente) > 0 else 1
            )
            df["cliente_id"] = df["cliente_id"].fillna(first_valid_cliente).astype(int)
            logger.info(
                f"   ✓ Clientes resueltos: {df['cliente_id'].nunique():,} únicos"
            )
            df = df.drop(columns=["cliente_externo_id"], errors="ignore")
        except Exception as e:
            logger.warning(f"   ⚠️  No se pudo resolver cliente_id: {e}")
            df["cliente_id"] = 1

        # Resolver usuario_id desde dim_usuario usando usuario_externo_id
        logger.info("   👤 Resolviendo usuario_id desde dim_usuario...")
        try:
            dim_usuario = pd.read_sql_query(
                "SELECT usuario_id, usuario_externo_id FROM dim_usuario",
                self.dw_conn,
            )
            df = df.merge(
                dim_usuario,
                left_on="usuario_id_externo",
                right_on="usuario_externo_id",
                how="left",
            )
            first_valid_usuario = (
                dim_usuario["usuario_id"].min() if len(dim_usuario) > 0 else 1
            )
            df["usuario_id"] = df["usuario_id"].fillna(first_valid_usuario).astype(int)
            logger.info(
                f"   ✓ Usuarios resueltos: {df['usuario_id'].nunique():,} únicos"
            )
            df = df.drop(columns=["usuario_externo_id"], errors="ignore")
        except Exception as e:
            logger.warning(f"   ⚠️  No se pudo resolver usuario_id: {e}")
            df["usuario_id"] = 1

        # Resolver almacen_id - usar el primero disponible
        logger.info("   🏪 Resolviendo almacen_id desde dim_almacen...")
        try:
            dim_almacen = pd.read_sql_query(
                "SELECT almacen_id FROM dim_almacen LIMIT 1",
                self.dw_conn,
            )
            almacen_default = (
                dim_almacen["almacen_id"].iloc[0] if len(dim_almacen) > 0 else 1
            )
            df["almacen_id"] = almacen_default
        except Exception as e:
            logger.warning(f"   ⚠️  No se pudo resolver almacen_id: {e}")
            df["almacen_id"] = 1

        # Asignar impuesto_id (1=IVA 13%, 3=EXENTO)
        df["impuesto_id"] = df["impuesto"].apply(lambda x: 1 if x > 0 else 3)

        # Renombrar columna de descuento para que coincida con el esquema
        df["descuento"] = df["descuento_total"]

        # Resolver sk_promocion desde dim_promocion
        logger.info("   🎁 Resolviendo promociones desde dim_promocion...")
        try:
            dim_promocion = pd.read_sql_query(
                "SELECT sk_promocion, id_promocion_source FROM dim_promocion",
                self.dw_conn,
            )
            
            # Merge con las promociones
            df = df.merge(
                dim_promocion,
                left_on="promocion_id_externo",
                right_on="id_promocion_source",
                how="left",
            )
            
            # Si no hay promoción, usar 1 (Sin Promoción)
            df["sk_promocion"] = df["sk_promocion"].fillna(1).astype(int)
            
            # Limpiar columnas temporales
            df = df.drop(columns=["promocion_id_externo", "id_promocion_source"], errors="ignore")
            
            tiene_promocion = df["sk_promocion"] > 1
            logger.info(
                f"   ✓ Promociones: {(~tiene_promocion).sum():,} sin promoción, {tiene_promocion.sum():,} con promoción"
            )
        except Exception as e:
            logger.warning(f"   ⚠️  No se pudo resolver promociones: {e}")
            df["sk_promocion"] = 1

        # Seleccionar columnas finales (nota: fact_ventas usa sk_promocion)
        # venta_id NO se incluye porque es SERIAL (autogenerado por la DB)
        fact_cols = [
            "fecha_id",
            "cliente_id",
            "producto_id",
            "orden_id",
            "usuario_id",
            "almacen_id",
            "impuesto_id",
            "sk_promocion",
            "cantidad",
            "precio_unitario",
            "subtotal",
            "descuento",
            "impuesto",
            "envio",
            "total",
            "costo_unitario",
            "costo_total",
            "margen",
        ]

        df_final = df[fact_cols].copy()
        df_final["created_at"] = datetime.now()

        # NO agregar venta_id - es SERIAL autogenerado

        logger.info(f"   ✅ fact_ventas: {len(df_final):,} registros construidos")
        logger.info(
            f"   📊 IDs únicos: clientes={df_final['cliente_id'].nunique()}, productos={df_final['producto_id'].nunique()}, ordenes={df_final['orden_id'].nunique()}"
        )

        return df_final

    def build_fact_inventario(self) -> pd.DataFrame:
        """Construir fact_inventario desde CSV movimientos_inventario"""
        logger.info("📦 Construyendo fact_inventario...")

        csv_path = (
            ROOT / "data" / "inputs" / "inventario" / "movimientos_inventario.csv"
        )
        df = pd.read_csv(csv_path)
        logger.info(f"   📥 Cargados {len(df):,} registros desde CSV")

        # Mapeo de códigos CSV → códigos DB
        almacen_map = {
            "ALM001": "ALM_CENTRAL",
            "ALM002": "TIENDA_01",
            "ALM003": "TIENDA_02",
            "ALM004": "TIENDA_03",
            "ALM005": "TIENDA_04",
            "ALM006": "TIENDA_05",
        }
        
        tipo_mov_map = {
            "ENTRADA_COMPRA": "MOV_ENTRADA",
            "SALIDA_VENTA": "MOV_SALIDA_VENTA",
            "DEVOLUCION_CLIENTE": "MOV_DEVOLUCION_CLIENTE",
            "DEVOLUCION_PROVEEDOR": "MOV_DEVOLUCION_PROVEEDOR",
            "AJUSTE_POSITIVO": "MOV_AJUSTE_POSITIVO",
            "AJUSTE_NEGATIVO": "MOV_AJUSTE_NEGATIVO",
            "TRASLADO_ENTRADA": "MOV_TRASLADO_ENTRADA",
            "TRASLADO_SALIDA": "MOV_TRASLADO_SALIDA",
            "MERMA": "MOV_MERMA",
        }

        # Aplicar mapeos a los códigos del CSV
        df["almacen_codigo"] = df["almacen_id"].map(almacen_map)
        df["tipo_mov_codigo"] = df["tipo_movimiento_id"].map(tipo_mov_map)
        
        # Convertir fecha a ID
        df["fecha_id"] = (
            pd.to_datetime(df["fecha"]).dt.strftime("%Y%m%d").astype(int)
        )

        # Resolver SKs de dimensiones desde la base de datos
        logger.info("   🔗 Resolviendo FKs desde DW...")

        # Cargar dim_producto desde DB
        dim_producto = pd.read_sql_query(
            "SELECT producto_id, producto_externo_id FROM dim_producto", self.dw_conn
        )
        
        # El CSV usa product_id que corresponde a producto_externo_id
        df["product_id"] = df["product_id"].astype(int)
        dim_producto["producto_externo_id"] = dim_producto["producto_externo_id"].astype(int)
        
        df = df.merge(
            dim_producto,
            left_on="product_id",
            right_on="producto_externo_id",
            how="left",
        )
        df["producto_id"] = df["producto_id"].fillna(1).astype(int)
        logger.info(f"   ✓ Productos resueltos: {(df['producto_id'] > 1).sum():,}")

        # Cargar dim_almacen desde DB
        dim_almacen = pd.read_sql_query(
            "SELECT almacen_id, codigo FROM dim_almacen", self.dw_conn
        )
        
        df = df.merge(
            dim_almacen,
            left_on="almacen_codigo",
            right_on="codigo",
            how="left",
            suffixes=("_csv", "_db"),
        )
        df["almacen_id_final"] = df["almacen_id_db"].fillna(dim_almacen["almacen_id"].iloc[0]).astype(int)
        logger.info(f"   ✓ Almacenes resueltos: {df['almacen_id_final'].nunique()} únicos")

        # Cargar dim_proveedor desde DB (el CSV ya usa códigos PROV001)
        dim_proveedor = pd.read_sql_query(
            "SELECT proveedor_id, codigo FROM dim_proveedor", self.dw_conn
        )
        
        # Rellenar proveedores vacíos con vacío para el merge
        df["proveedor_id_orig"] = df["proveedor_id"].fillna("")
        
        df = df.merge(
            dim_proveedor,
            left_on="proveedor_id_orig",
            right_on="codigo",
            how="left",
            suffixes=("", "_prov"),
        )
        # Si no hay proveedor, usar el primero como default
        df["proveedor_id_final"] = df["proveedor_id_prov"].fillna(dim_proveedor["proveedor_id"].iloc[0]).astype(int)
        logger.info(f"   ✓ Proveedores resueltos: {df['proveedor_id_final'].nunique()} únicos")

        # Cargar dim_tipo_movimiento desde DB
        dim_tipo_mov = pd.read_sql_query(
            "SELECT tipo_movimiento_id, codigo FROM dim_tipo_movimiento", self.dw_conn
        )
        
        df = df.merge(
            dim_tipo_mov,
            left_on="tipo_mov_codigo",
            right_on="codigo",
            how="left",
            suffixes=("", "_tipo"),
        )
        df["tipo_movimiento_id_final"] = df["tipo_movimiento_id_tipo"].fillna(dim_tipo_mov["tipo_movimiento_id"].iloc[0]).astype(int)
        logger.info(f"   ✓ Tipos movimiento resueltos: {df['tipo_movimiento_id_final'].nunique()} únicos")

        # Cargar dim_usuario para obtener ID válido
        dim_usuario = pd.read_sql_query(
            "SELECT MIN(usuario_id) as usuario_id FROM dim_usuario", self.dw_conn
        )
        df["usuario_id"] = int(dim_usuario["usuario_id"].iloc[0])
        
        # Columna created_at
        df["created_at"] = pd.Timestamp.now()

        # Limpiar observaciones NULL
        df["observaciones"] = df["observaciones"].fillna("")

        # Renombrar columnas para el resultado final
        df["almacen_id"] = df["almacen_id_final"]
        df["proveedor_id"] = df["proveedor_id_final"]
        df["tipo_movimiento_id"] = df["tipo_movimiento_id_final"]

        logger.info(f"   ✅ fact_inventario: {len(df):,} registros construidos")
        logger.info(
            f"   📊 Productos únicos: {df['producto_id'].nunique()}, Almacenes: {df['almacen_id'].nunique()}"
        )

        # Seleccionar solo columnas del esquema (sin movimiento_id, es SERIAL)
        return df[
            [
                "fecha_id",
                "producto_id",
                "almacen_id",
                "tipo_movimiento_id",
                "proveedor_id",
                "usuario_id",
                "cantidad",
                "costo_unitario",
                "costo_total",
                "stock_anterior",
                "stock_resultante",
                "documento",
                "observaciones",
                "created_at",
            ]
        ]

    def build_fact_transacciones(self) -> pd.DataFrame:
        """
        Construir fact_transacciones generando asientos contables desde ventas.
        Cada venta genera asientos de: Ingreso, IVA, Costo de Ventas.
        """
        logger.info("💳 Construyendo fact_transacciones desde ventas...")

        # Obtener ventas resumidas por orden desde OroCommerce
        query = """
        SELECT 
            o.id as orden_id,
            o.created_at::date as fecha,
            o.subtotal_value as subtotal,
            o.total_discounts_amount as descuento,
            o.total_value as total,
            o.user_owner_id as usuario_id
        FROM oro_order o
        WHERE o.created_at IS NOT NULL
        ORDER BY o.created_at
        """
        
        df_ventas = pd.read_sql_query(query, self.oro_conn)
        logger.info(f"   📥 Órdenes cargadas: {len(df_ventas):,}")
        
        if df_ventas.empty:
            logger.warning("   ⚠️ No hay órdenes para generar transacciones")
            return pd.DataFrame()

        # Calcular IVA (13% sobre subtotal)
        df_ventas["iva"] = df_ventas["subtotal"] * 0.13
        # Estimar costo (40% del subtotal - margen aproximado 60%)
        df_ventas["costo_venta"] = df_ventas["subtotal"] * 0.40

        # Convertir fecha a fecha_id
        df_ventas["fecha_id"] = pd.to_datetime(df_ventas["fecha"]).dt.strftime("%Y%m%d").astype(int)

        # Cargar cuentas contables desde DW
        dim_cuenta = pd.read_sql_query(
            "SELECT cuenta_id, codigo, nombre FROM dim_cuenta_contable", self.dw_conn
        )
        
        # Mapear cuentas por código (usar IDs existentes o defaults)
        cuenta_map = {}
        for _, row in dim_cuenta.iterrows():
            cuenta_map[row['codigo']] = row['cuenta_id']
        
        # Cuentas típicas para asientos de ventas:
        # 4101 = Ventas, 1102 = Bancos, 2102 = IVA por Pagar, 5101 = Costo de Ventas
        cuenta_ventas = cuenta_map.get('4101', 1)
        cuenta_bancos = cuenta_map.get('1102', 1)
        cuenta_iva = cuenta_map.get('2102', 1)
        cuenta_costo = cuenta_map.get('5101', 1)

        # Cargar tipo_transaccion desde DW
        dim_tipo = pd.read_sql_query(
            "SELECT tipo_transaccion_id, codigo FROM dim_tipo_transaccion", self.dw_conn
        )
        tipo_venta = dim_tipo[dim_tipo['codigo'].str.contains('VENTA', case=False, na=False)]
        tipo_venta_id = int(tipo_venta['tipo_transaccion_id'].iloc[0]) if len(tipo_venta) > 0 else int(dim_tipo['tipo_transaccion_id'].iloc[0])

        # Cargar centro de costo desde DW
        dim_centro = pd.read_sql_query(
            "SELECT centro_costo_id FROM dim_centro_costo LIMIT 1", self.dw_conn
        )
        centro_costo_id = int(dim_centro['centro_costo_id'].iloc[0])

        # Cargar usuario desde DW
        dim_usuario = pd.read_sql_query(
            "SELECT MIN(usuario_id) as usuario_id FROM dim_usuario", self.dw_conn
        )
        usuario_default = int(dim_usuario['usuario_id'].iloc[0])

        # Generar asientos contables
        transacciones = []
        asiento_num = 1
        
        for _, venta in df_ventas.iterrows():
            fecha_id = venta['fecha_id']
            orden_id = venta['orden_id']
            usuario_id = venta['usuario_id'] if pd.notna(venta['usuario_id']) else usuario_default
            
            # Asiento 1: Débito a Bancos (entrada de efectivo)
            transacciones.append({
                'fecha_id': fecha_id,
                'cuenta_id': cuenta_bancos,
                'centro_costo_id': centro_costo_id,
                'tipo_transaccion_id': tipo_venta_id,
                'usuario_id': usuario_id,
                'numero_asiento': f'AST-{asiento_num:06d}',
                'tipo_movimiento': 'DEBITO',
                'monto': float(venta['total']),
                'documento_referencia': f'ORD-{orden_id}',
                'descripcion': f'Cobro orden #{orden_id}',
                'orden_id': orden_id,
                'movimiento_inventario_id': None,
            })
            
            # Asiento 2: Crédito a Ventas (ingreso)
            transacciones.append({
                'fecha_id': fecha_id,
                'cuenta_id': cuenta_ventas,
                'centro_costo_id': centro_costo_id,
                'tipo_transaccion_id': tipo_venta_id,
                'usuario_id': usuario_id,
                'numero_asiento': f'AST-{asiento_num:06d}',
                'tipo_movimiento': 'CREDITO',
                'monto': float(venta['subtotal']),
                'documento_referencia': f'ORD-{orden_id}',
                'descripcion': f'Ingreso venta orden #{orden_id}',
                'orden_id': orden_id,
                'movimiento_inventario_id': None,
            })
            
            # Asiento 3: Crédito a IVA por Pagar
            if venta['iva'] > 0:
                transacciones.append({
                    'fecha_id': fecha_id,
                    'cuenta_id': cuenta_iva,
                    'centro_costo_id': centro_costo_id,
                    'tipo_transaccion_id': tipo_venta_id,
                    'usuario_id': usuario_id,
                    'numero_asiento': f'AST-{asiento_num:06d}',
                    'tipo_movimiento': 'CREDITO',
                    'monto': float(venta['iva']),
                    'documento_referencia': f'ORD-{orden_id}',
                    'descripcion': f'IVA venta orden #{orden_id}',
                    'orden_id': orden_id,
                    'movimiento_inventario_id': None,
                })
            
            asiento_num += 1

        df = pd.DataFrame(transacciones)
        df["created_at"] = pd.Timestamp.now()

        logger.info(f"   ✅ fact_transacciones: {len(df):,} asientos generados")
        logger.info(f"   📊 Tipo movimiento: {df['tipo_movimiento'].value_counts().to_dict()}")
        logger.info(f"   📊 Cuentas únicas: {df['cuenta_id'].nunique()}")

        # Seleccionar columnas del esquema (sin transaccion_id, es SERIAL)
        return df[
            [
                "fecha_id",
                "cuenta_id",
                "centro_costo_id",
                "tipo_transaccion_id",
                "usuario_id",
                "numero_asiento",
                "tipo_movimiento",
                "monto",
                "documento_referencia",
                "descripcion",
                "orden_id",
                "movimiento_inventario_id",
                "created_at",
            ]
        ]

    def build_fact_balance(self) -> pd.DataFrame:
        """Construir fact_balance desde CSV o fact_transacciones"""
        logger.info("📊 Construyendo fact_balance...")

        # Primero intentar cargar desde CSV
        csv_path = ROOT / "data" / "inputs" / "balance.csv"
        if csv_path.exists():
            logger.info(f"   📂 Cargando desde CSV: {csv_path}")
            try:
                df = pd.read_csv(csv_path)

                # Eliminar fecha_id del CSV si existe (lo recalcularemos)
                if "fecha_id" in df.columns:
                    df = df.drop(columns=["fecha_id"])

                # Los cuenta_id del CSV ya son surrogate keys (1,2,3...)
                # Solo necesitamos validar que existan en dim_cuenta_contable
                parquet_dir = ROOT / "data" / "outputs" / "parquet"
                dim_cuenta = pd.read_parquet(
                    parquet_dir / "dim_cuenta_contable.parquet"
                )
                cuentas_validas = dim_cuenta["cuenta_contable_id"].unique()

                # Filtrar solo cuentas que existen
                df_original_count = len(df)
                df = df[df["cuenta_id"].isin(cuentas_validas)]
                if df_original_count > len(df):
                    logger.warning(
                        f"   ⚠️  Filtrados {df_original_count - len(df)} registros con cuentas inexistentes"
                    )

                # Convertir tipos
                for col in ["periodo_id", "cuenta_id"]:
                    df[col] = df[col].astype(int)
                for col in ["saldo_inicial", "debitos", "creditos", "saldo_final"]:
                    df[col] = df[col].astype(float).round(2)

                # Si periodo_id es secuencial (1,2,3...) convertir a formato YYYYMM
                if df["periodo_id"].min() < 1000:
                    logger.info(
                        f"   🔄 Convirtiendo periodo_id secuencial a formato YYYYMM..."
                    )
                    df["periodo_id"] = 202400 + df["periodo_id"]

                # Generar fecha_id desde periodo_id (YYYYMM → YYYYMM01)
                df["fecha_id"] = (df["periodo_id"] * 100 + 1).astype(int)

                # Agregar surrogate key (PK)
                df.insert(0, "balance_id", range(1, len(df) + 1))

                logger.info(f"   ✓ fact_balance: {len(df):,} registros desde CSV")
                logger.info(f"   Períodos: {sorted(df['periodo_id'].unique())}")
                return df
            except Exception as e:
                logger.warning(f"   ⚠️  Error leyendo CSV: {e}")

        # Si no hay CSV, intentar construir desde fact_transacciones
        logger.info("   📊 Construyendo desde fact_transacciones...")
        query = """
        SELECT 
            periodo_id,
            cuenta_id,
            SUM(CASE WHEN tipo_movimiento = 'debe' THEN monto ELSE 0 END) as debitos,
            SUM(CASE WHEN tipo_movimiento = 'haber' THEN monto ELSE 0 END) as creditos
        FROM fact_transacciones
        WHERE cuenta_id IS NOT NULL AND periodo_id IS NOT NULL
        GROUP BY periodo_id, cuenta_id
        ORDER BY periodo_id, cuenta_id
        """

        try:
            df = pd.read_sql_query(query, self.dw_conn)

            # Calcular saldos
            # Saldo inicial = saldo final del período anterior
            # Ordenar por cuenta y período
            df = df.sort_values(["cuenta_id", "periodo_id"])

            # Para cada cuenta, calcular el saldo acumulado
            df["saldo_inicial"] = 0.0
            df["saldo_final"] = df["debitos"] - df["creditos"]

            # Calcular saldo inicial como el saldo final del período anterior
            for cuenta_id in df["cuenta_id"].unique():
                mask = df["cuenta_id"] == cuenta_id
                # Acumular saldo para esta cuenta
                saldos = df.loc[mask, "saldo_final"].cumsum()
                # El saldo inicial es el saldo acumulado del período anterior
                df.loc[mask, "saldo_final"] = saldos
                df.loc[mask, "saldo_inicial"] = saldos.shift(1).fillna(0)

            # Convertir tipos numpy a tipos nativos de Python
            for col in ["periodo_id", "cuenta_id"]:
                df[col] = df[col].astype(int)
            for col in ["debitos", "creditos", "saldo_inicial", "saldo_final"]:
                df[col] = df[col].astype(float).round(2)

            # Generar fecha_id desde periodo_id (YYYYMM → YYYYMM01)
            df["fecha_id"] = (df["periodo_id"] * 100 + 1).astype(int)

            # Agregar surrogate key (PK)
            df.insert(0, "balance_id", range(1, len(df) + 1))

            logger.info(f"✓ fact_balance: {len(df):,} registros agregados")
            logger.info(
                f"   Períodos: {df['periodo_id'].nunique()}, Cuentas: {df['cuenta_id'].nunique()}"
            )

        except Exception as e:
            logger.error(f"❌ Error construyendo fact_balance: {e}")
            import traceback

            traceback.print_exc()
            df = pd.DataFrame(
                columns=[
                    "periodo_id",
                    "cuenta_id",
                    "saldo_inicial",
                    "debitos",
                    "creditos",
                    "saldo_final",
                ]
            )

        return df

    def build_fact_estado_resultados(self) -> pd.DataFrame:
        """Construir fact_estado_resultados desde CSV o fact_transacciones"""
        logger.info("📈 Construyendo fact_estado_resultados...")

        # Primero intentar cargar desde CSV
        csv_path = ROOT / "data" / "inputs" / "estado_resultados.csv"
        if csv_path.exists():
            logger.info(f"   📂 Cargando desde CSV: {csv_path}")
            try:
                df = pd.read_csv(csv_path)

                # Eliminar fecha_id del CSV si existe (lo recalcularemos)
                if "fecha_id" in df.columns:
                    df = df.drop(columns=["fecha_id"])

                # Los cuenta_id del CSV ya son surrogate keys (7,8,9,10,11...)
                # Solo necesitamos validar que existan en dim_cuenta_contable
                parquet_dir = ROOT / "data" / "outputs" / "parquet"
                dim_cuenta = pd.read_parquet(
                    parquet_dir / "dim_cuenta_contable.parquet"
                )
                cuentas_validas = dim_cuenta["cuenta_contable_id"].unique()

                # Filtrar solo cuentas que existen
                df_original_count = len(df)
                df = df[df["cuenta_id"].isin(cuentas_validas)]
                if df_original_count > len(df):
                    logger.warning(
                        f"   ⚠️  Filtrados {df_original_count - len(df)} registros con cuentas inexistentes"
                    )

                # Convertir tipos
                for col in ["periodo_id", "cuenta_id", "centro_costo_id"]:
                    df[col] = df[col].astype(int)
                for col in [
                    "ingresos",
                    "costos",
                    "gastos",
                    "utilidad_bruta",
                    "utilidad_neta",
                ]:
                    df[col] = df[col].astype(float).round(2)

                # Si periodo_id es secuencial (1,2,3...) convertir a formato YYYYMM
                if df["periodo_id"].min() < 1000:
                    logger.info(
                        f"   🔄 Convirtiendo periodo_id secuencial a formato YYYYMM..."
                    )
                    # Asumir que 1=Ene 2024 (202401), 2=Feb 2024 (202402), etc.
                    df["periodo_id"] = 202400 + df["periodo_id"]

                # Generar fecha_id desde periodo_id (YYYYMM → YYYYMM01)
                df["fecha_id"] = (df["periodo_id"] * 100 + 1).astype(int)

                # Agregar surrogate key (PK)
                df.insert(0, "estado_resultados_id", range(1, len(df) + 1))

                logger.info(
                    f"   ✓ fact_estado_resultados: {len(df):,} registros desde CSV"
                )
                logger.info(f"   Períodos: {sorted(df['periodo_id'].unique())}")
                return df
            except Exception as e:
                logger.warning(f"   ⚠️  Error leyendo CSV: {e}")

        # Si no hay CSV, intentar construir desde fact_transacciones
        logger.info("   📈 Construyendo desde fact_transacciones...")
        query = """
        SELECT 
            ft.periodo_id,
            ft.cuenta_id,
            ft.centro_costo_id,
            dc.tipo as naturaleza_cuenta,
            dc.nombre as nombre_cuenta,
            SUM(CASE WHEN ft.tipo_movimiento = 'debe' THEN ft.monto ELSE 0 END) as debitos,
            SUM(CASE WHEN ft.tipo_movimiento = 'haber' THEN ft.monto ELSE 0 END) as creditos,
            SUM(CASE WHEN ft.tipo_movimiento = 'debe' THEN ft.monto 
                     ELSE -ft.monto END) as monto_neto
        FROM fact_transacciones ft
        INNER JOIN dim_cuenta_contable dc ON ft.cuenta_id = dc.cuenta_id
        WHERE ft.cuenta_id IS NOT NULL 
          AND ft.periodo_id IS NOT NULL
          AND dc.codigo IS NOT NULL
        GROUP BY ft.periodo_id, ft.cuenta_id, ft.centro_costo_id, dc.tipo, dc.nombre
        ORDER BY ft.periodo_id, ft.cuenta_id
        """

        try:
            df = pd.read_sql_query(query, self.dw_conn)

            # Clasificar cuentas por tipo de estado de resultados
            # Las cuentas 4000-4999 son ingresos, 5000-5999 son costos, 6000-6999 son gastos
            def clasificar_cuenta(row):
                # Aquí clasificamos por nombre o naturaleza
                nombre = str(row["nombre_cuenta"]).lower()
                if "ingreso" in nombre or "venta" in nombre:
                    return "ingreso"
                elif "costo" in nombre or "compra" in nombre:
                    return "costo"
                elif "gasto" in nombre:
                    return "gasto"
                else:
                    return "otro"

            df["tipo_cuenta"] = df.apply(clasificar_cuenta, axis=1)

            # Pivotar para calcular componentes del estado de resultados
            pivot = df.pivot_table(
                index=["periodo_id", "cuenta_id", "centro_costo_id"],
                columns="tipo_cuenta",
                values="creditos",  # Ingresos y costos usan créditos generalmente
                aggfunc="sum",
                fill_value=0,
            ).reset_index()

            # Calcular métricas financieras
            pivot["ingresos"] = pivot.get("ingreso", 0)
            pivot["costos"] = pivot.get("costo", 0)
            pivot["gastos"] = pivot.get("gasto", 0)
            pivot["utilidad_bruta"] = pivot["ingresos"] - pivot["costos"]
            pivot["utilidad_neta"] = pivot["utilidad_bruta"] - pivot["gastos"]

            # Seleccionar solo las columnas finales
            result = pivot[
                [
                    "periodo_id",
                    "cuenta_id",
                    "centro_costo_id",
                    "ingresos",
                    "costos",
                    "gastos",
                    "utilidad_bruta",
                    "utilidad_neta",
                ]
            ]

            # Filtrar filas con al menos un valor no cero
            result = result[
                (result["ingresos"] != 0)
                | (result["costos"] != 0)
                | (result["gastos"] != 0)
            ]

            # Convertir tipos numpy a tipos nativos de Python
            for col in ["periodo_id", "cuenta_id", "centro_costo_id"]:
                result[col] = result[col].astype(int)
            for col in [
                "ingresos",
                "costos",
                "gastos",
                "utilidad_bruta",
                "utilidad_neta",
            ]:
                result[col] = result[col].astype(float).round(2)

            # Generar fecha_id desde periodo_id (YYYYMM → YYYYMM01)
            result["fecha_id"] = (result["periodo_id"] * 100 + 1).astype(int)

            # Agregar surrogate key (PK)
            result.insert(0, "estado_resultados_id", range(1, len(result) + 1))

            logger.info(
                f"✓ fact_estado_resultados: {len(result):,} registros agregados"
            )
            logger.info(
                f"   Períodos: {result['periodo_id'].nunique()}, Cuentas: {result['cuenta_id'].nunique()}"
            )

        except Exception as e:
            logger.error(f"❌ Error construyendo fact_estado_resultados: {e}")
            import traceback

            traceback.print_exc()
            result = pd.DataFrame(
                columns=[
                    "periodo_id",
                    "cuenta_id",
                    "centro_costo_id",
                    "ingresos",
                    "costos",
                    "gastos",
                    "utilidad_bruta",
                    "utilidad_neta",
                ]
            )

        return result

    def __del__(self):
        """Cerrar conexiones"""
        try:
            self.oro_conn.close()
            self.dw_conn.close()
        except:
            pass
