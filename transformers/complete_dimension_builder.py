#!/usr/bin/env python3
"""
DIMENSION TRANSFORMERS - Transformadores completos para todas las dimensiones
Puebla dimensiones con datos reales desde OroCommerce, OroCRM y CSVs
"""

import pandas as pd
import psycopg2
import os
from datetime import datetime, timedelta
from typing import Dict, Any
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# ROOT del proyecto (Data_Warehouse_Punta_Fina)
ROOT = Path(__file__).resolve().parent.parent


class CompleteDimensionBuilder:
    """Constructor completo de todas las dimensiones del DW"""

    def __init__(self):
        self.oro_conn = self._get_oro_connection()
        self.crm_conn = self._get_crm_connection()

    def build(
        self, dimension_name: str, dimension_config: Dict[str, Any] = None
    ) -> pd.DataFrame:
        """
        Método genérico para construir cualquier dimensión
        Delegación a métodos específicos
        """
        method_name = f"build_{dimension_name}"
        if hasattr(self, method_name):
            method = getattr(self, method_name)
            return method()
        else:
            logger.warning(
                f"Dimensión {dimension_name} no implementada en CompleteDimensionBuilder"
            )
            return pd.DataFrame()

    def get_schema(self, dimension_name: str) -> Dict[str, str]:
        """
        Retorna el esquema de la dimensión para el loader
        Este método es requerido por el orchestrator pero no lo usamos
        porque las dimensiones ya tienen sus tablas creadas
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

    def _get_crm_connection(self):
        """Conexión a OroCRM"""
        return psycopg2.connect(
            host=os.getenv("CRM_DB_HOST"),
            port=int(os.getenv("CRM_DB_PORT")),
            dbname=os.getenv("CRM_DB_NAME"),
            user=os.getenv("CRM_DB_USER"),
            password=os.getenv("CRM_DB_PASS"),
        )

    # ==================== DIMENSIONES CONFORMADAS ====================

    def build_dim_fecha(self) -> pd.DataFrame:
        """Construir dim_fecha completa"""
        logger.info("📅 Construyendo dim_fecha...")

        date_range = pd.date_range(start="2020-01-01", end="2030-12-31", freq="D")
        df = pd.DataFrame({"fecha": date_range})

        df["fecha_id"] = df["fecha"].apply(lambda x: int(x.strftime("%Y%m%d")))
        df["anio"] = df["fecha"].dt.year
        df["mes"] = df["fecha"].dt.month
        df["dia"] = df["fecha"].dt.day
        df["trimestre"] = df["fecha"].dt.quarter
        df["semana_anio"] = df["fecha"].dt.isocalendar().week.astype(int)
        df["dia_semana"] = df["fecha"].dt.dayofweek + 1

        dias = [
            "Lunes",
            "Martes",
            "Miércoles",
            "Jueves",
            "Viernes",
            "Sábado",
            "Domingo",
        ]
        df["dia_semana_nombre"] = df["dia_semana"].apply(lambda x: dias[x - 1])

        meses = [
            "Enero",
            "Febrero",
            "Marzo",
            "Abril",
            "Mayo",
            "Junio",
            "Julio",
            "Agosto",
            "Septiembre",
            "Octubre",
            "Noviembre",
            "Diciembre",
        ]
        df["mes_nombre"] = df["mes"].apply(lambda x: meses[x - 1])

        df["es_fin_semana"] = df["dia_semana"].isin([6, 7])
        df["es_festivo"] = False  # Se puede enriquecer con días festivos reales
        df["nombre_festivo"] = ""  # String vacío en lugar de NULL
        df["created_at"] = pd.Timestamp.now()

        # Schema: fecha_id, fecha, anio, mes, dia, trimestre, semana_anio, dia_semana,
        #         dia_semana_nombre, mes_nombre, es_fin_semana, es_festivo, nombre_festivo, created_at
        df = df[
            [
                "fecha_id",
                "fecha",
                "anio",
                "mes",
                "dia",
                "trimestre",
                "semana_anio",
                "dia_semana",
                "dia_semana_nombre",
                "mes_nombre",
                "es_fin_semana",
                "es_festivo",
                "nombre_festivo",
                "created_at",
            ]
        ]

        logger.info(f"✓ dim_fecha: {len(df):,} registros")
        return df

    def build_dim_usuario(self) -> pd.DataFrame:
        """Construir dim_usuario desde oro_user"""
        logger.info("👤 Construyendo dim_usuario...")

        query = """
        SELECT 
            id as usuario_id,
            id as usuario_externo_id,
            username,
            email,
            COALESCE(NULLIF(CONCAT(first_name, ' ', last_name), ' '), username) as nombre_completo,
            enabled as activo,
            createdat as created_at
        FROM oro_user
        WHERE enabled = true
        ORDER BY id
        """

        df = pd.read_sql_query(query, self.oro_conn)
        df["updated_at"] = df["created_at"]

        # Limpiar NULLs - Asegurar que no queden valores vacíos
        df["nombre_completo"] = df["nombre_completo"].fillna(df["username"])
        # Reemplazar cadenas vacías
        mask = df["nombre_completo"] == ""
        df.loc[mask, "nombre_completo"] = df.loc[mask, "username"]
        df["email"] = df["email"].fillna("sin-email@puntafina.com")
        df["username"] = df["username"].fillna(
            "usuario_" + df["usuario_externo_id"].astype(str)
        )

        # Crear columna 'nombre' para compatibilidad con validaciones
        df["nombre"] = df["nombre_completo"]

        # Schema: usuario_id (OroCommerce ID), usuario_externo_id, username, email, nombre_completo, activo, created_at, updated_at
        df = df[
            [
                "usuario_id",
                "usuario_externo_id",
                "username",
                "email",
                "nombre_completo",
                "activo",
                "created_at",
                "updated_at",
            ]
        ]

        logger.info(f"✓ dim_usuario: {len(df):,} registros desde oro_user")
        return df

    def build_dim_producto(self) -> pd.DataFrame:
        """Construir dim_producto desde oro_product con precios y costos reales"""
        logger.info("📦 Construyendo dim_producto...")

        # Extraer productos base con categoría real de oro_catalog_category
        query = """
        SELECT 
            p.id as producto_id,
            p.id as producto_externo_id,
            p.sku,
            COALESCE(NULLIF(TRIM(p.name), ''), 'Producto ' || p.id) as nombre,
            p.type as tipo,
            p.created_at,
            CASE WHEN p.status = 'enabled' THEN true ELSE false END as activo,
            COALESCE(c.title, 'Sin Categoría') as categoria
        FROM oro_product p
        LEFT JOIN oro_catalog_category c ON p.category_id = c.id
        ORDER BY p.id
        """

        df = pd.read_sql_query(query, self.oro_conn)

        # Limpiar NULLs en nombre
        df["nombre"] = df["nombre"].fillna(
            "Producto " + df["producto_externo_id"].astype(str)
        )
        # Reemplazar cadenas vacías
        mask = df["nombre"] == ""
        df.loc[mask, "nombre"] = "Producto " + df.loc[
            mask, "producto_externo_id"
        ].astype(str)

        df["descripcion"] = df["nombre"]
        # categoria ya viene del JOIN, no sobrescribir
        df["marca"] = df["nombre"].str.split().str[0].fillna("Sin Marca")
        df["unidad_medida"] = "Pieza"

        # Obtener precios desde oro_price_product (precio de venta)
        logger.info("   📊 Obteniendo precios desde oro_price_product...")
        query_precios = """
        SELECT 
            product_id,
            AVG(CAST(value AS NUMERIC)) as precio_promedio
        FROM oro_price_product
        WHERE value > 0
        GROUP BY product_id
        """

        try:
            df_precios = pd.read_sql_query(query_precios, self.oro_conn)
            df = df.merge(
                df_precios,
                left_on="producto_id",
                right_on="product_id",
                how="left",
            )
            df = df.drop(columns=["product_id"], errors="ignore")
            df = df.rename(columns={"precio_promedio": "precio_base"})
        except Exception as e:
            logger.warning(f"   ⚠️  No se pudieron obtener precios: {e}")
            df["precio_base"] = 0.0

        # Obtener costos desde CSV de compras
        logger.info("   📦 Obteniendo costos desde CSV de compras...")
        try:
            csv_compras = ROOT / "Compras_Productos_PuntaFina.csv"
            df_compras = pd.read_csv(csv_compras)

            # Calcular costo promedio por producto
            df_costos = (
                df_compras.groupby("Producto_ID")
                .agg(
                    {
                        "Precio_Unitario_USD": "mean",
                        "Costo_Promedio_USD": "last",  # Tomar el último costo promedio
                    }
                )
                .reset_index()
            )

            df_costos = df_costos.rename(
                columns={
                    "Producto_ID": "producto_id",
                    "Costo_Promedio_USD": "costo_estandar",
                }
            )

            df = df.merge(
                df_costos[["producto_id", "costo_estandar"]],
                on="producto_id",
                how="left",
            )
        except Exception as e:
            logger.warning(f"   ⚠️  No se pudieron obtener costos: {e}")
            df["costo_estandar"] = 0.0

        # Limpiar valores nulos
        df["precio_base"] = df["precio_base"].fillna(0.0)
        df["costo_estandar"] = df["costo_estandar"].fillna(0.0)

        # Para productos sin precio, estimar basándose en costo (margen ~60%)
        mask_sin_precio = (df["precio_base"] == 0) & (df["costo_estandar"] > 0)
        df.loc[mask_sin_precio, "precio_base"] = (
            df.loc[mask_sin_precio, "costo_estandar"] * 2.5
        )

        # Para productos sin costo, estimar basándose en precio (margen ~60%)
        mask_sin_costo = (df["costo_estandar"] == 0) & (df["precio_base"] > 0)
        df.loc[mask_sin_costo, "costo_estandar"] = (
            df.loc[mask_sin_costo, "precio_base"] * 0.4
        )

        # Asegurar que el precio base sea coherente con el costo (margen razonable ~60%)
        # Si precio_base > costo_estandar * 5, ajustar a un margen del 60%
        mask_precio_excesivo = (df["costo_estandar"] > 0) & (
            df["precio_base"] > df["costo_estandar"] * 5
        )
        df.loc[mask_precio_excesivo, "precio_base"] = (
            df.loc[mask_precio_excesivo, "costo_estandar"] * 2.5
        )

        # Seleccionar solo columnas del esquema DW (sin updated_at)
        df_final = df[
            [
                "producto_id",
                "producto_externo_id",
                "sku",
                "nombre",
                "descripcion",
                "categoria",
                "marca",
                "tipo",
                "unidad_medida",
                "precio_base",
                "costo_estandar",
                "activo",
                "created_at",
            ]
        ].copy()

        logger.info(f"✓ dim_producto: {len(df_final):,} registros desde oro_product")
        logger.info(
            f"   📊 Con precio: {(df_final['precio_base'] > 0).sum()} ({(df_final['precio_base'] > 0).sum()/len(df_final)*100:.1f}%)"
        )
        logger.info(
            f"   📦 Con costo: {(df_final['costo_estandar'] > 0).sum()} ({(df_final['costo_estandar'] > 0).sum()/len(df_final)*100:.1f}%)"
        )

        return df_final

    # ==================== DIMENSIONES DE VENTAS ====================

    def build_dim_cliente(self) -> pd.DataFrame:
        """Construir dim_cliente desde oro_customer con email desde oro_customer_user"""
        logger.info("👥 Construyendo dim_cliente...")

        # Query principal de clientes con email desde customer_user
        query = """
        SELECT 
            c.id as cliente_id,
            c.id as cliente_externo_id,
            COALESCE(NULLIF(TRIM(c.name), ''), 'Cliente ' || c.id) as nombre,
            c.created_at as fecha_registro,
            COALESCE(cu.email, 'sin-email@puntafina.com') as email
        FROM oro_customer c
        LEFT JOIN oro_customer_user cu ON cu.customer_id = c.id
        ORDER BY c.id
        """

        df = pd.read_sql_query(query, self.oro_conn)

        # Eliminar duplicados por cliente (puede haber múltiples usuarios por cliente)
        df = df.drop_duplicates(subset=["cliente_id"], keep="first")

        # Limpiar NULLs en nombre
        df["nombre"] = df["nombre"].fillna("Cliente " + df["cliente_id"].astype(str))
        # Reemplazar cadenas vacías
        mask = df["nombre"] == ""
        df.loc[mask, "nombre"] = "Cliente " + df.loc[mask, "cliente_id"].astype(str)

        df["codigo_cliente"] = "CLI-" + df["cliente_id"].astype(str).str.zfill(6)
        df["tipo_cliente"] = "B2B"
        df["segmento"] = "Regular"
        df["telefono"] = "N/A"  # No disponible en origen
        df["activo"] = True
        df["created_at"] = pd.Timestamp.now()

        # Schema: cliente_id (OroCommerce ID), cliente_externo_id, codigo_cliente, nombre, tipo_cliente,
        #         segmento, email, telefono, activo, fecha_registro, created_at
        df = df[
            [
                "cliente_id",
                "cliente_externo_id",
                "codigo_cliente",
                "nombre",
                "tipo_cliente",
                "segmento",
                "email",
                "telefono",
                "activo",
                "fecha_registro",
                "created_at",
            ]
        ]

        logger.info(f"✓ dim_cliente: {len(df):,} registros desde oro_customer")
        return df

    def build_dim_sitio_web(self) -> pd.DataFrame:
        """Construir dim_sitio_web desde CSV (oro_website está vacío)"""
        logger.info("🌐 Construyendo dim_sitio_web...")

        try:
            # Primero intentar desde CSV
            csv_path = "/root/PuntaFina_DW_Oro/data/inputs/ventas/sitios_web.csv"
            df = pd.read_csv(csv_path)

            # Renombrar columnas para match con DW
            df = df.rename(columns={"sitio_web_id": "sitio_externo_id"})

            # Agregar timestamps
            df["created_at"] = pd.Timestamp.now()
            df["updated_at"] = pd.Timestamp.now()

            logger.info(f"✓ dim_sitio_web: {len(df):,} registros desde CSV")

        except Exception as e:
            logger.warning(f"No se pudo leer CSV, intentando oro_website: {e}")
            query = """
            SELECT 
                id as sitio_externo_id,
                name as nombre,
                created_at,
                updated_at
            FROM oro_website
            ORDER BY id
            """
            df = pd.read_sql_query(query, self.oro_conn)
            df["url"] = "https://puntafina.com"
            df["activo"] = True
            logger.info(f"✓ dim_sitio_web: {len(df):,} registros desde oro_website")

        return df

    def build_dim_canal(self) -> pd.DataFrame:
        """Construir dim_canal desde orocrm_channel"""
        logger.info("📡 Construyendo dim_canal...")

        try:
            query = """
            SELECT 
                id as canal_externo_id,
                name as nombre,
                channel_type as tipo,
                status as estado
            FROM orocrm_channel
            ORDER BY id
            """
            df = pd.read_sql_query(
                query, self.crm_conn
            )  # ✅ Usar crm_conn en vez de oro_conn
            df["activo"] = df["estado"] == True
        except Exception as e:
            logger.warning(f"Error leyendo orocrm_channel: {e}")
            # Si no existe la tabla, crear canales por defecto
            df = pd.DataFrame(
                {
                    "canal_externo_id": [1, 2, 3, 4],
                    "nombre": [
                        "E-Commerce",
                        "Tienda Física",
                        "Mayorista",
                        "Distribuidores",
                    ],
                    "tipo": ["b2c", "retail", "b2b", "wholesale"],
                    "estado": [True, True, True, True],
                }
            )
            df["activo"] = True

        logger.info(f"✓ dim_canal: {len(df):,} registros")
        return df

    def build_dim_direccion(self) -> pd.DataFrame:
        """Construir dim_direccion desde oro_order_address"""
        logger.info("📍 Construyendo dim_direccion...")

        query = """
        SELECT DISTINCT
            id as direccion_externo_id,
            street as calle,
            city as ciudad,
            postal_code as codigo_postal,
            region_text as region,
            country_code as pais_codigo,
            CONCAT_WS(', ', street, city, region_text, country_code) as direccion_completa
        FROM oro_order_address
        WHERE street IS NOT NULL
        ORDER BY id
        """

        df = pd.read_sql_query(query, self.oro_conn)
        df["activo"] = True

        logger.info(f"✓ dim_direccion: {len(df):,} registros desde oro_order_address")
        return df

    def build_dim_orden(self) -> pd.DataFrame:
        """Construir dim_orden (lookup table para atributos degenerados)"""
        logger.info("📋 Construyendo dim_orden...")

        query = """
        SELECT 
            id as orden_id,
            id as orden_externo_id,
            COALESCE(identifier, 'ORD-' || id::text) as numero_orden,
            COALESCE(currency, 'USD') as moneda,
            created_at
        FROM oro_order
        ORDER BY id
        """

        df = pd.read_sql_query(query, self.oro_conn)
        df["tipo_orden"] = "Venta"
        df["canal"] = "E-Commerce"
        df["tasa_cambio"] = 1.0

        # ✅ orden_id ahora es el ID de OroCommerce (no un serial autogenerado)
        # Schema: orden_id, orden_externo_id, numero_orden, tipo_orden, canal, moneda, tasa_cambio, created_at
        df = df[
            [
                "orden_id",
                "orden_externo_id",
                "numero_orden",
                "tipo_orden",
                "canal",
                "moneda",
                "tasa_cambio",
                "created_at",
            ]
        ]

        logger.info(f"✓ dim_orden: {len(df):,} registros desde oro_order")
        logger.info(f"   ✓ orden_id = OroCommerce id (auditable)")
        return df

    def build_dim_line_item(self) -> pd.DataFrame:
        """Construir dim_line_item desde oro_order_line_item"""
        logger.info("📝 Construyendo dim_line_item...")

        query = """
        SELECT 
            id as line_item_externo_id,
            product_name as producto_nombre,
            quantity as cantidad,
            value as precio_unitario
        FROM oro_order_line_item
        WHERE id IS NOT NULL
        ORDER BY id
        """

        df = pd.read_sql_query(query, self.oro_conn)

        # Asignar surrogate keys
        df.insert(0, "line_item_id", range(1, len(df) + 1))

        # Limpiar y convertir tipos
        df["cantidad"] = (
            pd.to_numeric(df["cantidad"], errors="coerce").fillna(0).round(2)
        )
        df["precio_unitario"] = (
            pd.to_numeric(df["precio_unitario"], errors="coerce").fillna(0).round(2)
        )
        df["producto_nombre"] = df["producto_nombre"].fillna("Sin nombre").astype(str)

        logger.info(f"✓ dim_line_item: {len(df):,} registros desde oro_order_line_item")
        logger.info(f"   Productos únicos: {df['producto_nombre'].nunique()}")
        return df[
            [
                "line_item_id",
                "line_item_externo_id",
                "producto_nombre",
                "cantidad",
                "precio_unitario",
            ]
        ]

    def build_dim_detalle_venta(self) -> pd.DataFrame:
        """Construir dim_detalle_venta desde oro_order_line_item"""
        logger.info("📋 Construyendo dim_detalle_venta...")

        query = """
        SELECT 
            id as detalle_externo_id,
            product_sku as codigo,
            COALESCE(comment, 
                     CASE 
                         WHEN shipping_method IS NOT NULL 
                         THEN 'Envío: ' || shipping_method || 
                              CASE WHEN shipping_method_type IS NOT NULL 
                                   THEN ' (' || shipping_method_type || ')' 
                                   ELSE '' END
                         ELSE 'Venta estándar'
                     END) as descripcion
        FROM oro_order_line_item
        WHERE id IS NOT NULL
        ORDER BY id
        """

        df = pd.read_sql_query(query, self.oro_conn)

        # Asignar surrogate keys
        df.insert(0, "detalle_id", range(1, len(df) + 1))

        # Limpiar datos
        df["codigo"] = df["codigo"].fillna("").astype(str)
        df["descripcion"] = df["descripcion"].fillna("Sin descripción").astype(str)

        logger.info(
            f"✓ dim_detalle_venta: {len(df):,} registros desde oro_order_line_item"
        )
        logger.info(f"   Códigos únicos: {df['codigo'].nunique()}")
        return df[["detalle_id", "codigo", "descripcion"]]

    # ==================== DIMENSIONES DESDE CSV ====================

    def build_dim_envio(self) -> pd.DataFrame:
        """Construir dim_envio desde CSV"""
        logger.info("🚚 Construyendo dim_envio desde CSV...")

        csv_path = ROOT / "data" / "inputs" / "ventas" / "metodos_envio.csv"
        df = pd.read_csv(csv_path)

        # Extraer ID numérico de ENV001 -> 1
        df["envio_externo_id"] = df["id_envio"].str.extract(r"(\d+)").astype(int)

        # Mapeo de columnas a estructura de tabla
        df = df.rename(columns={"metodo_envio": "metodo_envio", "costo": "costo_envio"})

        # Extraer días numéricos del tiempo_entrega
        df["tiempo_estimado_dias"] = (
            df["tiempo_entrega"].str.extract(r"(\d+)").fillna(1).astype(int)
        )

        # Transportista genérico
        df["transportista"] = "PuntaFina Logistics"

        logger.info(f"✓ dim_envio: {len(df):,} registros desde CSV")
        return df[
            [
                "envio_externo_id",
                "metodo_envio",
                "transportista",
                "costo_envio",
                "tiempo_estimado_dias",
            ]
        ]

    def build_dim_estado_orden(self) -> pd.DataFrame:
        """Construir dim_estado_orden desde CSV"""
        logger.info("📊 Construyendo dim_estado_orden desde CSV...")

        csv_path = ROOT / "data" / "inputs" / "ventas" / "estados_orden.csv"
        df = pd.read_csv(csv_path)
        df = df.rename(
            columns={
                "id_estado_orden": "estado_orden_externo_id",
                "codigo_estado": "codigo",
                "nombre_estado": "nombre",
                "descripcion": "descripcion",
            }
        )

        logger.info(f"✓ dim_estado_orden: {len(df):,} registros desde CSV")
        return df[["estado_orden_externo_id", "codigo", "nombre", "descripcion"]]

    def build_dim_estado_pago(self) -> pd.DataFrame:
        """Construir dim_estado_pago desde CSV"""
        logger.info("💳 Construyendo dim_estado_pago desde CSV...")

        csv_path = ROOT / "data" / "inputs" / "ventas" / "estados_pago.csv"
        df = pd.read_csv(csv_path)

        # Mapeo correcto: estado_pago es el código, metodo_pago es el nombre
        df = df.rename(
            columns={
                "estado_pago": "codigo",
                "metodo_pago": "nombre",
                "descripcion": "descripcion",
            }
        )

        # Eliminar duplicados por código (mantener primera ocurrencia)
        df = df.drop_duplicates(subset=["codigo"], keep="first")
        df["activo"] = True

        logger.info(f"✓ dim_estado_pago: {len(df):,} registros desde CSV")
        return df[["codigo", "nombre", "descripcion", "activo"]]

    def build_dim_pago(self) -> pd.DataFrame:
        """Construir dim_pago con métodos de pago comunes"""
        logger.info("💰 Construyendo dim_pago...")

        df = pd.DataFrame(
            {
                "pago_externo_id": range(1, 11),
                "metodo_pago": [
                    "Efectivo",
                    "Tarjeta Crédito",
                    "Tarjeta Débito",
                    "Transferencia",
                    "Cheque",
                    "PayPal",
                    "Stripe",
                    "Bitcoin",
                    "Crédito 30 días",
                    "Crédito 60 días",
                ],
                "procesador": [
                    "Manual",
                    "Visa/MC",
                    "Visa/MC",
                    "Banco",
                    "Banco",
                    "PayPal",
                    "Stripe",
                    "Blockchain",
                    "Interno",
                    "Interno",
                ],
                "tipo_pago": [
                    "Inmediato",
                    "Inmediato",
                    "Inmediato",
                    "Inmediato",
                    "Diferido",
                    "Inmediato",
                    "Inmediato",
                    "Inmediato",
                    "Crédito",
                    "Crédito",
                ],
            }
        )

        logger.info(f"✓ dim_pago: {len(df):,} registros")
        return df

    def build_dim_impuestos(self) -> pd.DataFrame:
        """Construir dim_impuestos (solo IVA, ISR y EXENTO)"""
        logger.info("📊 Construyendo dim_impuestos...")

        df = pd.DataFrame(
            {
                "impuesto_id": [1, 2, 3],
                "codigo": ["IVA", "ISR", "EXENTO"],
                "nombre": [
                    "IVA 13%",
                    "ISR",
                    "Exento",
                ],
                "tasa": [0.13, 0.25, 0.0],
                "tipo": ["ventas", "renta", "exento"],
            }
        )

        logger.info(f"✓ dim_impuestos: {len(df):,} registros (sin IMPCONS ni IMPADVAL)")
        return df

    def build_dim_promocion(self) -> pd.DataFrame:
        """Construir dim_promocion desde OroCommerce + registro default"""
        logger.info("🎁 Construyendo dim_promocion...")

        # Extraer promociones reales desde OroCommerce
        query = """
            SELECT 
                id as id_promocion_source,
                COALESCE(serialized_data->>'nombre', 'Promocion ' || id) as nombre_promocion,
                COALESCE(serialized_data->>'codigo', 'PROMO' || id) as tipo_promocion,
                use_coupons as usa_cupones,
                TRUE as activa,
                created_at as fecha_creacion,
                updated_at as fecha_actualizacion
            FROM oro_promotion
            ORDER BY id
        """

        try:
            df_promos = pd.read_sql_query(query, self.oro_conn)
            logger.info(
                f"   📥 Extraídas {len(df_promos)} promociones desde OroCommerce"
            )
        except Exception as e:
            logger.warning(f"   ⚠️  Error extrayendo promociones: {e}")
            df_promos = pd.DataFrame()

        # Agregar registro por defecto (sk_promocion será autogenerado)
        df_default = pd.DataFrame(
            {
                "id_promocion_source": [-1],
                "nombre_promocion": ["Sin Promoción"],
                "tipo_promocion": ["Ninguno"],
                "usa_cupones": [False],
                "activa": [True],
                "fecha_creacion": [pd.Timestamp("2020-01-01")],
                "fecha_actualizacion": [pd.Timestamp("2020-01-01")],
            }
        )

        # Combinar
        if not df_promos.empty:
            df = pd.concat([df_default, df_promos], ignore_index=True)
        else:
            df = df_default

        df["fecha_carga"] = pd.Timestamp.now()

        logger.info(f"   ✓ dim_promocion: {len(df):,} registros")
        return df

    # ==================== DIMENSIONES DE INVENTARIO ====================

    def build_dim_almacen(self) -> pd.DataFrame:
        """Construir dim_almacen desde CSV"""
        logger.info("🏪 Construyendo dim_almacen desde CSV...")

        csv_path = ROOT / "data" / "inputs" / "inventario" / "almacenes.csv"
        df = pd.read_csv(csv_path)

        # Mapear columnas del CSV al schema de DB
        # CSV: id_almacen, nombre_almacen, tipo_almacen, ciudad, departamento, direccion, capacidad_m3, ...
        # DB: almacen_id (serial), codigo, nombre, direccion, ciudad, pais, capacidad, tipo, activo
        df = df.rename(
            columns={
                "id_almacen": "codigo",
                "nombre_almacen": "nombre",
                "tipo_almacen": "tipo",
                "capacidad_m3": "capacidad",
            }
        )

        # Agregar columna pais si no existe
        if "pais" not in df.columns:
            df["pais"] = "El Salvador"

        # Convertir activo a boolean
        if "activo" in df.columns:
            df["activo"] = df["activo"].apply(
                lambda x: x in [True, "TRUE", "true", 1, "1"]
            )
        else:
            df["activo"] = True

        # Convertir capacidad a int
        df["capacidad"] = (
            pd.to_numeric(df["capacidad"], errors="coerce").fillna(0).astype(int)
        )

        # Seleccionar solo columnas que existen en el schema
        df = df[
            [
                "codigo",
                "nombre",
                "direccion",
                "ciudad",
                "pais",
                "capacidad",
                "tipo",
                "activo",
            ]
        ]

        logger.info(f"✓ dim_almacen: {len(df):,} registros desde CSV")
        return df

    def build_dim_proveedor(self) -> pd.DataFrame:
        """Construir dim_proveedor desde CSV"""
        logger.info("🏭 Construyendo dim_proveedor desde CSV...")

        csv_path = ROOT / "data" / "inputs" / "inventario" / "proveedores.csv"
        df = pd.read_csv(csv_path)

        # Mapear columnas del CSV al schema de DB
        # CSV: id_proveedor, nombre_proveedor, razon_social, nit, pais_origen, ciudad, direccion, telefono, email, contacto_principal, ...
        # DB: proveedor_id (serial), codigo, nombre, contacto, email, telefono, direccion, ciudad, pais, activo
        df = df.rename(
            columns={
                "id_proveedor": "codigo",
                "nombre_proveedor": "nombre",
                "contacto_principal": "contacto",
                "pais_origen": "pais",
            }
        )

        # Convertir activo a boolean
        if "activo" in df.columns:
            df["activo"] = df["activo"].apply(
                lambda x: x in [True, "TRUE", "true", 1, "1"]
            )
        else:
            df["activo"] = True

        # Seleccionar solo columnas que existen en el schema
        df = df[
            [
                "codigo",
                "nombre",
                "contacto",
                "email",
                "telefono",
                "direccion",
                "ciudad",
                "pais",
                "activo",
            ]
        ]

        logger.info(f"✓ dim_proveedor: {len(df):,} registros desde CSV")
        return df

    def build_dim_tipo_movimiento(self) -> pd.DataFrame:
        """Construir dim_tipo_movimiento desde CSV"""
        logger.info("📦 Construyendo dim_tipo_movimiento desde CSV...")

        csv_path = ROOT / "data" / "inputs" / "inventario" / "tipos_movimiento.csv"
        df = pd.read_csv(csv_path)

        # Mapear columnas del CSV al schema de DB
        # CSV: id_tipo_movimiento, nombre_tipo, categoria, afecta_stock, descripcion
        # DB: tipo_movimiento_id (serial), codigo, nombre, descripcion, tipo, afecta_stock, activo
        df = df.rename(
            columns={
                "id_tipo_movimiento": "codigo",
                "nombre_tipo": "nombre",
                "categoria": "tipo",
            }
        )

        # Agregar activo si no existe
        if "activo" not in df.columns:
            df["activo"] = True

        # Seleccionar solo columnas que existen en el schema
        df = df[["codigo", "nombre", "descripcion", "tipo", "afecta_stock", "activo"]]

        logger.info(f"✓ dim_tipo_movimiento: {len(df):,} registros desde CSV")
        return df

    def build_dim_categoria_producto(self) -> pd.DataFrame:
        """Construir dim_categoria_producto"""
        logger.info("📂 Construyendo dim_categoria_producto...")

        df = pd.DataFrame(
            {
                "categoria_externo_id": range(1, 11),
                "codigo": [
                    "CAT001",
                    "CAT002",
                    "CAT003",
                    "CAT004",
                    "CAT005",
                    "CAT006",
                    "CAT007",
                    "CAT008",
                    "CAT009",
                    "CAT010",
                ],
                "nombre": [
                    "Calzado Deportivo",
                    "Calzado Casual",
                    "Calzado Formal",
                    "Botas",
                    "Sandalias",
                    "Zapatos de Niño",
                    "Zapatos de Mujer",
                    "Zapatos de Hombre",
                    "Accesorios",
                    "Otros",
                ],
                "descripcion": [
                    "Zapatillas deportivas",
                    "Zapatos casuales",
                    "Zapatos formales",
                    "Botas diversas",
                    "Sandalias verano",
                    "Calzado infantil",
                    "Calzado femenino",
                    "Calzado masculino",
                    "Accesorios varios",
                    "Otros productos",
                ],
                "categoria_padre_id": [
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
                "nivel": [1] * 10,
                "activo": [True] * 10,
            }
        )

        logger.info(f"✓ dim_categoria_producto: {len(df):,} registros")
        return df

    # NOTA: build_dim_promocion ya está definido arriba (línea ~632)
    # Esta función era duplicada y fue eliminada

    # ==================== DIMENSIONES DE FINANZAS ====================

    def build_dim_cuenta_contable(self) -> pd.DataFrame:
        """Construir dim_cuenta_contable desde CSV"""
        logger.info("💼 Construyendo dim_cuenta_contable desde CSV...")

        csv_path = ROOT / "data" / "inputs" / "finanzas" / "cuentas_contables.csv"
        df = pd.read_csv(csv_path)

        # Mapear columnas CSV a esquema de DW
        df = df.rename(
            columns={
                "id_cuenta": "codigo",
                "nombre_cuenta": "nombre",
                "clasificacion": "categoria",
                "naturaleza": "tipo",
                "activa": "activo",
            }
        )

        # Mantener solo columnas necesarias
        df = df[
            [
                "codigo",
                "nombre",
                "descripcion",
                "tipo",
                "categoria",
                "nivel",
                "cuenta_padre",
                "activo",
            ]
        ]

        # Limpiar nulos y NaN - Reemplazar con valores por defecto
        # IMPORTANTE: Convertir a string DESPUÉS de fillna para evitar errores de tipo
        df["codigo"] = df["codigo"].fillna("").astype(str)
        df["nombre"] = df["nombre"].fillna("Sin nombre").astype(str)
        df["descripcion"] = df["descripcion"].fillna("Sin descripción").astype(str)
        df["tipo"] = df["tipo"].fillna("Sin tipo").astype(str)
        df["categoria"] = df["categoria"].fillna("").astype(str)
        df["nivel"] = pd.to_numeric(df["nivel"], errors="coerce").fillna(1).astype(int)
        # cuenta_padre puede tener NaN (floats) - convertir a string vacío
        df["cuenta_padre"] = df["cuenta_padre"].fillna("").astype(str)
        # Si cuenta_padre es 'nan' (string), reemplazar con vacío
        df["cuenta_padre"] = df["cuenta_padre"].replace("nan", "")
        df["activo"] = df["activo"].apply(
            lambda x: x in [True, "TRUE", "true", 1, "1", "activa"]
        )

        logger.info(
            f"✓ dim_cuenta_contable: {len(df):,} registros desde CSV (sin nulos)"
        )
        return df

    def build_dim_centro_costo(self) -> pd.DataFrame:
        """Construir dim_centro_costo desde CSV"""
        logger.info("🏢 Construyendo dim_centro_costo desde CSV...")

        csv_path = ROOT / "data" / "inputs" / "finanzas" / "centros_costo.csv"
        df = pd.read_csv(csv_path)

        # Mapear columnas del CSV al schema de DB
        # CSV: id_centro_costo, nombre_centro, tipo_centro, responsable, activo
        # DB: centro_costo_id (serial), codigo, nombre, descripcion, tipo, responsable, activo
        df = df.rename(
            columns={
                "id_centro_costo": "codigo",
                "nombre_centro": "nombre",
                "tipo_centro": "tipo",
            }
        )

        # Agregar descripcion si no existe
        if "descripcion" not in df.columns:
            df["descripcion"] = df["nombre"]

        # Convertir activo a boolean
        if "activo" in df.columns:
            df["activo"] = df["activo"].apply(
                lambda x: x in [True, "TRUE", "true", 1, "1"]
            )
        else:
            df["activo"] = True

        # Seleccionar solo columnas que existen en el schema
        df = df[["codigo", "nombre", "descripcion", "tipo", "responsable", "activo"]]

        logger.info(f"✓ dim_centro_costo: {len(df):,} registros desde CSV")
        return df

    def build_dim_tipo_transaccion(self) -> pd.DataFrame:
        """Construir dim_tipo_transaccion desde CSV"""
        logger.info("📋 Construyendo dim_tipo_transaccion desde CSV...")

        csv_path = ROOT / "data" / "inputs" / "finanzas" / "tipos_transaccion.csv"
        df = pd.read_csv(csv_path)

        # Mapear columnas del CSV al schema de DB
        # CSV: id_tipo_transaccion, nombre_tipo, categoria, descripcion
        # DB: tipo_transaccion_id (serial), codigo, nombre, descripcion, categoria, afecta_flujo, activo
        df = df.rename(
            columns={"id_tipo_transaccion": "codigo", "nombre_tipo": "nombre"}
        )

        # Agregar columnas faltantes
        if "afecta_flujo" not in df.columns:
            # Determinar afecta_flujo basado en categoria
            df["afecta_flujo"] = df["categoria"].apply(
                lambda x: (
                    "positivo"
                    if x in ["ingreso", "entrada"]
                    else "negativo" if x in ["gasto", "egreso"] else "neutro"
                )
            )

        if "activo" not in df.columns:
            df["activo"] = True

        # Seleccionar solo columnas que existen en el schema
        df = df[
            ["codigo", "nombre", "descripcion", "categoria", "afecta_flujo", "activo"]
        ]

        logger.info(f"✓ dim_tipo_transaccion: {len(df):,} registros desde CSV")
        return df

    def build_dim_periodo_contable(self) -> pd.DataFrame:
        """Construir dim_periodo_contable"""
        logger.info("📅 Construyendo dim_periodo_contable...")

        periodos = []
        for anio in range(2020, 2027):
            for mes in range(1, 13):
                periodo_id = anio * 100 + mes
                trimestre = (mes - 1) // 3 + 1
                nombre = f"{anio}-{mes:02d}"
                fecha_inicio = pd.to_datetime(f"{anio}-{mes:02d}-01")

                if mes == 12:
                    fecha_fin = pd.to_datetime(f"{anio}-12-31")
                else:
                    siguiente_mes = fecha_inicio + pd.DateOffset(months=1)
                    fecha_fin = siguiente_mes - pd.DateOffset(days=1)

                periodos.append(
                    {
                        "periodo_id": periodo_id,
                        "anio": anio,
                        "mes": mes,
                        "trimestre": trimestre,
                        "nombre_periodo": nombre,
                        "fecha_inicio": fecha_inicio,
                        "fecha_fin": fecha_fin,
                        "cerrado": False,
                    }
                )

        df = pd.DataFrame(periodos)
        logger.info(f"✓ dim_periodo_contable: {len(df):,} registros")
        return df

    def __del__(self):
        """Cerrar conexiones"""
        try:
            self.oro_conn.close()
            self.crm_conn.close()
        except:
            pass
