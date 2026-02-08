#!/usr/bin/env python3
"""
Script para generar movimientos_inventario.csv desde las ventas reales de OroCommerce
"""

import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Conectar a OroCommerce
oro_conn = psycopg2.connect(
    host=os.getenv("ORO_DB_HOST"),
    port=int(os.getenv("ORO_DB_PORT")),
    dbname=os.getenv("ORO_DB_NAME"),
    user=os.getenv("ORO_DB_USER"),
    password=os.getenv("ORO_DB_PASS"),
)

# Conectar al DW para obtener costos
dw_conn = psycopg2.connect(
    host=os.getenv("DW_DB_HOST"),
    port=int(os.getenv("DW_DB_PORT")),
    dbname=os.getenv("DW_DB_NAME"),
    user=os.getenv("DW_DB_USER"),
    password=os.getenv("DW_DB_PASS"),
)

print("🔄 Generando movimientos_inventario.csv desde ventas reales...")

# Obtener ventas desde OroCommerce
query_ventas = """
SELECT 
    o.created_at::date as fecha_movimiento,
    oli.product_id as id_producto,
    CAST(oli.quantity AS INTEGER) as cantidad,
    CAST(oli.value / NULLIF(oli.quantity, 0) AS NUMERIC(10,2)) as precio_unitario,
    oli.id as line_item_id,
    o.id as orden_id
FROM oro_order o
INNER JOIN oro_order_line_item oli ON o.id = oli.order_id
WHERE o.created_at IS NOT NULL 
  AND oli.product_id IS NOT NULL
  AND oli.quantity > 0
ORDER BY o.created_at, oli.id
"""

df_ventas = pd.read_sql_query(query_ventas, oro_conn)
print(f"   📥 {len(df_ventas):,} Ventas")

# Calcular costo como 40% del precio de venta
# IMPORTANTE: oro_order_line_item.value YA está SIN IVA
# No dividir por 1.13 porque ya está neto
df_ventas['costo_unitario'] = df_ventas['precio_unitario'] * 0.4  # Costo es 40% del precio (ya sin IVA)

# Crear movimientos de salida (cantidad negativa)
df_ventas['cantidad_mov'] = -df_ventas['cantidad']
df_ventas['costo_total'] = (df_ventas['costo_unitario'] * df_ventas['cantidad']).abs()

# Asignar almacenes rotativamente (simulando distribución)
almacenes = ['ALM_CENTRAL', 'TIENDA_01', 'TIENDA_02', 'TIENDA_03', 'TIENDA_04', 'TIENDA_05']
df_ventas['id_almacen'] = [almacenes[i % len(almacenes)] for i in range(len(df_ventas))]

# Asignar proveedores rotativamente
proveedores = ['PROV001', 'PROV002', 'PROV003']
df_ventas['id_proveedor'] = [proveedores[i % len(proveedores)] for i in range(len(df_ventas))]

# Tipo de movimiento: salida por venta
df_ventas['id_tipo_movimiento'] = 'MOV_SALIDA_VENTA'

# Usuario por defecto
df_ventas['id_usuario'] = 1

# Documento
df_ventas['numero_documento'] = 'VTA-' + df_ventas['orden_id'].astype(str).str.zfill(6)

# Observaciones
df_ventas['observaciones'] = 'Venta desde OroCommerce'
df_ventas['motivo'] = 'Venta de producto'

# Calcular stocks e insertar compras cuando sea necesario
print("   🔄 Calculando stocks e insertando compras automáticas...")
df_ventas = df_ventas.sort_values(['id_producto', 'id_almacen', 'fecha_movimiento']).reset_index(drop=True)

movimientos = []  # Lista para todos los movimientos (ventas + compras)
stocks = {}  # (producto, almacen) -> stock_actual
ultima_compra = {}  # (producto, almacen) -> fecha_ultima_compra
stock_inicial = 0  # Stock inicial en CERO
stock_maximo = 400  # Stock máximo permitido
umbral_compra_pct = 0.20  # Comprar cuando stock < 20% del máximo
umbral_compra = int(stock_maximo * umbral_compra_pct)  # 80 unidades
cantidad_compra_normal = 150  # Cantidad de compra regular
cantidad_compra_inicial = 250  # Primera compra más grande
cantidad_compra_preventiva = 100  # Cantidad de compra preventiva
dias_entre_compras = 20  # Comprar cada ~20 días si es posible
compra_counter = 1

for idx in df_ventas.index:
    producto = df_ventas.at[idx, 'id_producto']
    almacen = df_ventas.at[idx, 'id_almacen']
    cantidad_venta = abs(df_ventas.at[idx, 'cantidad_mov'])  # Positivo
    fecha = pd.to_datetime(df_ventas.at[idx, 'fecha_movimiento'])
    costo_unit = df_ventas.at[idx, 'costo_unitario']
    key = (producto, almacen)
    
    # Inicializar stock en CERO
    if key not in stocks:
        stocks[key] = stock_inicial
        ultima_compra[key] = None  # Nunca ha comprado
    
    stock_actual = stocks[key]
    
    # PRIMERA COMPRA: Si nunca ha comprado, hacer compra inicial
    if ultima_compra[key] is None:
        cantidad_a_comprar = cantidad_compra_inicial
        # Costo de compra = 40% del precio de venta (precio_unitario ya es sin IVA)
        costo_compra = df_ventas.at[idx, 'precio_unitario'] * 0.4
        
        movimientos.append({
            'id_producto': producto,
            'id_almacen': almacen,
            'id_proveedor': df_ventas.at[idx, 'id_proveedor'],
            'id_tipo_movimiento': 'MOV_ENTRADA',
            'fecha_movimiento': fecha,
            'id_usuario': 1,
            'numero_documento': f'COMP-{compra_counter:06d}',
            'cantidad': cantidad_a_comprar,
            'costo_unitario': costo_compra,
            'costo_total': cantidad_a_comprar * costo_compra,
            'stock_anterior': 0,  # CERO inicial
            'stock_resultante': cantidad_a_comprar,
            'motivo': 'Compra inicial',
            'observaciones': 'Primera compra - stock inicial'
        })
        
        stock_actual = cantidad_a_comprar
        stocks[key] = stock_actual
        ultima_compra[key] = fecha
        compra_counter += 1
    
    dias_desde_ultima_compra = (fecha - ultima_compra[key]).days if ultima_compra[key] else 999
    
    # COMPRA PERIÓDICA: Si han pasado más de X días y hay espacio en inventario
    if dias_desde_ultima_compra >= dias_entre_compras and stock_actual < (stock_maximo * 0.7):
        espacio_disponible = stock_maximo - stock_actual
        cantidad_a_comprar = min(cantidad_compra_normal, espacio_disponible)
        
        if cantidad_a_comprar >= 50:  # Solo comprar si vale la pena
            # Costo de compra = 40% del precio de venta (precio_unitario ya es sin IVA)
            costo_compra = df_ventas.at[idx, 'precio_unitario'] * 0.4
            
            movimientos.append({
                'id_producto': producto,
                'id_almacen': almacen,
                'id_proveedor': df_ventas.at[idx, 'id_proveedor'],
                'id_tipo_movimiento': 'MOV_ENTRADA',
                'fecha_movimiento': fecha,
                'id_usuario': 1,
                'numero_documento': f'COMP-{compra_counter:06d}',
                'cantidad': cantidad_a_comprar,
                'costo_unitario': costo_compra,
                'costo_total': cantidad_a_comprar * costo_compra,
                'stock_anterior': stock_actual,
                'stock_resultante': stock_actual + cantidad_a_comprar,
                'motivo': 'Compra periódica',
                'observaciones': f'Compra regular a proveedor'
            })
            
            stock_actual += cantidad_a_comprar
            stocks[key] = stock_actual
            ultima_compra[key] = fecha
            compra_counter += 1
    
    # COMPRA PREVENTIVA: Si el stock está bajo (< 20% del máximo)
    elif stock_actual < umbral_compra:
        espacio_disponible = stock_maximo - stock_actual
        cantidad_a_comprar = min(cantidad_compra_preventiva, espacio_disponible)
        
        if cantidad_a_comprar > 0:
            # Costo de compra = 40% del precio de venta (precio_unitario ya es sin IVA)
            costo_compra = df_ventas.at[idx, 'precio_unitario'] * 0.4
            
            movimientos.append({
                'id_producto': producto,
                'id_almacen': almacen,
                'id_proveedor': df_ventas.at[idx, 'id_proveedor'],
                'id_tipo_movimiento': 'MOV_ENTRADA',
                'fecha_movimiento': fecha,
                'id_usuario': 1,
                'numero_documento': f'COMP-{compra_counter:06d}',
                'cantidad': cantidad_a_comprar,
                'costo_unitario': costo_compra,
                'costo_total': cantidad_a_comprar * costo_compra,
                'stock_anterior': stock_actual,
                'stock_resultante': stock_actual + cantidad_a_comprar,
                'motivo': 'Reposición preventiva de stock',
                'observaciones': f'Stock bajo ({stock_actual} < {umbral_compra})'
            })
            
            stock_actual += cantidad_a_comprar
            stocks[key] = stock_actual
            ultima_compra[key] = fecha
            compra_counter += 1
    
    # COMPRA URGENTE: Si aún no alcanza para la venta
    if cantidad_venta > stock_actual:
        faltante = cantidad_venta - stock_actual + 50
        cantidad_urgente = min(faltante, stock_maximo - stock_actual)
        
        if cantidad_urgente > 0:
            # Costo de compra = 40% del precio de venta (precio_unitario ya es sin IVA)
            costo_compra = df_ventas.at[idx, 'precio_unitario'] * 0.4
            
            movimientos.append({
                'id_producto': producto,
                'id_almacen': almacen,
                'id_proveedor': df_ventas.at[idx, 'id_proveedor'],
                'id_tipo_movimiento': 'MOV_ENTRADA',
                'fecha_movimiento': fecha,
                'id_usuario': 1,
                'numero_documento': f'COMP-URG-{compra_counter:06d}',
                'cantidad': cantidad_urgente,
                'costo_unitario': costo_compra,
                'costo_total': cantidad_urgente * costo_compra,
                'stock_anterior': stock_actual,
                'stock_resultante': stock_actual + cantidad_urgente,
                'motivo': 'Compra urgente',
                'observaciones': f'Stock insuficiente para venta'
            })
            
            stock_actual += cantidad_urgente
            stocks[key] = stock_actual
            ultima_compra[key] = fecha
            compra_counter += 1
    
    # Registrar la VENTA
    cantidad_venta_real = min(cantidad_venta, stock_actual)
    stock_resultante = stock_actual - cantidad_venta_real
    
    movimientos.append({
        'id_producto': producto,
        'id_almacen': almacen,
        'id_proveedor': df_ventas.at[idx, 'id_proveedor'],
        'id_tipo_movimiento': 'MOV_SALIDA_VENTA',
        'fecha_movimiento': fecha,
        'id_usuario': df_ventas.at[idx, 'id_usuario'],
        'numero_documento': df_ventas.at[idx, 'numero_documento'],
        'cantidad': -cantidad_venta_real,
        'costo_unitario': costo_unit,
        'costo_total': cantidad_venta_real * costo_unit,
        'stock_anterior': stock_actual,
        'stock_resultante': stock_resultante,
        'motivo': df_ventas.at[idx, 'motivo'],
        'observaciones': df_ventas.at[idx, 'observaciones']
    })
    
    stocks[key] = stock_resultante

print(f"   ✓ Movimientos generados: {len(movimientos):,}")
print(f"   ✓ Stocks para {len(stocks)} combinaciones producto-almacén")
print(f"   💡 Stock inicial: {stock_inicial}, Compra inicial: {cantidad_compra_inicial}, Regular: {cantidad_compra_normal}")

# Convertir a DataFrame
df_ventas = pd.DataFrame(movimientos)

# Seleccionar columnas para el CSV
df_final = df_ventas[[
    'id_producto',
    'id_almacen',
    'id_proveedor',
    'id_tipo_movimiento',
    'fecha_movimiento',
    'id_usuario',
    'numero_documento',
    'cantidad',
    'costo_unitario',
    'costo_total',
    'stock_anterior',
    'stock_resultante',
    'motivo',
    'observaciones'
]].copy()

# Guardar CSV
output_path = 'data/inputs/inventario/movimientos_inventario.csv'
df_final.to_csv(output_path, index=False)

print(f"   ✅ CSV generado: {output_path}")
print(f"   📊 Total movimientos: {len(df_final):,}")
print(f"   📦 Productos únicos: {df_final['id_producto'].nunique()}")
print(f"   🏪 Almacenes: {df_final['id_almacen'].nunique()}")

oro_conn.close()
dw_conn.close()
