import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DW_DB_HOST"),
    port=os.getenv("DW_DB_PORT"),
    database=os.getenv("DW_DB_NAME"),
    user=os.getenv("DW_DB_USER"),
    password=os.getenv("DW_DB_PASS")
)

print("\n" + "=" * 80)
print("📊 EXPORTANDO DIMENSIONES Y HECHOS A EXCEL")
print("=" * 80)

# Nombre del archivo
filename = f"DataWarehouse_Completo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

# Crear el writer de Excel
with pd.ExcelWriter(filename, engine='openpyxl') as writer:
    
    # 1. dim_producto
    print("\n📦 Exportando dim_producto...")
    df = pd.read_sql_query("SELECT * FROM dim_producto ORDER BY producto_id", conn)
    df.to_excel(writer, sheet_name='dim_producto', index=False)
    print(f"   ✓ {len(df):,} productos")
    
    # 2. dim_cliente
    print("👥 Exportando dim_cliente...")
    df = pd.read_sql_query("SELECT * FROM dim_cliente ORDER BY cliente_id", conn)
    df.to_excel(writer, sheet_name='dim_cliente', index=False)
    print(f"   ✓ {len(df):,} clientes")
    
    # 3. dim_usuario
    print("👤 Exportando dim_usuario...")
    df = pd.read_sql_query("SELECT * FROM dim_usuario ORDER BY usuario_id", conn)
    df.to_excel(writer, sheet_name='dim_usuario', index=False)
    print(f"   ✓ {len(df):,} usuarios")
    
    # 4. dim_fecha
    print("📅 Exportando dim_fecha...")
    df = pd.read_sql_query("SELECT * FROM dim_fecha ORDER BY fecha_id", conn)
    df.to_excel(writer, sheet_name='dim_fecha', index=False)
    print(f"   ✓ {len(df):,} fechas")
    
    # 5. dim_orden
    print("📋 Exportando dim_orden...")
    df = pd.read_sql_query("SELECT * FROM dim_orden ORDER BY orden_id", conn)
    df.to_excel(writer, sheet_name='dim_orden', index=False)
    print(f"   ✓ {len(df):,} órdenes")
    
    # 6. dim_almacen
    print("🏢 Exportando dim_almacen...")
    df = pd.read_sql_query("SELECT * FROM dim_almacen ORDER BY almacen_id", conn)
    df.to_excel(writer, sheet_name='dim_almacen', index=False)
    print(f"   ✓ {len(df):,} almacenes")
    
    # 7. dim_cuenta_contable
    print("💰 Exportando dim_cuenta_contable...")
    df = pd.read_sql_query("SELECT * FROM dim_cuenta_contable ORDER BY cuenta_id", conn)
    df.to_excel(writer, sheet_name='dim_cuenta_contable', index=False)
    print(f"   ✓ {len(df):,} cuentas")
    
    # 8. dim_centro_costo
    print("🏗️ Exportando dim_centro_costo...")
    df = pd.read_sql_query("SELECT * FROM dim_centro_costo ORDER BY centro_costo_id", conn)
    df.to_excel(writer, sheet_name='dim_centro_costo', index=False)
    print(f"   ✓ {len(df):,} centros de costo")
    
    # 9. dim_promocion
    print("🎁 Exportando dim_promocion...")
    df = pd.read_sql_query("SELECT * FROM dim_promocion ORDER BY sk_promocion", conn)
    df.to_excel(writer, sheet_name='dim_promocion', index=False)
    print(f"   ✓ {len(df):,} promociones")
    
    # 10. dim_proveedor
    print("🚚 Exportando dim_proveedor...")
    df = pd.read_sql_query("SELECT * FROM dim_proveedor ORDER BY proveedor_id", conn)
    df.to_excel(writer, sheet_name='dim_proveedor', index=False)
    print(f"   ✓ {len(df):,} proveedores")
    
    # 11. dim_tipo_movimiento
    print("🔄 Exportando dim_tipo_movimiento...")
    df = pd.read_sql_query("SELECT * FROM dim_tipo_movimiento ORDER BY tipo_movimiento_id", conn)
    df.to_excel(writer, sheet_name='dim_tipo_movimiento', index=False)
    print(f"   ✓ {len(df):,} tipos de movimiento")
    
    # 12. dim_tipo_transaccion
    print("💳 Exportando dim_tipo_transaccion...")
    df = pd.read_sql_query("SELECT * FROM dim_tipo_transaccion ORDER BY tipo_transaccion_id", conn)
    df.to_excel(writer, sheet_name='dim_tipo_transaccion', index=False)
    print(f"   ✓ {len(df):,} tipos de transacción")
    
    # 13. dim_impuestos
    print("📊 Exportando dim_impuestos...")
    df = pd.read_sql_query("SELECT * FROM dim_impuestos ORDER BY impuesto_id", conn)
    df.to_excel(writer, sheet_name='dim_impuestos', index=False)
    print(f"   ✓ {len(df):,} impuestos")
    
    # TABLAS DE HECHOS
    print("\n" + "=" * 80)
    print("📈 EXPORTANDO TABLAS DE HECHOS")
    print("=" * 80)
    
    # 1. fact_ventas
    print("\n💰 Exportando fact_ventas...")
    df = pd.read_sql_query("SELECT * FROM fact_ventas ORDER BY fecha_id, orden_id", conn)
    df.to_excel(writer, sheet_name='fact_ventas', index=False)
    print(f"   ✓ {len(df):,} líneas de venta")
    
    # 2. fact_transacciones
    print("📒 Exportando fact_transacciones...")
    df = pd.read_sql_query("SELECT * FROM fact_transacciones ORDER BY fecha_id, numero_asiento", conn)
    df.to_excel(writer, sheet_name='fact_transacciones', index=False)
    print(f"   ✓ {len(df):,} asientos contables")
    
    # 3. fact_balance
    print("⚖️ Exportando fact_balance...")
    df = pd.read_sql_query("SELECT * FROM fact_balance ORDER BY periodo_id, cuenta_id", conn)
    df.to_excel(writer, sheet_name='fact_balance', index=False)
    print(f"   ✓ {len(df):,} balances")
    
    # 4. fact_inventario
    print("📦 Exportando fact_inventario...")
    df = pd.read_sql_query("SELECT * FROM fact_inventario ORDER BY fecha_id, producto_id", conn)
    df.to_excel(writer, sheet_name='fact_inventario', index=False)
    print(f"   ✓ {len(df):,} movimientos")
    
    # 5. fact_estado_resultados
    print("📊 Exportando fact_estado_resultados...")
    df = pd.read_sql_query("SELECT * FROM fact_estado_resultados ORDER BY periodo_id", conn)
    df.to_excel(writer, sheet_name='fact_estado_resultados', index=False)
    print(f"   ✓ {len(df):,} períodos")
    
    # Resumen
    print("\n📄 Creando hoja de resumen...")
    cur = conn.cursor()
    resumen_data = []
    
    # Dimensiones
    dimensiones = [
        'dim_producto', 'dim_cliente', 'dim_usuario', 'dim_fecha',
        'dim_orden', 'dim_almacen', 'dim_cuenta_contable', 'dim_centro_costo',
        'dim_promocion', 'dim_proveedor', 'dim_tipo_movimiento',
        'dim_tipo_transaccion', 'dim_impuestos'
    ]
    
    for dim in dimensiones:
        cur.execute(f"SELECT COUNT(*) FROM {dim}")
        count = cur.fetchone()[0]
        resumen_data.append({'Tipo': 'Dimensión', 'Tabla': dim, 'Registros': count})
    
    # Hechos
    hechos = [
        'fact_ventas', 'fact_transacciones', 'fact_balance', 
        'fact_inventario', 'fact_estado_resultados'
    ]
    
    for fact in hechos:
        cur.execute(f"SELECT COUNT(*) FROM {fact}")
        count = cur.fetchone()[0]
        resumen_data.append({'Tipo': 'Hecho', 'Tabla': fact, 'Registros': count})
    
    df_resumen = pd.DataFrame(resumen_data)
    df_resumen.to_excel(writer, sheet_name='Resumen', index=False)
    print(f"   ✓ Resumen creado")

conn.close()

print("\n" + "=" * 80)
print(f"✅ ARCHIVO CREADO: {filename}")
print("=" * 80)
print(f"\n📁 Ubicación: {os.path.abspath(filename)}")
print(f"📊 Total hojas: 19 (13 dimensiones + 5 hechos + 1 resumen)")
