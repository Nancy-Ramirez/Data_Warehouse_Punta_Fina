#!/usr/bin/env python3
"""
Script maestro para ejecutar ETL completo con limpieza y estructura
"""

import sys
import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

print("\n" + "=" * 80)
print("🚀 ETL COMPLETO - PUNTAFINA DATA WAREHOUSE")
print("=" * 80)

# ============================================================================
# PASO 1: LIMPIEZA DE TABLAS AFECTADAS
# ============================================================================
print("\n📋 PASO 1: Limpiando tablas afectadas...")
print("-" * 80)

try:
    conn = psycopg2.connect(
        host=os.getenv("DW_DB_HOST"),
        port=int(os.getenv("DW_DB_PORT")),
        dbname=os.getenv("DW_DB_NAME"),
        user=os.getenv("DW_DB_USER"),
        password=os.getenv("DW_DB_PASS"),
    )

    cur = conn.cursor()

    # NO truncar dim_impuestos y dim_promocion - ya están correctamente pobladas
    print("   ℹ️  Saltando limpieza de dim_impuestos y dim_promocion (ya pobladas)")

    # Truncar fact_ventas
    print("   🗑️  Truncando fact_ventas...")
    cur.execute("TRUNCATE TABLE fact_ventas CASCADE")
    conn.commit()
    print("      ✓ fact_ventas limpiada")

    cur.close()
    conn.close()

    print("   ✅ Limpieza completada exitosamente")

except Exception as e:
    print(f"   ❌ Error en limpieza: {e}")
    sys.exit(1)

# ============================================================================
# PASO 2: VERIFICAR/CREAR ESTRUCTURA DE TABLAS
# ============================================================================
print("\n📋 PASO 2: Verificando estructura de tablas...")
print("-" * 80)

try:
    conn = psycopg2.connect(
        host=os.getenv("DW_DB_HOST"),
        port=int(os.getenv("DW_DB_PORT")),
        dbname=os.getenv("DW_DB_NAME"),
        user=os.getenv("DW_DB_USER"),
        password=os.getenv("DW_DB_PASS"),
    )

    cur = conn.cursor()

    # Verificar si sk_promocion existe en fact_ventas
    cur.execute(
        """
        SELECT COUNT(*) 
        FROM information_schema.columns 
        WHERE table_name='fact_ventas' AND column_name='sk_promocion'
    """
    )

    if cur.fetchone()[0] == 0:
        print("   ➕ Agregando columna sk_promocion a fact_ventas...")
        cur.execute("ALTER TABLE fact_ventas ADD COLUMN sk_promocion INTEGER DEFAULT 1")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_fact_ventas_sk_promocion ON fact_ventas(sk_promocion)"
        )
        conn.commit()
        print("      ✓ Columna sk_promocion agregada")
    else:
        print("   ✓ Columna sk_promocion ya existe")

    cur.close()
    conn.close()

    print("   ✅ Estructura verificada")

except Exception as e:
    print(f"   ❌ Error verificando estructura: {e}")
    sys.exit(1)

# ============================================================================
# PASO 3: EJECUTAR ETL PRINCIPAL
# ============================================================================
print("\n📋 PASO 3: Ejecutando ETL principal...")
print("-" * 80)

# Cambiar al directorio etl_batch
os.chdir(Path(__file__).parent)

# Ejecutar el ETL usando el main.py
import subprocess

result = subprocess.run(
    [sys.executable, "main.py", "run"], capture_output=True, text=True
)

if result.returncode != 0:
    print(f"   ❌ Error ejecutando ETL:")
    print(result.stderr)
    sys.exit(1)

print(result.stdout)

# ============================================================================
# PASO 4: VERIFICACIÓN FINAL
# ============================================================================
print("\n📋 PASO 4: Verificación final de resultados...")
print("-" * 80)

try:
    conn = psycopg2.connect(
        host=os.getenv("DW_DB_HOST"),
        port=int(os.getenv("DW_DB_PORT")),
        dbname=os.getenv("DW_DB_NAME"),
        user=os.getenv("DW_DB_USER"),
        password=os.getenv("DW_DB_PASS"),
    )

    cur = conn.cursor()

    # Verificar dim_impuestos
    cur.execute(
        "SELECT COUNT(*), STRING_AGG(codigo, ', ' ORDER BY impuesto_id) FROM dim_impuestos"
    )
    count, codigos = cur.fetchone()
    print(f"\n   📊 dim_impuestos: {count} registros")
    print(f"      Códigos: {codigos}")

    if count != 3:
        print(f"      ⚠️  ADVERTENCIA: Se esperaban 3 registros, se encontraron {count}")
    else:
        print("      ✓ Cantidad correcta")

    # Verificar dim_promocion
    cur.execute(
        "SELECT COUNT(*), MIN(sk_promocion), MAX(sk_promocion) FROM dim_promocion"
    )
    count, min_id, max_id = cur.fetchone()
    print(f"\n   📊 dim_promocion: {count} registros")
    print(f"      Rango SKs: {min_id} - {max_id}")

    # Verificar si existe "Sin Promoción"
    cur.execute(
        "SELECT COUNT(*) FROM dim_promocion WHERE nombre_promocion LIKE '%Sin Promoción%'"
    )
    sin_promo = cur.fetchone()[0]

    if sin_promo > 0:
        print('      ✓ Registro "Sin Promoción" encontrado')
    else:
        print('      ⚠️  ADVERTENCIA: No se encontró "Sin Promoción"')

    # Verificar fact_ventas
    cur.execute("SELECT COUNT(*) FROM fact_ventas")
    count = cur.fetchone()[0]
    print(f"\n   📊 fact_ventas: {count:,} registros")

    if count > 0:
        # Distribución de promociones
        cur.execute(
            """
            SELECT sk_promocion, COUNT(*), ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(), 2) 
            FROM fact_ventas 
            GROUP BY sk_promocion 
            ORDER BY COUNT(*) DESC 
            LIMIT 5
        """
        )

        print("      Distribución de promociones:")
        for sk, cnt, pct in cur.fetchall():
            print(f"        SK {sk}: {cnt:,} ({pct}%)")

        # Distribución de impuestos
        cur.execute(
            """
            SELECT impuesto_id, COUNT(*), ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(), 2) 
            FROM fact_ventas 
            GROUP BY impuesto_id 
            ORDER BY COUNT(*) DESC
        """
        )

        print("      Distribución de impuestos:")
        for imp_id, cnt, pct in cur.fetchall():
            print(f"        impuesto_id={imp_id}: {cnt:,} registros ({pct}%)")

        # Descuentos
        cur.execute(
            """
            SELECT 
                COUNT(*) FILTER (WHERE descuento > 0) as con_descuento,
                ROUND(AVG(descuento), 2) as promedio,
                ROUND(SUM(descuento), 2) as total
            FROM fact_ventas
        """
        )

        con_desc, prom, total = cur.fetchone()
        print(f"      Descuentos:")
        print(f"        Con descuento: {con_desc:,} ({100*con_desc/count:.2f}%)")
        print(f"        Promedio: ${float(prom):.2f}")
        print(f"        Total: ${float(total):,.2f}")

        print("      ✓ fact_ventas cargada correctamente")
    else:
        print("      ⚠️  ADVERTENCIA: fact_ventas está vacía")

    cur.close()
    conn.close()

    print("\n" + "=" * 80)
    print("✅ ETL COMPLETADO EXITOSAMENTE")
    print("=" * 80)

except Exception as e:
    print(f"\n   ❌ Error en verificación: {e}")
    sys.exit(1)
