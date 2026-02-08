#!/usr/bin/env python3
"""Script para agregar la columna venta_id a fact_ventas"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# Conectar al DW
conn = psycopg2.connect(
    host=os.getenv("DW_DB_HOST"),
    port=int(os.getenv("DW_DB_PORT")),
    dbname=os.getenv("DW_DB_NAME"),
    user=os.getenv("DW_DB_USER"),
    password=os.getenv("DW_DB_PASS"),
)

cursor = conn.cursor()

print("Agregando columna venta_id a fact_ventas...")

try:
    # Agregar la columna venta_id como INTEGER
    cursor.execute("""
        ALTER TABLE fact_ventas 
        ADD COLUMN IF NOT EXISTS venta_id INTEGER;
    """)
    
    conn.commit()
    print("✅ Columna venta_id agregada exitosamente")
    
except Exception as e:
    print(f"❌ Error: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
