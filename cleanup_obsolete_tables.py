#!/usr/bin/env python3
"""
CLEANUP OBSOLETE TABLES - Eliminar dimensiones obsoletas
=========================================================
Elimina tablas de dimensiones que ya no forman parte del modelo
"""

import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path
import logging

# Cargar variables de entorno
env_file = Path(__file__).parent.parent / ".env"
if env_file.exists():
    load_dotenv(env_file)

logger = logging.getLogger(__name__)

# Dimensiones obsoletas que deben ser eliminadas
OBSOLETE_DIMENSIONS = [
    'dim_sitio_web',
    'dim_canal',
    'dim_direccion',
    'dim_envio',
    'dim_pago',
    'dim_promocion',
    'dim_line_item',
    'dim_estado_orden',
    'dim_estado_pago',
    'dim_periodo_contable',
    'dim_categoria_producto',
]

def cleanup_obsolete_tables():
    """Eliminar tablas obsoletas del data warehouse"""
    
    print("🧹 Limpiando dimensiones obsoletas...")
    
    try:
        conn = psycopg2.connect(
            host=os.getenv("DW_DB_HOST"),
            port=int(os.getenv("DW_DB_PORT")),
            dbname=os.getenv("DW_DB_NAME"),
            user=os.getenv("DW_DB_USER"),
            password=os.getenv("DW_DB_PASS"),
            connect_timeout=120,
            options="-c statement_timeout=1800000"
        )
        
        cursor = conn.cursor()
        
        for table in OBSOLETE_DIMENSIONS:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                print(f"   ✓ Eliminada: {table}")
            except Exception as e:
                print(f"   ⚠️  Error eliminando {table}: {e}")
        
        conn.commit()
        
        # Verificar tablas restantes
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
                AND table_name LIKE 'dim_%'
            ORDER BY table_name
        """)
        
        remaining_dims = [row[0] for row in cursor.fetchall()]
        
        print(f"\n📊 Dimensiones activas ({len(remaining_dims)}):")
        for dim in remaining_dims:
            cursor.execute(f"SELECT COUNT(*) FROM {dim}")
            count = cursor.fetchone()[0]
            print(f"   • {dim}: {count:,} registros")
        
        cursor.close()
        conn.close()
        
        print("\n✅ Limpieza completada")
        return True
        
    except Exception as e:
        print(f"\n❌ Error durante limpieza: {e}")
        return False

if __name__ == "__main__":
    cleanup_obsolete_tables()
