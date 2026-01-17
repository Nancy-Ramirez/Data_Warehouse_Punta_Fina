#!/usr/bin/env python3
"""
Script para listar estructura de tablas en datawarehouse_bi
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_table_structure():
    conn = psycopg2.connect(
        host=os.getenv("DW_DB_HOST"),
        port=int(os.getenv("DW_DB_PORT")),
        dbname=os.getenv("DW_DB_NAME"),
        user=os.getenv("DW_DB_USER"),
        password=os.getenv("DW_DB_PASS")
    )
    
    cursor = conn.cursor()
    
    # Obtener todas las tablas
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    
    tables = cursor.fetchall()
    
    print("\n" + "="*80)
    print("TABLAS EN datawarehouse_bi")
    print("="*80 + "\n")
    
    for (table_name,) in tables:
        print(f"\n{'='*80}")
        print(f"TABLA: {table_name}")
        print('='*80)
        
        # Obtener columnas
        cursor.execute("""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        
        columns = cursor.fetchall()
        
        print(f"\n{'Columna':<30} {'Tipo':<25} {'Nulo':<6} {'Default'}")
        print("-" * 80)
        
        for col in columns:
            col_name, data_type, char_len, num_prec, num_scale, nullable, default = col
            
            # Formatear tipo de dato
            if data_type in ('character varying', 'varchar'):
                tipo = f"VARCHAR({char_len})" if char_len else "VARCHAR"
            elif data_type == 'numeric':
                if num_prec and num_scale:
                    tipo = f"NUMERIC({num_prec},{num_scale})"
                else:
                    tipo = "NUMERIC"
            elif data_type == 'integer':
                tipo = "INT"
            elif data_type == 'timestamp without time zone':
                tipo = "TIMESTAMP"
            elif data_type == 'date':
                tipo = "DATE"
            elif data_type == 'boolean':
                tipo = "BOOLEAN"
            elif data_type == 'text':
                tipo = "TEXT"
            elif data_type == 'double precision':
                tipo = "DOUBLE PRECISION"
            else:
                tipo = data_type.upper()
            
            # Formatear default
            default_str = str(default)[:30] if default else ""
            
            print(f"{col_name:<30} {tipo:<25} {nullable:<6} {default_str}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    get_table_structure()
