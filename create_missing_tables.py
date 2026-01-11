#!/usr/bin/env python3
"""
Script para crear tablas faltantes en el DW
"""
import os
import psycopg2
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# SQL para crear tablas
CREATE_TABLES_SQL = """
-- Tabla dim_envio
CREATE TABLE IF NOT EXISTS dim_envio (
    envio_id SERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE,
    nombre VARCHAR(200),
    descripcion TEXT,
    activo BOOLEAN DEFAULT TRUE
);

-- Tabla dim_pago
CREATE TABLE IF NOT EXISTS dim_pago (
    pago_id SERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE,
    nombre VARCHAR(200),
    descripcion TEXT,
    activo BOOLEAN DEFAULT TRUE
);

-- Tabla dim_direccion
CREATE TABLE IF NOT EXISTS dim_direccion (
    direccion_id SERIAL PRIMARY KEY,
    direccion_externo_id INTEGER,
    calle VARCHAR(500),
    ciudad VARCHAR(100),
    departamento VARCHAR(100),
    codigo_postal VARCHAR(20),
    pais VARCHAR(100)
);

-- Tabla dim_estado_orden
CREATE TABLE IF NOT EXISTS dim_estado_orden (
    estado_orden_id SERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE,
    nombre VARCHAR(200),
    descripcion TEXT,
    orden_proceso INTEGER
);

-- Tabla dim_estado_pago
CREATE TABLE IF NOT EXISTS dim_estado_pago (
    estado_pago_id SERIAL PRIMARY KEY,
    codigo VARCHAR(50) UNIQUE,
    nombre VARCHAR(200),
    descripcion TEXT
);

-- Tabla dim_canal
CREATE TABLE IF NOT EXISTS dim_canal (
    canal_id SERIAL PRIMARY KEY,
    canal_externo_id INTEGER,
    nombre VARCHAR(200),
    tipo VARCHAR(100),
    activo BOOLEAN DEFAULT TRUE
);

-- Tabla dim_line_item
CREATE TABLE IF NOT EXISTS dim_line_item (
    line_item_id SERIAL PRIMARY KEY,
    line_item_externo_id INTEGER,
    producto_nombre VARCHAR(500),
    cantidad DECIMAL(10,2),
    precio_unitario DECIMAL(15,2)
);

-- Tabla dim_categoria_producto
CREATE TABLE IF NOT EXISTS dim_categoria_producto (
    categoria_id SERIAL PRIMARY KEY,
    nombre VARCHAR(200),
    descripcion TEXT,
    categoria_padre_id INTEGER
);

-- Tabla dim_periodo_contable
CREATE TABLE IF NOT EXISTS dim_periodo_contable (
    periodo_id SERIAL PRIMARY KEY,
    anio INTEGER,
    mes INTEGER,
    trimestre INTEGER,
    nombre VARCHAR(50)
);
"""

def main():
    print("🔧 Creando tablas faltantes en el DW...")
    
    try:
        # Conectar al DW
        conn = psycopg2.connect(
            host=os.getenv("DW_DB_HOST"),
            port=int(os.getenv("DW_DB_PORT")),
            dbname=os.getenv("DW_DB_NAME"),
            user=os.getenv("DW_DB_USER"),
            password=os.getenv("DW_DB_PASS")
        )
        
        cursor = conn.cursor()
        
        # Ejecutar SQL
        cursor.execute(CREATE_TABLES_SQL)
        conn.commit()
        
        print("✅ Tablas creadas exitosamente:")
        print("   - dim_envio")
        print("   - dim_pago")
        print("   - dim_direccion")
        print("   - dim_estado_orden")
        print("   - dim_estado_pago")
        print("   - dim_canal")
        print("   - dim_line_item")
        print("   - dim_categoria_producto")
        print("   - dim_periodo_contable")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
