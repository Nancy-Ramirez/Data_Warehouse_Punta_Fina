#!/bin/bash

# Script de limpieza del repositorio
# Elimina archivos temporales, logs y cache

echo "🧹 Limpiando repositorio..."

# Eliminar archivos de log
echo "  📝 Eliminando logs..."
find . -name "*.log" -type f -delete

# Eliminar cache de Python
echo "  🐍 Eliminando __pycache__..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null

# Eliminar archivos backup
echo "  💾 Eliminando backups..."
find . -name "*.backup" -type f -delete
find . -name "*.bak" -type f -delete

# Eliminar archivos temporales
echo "  📄 Eliminando temporales..."
rm -f .DS_Store
rm -f temp_*
rm -f *_temp.py
rm -f db_structure.txt
rm -f temp_tables.csv

# Eliminar outputs (mantener estructura)
echo "  📊 Limpiando outputs..."
rm -f data/outputs/csv/*.csv 2>/dev/null
rm -f data/outputs/parquet/*.parquet 2>/dev/null

echo "✅ Limpieza completada!"
