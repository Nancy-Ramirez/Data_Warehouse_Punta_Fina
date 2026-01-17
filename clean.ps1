# Script de limpieza del repositorio (PowerShell)
# Elimina archivos temporales, logs y cache

Write-Host "🧹 Limpiando repositorio..." -ForegroundColor Cyan

# Eliminar archivos de log
Write-Host "  📝 Eliminando logs..." -ForegroundColor Yellow
Get-ChildItem -Path . -Recurse -Filter "*.log" -File | Remove-Item -Force

# Eliminar cache de Python
Write-Host "  🐍 Eliminando __pycache__..." -ForegroundColor Yellow
Get-ChildItem -Path . -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force

# Eliminar archivos backup
Write-Host "  💾 Eliminando backups..." -ForegroundColor Yellow
Get-ChildItem -Path . -Recurse -Include "*.backup","*.bak" -File | Remove-Item -Force

# Eliminar archivos temporales
Write-Host "  📄 Eliminando temporales..." -ForegroundColor Yellow
Remove-Item -Path ".DS_Store","temp_*","*_temp.py","db_structure.txt","temp_tables.csv" -Force -ErrorAction SilentlyContinue

# Eliminar outputs (mantener estructura)
Write-Host "  📊 Limpiando outputs..." -ForegroundColor Yellow
Get-ChildItem -Path "data\outputs\csv" -Filter "*.csv" -File -ErrorAction SilentlyContinue | Remove-Item -Force
Get-ChildItem -Path "data\outputs\parquet" -Filter "*.parquet" -File -ErrorAction SilentlyContinue | Remove-Item -Force

Write-Host "✅ Limpieza completada!" -ForegroundColor Green
