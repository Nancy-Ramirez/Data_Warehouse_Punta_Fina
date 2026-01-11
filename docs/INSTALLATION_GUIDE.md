# 🚀 GUÍA DE INSTALACIÓN Y CONFIGURACIÓN
## PuntaFina ETL Batch - Sistema Optimizado para Ubuntu 22.04

---

## 📋 Tabla de Contenidos

- [Requisitos del Sistema](#requisitos-del-sistema)
- [Instalación Rápida](#instalación-rápida)
- [Instalación Manual](#instalación-manual)
- [Configuración](#configuración)
- [Verificación](#verificación)
- [Solución de Problemas](#solución-de-problemas)

---

## 🖥️ Requisitos del Sistema

### Requisitos Mínimos

- **Sistema Operativo**: Ubuntu 22.04 LTS
- **Procesador**: 2 cores (64-bit)
- **Memoria RAM**: 2 GB
- **Espacio en Disco**: 5 GB libres
- **Python**: 3.10 o superior
- **PostgreSQL**: 12 o superior

### Requisitos Recomendados

- **Procesador**: 4+ cores
- **Memoria RAM**: 8+ GB
- **Espacio en Disco**: 20+ GB
- **Red**: Conexión estable a base de datos

---

## ⚡ Instalación Rápida

### Opción 1: Script Automático (Recomendado)

```bash
# 1. Clonar o descargar el proyecto
cd /path/to/PuntaFina_DW_Oro-main

# 2. Dar permisos de ejecución al script
chmod +x etl_batch/install.sh

# 3. Ejecutar instalación
./etl_batch/install.sh
```

El script automático realizará:
- ✅ Verificación de requisitos del sistema
- ✅ Actualización de paquetes del sistema
- ✅ Instalación de dependencias
- ✅ Configuración del entorno virtual Python
- ✅ Creación de estructura de directorios
- ✅ Configuración de archivos de entorno
- ✅ Verificación de conexiones

---

## 🔧 Instalación Manual

### Paso 1: Actualizar Sistema

```bash
sudo apt-get update
sudo apt-get upgrade -y
```

### Paso 2: Instalar Dependencias del Sistema

```bash
sudo apt-get install -y \
    postgresql-client \
    python3.10 \
    python3.10-venv \
    python3-pip \
    libpq-dev \
    build-essential \
    git \
    curl \
    wget
```

### Paso 3: Crear Entorno Virtual Python

```bash
# Navegar al directorio del proyecto
cd /path/to/PuntaFina_DW_Oro-main

# Crear entorno virtual
python3 -m venv venv

# Activar entorno virtual
source venv/bin/activate
```

### Paso 4: Instalar Dependencias Python

```bash
# Actualizar pip
pip install --upgrade pip setuptools wheel

# Instalar dependencias
pip install -r etl_batch/requirements.txt
```

### Paso 5: Crear Estructura de Directorios

```bash
# Crear directorios necesarios
mkdir -p etl_batch/{config,core,extractors,transformers,loaders,utils}
mkdir -p data/{inputs,outputs,staging,checkpoints}/{ventas,inventario,finanzas}
mkdir -p logs/{etl,audit,errors}
```

### Paso 6: Configurar Archivo .env

```bash
# Copiar plantilla de .env
cp etl_batch/.env.example etl_batch/.env

# Editar con credenciales reales
nano etl_batch/.env
```

Configurar las siguientes variables:

```bash
# Base de datos OroCommerce (FUENTE)
ORO_DB_HOST=localhost
ORO_DB_PORT=5432
ORO_DB_NAME=oro_commerce
ORO_DB_USER=oro_user
ORO_DB_PASS=tu_contraseña_aqui

# Base de datos OroCRM (FUENTE)
CRM_DB_HOST=localhost
CRM_DB_PORT=5432
CRM_DB_NAME=oro_crm
CRM_DB_USER=oro_user
CRM_DB_PASS=tu_contraseña_aqui

# Base de datos Data Warehouse (DESTINO)
DW_ORO_DB_HOST=localhost
DW_ORO_DB_PORT=5432
DW_ORO_DB_NAME=DW_oro
DW_ORO_DB_USER=dw_user
DW_ORO_DB_PASS=tu_contraseña_aqui

# Configuración de ETL
ETL_BATCH_SIZE=1000
ETL_MAX_WORKERS=4
ETL_LOG_LEVEL=INFO
```

### Paso 7: Configurar PostgreSQL

```bash
# Crear base de datos Data Warehouse
sudo -u postgres psql -c "CREATE DATABASE DW_oro;"

# Crear usuario con permisos
sudo -u postgres psql -c "CREATE USER dw_user WITH PASSWORD 'tu_contraseña';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE DW_oro TO dw_user;"
```

---

## ⚙️ Configuración

### Configuración del ETL

Editar `etl_batch/config/etl_config.yaml`:

```yaml
# Tamaño de lote
batch:
  chunk_size: 1000  # Ajustar según memoria disponible
  max_workers: 4    # Ajustar según cores del CPU
  
# Validación de datos
data_validation:
  check_referential_integrity: true
  auto_populate_missing: true
  enforce_symmetry: true
```

### Configuración de Logs

```yaml
monitoring:
  log_level: "INFO"  # DEBUG para más detalle
  log_format: "json"
```

### Configuración de Carga

```yaml
loading:
  strategy: "truncate_and_load"  # o "incremental" o "upsert"
  insert_batch_size: 500
  use_copy: true
```

---

## ✅ Verificación

### Verificar Instalación

```bash
# Activar entorno virtual
source venv/bin/activate

# Verificar Python y paquetes
python --version
python -c "import pandas, psycopg2, yaml; print('✓ Paquetes OK')"

# Verificar estructura
ls -la etl_batch/
ls -la data/
```

### Verificar Conexión a Base de Datos

```bash
# Probar conexión a Data Warehouse
PGPASSWORD='tu_contraseña' psql \
  -h localhost \
  -p 5432 \
  -U dw_user \
  -d DW_oro \
  -c "SELECT version();"
```

### Ejecutar Validación del Sistema

```bash
cd etl_batch
python main.py validate
```

---

## 🏃 Ejecutar ETL

### Primera Ejecución (Setup)

```bash
# Activar entorno virtual
source venv/bin/activate

# Navegar al directorio ETL
cd etl_batch

# Ejecutar setup inicial
python main.py setup
```

### Ejecución Normal

```bash
# Ejecutar proceso ETL completo
python main.py run
```

### Ejecución con Configuración Personalizada

```bash
# Usar archivo de configuración específico
python main.py run --config /path/to/custom_config.yaml
```

### Monitoreo en Tiempo Real

```bash
# Ver logs en tiempo real
tail -f logs/etl/ETLOrchestrator_$(date +%Y%m%d).log
```

---

## 🔄 Configurar Ejecución Automática

### Opción 1: Cron Job

```bash
# Editar crontab
crontab -e

# Agregar ejecución diaria a las 2 AM
0 2 * * * cd /path/to/PuntaFina_DW_Oro-main/etl_batch && /path/to/venv/bin/python main.py run >> /path/to/logs/cron.log 2>&1
```

### Opción 2: Systemd Service

```bash
# Crear servicio
sudo nano /etc/systemd/system/puntafina-etl.service

# Contenido del servicio (ver install.sh)

# Habilitar e iniciar
sudo systemctl enable puntafina-etl
sudo systemctl start puntafina-etl

# Ver estado
sudo systemctl status puntafina-etl

# Ver logs
sudo journalctl -u puntafina-etl -f
```

---

## 🐛 Solución de Problemas

### Error: "ModuleNotFoundError"

```bash
# Asegurar que el entorno virtual está activado
source venv/bin/activate

# Reinstalar dependencias
pip install -r etl_batch/requirements.txt
```

### Error: "Connection refused" (PostgreSQL)

```bash
# Verificar que PostgreSQL está corriendo
sudo systemctl status postgresql

# Iniciar PostgreSQL si está detenido
sudo systemctl start postgresql

# Verificar que el puerto está escuchando
sudo netstat -tlnp | grep 5432
```

### Error: "Permission denied"

```bash
# Dar permisos a scripts
chmod +x etl_batch/*.sh
chmod +x etl_batch/main.py

# Verificar permisos de directorios
chmod -R 755 etl_batch/
chmod -R 755 data/
chmod -R 755 logs/
```

### Error: "Out of memory"

```yaml
# Reducir tamaño de lote en etl_config.yaml
batch:
  chunk_size: 500  # Reducir de 1000 a 500
  max_workers: 2   # Reducir workers
  max_memory_mb: 256  # Limitar memoria por worker
```

### Error: "Checkpoint not found"

```bash
# Limpiar checkpoints obsoletos
rm -rf data/checkpoints/*.checkpoint

# Reiniciar proceso
python main.py run
```

### Logs con Errores

```bash
# Ver logs de error
cat logs/errors/*.log

# Ver últimas 100 líneas de log de ETL
tail -n 100 logs/etl/ETLOrchestrator_*.log

# Buscar errores específicos
grep -r "ERROR" logs/
```

---

## 📊 Monitoreo de Performance

### Ver Uso de Recursos

```bash
# Ver procesos Python activos
ps aux | grep python

# Monitorear uso de CPU y memoria
htop

# Ver uso de disco
df -h
du -sh data/
```

### Métricas del ETL

Los logs incluyen métricas automáticas:
- Registros procesados por segundo
- Tiempo de ejecución por fase
- Uso de memoria por lote
- Tasa de éxito/error

Ubicación: `logs/etl/ETLOrchestrator_YYYYMMDD.log`

---

## 🔒 Seguridad

### Proteger Credenciales

```bash
# Asegurar que .env no está en control de versiones
echo "etl_batch/.env" >> .gitignore

# Restringir permisos del archivo .env
chmod 600 etl_batch/.env
```

### Conexiones Seguras

Para conexiones remotas a PostgreSQL, usar SSL:

```bash
# Agregar a .env
DW_ORO_DB_SSLMODE=require
DW_ORO_DB_SSLROOTCERT=/path/to/ca-certificate.crt
```

---

## 📚 Recursos Adicionales

- **Documentación Completa**: `docs/`
- **Configuración Avanzada**: `docs/ADVANCED_CONFIGURATION.md`
- **Guía de Troubleshooting**: `docs/TROUBLESHOOTING.md`
- **API Reference**: `docs/API_REFERENCE.md`

---

## 🆘 Soporte

Si encuentras problemas:

1. Revisa los logs en `logs/errors/`
2. Consulta la documentación de troubleshooting
3. Verifica la configuración en `.env` y `etl_config.yaml`
4. Contacta al equipo de soporte técnico

---

## ✅ Checklist de Instalación

- [ ] Ubuntu 22.04 instalado y actualizado
- [ ] PostgreSQL instalado y corriendo
- [ ] Python 3.10+ instalado
- [ ] Entorno virtual creado
- [ ] Dependencias instaladas
- [ ] Estructura de directorios creada
- [ ] Archivo .env configurado con credenciales reales
- [ ] Conexión a base de datos verificada
- [ ] Primera ejecución de ETL exitosa
- [ ] Logs generándose correctamente
- [ ] Datos cargados en Data Warehouse

---

**¡Felicitaciones! Tu sistema ETL está listo para uso en producción.** 🎉
