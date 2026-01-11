# 📘 GUÍA DE USO - PUNTAFINA ETL BATCH
## Sistema de Procesamiento por Lotes para Data Warehouse

---

## 📋 Tabla de Contenidos

- [Introducción](#introducción)
- [Conceptos Básicos](#conceptos-básicos)
- [Ejecución del ETL](#ejecución-del-etl)
- [Procesamiento por Lotes](#procesamiento-por-lotes)
- [Validación y Población de Datos](#validación-y-población-de-datos)
- [Monitoreo y Logs](#monitoreo-y-logs)
- [Recuperación de Errores](#recuperación-de-errores)
- [Casos de Uso Comunes](#casos-de-uso-comunes)

---

## 🎯 Introducción

El sistema ETL Batch de PuntaFina está diseñado para:

- ✅ Procesar grandes volúmenes de datos eficientemente
- ✅ Mantener coherencia entre OroCommerce, OroCRM y archivos CSV
- ✅ Validar y poblar datos faltantes automáticamente
- ✅ Recuperarse de errores con checkpoints
- ✅ Ejecutar en Ubuntu 22.04 de manera optimizada

---

## 🧠 Conceptos Básicos

### Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────┐
│                   FUENTES DE DATOS                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ OroCommerce  │  │   OroCRM     │  │  CSV Files   │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
└─────────────┬───────────────┬────────────────┬─────────┘
              │               │                │
              v               v                v
┌─────────────────────────────────────────────────────────┐
│              EXTRACCIÓN (Extractors)                    │
│  - DatabaseExtractor: Extrae de bases de datos          │
│  - CSVExtractor: Extrae de archivos CSV                 │
└─────────────────────────┬───────────────────────────────┘
                          │
                          v
┌─────────────────────────────────────────────────────────┐
│            TRANSFORMACIÓN (Transformers)                │
│  - DimensionBuilder: Construye dimensiones              │
│  - FactBuilder: Construye tablas de hechos              │
│  - DataValidator: Valida y puebla datos                 │
│  - BatchProcessor: Procesa por lotes                    │
└─────────────────────────┬───────────────────────────────┘
                          │
                          v
┌─────────────────────────────────────────────────────────┐
│              CARGA (Loaders)                            │
│  - DatabaseLoader: Carga a Data Warehouse               │
└─────────────────────────┬───────────────────────────────┘
                          │
                          v
┌─────────────────────────────────────────────────────────┐
│                 DATA WAREHOUSE                          │
│  - Dimensiones (20 tablas)                              │
│  - Hechos (5 tablas)                                    │
└─────────────────────────────────────────────────────────┘
```

### Flujo del Proceso ETL

1. **Extracción**: Lee datos de bases de datos y CSVs
2. **Validación**: Verifica coherencia y calidad de datos
3. **Población**: Completa datos faltantes automáticamente
4. **Transformación**: Construye dimensiones y facts
5. **Carga**: Inserta en Data Warehouse
6. **Verificación**: Valida integridad final

---

## 🚀 Ejecución del ETL

### Comando Básico

```bash
# Activar entorno virtual
source venv/bin/activate

# Ejecutar ETL completo
cd etl_batch
python main.py run
```

### Comandos Disponibles

```bash
# Ver ayuda
python main.py --help

# Ejecutar ETL completo
python main.py run

# Validar configuración
python main.py validate

# Setup inicial
python main.py setup

# Con configuración personalizada
python main.py run --config /path/to/config.yaml
```

### Salida Típica

```
════════════════════════════════════════════════════════════
🏪 PUNTAFINA ETL BATCH - PROCESO COMPLETO
════════════════════════════════════════════════════════════

📥 FASE 1: EXTRACCIÓN
   📊 Extrayendo de OroCommerce...
      ✓ 25,342 registros
   📊 Extrayendo de OroCRM...
      ✓ 1,234 registros
   📁 Extrayendo de archivos CSV...
      ✓ 2,142 registros
   
   ✅ Extracción completada: 28,718 registros totales

🔄 FASE 2: TRANSFORMACIÓN - DIMENSIONES
   🔨 Construyendo dim_fecha...
      ✓ 2,557 registros
   🔨 Construyendo dim_cliente...
      ✓ 856 registros
   ...

🔄 FASE 3: TRANSFORMACIÓN - TABLAS DE HECHOS
   🏗️  Construyendo fact_ventas...
      ✓ 30,245 registros
   ...

📤 FASE 4: CARGA
   🚛 Cargando dimensiones...
      📤 dim_fecha...
         ✓ 2,557 registros
   ...

✅ FASE 5: VALIDACIÓN FINAL
   🔍 Verificando integridad de datos...
      ✓ Integridad verificada

════════════════════════════════════════════════════════════
📊 RESUMEN FINAL DEL PROCESO ETL
════════════════════════════════════════════════════════════

⏱️  Tiempo total: 125.34 segundos
✅ Estado: success

📥 Extracción:
   Total registros: 28,718

🔄 Transformación:
   Dimensiones: 20
   Facts: 5
   Total registros: 145,623

📤 Carga:
   Tablas: 25
   Total registros: 145,623
```

---

## 🔄 Procesamiento por Lotes

### Configuración de Lotes

En `etl_config.yaml`:

```yaml
batch:
  chunk_size: 1000        # Registros por lote
  max_workers: 4          # Procesos paralelos
  timeout: 300            # Timeout por lote (segundos)
  max_retries: 3          # Reintentos en caso de error
  retry_delay: 5          # Espera entre reintentos (segundos)
  max_memory_mb: 512      # Memoria máxima por worker
```

### Cómo Funciona

1. **División**: Datos se dividen en chunks de `chunk_size` registros
2. **Procesamiento Paralelo**: Hasta `max_workers` procesando simultáneamente
3. **Checkpoints**: Se guarda progreso cada `checkpoint_interval` lotes
4. **Recuperación**: Si falla, reanuda desde último checkpoint

### Ejemplo de Uso

```python
from core.batch_processor import BatchProcessor, BatchConfig

# Configurar
config = BatchConfig(
    chunk_size=500,
    max_workers=2,
    enable_checkpoints=True
)

processor = BatchProcessor(config)

# Procesar DataFrame en lotes
def process_chunk(df):
    # Tu lógica de transformación
    return df.apply(lambda x: x * 2)

results = processor.process_dataframe(
    df=my_large_dataframe,
    process_func=process_chunk,
    job_name="mi_proceso"
)
```

### Procesamiento Streaming

Para archivos MUY grandes que no caben en memoria:

```python
from core.batch_processor import StreamingBatchProcessor

streaming = StreamingBatchProcessor(config)

results = streaming.process_large_file(
    file_path="data/huge_file.csv",
    process_func=process_chunk,
    job_name="streaming_job",
    file_format="csv"
)
```

---

## ✅ Validación y Población de Datos

### Validaciones Automáticas

El sistema valida automáticamente:

1. **Estructura**: Columnas requeridas existen
2. **Tipos de Datos**: Tipos correctos (int, float, date, etc.)
3. **Valores Obligatorios**: Campos required no son nulos
4. **Integridad Referencial**: Foreign keys válidas
5. **Rangos**: Valores dentro de rangos permitidos
6. **Duplicados**: Elimina duplicados por primary key

### Población Automática

Si faltan datos, el sistema los puebla automáticamente:

```yaml
population_rules:
  # Generar IDs automáticamente
  auto_generate_ids: true
  id_prefix: "AUTO_"
  
  # Valores por defecto
  default_values:
    estado: "activo"
    tipo: "general"
    moneda: "USD"
    pais: "El Salvador"
    
  # Fechas por defecto
  default_dates:
    created_at: "current_timestamp"
    updated_at: "current_timestamp"
```

### Mantener Simetría

El validador asegura simetría entre fuentes:

```python
from core.data_validator import DataValidator

validator = DataValidator(config)

# Validar simetría
symmetry_report = validator.validate_symmetry(
    db_data=oro_commerce_data,
    csv_data=csv_data,
    key_columns=['id', 'codigo']
)

# Fusionar y reconciliar
merged = validator.merge_and_reconcile(
    db_data=oro_commerce_data,
    csv_data=csv_data,
    key_columns=['id'],
    priority='db'  # BD tiene prioridad
)
```

### Reportes de Validación

Cada validación genera un reporte:

```json
{
  "source": "dim_producto",
  "original_rows": 1000,
  "final_rows": 1050,
  "rows_added": 50,
  "validations": [
    {
      "validation": "structure",
      "status": "passed",
      "missing_columns": []
    },
    {
      "validation": "required_fields",
      "status": "fixed",
      "issues": ["precio: 50 valores poblados"]
    }
  ],
  "populations": [
    {
      "population": "missing_data",
      "status": "completed",
      "fields_populated": ["estado: 25 valores", "moneda: 25 valores"]
    }
  ]
}
```

---

## 📊 Monitoreo y Logs

### Niveles de Log

- **DEBUG**: Información detallada para debugging
- **INFO**: Información general del proceso (default)
- **WARNING**: Advertencias no críticas
- **ERROR**: Errores que requieren atención
- **CRITICAL**: Errores críticos que detienen el proceso

### Ubicación de Logs

```
logs/
├── etl/                        # Logs del proceso ETL
│   └── ETLOrchestrator_20260101.log
├── audit/                      # Auditoría de cambios
│   └── audit_20260101.log
└── errors/                     # Solo errores
    └── errors_20260101.log
```

### Ver Logs en Tiempo Real

```bash
# Ver log principal
tail -f logs/etl/ETLOrchestrator_*.log

# Ver solo errores
tail -f logs/errors/errors_*.log

# Buscar texto específico
grep -r "dim_fecha" logs/

# Últimas 100 líneas
tail -n 100 logs/etl/ETLOrchestrator_*.log
```

### Logs en Formato JSON

Los logs pueden configurarse en formato JSON para análisis:

```yaml
monitoring:
  log_format: "json"
```

Ejemplo de log JSON:

```json
{
  "timestamp": "2026-01-01T10:30:45.123456",
  "level": "INFO",
  "logger": "ETLOrchestrator",
  "message": "Extracción completada: 28,718 registros",
  "module": "main",
  "function": "_run_extraction",
  "line": 156
}
```

### Métricas del Proceso

El sistema recolecta métricas automáticamente:

```json
{
  "duration_seconds": 125.34,
  "records_processed": 145623,
  "records_failed": 12,
  "success_rate": 99.99,
  "tables_processed": 25,
  "errors_count": 1,
  "warnings_count": 5,
  "memory_usage_mb": 512.45,
  "cpu_percent": 45.2
}
```

---

## 🔧 Recuperación de Errores

### Checkpoints Automáticos

El sistema guarda checkpoints automáticamente:

```
data/checkpoints/
└── mi_proceso.checkpoint
```

Contenido del checkpoint:

```json
{
  "job_name": "dimension_builder",
  "chunk_id": 150,
  "timestamp": "2026-01-01T10:35:22",
  "total_processed": 150000,
  "total_failed": 25
}
```

### Reanudar desde Checkpoint

Si el proceso falla, automáticamente reanuda:

```
📍 Reanudando desde lote 150
```

### Limpiar Checkpoints

Para forzar re-ejecución completa:

```bash
# Eliminar todos los checkpoints
rm -rf data/checkpoints/*.checkpoint

# Eliminar checkpoint específico
rm data/checkpoints/mi_proceso.checkpoint
```

### Manejo de Errores

Estrategias de manejo:

1. **Reintentos**: Hasta 3 intentos por lote
2. **Skip**: Continúa con siguiente lote
3. **Fail**: Detiene proceso (crítico)

Configuración:

```yaml
batch:
  max_retries: 3
  retry_delay: 5

recovery:
  enable_checkpoints: true
  resume_on_failure: true
```

---

## 📚 Casos de Uso Comunes

### Caso 1: Carga Inicial Completa

```bash
# Primera vez - Cargar todo desde cero
python main.py run
```

### Caso 2: Carga Incremental Diaria

```yaml
# Configurar en etl_config.yaml
loading:
  strategy: "incremental"

# Ejecutar
python main.py run
```

### Caso 3: Actualizar Solo Dimensiones

```python
from transformers.dimension_builder import DimensionBuilder

builder = DimensionBuilder(config)

# Construir solo dim_fecha
dim_fecha = builder.build('dim_fecha')
```

### Caso 4: Validar Datos sin Cargar

```bash
# Solo validación
python main.py validate
```

### Caso 5: Poblar CSV Faltante

```python
from core.data_validator import DataValidator
from extractors.csv_extractor import CSVExtractor

validator = DataValidator(config)
csv_extractor = CSVExtractor(config)

# Cargar CSV
df = csv_extractor.extract_file('ventas', 'estados_orden.csv')

# Validar y poblar
df_validated, report = validator.validate_and_populate(
    df,
    schema={'columns': {...}},
    source_name='estados_orden'
)

# Guardar CSV actualizado
csv_extractor.save_file(df_validated, 'ventas', 'estados_orden_completo.csv')
```

### Caso 6: Procesar Archivo Grande

```python
from core.batch_processor import StreamingBatchProcessor, BatchConfig

config = BatchConfig(chunk_size=1000)
processor = StreamingBatchProcessor(config)

def process_chunk(df):
    # Transformación
    df['total'] = df['cantidad'] * df['precio']
    return df

processor.process_large_file(
    'data/inputs/huge_transactions.csv',
    process_chunk,
    'huge_file_job'
)
```

---

## 🎯 Mejores Prácticas

### 1. Tamaño de Lote Óptimo

- **RAM < 4GB**: chunk_size = 500, max_workers = 2
- **RAM 4-8GB**: chunk_size = 1000, max_workers = 4
- **RAM > 8GB**: chunk_size = 2000, max_workers = 8

### 2. Monitoreo Regular

```bash
# Script de monitoreo
watch -n 5 'tail -n 20 logs/etl/ETLOrchestrator_*.log'
```

### 3. Backups Antes de Carga

```bash
# Backup automático antes de carga
pg_dump DW_oro > backup_$(date +%Y%m%d).sql
```

### 4. Validación Post-Carga

```sql
-- Verificar conteos
SELECT 'dim_fecha' as tabla, COUNT(*) FROM dim_fecha
UNION ALL
SELECT 'dim_cliente', COUNT(*) FROM dim_cliente
UNION ALL
SELECT 'fact_ventas', COUNT(*) FROM fact_ventas;
```

### 5. Limpieza Periódica

```bash
# Limpiar logs antiguos (mayores a 30 días)
find logs/ -name "*.log" -mtime +30 -delete

# Limpiar checkpoints antiguos
find data/checkpoints/ -name "*.checkpoint" -mtime +7 -delete
```

---

## 🆘 Contacto y Soporte

Para más ayuda:
- 📖 Documentación: `docs/`
- 🐛 Troubleshooting: `docs/TROUBLESHOOTING.md`
- ⚙️ Configuración Avanzada: `docs/ADVANCED_CONFIGURATION.md`

---

**¡Sistema ETL listo para producción!** 🎉
