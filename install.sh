#!/bin/bash
# ============================================================================
# SCRIPT DE INSTALACIÓN COMPLETA - PUNTAFINA ETL BATCH
# ============================================================================
# Sistema ETL optimizado para Ubuntu 22.04
# Autor: Sistema ETL Batch
# Fecha: 2026-01-01
# ============================================================================

set -e  # Salir en caso de error

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Variables globales
PROJECT_DIR=$(pwd)
VENV_DIR="$PROJECT_DIR/venv"
LOG_FILE="$PROJECT_DIR/logs/install_$(date +%Y%m%d_%H%M%S).log"

# Crear directorio de logs
mkdir -p "$PROJECT_DIR/logs"

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

print_header() {
    echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

check_command() {
    if command -v $1 &> /dev/null; then
        print_success "$1 está instalado"
        return 0
    else
        print_error "$1 NO está instalado"
        return 1
    fi
}

# ============================================================================
# VERIFICACIÓN DE REQUISITOS DEL SISTEMA
# ============================================================================

check_system_requirements() {
    print_header "🔍 VERIFICANDO REQUISITOS DEL SISTEMA"
    
    # Verificar Ubuntu version
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        print_info "Sistema operativo: $NAME $VERSION"
        
        if [[ "$VERSION_ID" != "22.04" ]]; then
            print_warning "Este script está optimizado para Ubuntu 22.04"
            print_warning "Versión detectada: $VERSION_ID"
        else
            print_success "Ubuntu 22.04 detectado"
        fi
    fi
    
    # Verificar arquitectura
    ARCH=$(uname -m)
    print_info "Arquitectura: $ARCH"
    
    # Verificar memoria disponible
    TOTAL_MEM=$(free -m | awk '/^Mem:/{print $2}')
    print_info "Memoria total: ${TOTAL_MEM}MB"
    
    if [ $TOTAL_MEM -lt 2048 ]; then
        print_warning "Se recomienda al menos 2GB de RAM"
    else
        print_success "Memoria suficiente disponible"
    fi
    
    # Verificar espacio en disco
    AVAILABLE_SPACE=$(df -BG . | awk 'NR==2 {print $4}' | sed 's/G//')
    print_info "Espacio disponible: ${AVAILABLE_SPACE}GB"
    
    if [ $AVAILABLE_SPACE -lt 5 ]; then
        print_warning "Se recomienda al menos 5GB de espacio libre"
    else
        print_success "Espacio suficiente disponible"
    fi
    
    log_message "Verificación de requisitos completada"
}

# ============================================================================
# ACTUALIZACIÓN DEL SISTEMA
# ============================================================================

update_system() {
    print_header "📦 ACTUALIZANDO SISTEMA"
    
    print_info "Actualizando lista de paquetes..."
    sudo apt-get update -qq >> "$LOG_FILE" 2>&1
    print_success "Lista de paquetes actualizada"
    
    print_info "Actualizando paquetes instalados..."
    sudo apt-get upgrade -y -qq >> "$LOG_FILE" 2>&1
    print_success "Paquetes actualizados"
    
    log_message "Sistema actualizado"
}

# ============================================================================
# INSTALACIÓN DE DEPENDENCIAS DEL SISTEMA
# ============================================================================

install_system_dependencies() {
    print_header "🔧 INSTALANDO DEPENDENCIAS DEL SISTEMA"
    
    PACKAGES=(
        "postgresql-client"
        "python3.10"
        "python3.10-venv"
        "python3-pip"
        "libpq-dev"
        "build-essential"
        "git"
        "curl"
        "wget"
        "vim"
        "htop"
    )
    
    for package in "${PACKAGES[@]}"; do
        print_info "Instalando $package..."
        sudo apt-get install -y $package -qq >> "$LOG_FILE" 2>&1
        
        if [ $? -eq 0 ]; then
            print_success "$package instalado"
        else
            print_error "Error instalando $package"
            exit 1
        fi
    done
    
    log_message "Dependencias del sistema instaladas"
}

# ============================================================================
# CONFIGURACIÓN DE PYTHON
# ============================================================================

setup_python_environment() {
    print_header "🐍 CONFIGURANDO ENTORNO PYTHON"
    
    # Verificar versión de Python
    PYTHON_VERSION=$(python3 --version)
    print_info "Versión de Python: $PYTHON_VERSION"
    
    # Crear entorno virtual
    if [ ! -d "$VENV_DIR" ]; then
        print_info "Creando entorno virtual..."
        python3 -m venv "$VENV_DIR"
        print_success "Entorno virtual creado"
    else
        print_warning "Entorno virtual ya existe"
    fi
    
    # Activar entorno virtual
    source "$VENV_DIR/bin/activate"
    print_success "Entorno virtual activado"
    
    # Actualizar pip
    print_info "Actualizando pip..."
    pip install --upgrade pip setuptools wheel --quiet >> "$LOG_FILE" 2>&1
    print_success "pip actualizado"
    
    log_message "Entorno Python configurado"
}

# ============================================================================
# INSTALACIÓN DE DEPENDENCIAS PYTHON
# ============================================================================

install_python_dependencies() {
    print_header "📚 INSTALANDO DEPENDENCIAS PYTHON"
    
    # Activar entorno virtual
    source "$VENV_DIR/bin/activate"
    
    # Instalar dependencias desde requirements.txt si existe
    if [ -f "$PROJECT_DIR/etl_batch/requirements.txt" ]; then
        print_info "Instalando desde requirements.txt..."
        pip install -r "$PROJECT_DIR/etl_batch/requirements.txt" --quiet >> "$LOG_FILE" 2>&1
        print_success "Dependencias instaladas desde requirements.txt"
    else
        # Instalar dependencias manualmente
        print_info "Instalando dependencias individuales..."
        
        PYTHON_PACKAGES=(
            "pandas>=2.0.0"
            "numpy>=1.24.0"
            "psycopg2-binary>=2.9.0"
            "python-dotenv>=1.0.0"
            "pyyaml>=6.0"
            "sqlalchemy>=2.0.0"
            "pyarrow>=12.0.0"
            "fastparquet>=2023.0.0"
            "psutil>=5.9.0"
            "tqdm>=4.65.0"
            "click>=8.1.0"
            "tabulate>=0.9.0"
            "colorama>=0.4.6"
        )
        
        for package in "${PYTHON_PACKAGES[@]}"; do
            print_info "Instalando $package..."
            pip install "$package" --quiet >> "$LOG_FILE" 2>&1
            
            if [ $? -eq 0 ]; then
                print_success "$package instalado"
            else
                print_warning "Advertencia instalando $package"
            fi
        done
    fi
    
    # Guardar dependencias instaladas
    print_info "Guardando lista de dependencias..."
    pip freeze > "$PROJECT_DIR/etl_batch/requirements_freeze.txt"
    print_success "Lista de dependencias guardada"
    
    log_message "Dependencias Python instaladas"
}

# ============================================================================
# CONFIGURACIÓN DE ESTRUCTURA DE DIRECTORIOS
# ============================================================================

setup_directory_structure() {
    print_header "📁 CONFIGURANDO ESTRUCTURA DE DIRECTORIOS"
    
    DIRECTORIES=(
        "etl_batch/config"
        "etl_batch/core"
        "etl_batch/extractors"
        "etl_batch/transformers"
        "etl_batch/loaders"
        "etl_batch/utils"
        "data/inputs/ventas"
        "data/inputs/inventario"
        "data/inputs/finanzas"
        "data/outputs/parquet"
        "data/outputs/csv"
        "data/staging"
        "data/checkpoints"
        "logs/etl"
        "logs/audit"
        "logs/errors"
    )
    
    for dir in "${DIRECTORIES[@]}"; do
        if [ ! -d "$PROJECT_DIR/$dir" ]; then
            mkdir -p "$PROJECT_DIR/$dir"
            print_success "Creado: $dir"
        else
            print_info "Existe: $dir"
        fi
    done
    
    # Crear archivos __init__.py
    for dir in etl_batch/core etl_batch/extractors etl_batch/transformers etl_batch/loaders etl_batch/utils; do
        touch "$PROJECT_DIR/$dir/__init__.py"
    done
    print_success "Archivos __init__.py creados"
    
    log_message "Estructura de directorios configurada"
}

# ============================================================================
# CONFIGURACIÓN DE ARCHIVO .ENV
# ============================================================================

setup_environment_file() {
    print_header "⚙️  CONFIGURANDO ARCHIVO .ENV"
    
    ENV_FILE="$PROJECT_DIR/etl_batch/.env"
    
    if [ -f "$ENV_FILE" ]; then
        print_warning "Archivo .env ya existe"
        read -p "¿Desea sobrescribirlo? (s/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Ss]$ ]]; then
            print_info "Manteniendo archivo .env existente"
            return
        fi
    fi
    
    print_info "Creando archivo .env..."
    
    cat > "$ENV_FILE" << 'EOF'
# ============================================================================
# CONFIGURACIÓN DE CONEXIONES - PUNTAFINA ETL BATCH
# ============================================================================

# Base de datos OroCommerce (FUENTE)
ORO_DB_HOST=localhost
ORO_DB_PORT=5432
ORO_DB_NAME=oro_commerce
ORO_DB_USER=oro_user
ORO_DB_PASS=oro_password

# Base de datos OroCRM (FUENTE)
CRM_DB_HOST=localhost
CRM_DB_PORT=5432
CRM_DB_NAME=oro_crm
CRM_DB_USER=oro_user
CRM_DB_PASS=oro_password

# Base de datos Data Warehouse (DESTINO)
DW_ORO_DB_HOST=localhost
DW_ORO_DB_PORT=5432
DW_ORO_DB_NAME=DW_oro
DW_ORO_DB_USER=dw_user
DW_ORO_DB_PASS=dw_password

# Configuración de ETL
ETL_BATCH_SIZE=1000
ETL_MAX_WORKERS=4
ETL_LOG_LEVEL=INFO

# Rutas
ETL_INPUT_PATH=data/inputs
ETL_OUTPUT_PATH=data/outputs
ETL_STAGING_PATH=data/staging
ETL_LOG_PATH=logs/etl

# Variables de entorno
PYTHONUNBUFFERED=1
LANG=en_US.UTF-8
LC_ALL=en_US.UTF-8
EOF
    
    print_success "Archivo .env creado"
    print_warning "⚠️  IMPORTANTE: Configure las credenciales de base de datos en .env"
    
    log_message "Archivo .env configurado"
}

# ============================================================================
# VERIFICACIÓN DE CONEXIÓN A BASE DE DATOS
# ============================================================================

test_database_connection() {
    print_header "🔌 VERIFICANDO CONEXIÓN A BASE DE DATOS"
    
    source "$VENV_DIR/bin/activate"
    source "$PROJECT_DIR/etl_batch/.env"
    
    print_info "Probando conexión a Data Warehouse..."
    
    # Test connection usando psql
    PGPASSWORD="$DW_ORO_DB_PASS" psql -h "$DW_ORO_DB_HOST" -p "$DW_ORO_DB_PORT" -U "$DW_ORO_DB_USER" -d "$DW_ORO_DB_NAME" -c "SELECT 1;" > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        print_success "Conexión a Data Warehouse exitosa"
    else
        print_warning "No se pudo conectar a Data Warehouse"
        print_info "Asegúrese de configurar correctamente las credenciales en .env"
    fi
    
    log_message "Verificación de conexión completada"
}

# ============================================================================
# CONFIGURACIÓN DE PERMISOS
# ============================================================================

setup_permissions() {
    print_header "🔐 CONFIGURANDO PERMISOS"
    
    # Hacer ejecutables los scripts
    find "$PROJECT_DIR/etl_batch" -name "*.sh" -exec chmod +x {} \;
    print_success "Scripts marcados como ejecutables"
    
    # Configurar permisos de directorios
    chmod -R 755 "$PROJECT_DIR/etl_batch"
    chmod -R 755 "$PROJECT_DIR/data"
    chmod -R 755 "$PROJECT_DIR/logs"
    print_success "Permisos de directorios configurados"
    
    log_message "Permisos configurados"
}

# ============================================================================
# INSTALACIÓN DE SERVICIOS SYSTEMD (OPCIONAL)
# ============================================================================

install_systemd_service() {
    print_header "🔄 INSTALANDO SERVICIO SYSTEMD (OPCIONAL)"
    
    read -p "¿Desea instalar el servicio systemd para ejecución automática? (s/n): " -n 1 -r
    echo
    
    if [[ ! $REPLY =~ ^[Ss]$ ]]; then
        print_info "Omitiendo instalación de servicio systemd"
        return
    fi
    
    SERVICE_FILE="/etc/systemd/system/puntafina-etl.service"
    
    print_info "Creando archivo de servicio..."
    
    sudo tee "$SERVICE_FILE" > /dev/null << EOF
[Unit]
Description=PuntaFina ETL Batch Service
After=network.target postgresql.service

[Service]
Type=simple
User=$USER
WorkingDirectory=$PROJECT_DIR/etl_batch
Environment="PATH=$VENV_DIR/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=$VENV_DIR/bin/python main.py
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
    
    print_success "Archivo de servicio creado"
    
    # Recargar systemd
    sudo systemctl daemon-reload
    print_success "Systemd recargado"
    
    print_info "Para habilitar el servicio:"
    print_info "  sudo systemctl enable puntafina-etl"
    print_info "  sudo systemctl start puntafina-etl"
    
    log_message "Servicio systemd instalado"
}

# ============================================================================
# VERIFICACIÓN FINAL
# ============================================================================

final_verification() {
    print_header "✅ VERIFICACIÓN FINAL"
    
    print_info "Verificando instalación..."
    
    # Verificar Python
    source "$VENV_DIR/bin/activate"
    python --version
    print_success "Python OK"
    
    # Verificar paquetes críticos
    python -c "import pandas; import psycopg2; import yaml" 2>/dev/null
    if [ $? -eq 0 ]; then
        print_success "Paquetes Python OK"
    else
        print_error "Error en paquetes Python"
    fi
    
    # Verificar estructura
    if [ -d "$PROJECT_DIR/etl_batch" ] && [ -d "$PROJECT_DIR/data" ]; then
        print_success "Estructura de directorios OK"
    else
        print_error "Error en estructura de directorios"
    fi
    
    log_message "Verificación final completada"
}

# ============================================================================
# RESUMEN DE INSTALACIÓN
# ============================================================================

print_installation_summary() {
    print_header "📋 RESUMEN DE INSTALACIÓN"
    
    echo -e "${GREEN}✓ Instalación completada exitosamente${NC}\n"
    
    echo "📂 Directorio del proyecto: $PROJECT_DIR"
    echo "🐍 Entorno virtual: $VENV_DIR"
    echo "📝 Log de instalación: $LOG_FILE"
    echo
    echo "🚀 PRÓXIMOS PASOS:"
    echo "   1. Configurar credenciales de base de datos en:"
    echo "      etl_batch/.env"
    echo
    echo "   2. Activar entorno virtual:"
    echo "      source venv/bin/activate"
    echo
    echo "   3. Ejecutar configuración inicial:"
    echo "      cd etl_batch"
    echo "      python main.py --setup"
    echo
    echo "   4. Ejecutar ETL:"
    echo "      python main.py --run"
    echo
    echo "📚 Documentación completa en:"
    echo "   docs/INSTALLATION_GUIDE.md"
    echo
    
    log_message "Instalación completada"
}

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

main() {
    clear
    
    cat << "EOF"
╔═══════════════════════════════════════════════════════════════════════════╗
║                                                                           ║
║               🏪  PUNTAFINA ETL BATCH - INSTALACIÓN                       ║
║                                                                           ║
║               Sistema ETL Optimizado para Ubuntu 22.04                   ║
║                                                                           ║
╚═══════════════════════════════════════════════════════════════════════════╝
EOF
    
    echo
    print_info "Iniciando instalación..."
    echo
    
    # Ejecutar pasos de instalación
    check_system_requirements
    
    read -p "¿Continuar con la instalación? (s/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Ss]$ ]]; then
        print_error "Instalación cancelada"
        exit 1
    fi
    
    update_system
    install_system_dependencies
    setup_python_environment
    install_python_dependencies
    setup_directory_structure
    setup_environment_file
    test_database_connection
    setup_permissions
    install_systemd_service
    final_verification
    print_installation_summary
    
    print_success "🎉 ¡Instalación completada exitosamente!"
}

# Ejecutar script principal
main
