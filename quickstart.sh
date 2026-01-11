#!/bin/bash
# ============================================================================
# QUICK START - INICIO RÁPIDO
# ============================================================================
# Script para iniciar rápidamente el sistema ETL después de la instalación

set -e

# Colores
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}"
cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║           🏪  PUNTAFINA ETL BATCH - QUICK START              ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

# Verificar que estamos en el directorio correcto
if [ ! -f "main.py" ]; then
    echo -e "${YELLOW}⚠️  Debe ejecutar este script desde el directorio etl_batch/${NC}"
    echo "Ejemplo: cd etl_batch && ./quickstart.sh"
    exit 1
fi

# Verificar entorno virtual
if [ ! -d "../venv" ]; then
    echo -e "${YELLOW}⚠️  Entorno virtual no encontrado${NC}"
    echo "Ejecute primero: ./install.sh"
    exit 1
fi

# Activar entorno virtual
echo -e "${BLUE}🔧 Activando entorno virtual...${NC}"
source ../venv/bin/activate

# Verificar archivo .env
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}⚠️  Archivo .env no encontrado${NC}"
    
    if [ -f ".env.example" ]; then
        echo -e "${BLUE}📝 Creando .env desde .env.example...${NC}"
        cp .env.example .env
        echo -e "${GREEN}✓ Archivo .env creado${NC}"
        echo -e "${YELLOW}⚠️  IMPORTANTE: Configure las credenciales en .env antes de continuar${NC}"
        echo "Edite el archivo: nano .env"
        exit 0
    else
        echo -e "${YELLOW}Error: .env.example no encontrado${NC}"
        exit 1
    fi
fi

# Menú de opciones
echo -e "\n${BLUE}Seleccione una opción:${NC}"
echo "  1) Validar configuración"
echo "  2) Ejecutar setup inicial"
echo "  3) Ejecutar ETL completo"
echo "  4) Ver logs en tiempo real"
echo "  5) Limpiar checkpoints"
echo "  6) Salir"
echo ""
read -p "Opción [1-6]: " option

case $option in
    1)
        echo -e "\n${BLUE}🔍 Validando configuración...${NC}"
        python main.py validate
        ;;
    2)
        echo -e "\n${BLUE}⚙️  Ejecutando setup inicial...${NC}"
        python main.py setup
        ;;
    3)
        echo -e "\n${BLUE}🚀 Ejecutando ETL completo...${NC}"
        python main.py run
        ;;
    4)
        echo -e "\n${BLUE}📊 Mostrando logs en tiempo real...${NC}"
        echo "Presione Ctrl+C para salir"
        sleep 2
        tail -f ../logs/etl/ETLOrchestrator_*.log
        ;;
    5)
        echo -e "\n${YELLOW}🗑️  Limpiando checkpoints...${NC}"
        rm -rf ../data/checkpoints/*.checkpoint
        echo -e "${GREEN}✓ Checkpoints eliminados${NC}"
        ;;
    6)
        echo -e "\n${GREEN}👋 Hasta luego${NC}"
        exit 0
        ;;
    *)
        echo -e "\n${YELLOW}Opción inválida${NC}"
        exit 1
        ;;
esac

echo -e "\n${GREEN}✓ Operación completada${NC}\n"
