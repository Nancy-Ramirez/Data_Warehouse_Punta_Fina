#!/bin/bash
# ============================================================================
# SCRIPT DE VERIFICACIÓN POST-INSTALACIÓN
# ============================================================================
# Verifica que todos los componentes del nuevo sistema ETL estén correctos

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}"
cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║        🔍  VERIFICACIÓN DEL SISTEMA ETL BATCH                ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}\n"

ERRORS=0
WARNINGS=0

check_file() {
    if [ -f "$1" ]; then
        echo -e "${GREEN}✓${NC} $1"
        return 0
    else
        echo -e "${RED}✗${NC} $1 ${RED}(FALTA)${NC}"
        ERRORS=$((ERRORS + 1))
        return 1
    fi
}

check_dir() {
    if [ -d "$1" ]; then
        echo -e "${GREEN}✓${NC} $1/"
        return 0
    else
        echo -e "${RED}✗${NC} $1/ ${RED}(FALTA)${NC}"
        ERRORS=$((ERRORS + 1))
        return 1
    fi
}

echo -e "${BLUE}📁 Verificando estructura de directorios...${NC}\n"

check_dir "config"
check_dir "core"
check_dir "extractors"
check_dir "transformers"
check_dir "loaders"
check_dir "utils"
check_dir "docs"
check_dir "../data/inputs"
check_dir "../data/outputs"
check_dir "../logs"

echo -e "\n${BLUE}📄 Verificando archivos principales...${NC}\n"

check_file "main.py"
check_file "install.sh"
check_file "quickstart.sh"
check_file "requirements.txt"
check_file "README.md"
check_file ".env.example"

echo -e "\n${BLUE}🔧 Verificando módulos core...${NC}\n"

check_file "core/__init__.py"
check_file "core/batch_processor.py"
check_file "core/data_validator.py"

echo -e "\n${BLUE}📊 Verificando extractors...${NC}\n"

check_file "extractors/__init__.py"
check_file "extractors/database_extractor.py"
check_file "extractors/csv_extractor.py"

echo -e "\n${BLUE}🔄 Verificando transformers...${NC}\n"

check_file "transformers/__init__.py"
check_file "transformers/dimension_builder.py"
check_file "transformers/fact_builder.py"

echo -e "\n${BLUE}📤 Verificando loaders...${NC}\n"

check_file "loaders/__init__.py"
check_file "loaders/database_loader.py"

echo -e "\n${BLUE}🛠️  Verificando utilidades...${NC}\n"

check_file "utils/__init__.py"
check_file "utils/logger.py"
check_file "utils/metrics.py"

echo -e "\n${BLUE}📚 Verificando documentación...${NC}\n"

check_file "docs/INSTALLATION_GUIDE.md"
check_file "docs/USER_GUIDE.md"

echo -e "\n${BLUE}⚙️  Verificando configuración...${NC}\n"

check_file "config/etl_config.yaml"

echo -e "\n${BLUE}🐍 Verificando Python y dependencias...${NC}\n"

if [ -d "../venv" ]; then
    echo -e "${GREEN}✓${NC} Entorno virtual existe"
    
    if [ -f "../venv/bin/activate" ]; then
        echo -e "${GREEN}✓${NC} Script de activación existe"
        
        # Activar y verificar paquetes
        source ../venv/bin/activate
        
        echo -n "Verificando pandas... "
        if python -c "import pandas" 2>/dev/null; then
            echo -e "${GREEN}✓${NC}"
        else
            echo -e "${RED}✗${NC}"
            ERRORS=$((ERRORS + 1))
        fi
        
        echo -n "Verificando psycopg2... "
        if python -c "import psycopg2" 2>/dev/null; then
            echo -e "${GREEN}✓${NC}"
        else
            echo -e "${RED}✗${NC}"
            ERRORS=$((ERRORS + 1))
        fi
        
        echo -n "Verificando yaml... "
        if python -c "import yaml" 2>/dev/null; then
            echo -e "${GREEN}✓${NC}"
        else
            echo -e "${RED}✗${NC}"
            ERRORS=$((ERRORS + 1))
        fi
        
    else
        echo -e "${RED}✗${NC} Script de activación no encontrado"
        ERRORS=$((ERRORS + 1))
    fi
else
    echo -e "${RED}✗${NC} Entorno virtual no encontrado"
    ERRORS=$((ERRORS + 1))
fi

echo -e "\n${BLUE}🔐 Verificando permisos...${NC}\n"

if [ -x "install.sh" ]; then
    echo -e "${GREEN}✓${NC} install.sh es ejecutable"
else
    echo -e "${YELLOW}⚠${NC} install.sh no es ejecutable"
    WARNINGS=$((WARNINGS + 1))
fi

if [ -x "quickstart.sh" ]; then
    echo -e "${GREEN}✓${NC} quickstart.sh es ejecutable"
else
    echo -e "${YELLOW}⚠${NC} quickstart.sh no es ejecutable"
    WARNINGS=$((WARNINGS + 1))
fi

echo -e "\n${BLUE}📋 Verificando configuración...${NC}\n"

if [ -f ".env" ]; then
    echo -e "${GREEN}✓${NC} Archivo .env configurado"
else
    echo -e "${YELLOW}⚠${NC} Archivo .env no encontrado (use .env.example como plantilla)"
    WARNINGS=$((WARNINGS + 1))
fi

# Resumen final
echo -e "\n${"═"*60}"
echo -e "${BLUE}RESUMEN DE VERIFICACIÓN${NC}"
echo -e "${"═"*60}\n"

if [ $ERRORS -eq 0 ] && [ $WARNINGS -eq 0 ]; then
    echo -e "${GREEN}✅ TODO CORRECTO${NC}"
    echo -e "${GREEN}El sistema ETL está completamente instalado y listo para usar.${NC}\n"
    echo -e "Próximos pasos:"
    echo -e "  1. Configurar credenciales en .env (si aún no lo hizo)"
    echo -e "  2. Ejecutar: ${BLUE}./quickstart.sh${NC}"
    exit 0
elif [ $ERRORS -eq 0 ]; then
    echo -e "${YELLOW}⚠️  ADVERTENCIAS: $WARNINGS${NC}"
    echo -e "${YELLOW}El sistema está instalado pero tiene algunas advertencias.${NC}\n"
    echo -e "Se recomienda revisar las advertencias antes de continuar."
    exit 0
else
    echo -e "${RED}❌ ERRORES ENCONTRADOS: $ERRORS${NC}"
    echo -e "${RED}⚠️  ADVERTENCIAS: $WARNINGS${NC}\n"
    echo -e "${RED}El sistema tiene errores que deben corregirse.${NC}"
    echo -e "Por favor, ejecute nuevamente: ${BLUE}./install.sh${NC}"
    exit 1
fi
