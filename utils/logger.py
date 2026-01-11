#!/usr/bin/env python3
"""
LOGGER CONFIGURATION - CONFIGURACIÓN DE LOGGING
===============================================
Sistema de logging centralizado y configurable
"""

import logging
from pathlib import Path
from datetime import datetime
import json
import sys


def setup_logger(
    name: str, log_dir: str = "logs", level: str = "INFO", log_format: str = "json"
) -> logging.Logger:
    """
    Configura y retorna un logger

    Args:
        name: Nombre del logger
        log_dir: Directorio para logs
        level: Nivel de logging
        log_format: Formato ('json' o 'text')

    Returns:
        Logger configurado
    """
    # Crear directorio de logs
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    # Crear logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))

    # Evitar duplicar handlers
    if logger.handlers:
        return logger

    # Archivo de log
    log_file = log_path / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"

    # Handler para archivo
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)

    # Handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))

    # Formato
    if log_format == "json":
        file_formatter = JSONFormatter()
        console_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    else:
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_formatter = file_formatter

    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


class JSONFormatter(logging.Formatter):
    """Formateador JSON para logs"""

    def format(self, record):
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, ensure_ascii=False)
