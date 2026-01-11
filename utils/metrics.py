#!/usr/bin/env python3
"""
METRICS COLLECTOR - RECOLECCIÓN DE MÉTRICAS
===========================================
Recolecta y reporta métricas del proceso ETL
"""

from typing import Dict, Any, List
from datetime import datetime
import psutil


class MetricsCollector:
    """Recolector de métricas del proceso"""

    def __init__(self):
        self.metrics = {
            "start_time": datetime.now(),
            "records_processed": 0,
            "records_failed": 0,
            "tables_processed": 0,
            "errors": [],
            "warnings": [],
        }

    def record_processed(self, count: int):
        """Registra registros procesados"""
        self.metrics["records_processed"] += count

    def record_failed(self, count: int):
        """Registra registros fallidos"""
        self.metrics["records_failed"] += count

    def record_table(self):
        """Registra tabla procesada"""
        self.metrics["tables_processed"] += 1

    def add_error(self, error: str):
        """Agrega error"""
        self.metrics["errors"].append(
            {"timestamp": datetime.now().isoformat(), "error": error}
        )

    def add_warning(self, warning: str):
        """Agrega advertencia"""
        self.metrics["warnings"].append(
            {"timestamp": datetime.now().isoformat(), "warning": warning}
        )

    def get_summary(self) -> Dict[str, Any]:
        """Retorna resumen de métricas"""
        elapsed = (datetime.now() - self.metrics["start_time"]).total_seconds()

        return {
            "duration_seconds": elapsed,
            "records_processed": self.metrics["records_processed"],
            "records_failed": self.metrics["records_failed"],
            "success_rate": (
                (
                    self.metrics["records_processed"]
                    / (
                        self.metrics["records_processed"]
                        + self.metrics["records_failed"]
                    )
                    * 100
                )
                if (self.metrics["records_processed"] + self.metrics["records_failed"])
                > 0
                else 0
            ),
            "tables_processed": self.metrics["tables_processed"],
            "errors_count": len(self.metrics["errors"]),
            "warnings_count": len(self.metrics["warnings"]),
            "memory_usage_mb": psutil.Process().memory_info().rss / 1024 / 1024,
            "cpu_percent": psutil.Process().cpu_percent(),
        }
