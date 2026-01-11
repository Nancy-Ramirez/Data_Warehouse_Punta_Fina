#!/usr/bin/env python3
"""
DIMENSION BUILDER - CONSTRUCTOR DE DIMENSIONES
==============================================
Construye dimensiones del data warehouse con datos de múltiples fuentes
"""

import pandas as pd
from typing import Dict, Any
from pathlib import Path
import logging
from datetime import datetime, timedelta


class DimensionBuilder:
    """Constructor de dimensiones del data warehouse"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Esquemas de dimensiones
        self.schemas = self._load_schemas()

    def build(self, dimension_name: str) -> pd.DataFrame:
        """
        Construye una dimensión específica

        Args:
            dimension_name: Nombre de la dimensión (ej: 'dim_fecha')

        Returns:
            DataFrame con la dimensión construida
        """
        method_name = f"_build_{dimension_name}"

        if hasattr(self, method_name):
            return getattr(self, method_name)()
        else:
            raise NotImplementedError(f"Dimensión {dimension_name} no implementada")

    def get_schema(self, dimension_name: str) -> Dict[str, Any]:
        """Retorna el esquema de una dimensión"""
        return self.schemas.get(dimension_name, {})

    def _load_schemas(self) -> Dict[str, Dict[str, Any]]:
        """Carga esquemas de todas las dimensiones"""
        return {
            "dim_fecha": {
                "primary_key": "id_fecha",
                "columns": {
                    "id_fecha": {"type": "integer", "required": True},
                    "fecha": {"type": "date", "required": True},
                    "año": {"type": "integer", "required": True},
                    "mes": {"type": "integer", "required": True, "min": 1, "max": 12},
                    "dia": {"type": "integer", "required": True, "min": 1, "max": 31},
                    "dia_semana": {
                        "type": "integer",
                        "required": True,
                        "min": 0,
                        "max": 6,
                    },
                    "nombre_dia": {"type": "string", "required": True},
                    "nombre_mes": {"type": "string", "required": True},
                    "trimestre": {
                        "type": "integer",
                        "required": True,
                        "min": 1,
                        "max": 4,
                    },
                    "semestre": {
                        "type": "integer",
                        "required": True,
                        "min": 1,
                        "max": 2,
                    },
                    "es_feriado": {
                        "type": "boolean",
                        "required": True,
                        "default": False,
                    },
                    "es_fin_semana": {
                        "type": "boolean",
                        "required": True,
                        "default": False,
                    },
                },
            },
            "dim_cliente": {
                "primary_key": "id_cliente",
                "columns": {
                    "id_cliente": {"type": "integer", "required": True},
                    "nombre": {"type": "string", "required": True},
                    "email": {"type": "string", "required": False},
                    "telefono": {"type": "string", "required": False},
                    "fecha_registro": {"type": "date", "required": True},
                },
            },
            # Agregar más esquemas según sea necesario
        }

    def _build_dim_fecha(self) -> pd.DataFrame:
        """Construye dimensión de fechas"""
        self.logger.info("Construyendo dim_fecha...")

        # Rango de fechas
        min_date = pd.to_datetime(self.config["data_validation"]["min_date"])
        max_date = pd.to_datetime(self.config["data_validation"]["max_date"])

        # Generar todas las fechas
        date_range = pd.date_range(start=min_date, end=max_date, freq="D")

        df = pd.DataFrame({"fecha": date_range})

        # Calcular campos
        df["id_fecha"] = df["fecha"].dt.strftime("%Y%m%d").astype(int)
        df["año"] = df["fecha"].dt.year
        df["mes"] = df["fecha"].dt.month
        df["dia"] = df["fecha"].dt.day
        df["dia_semana"] = df["fecha"].dt.dayofweek
        df["nombre_dia"] = df["fecha"].dt.day_name()
        df["nombre_mes"] = df["fecha"].dt.month_name()
        df["trimestre"] = df["fecha"].dt.quarter
        df["semestre"] = (df["mes"] - 1) // 6 + 1
        df["es_fin_semana"] = df["dia_semana"].isin([5, 6])
        df["es_feriado"] = df["fecha"].apply(self._is_holiday)

        return df

    def _is_holiday(self, date: pd.Timestamp) -> bool:
        """Determina si una fecha es feriado de El Salvador"""
        feriados = [
            (1, 1),  # Año Nuevo
            (5, 1),  # Día del Trabajo
            (5, 10),  # Día de la Madre
            (6, 17),  # Día del Padre
            (8, 6),  # Fiestas Patronales
            (9, 15),  # Día de la Independencia
            (11, 2),  # Día de los Difuntos
            (12, 25),  # Navidad
        ]

        return (date.month, date.day) in feriados

    def _build_dim_detalle_venta(self) -> pd.DataFrame:
        """Construye dimensión de detalle de venta"""
        self.logger.info("Construyendo dim_detalle_venta...")

        # Esta es una dimensión sin información (degenerada)
        df = pd.DataFrame({"id_detalle_venta": [0], "descripcion": ["Sin detalle"]})

        return df

    def _build_dim_usuario(self) -> pd.DataFrame:
        """Construye dimensión de usuarios"""
        self.logger.info("Construyendo dim_usuario...")

        # TODO: Extraer de oro_user
        df = pd.DataFrame(
            {
                "id_usuario": [0, 1],
                "nombre": ["Sistema", "Admin"],
                "email": ["system@puntafina.com", "admin@puntafina.com"],
                "activo": [True, True],
            }
        )

        return df
