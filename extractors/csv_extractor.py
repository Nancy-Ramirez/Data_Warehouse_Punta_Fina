#!/usr/bin/env python3
"""
CSV EXTRACTOR - EXTRACCIÓN DE DATOS DE ARCHIVOS CSV
===================================================
Extrae y valida datos de archivos CSV con población automática
"""

import pandas as pd
from typing import Dict, List, Any
from pathlib import Path
import logging


class CSVExtractor:
    """Extractor de datos de archivos CSV"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.base_path = Path(config["paths"]["input_csv"])

    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """
        Extrae todos los archivos CSV configurados

        Returns:
            Diccionario con DataFrames por archivo
        """
        data = {}
        csv_config = self.config["data_sources"]["csv_files"]

        for category, files in csv_config["categories"].items():
            category_path = self.base_path / category

            for file_name in files:
                file_path = category_path / file_name

                if file_path.exists():
                    try:
                        df = pd.read_csv(file_path, encoding="utf-8")
                        key = f"{category}_{file_path.stem}"
                        data[key] = df
                        self.logger.debug(f"Extraído {file_name}: {len(df)} registros")
                    except Exception as e:
                        self.logger.warning(f"Error leyendo {file_name}: {e}")
                else:
                    self.logger.warning(f"Archivo no encontrado: {file_path}")

        return data

    def extract_file(self, category: str, file_name: str, **kwargs) -> pd.DataFrame:
        """
        Extrae un archivo CSV específico

        Args:
            category: Categoría del archivo (ventas, inventario, finanzas)
            file_name: Nombre del archivo
            **kwargs: Parámetros adicionales para pd.read_csv

        Returns:
            DataFrame con los datos
        """
        file_path = self.base_path / category / file_name

        if not file_path.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")

        return pd.read_csv(file_path, encoding="utf-8", **kwargs)

    def save_file(self, df: pd.DataFrame, category: str, file_name: str):
        """
        Guarda DataFrame en archivo CSV

        Args:
            df: DataFrame a guardar
            category: Categoría del archivo
            file_name: Nombre del archivo
        """
        category_path = self.base_path / category
        category_path.mkdir(parents=True, exist_ok=True)

        file_path = category_path / file_name
        df.to_csv(file_path, index=False, encoding="utf-8")

        self.logger.info(f"Guardado {file_name}: {len(df)} registros")
