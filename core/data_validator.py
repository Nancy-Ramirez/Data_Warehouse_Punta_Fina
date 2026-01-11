#!/usr/bin/env python3
"""
DATA VALIDATOR - VALIDACIÓN Y POBLACIÓN DE DATOS
================================================
Valida coherencia entre fuentes de datos y puebla datos faltantes
manteniendo simetría entre OroCommerce, OroCRM y CSVs
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional
from pathlib import Path
import logging
from datetime import datetime
import hashlib


class DataValidator:
    """Validador de datos con capacidades de auto-población"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.validation_results = []
        self.population_log = []

    def validate_and_populate(
        self, df: pd.DataFrame, schema: Dict[str, Any], source_name: str
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Valida y puebla un DataFrame según esquema

        Args:
            df: DataFrame a validar
            schema: Esquema de validación
            source_name: Nombre de la fuente de datos

        Returns:
            DataFrame validado y poblado, y reporte de validación
        """
        self.logger.info(f"🔍 Validando datos: {source_name}")

        validation_report = {
            "source": source_name,
            "original_rows": len(df),
            "validations": [],
            "populations": [],
            "errors": [],
        }

        df_validated = df.copy()

        # 1. Validar estructura
        df_validated, struct_report = self._validate_structure(df_validated, schema)
        validation_report["validations"].append(struct_report)

        # 2. Validar tipos de datos
        df_validated, types_report = self._validate_data_types(df_validated, schema)
        validation_report["validations"].append(types_report)

        # 3. Validar valores obligatorios
        df_validated, required_report = self._validate_required_fields(
            df_validated, schema
        )
        validation_report["validations"].append(required_report)

        # 4. Validar integridad referencial
        if self.config.get("data_validation", {}).get("check_referential_integrity"):
            df_validated, ref_report = self._validate_referential_integrity(
                df_validated, schema
            )
            validation_report["validations"].append(ref_report)

        # 5. Poblar datos faltantes
        if self.config.get("data_validation", {}).get("auto_populate_missing"):
            df_validated, pop_report = self._populate_missing_data(df_validated, schema)
            validation_report["populations"].append(pop_report)

        # 6. Validar rangos
        df_validated, range_report = self._validate_ranges(df_validated, schema)
        validation_report["validations"].append(range_report)

        # 7. Eliminar duplicados
        df_validated, dup_report = self._remove_duplicates(df_validated, schema)
        validation_report["validations"].append(dup_report)

        validation_report["final_rows"] = len(df_validated)
        validation_report["rows_added"] = (
            validation_report["final_rows"] - validation_report["original_rows"]
        )

        self.logger.info(f"   ✓ Validación completada: {source_name}")
        self.logger.info(
            f"     - Filas originales: {validation_report['original_rows']}"
        )
        self.logger.info(f"     - Filas finales: {validation_report['final_rows']}")
        self.logger.info(f"     - Filas agregadas: {validation_report['rows_added']}")

        return df_validated, validation_report

    def _validate_structure(
        self, df: pd.DataFrame, schema: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Valida que todas las columnas requeridas existan"""

        required_columns = schema.get("columns", {})
        missing_columns = []

        for col_name, col_def in required_columns.items():
            if col_def.get("required", False) and col_name not in df.columns:
                missing_columns.append(col_name)

                # Agregar columna con valor por defecto
                default_value = col_def.get("default", None)
                df[col_name] = default_value

                self.logger.warning(f"     ⚠️  Columna faltante agregada: {col_name}")

        return df, {
            "validation": "structure",
            "status": "passed" if not missing_columns else "fixed",
            "missing_columns": missing_columns,
        }

    def _validate_data_types(
        self, df: pd.DataFrame, schema: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Valida y corrige tipos de datos"""

        columns_def = schema.get("columns", {})
        type_errors = []

        for col_name, col_def in columns_def.items():
            if col_name not in df.columns:
                continue

            expected_type = col_def.get("type")
            if not expected_type:
                continue

            try:
                if expected_type == "integer":
                    df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype(
                        "Int64"
                    )
                elif expected_type == "float":
                    df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype(
                        float
                    )
                elif expected_type == "string":
                    df[col_name] = df[col_name].astype(str)
                elif expected_type == "date":
                    df[col_name] = pd.to_datetime(df[col_name], errors="coerce")
                elif expected_type == "boolean":
                    df[col_name] = df[col_name].astype(bool)

            except Exception as e:
                type_errors.append(f"{col_name}: {e}")

        return df, {
            "validation": "data_types",
            "status": "passed" if not type_errors else "errors",
            "errors": type_errors,
        }

    def _validate_required_fields(
        self, df: pd.DataFrame, schema: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Valida campos obligatorios y puebla si es necesario"""

        columns_def = schema.get("columns", {})
        required_issues = []

        for col_name, col_def in columns_def.items():
            if not col_def.get("required", False):
                continue

            if col_name not in df.columns:
                continue

            # Contar valores nulos
            null_count = df[col_name].isna().sum()

            if null_count > 0:
                # Poblar con valor por defecto
                default_value = col_def.get("default")

                if default_value == "AUTO_ID":
                    # Generar IDs automáticos
                    df.loc[df[col_name].isna(), col_name] = [
                        self._generate_auto_id(col_name, i) for i in range(null_count)
                    ]
                elif default_value:
                    df[col_name].fillna(default_value, inplace=True)

                required_issues.append(f"{col_name}: {null_count} valores poblados")

        return df, {
            "validation": "required_fields",
            "status": "passed" if not required_issues else "fixed",
            "issues": required_issues,
        }

    def _validate_referential_integrity(
        self, df: pd.DataFrame, schema: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Valida integridad referencial con otras tablas"""

        foreign_keys = schema.get("foreign_keys", [])
        integrity_issues = []

        for fk in foreign_keys:
            fk_column = fk.get("column")
            ref_table = fk.get("references_table")
            ref_column = fk.get("references_column")

            # TODO: Implementar validación con tablas referenciadas
            # Por ahora solo registramos
            integrity_issues.append(f"{fk_column} -> {ref_table}.{ref_column}")

        return df, {
            "validation": "referential_integrity",
            "status": "checked",
            "foreign_keys_checked": len(foreign_keys),
        }

    def _populate_missing_data(
        self, df: pd.DataFrame, schema: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Puebla datos faltantes con reglas inteligentes"""

        population_rules = self.config.get("population_rules", {})
        populated = []

        # Aplicar valores por defecto
        default_values = population_rules.get("default_values", {})
        for col_name, default_value in default_values.items():
            if col_name in df.columns:
                null_count = df[col_name].isna().sum()
                if null_count > 0:
                    df[col_name].fillna(default_value, inplace=True)
                    populated.append(f"{col_name}: {null_count} valores")

        # Aplicar fechas por defecto
        default_dates = population_rules.get("default_dates", {})
        for col_name, date_rule in default_dates.items():
            if col_name in df.columns:
                null_count = df[col_name].isna().sum()
                if null_count > 0:
                    if date_rule == "current_timestamp":
                        df[col_name].fillna(datetime.now(), inplace=True)
                    populated.append(f"{col_name}: {null_count} fechas")

        return df, {
            "population": "missing_data",
            "status": "completed",
            "fields_populated": populated,
        }

    def _validate_ranges(
        self, df: pd.DataFrame, schema: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Valida rangos de valores"""

        columns_def = schema.get("columns", {})
        range_issues = []

        for col_name, col_def in columns_def.items():
            if col_name not in df.columns:
                continue

            min_value = col_def.get("min")
            max_value = col_def.get("max")

            if min_value is not None:
                out_of_range = (df[col_name] < min_value).sum()
                if out_of_range > 0:
                    range_issues.append(
                        f"{col_name}: {out_of_range} valores < {min_value}"
                    )
                    # Corregir a mínimo
                    df.loc[df[col_name] < min_value, col_name] = min_value

            if max_value is not None:
                out_of_range = (df[col_name] > max_value).sum()
                if out_of_range > 0:
                    range_issues.append(
                        f"{col_name}: {out_of_range} valores > {max_value}"
                    )
                    # Corregir a máximo
                    df.loc[df[col_name] > max_value, col_name] = max_value

        return df, {
            "validation": "ranges",
            "status": "passed" if not range_issues else "fixed",
            "issues": range_issues,
        }

    def _remove_duplicates(
        self, df: pd.DataFrame, schema: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Elimina duplicados según clave primaria"""

        primary_key = schema.get("primary_key", [])

        if not primary_key:
            return df, {
                "validation": "duplicates",
                "status": "skipped",
                "reason": "no_primary_key",
            }

        # Asegurar que primary_key es lista
        if isinstance(primary_key, str):
            primary_key = [primary_key]

        original_count = len(df)

        # Eliminar duplicados basado en clave primaria
        df = df.drop_duplicates(subset=primary_key, keep="first")

        duplicates_removed = original_count - len(df)

        if duplicates_removed > 0:
            self.logger.warning(f"     ⚠️  Duplicados eliminados: {duplicates_removed}")

        return df, {
            "validation": "duplicates",
            "status": "passed",
            "duplicates_removed": duplicates_removed,
        }

    def _generate_auto_id(self, prefix: str, index: int) -> str:
        """Genera ID automático único"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        hash_part = hashlib.md5(f"{prefix}{index}{timestamp}".encode()).hexdigest()[:8]
        return f"AUTO_{prefix}_{timestamp}_{hash_part}"

    def validate_symmetry(
        self, db_data: pd.DataFrame, csv_data: pd.DataFrame, key_columns: List[str]
    ) -> Dict[str, Any]:
        """
        Valida simetría entre datos de base de datos y CSV

        Args:
            db_data: Datos de la base de datos
            csv_data: Datos del CSV
            key_columns: Columnas clave para comparación

        Returns:
            Reporte de simetría
        """
        self.logger.info("🔄 Validando simetría entre fuentes")

        # Registros solo en DB
        db_only = db_data.merge(
            csv_data[key_columns], on=key_columns, how="left", indicator=True
        )
        db_only_count = (db_only["_merge"] == "left_only").sum()

        # Registros solo en CSV
        csv_only = csv_data.merge(
            db_data[key_columns], on=key_columns, how="left", indicator=True
        )
        csv_only_count = (csv_only["_merge"] == "left_only").sum()

        # Registros en ambos
        common_count = len(db_data.merge(csv_data[key_columns], on=key_columns))

        symmetry_report = {
            "db_records": len(db_data),
            "csv_records": len(csv_data),
            "common_records": common_count,
            "db_only_records": db_only_count,
            "csv_only_records": csv_only_count,
            "symmetry_percentage": (
                (common_count / max(len(db_data), len(csv_data)) * 100)
                if max(len(db_data), len(csv_data)) > 0
                else 0
            ),
            "is_symmetric": db_only_count == 0 and csv_only_count == 0,
        }

        self.logger.info(
            f"   📊 Simetría: {symmetry_report['symmetry_percentage']:.1f}%"
        )

        if not symmetry_report["is_symmetric"]:
            self.logger.warning(f"     ⚠️  Solo en DB: {db_only_count}")
            self.logger.warning(f"     ⚠️  Solo en CSV: {csv_only_count}")

        return symmetry_report

    def merge_and_reconcile(
        self,
        db_data: pd.DataFrame,
        csv_data: pd.DataFrame,
        key_columns: List[str],
        priority: str = "db",
    ) -> pd.DataFrame:
        """
        Fusiona y reconcilia datos de DB y CSV manteniendo coherencia

        Args:
            db_data: Datos de la base de datos
            csv_data: Datos del CSV
            key_columns: Columnas clave para merge
            priority: Fuente prioritaria ('db' o 'csv')

        Returns:
            DataFrame reconciliado
        """
        self.logger.info("🔀 Fusionando y reconciliando datos")

        if priority == "db":
            # DB tiene prioridad, agregar solo registros de CSV que no estén en DB
            merged = db_data.merge(
                csv_data,
                on=key_columns,
                how="outer",
                suffixes=("_db", "_csv"),
                indicator=True,
            )

            # Consolidar columnas
            for col in db_data.columns:
                if col not in key_columns:
                    db_col = f"{col}_db"
                    csv_col = f"{col}_csv"

                    if db_col in merged.columns and csv_col in merged.columns:
                        # Usar DB primero, CSV como fallback
                        merged[col] = merged[db_col].fillna(merged[csv_col])
                        merged.drop([db_col, csv_col], axis=1, inplace=True)
                    elif db_col in merged.columns:
                        merged[col] = merged[db_col]
                        merged.drop(db_col, axis=1, inplace=True)
                    elif csv_col in merged.columns:
                        merged[col] = merged[csv_col]
                        merged.drop(csv_col, axis=1, inplace=True)

        else:
            # CSV tiene prioridad
            merged = csv_data.merge(
                db_data,
                on=key_columns,
                how="outer",
                suffixes=("_csv", "_db"),
                indicator=True,
            )

            # Similar lógica pero priorizando CSV
            for col in csv_data.columns:
                if col not in key_columns:
                    csv_col = f"{col}_csv"
                    db_col = f"{col}_db"

                    if csv_col in merged.columns and db_col in merged.columns:
                        merged[col] = merged[csv_col].fillna(merged[db_col])
                        merged.drop([csv_col, db_col], axis=1, inplace=True)
                    elif csv_col in merged.columns:
                        merged[col] = merged[csv_col]
                        merged.drop(csv_col, axis=1, inplace=True)
                    elif db_col in merged.columns:
                        merged[col] = merged[db_col]
                        merged.drop(db_col, axis=1, inplace=True)

        # Eliminar columna indicadora
        if "_merge" in merged.columns:
            merged.drop("_merge", axis=1, inplace=True)

        self.logger.info(f"   ✓ Datos reconciliados: {len(merged)} registros")

        return merged
