#!/usr/bin/env python3
"""
DATABASE EXTRACTOR - EXTRACCIÓN DE DATOS DE BASES DE DATOS
==========================================================
Extrae datos de OroCommerce y OroCRM con procesamiento por lotes
"""

import pandas as pd
import psycopg2
from typing import Dict, List, Any
import os
from pathlib import Path
import logging


class DatabaseExtractor:
    """Extractor de datos de bases de datos PostgreSQL"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def extract_orocommerce(self) -> Dict[str, pd.DataFrame]:
        """
        Extrae todas las tablas configuradas de OroCommerce

        Returns:
            Diccionario con DataFrames por tabla
        """
        tables = self.config["data_sources"]["orocommerce"]["tables"]
        conn = self._get_oro_connection()

        data = {}

        for table_name in tables:
            try:
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql_query(query, conn)
                data[table_name] = df
                self.logger.debug(f"Extraído {table_name}: {len(df)} registros")
            except Exception as e:
                self.logger.warning(f"Error extrayendo {table_name}: {e}")

        conn.close()
        return data

    def extract_orocrm(self) -> Dict[str, pd.DataFrame]:
        """
        Extrae todas las tablas configuradas de OroCRM

        Returns:
            Diccionario con DataFrames por tabla
        """
        tables = self.config["data_sources"]["orocrm"]["tables"]
        conn = self._get_crm_connection()

        data = {}

        for table_name in tables:
            try:
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql_query(query, conn)
                data[table_name] = df
                self.logger.debug(f"Extraído {table_name}: {len(df)} registros")
            except Exception as e:
                self.logger.warning(f"Error extrayendo {table_name}: {e}")

        conn.close()
        return data

    def extract_table(
        self,
        table_name: str,
        connection_type: str = "oro",
        filters: Dict[str, Any] = None,
        columns: List[str] = None,
    ) -> pd.DataFrame:
        """
        Extrae una tabla específica con filtros opcionales

        Args:
            table_name: Nombre de la tabla
            connection_type: Tipo de conexión ('oro' o 'crm')
            filters: Diccionario de filtros WHERE
            columns: Lista de columnas a seleccionar

        Returns:
            DataFrame con los datos
        """
        conn = (
            self._get_oro_connection()
            if connection_type == "oro"
            else self._get_crm_connection()
        )

        # Construir query
        select_clause = "*" if not columns else ", ".join(columns)
        query = f"SELECT {select_clause} FROM {table_name}"

        if filters:
            where_clauses = [f"{col} = %s" for col in filters.keys()]
            query += " WHERE " + " AND ".join(where_clauses)

        # Ejecutar
        df = pd.read_sql_query(
            query, conn, params=list(filters.values()) if filters else None
        )

        conn.close()
        return df

    def _get_oro_connection(self):
        """Obtiene conexión a OroCommerce"""
        return psycopg2.connect(
            host=os.getenv("ORO_DB_HOST"),
            port=int(os.getenv("ORO_DB_PORT")),
            dbname=os.getenv("ORO_DB_NAME"),
            user=os.getenv("ORO_DB_USER"),
            password=os.getenv("ORO_DB_PASS"),
        )

    def _get_crm_connection(self):
        """Obtiene conexión a OroCRM"""
        return psycopg2.connect(
            host=os.getenv("CRM_DB_HOST"),
            port=int(os.getenv("CRM_DB_PORT")),
            dbname=os.getenv("CRM_DB_NAME"),
            user=os.getenv("CRM_DB_USER"),
            password=os.getenv("CRM_DB_PASS"),
        )
