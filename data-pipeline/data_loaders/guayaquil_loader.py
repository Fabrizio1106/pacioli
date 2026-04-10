"""
===============================================================================
Project: PACIOLI
Module: data_loaders.guayaquil_loader
===============================================================================

Description:
    Implements GuayaquilLoader for ingesting Banco de Guayaquil settlement
    files. Legacy .xls files are transparently repaired and normalized to
    .xlsx via an Excel COM bridge before being parsed by pandas.

Responsibilities:
    - Detect legacy .xls inputs and normalize them to .xlsx through
      utils.excel_normalizer.
    - Parse transaction and settlement dates.
    - Normalize commission, retention and total monetary fields.
    - Generate a SHA-256 hash id using a strict canonical formatting of
      integer and decimal columns.

Key Components:
    - GuayaquilLoader: Concrete BaseLoader implementation.
    - read_file: XLS normalizer + Excel reader.
    - specific_business_rules: Date and money normalization.
    - generate_hash_id: SHA-256 hash with canonical integer/decimal helpers.

Notes:
    - When an .xls file is encountered, the original file is deleted after
      successful normalization and the temp .xlsx becomes the processed
      file path.
    - Inherits load_to_sql from BaseLoader unchanged.

Dependencies:
    - pandas, hashlib, os, sqlalchemy.text
    - data_loaders.base_loader (BaseLoader)
    - utils.parsers (parse_to_sql_date, parse_currency)
    - utils.excel_normalizer (normalize_to_temp_xlsx)

===============================================================================
"""

import pandas as pd
import hashlib
import os
from sqlalchemy import text
from .base_loader import BaseLoader
from utils.parsers import parse_to_sql_date, parse_currency
from utils.excel_normalizer import normalize_to_temp_xlsx


class GuayaquilLoader(BaseLoader):
    """
    Loader for Banco de Guayaquil settlement files.

    Purpose:
        Ingest Guayaquil merchant settlement reports, handling the legacy
        .xls format by normalizing it into .xlsx before parsing.

    Responsibilities:
        - Repair/convert .xls inputs via Excel COM bridge.
        - Parse multiple date columns and standardize monetary fields.
        - Build a SHA-256 hash id using canonical integer/decimal formats.
    """

    def read_file(self, file_path):

        final_read_path = file_path

        if file_path.lower().endswith('.xls'):
            try:
                temp_xlsx = normalize_to_temp_xlsx(file_path, self.logger)
                try:
                    os.remove(file_path)
                except OSError as e:
                    self.logger(f"Warning: No se pudo eliminar el original: {e}", "WARN")
                self.current_file_path = temp_xlsx
                final_read_path        = temp_xlsx
            except Exception as e:
                raise ValueError(f"Error crítico reparando XLS: {e}")

        header_row = self.config.get('header_row', 0)
        return pd.read_excel(final_read_path, header=header_row, dtype=str)

    def specific_business_rules(self, df):

        col_id = "moneda"
        if col_id in df.columns:
            df[col_id] = df[col_id].replace(r'^\s*$', float('nan'), regex=True)

        date_cols = ['fecha_transaccion', 'fecha_liquida']
        for col in date_cols:
            if col in df.columns:
                df[col] = df[col].apply(parse_to_sql_date)

        if 'fecha_transaccion' in df.columns:
            df = df.dropna(subset=['fecha_transaccion'])

        money_cols = [
            'neto', 'impuesto', 'servicio', 'total', 'comision',
            'comision_iva', 'retencion_fte', 'retencion_iva', 'a_pagar',
        ]
        for col in money_cols:
            if col in df.columns:
                df[col] = df[col].apply(parse_currency)

        return df

    def generate_hash_id(self, df):

        def _clean_int_str(val):
            if pd.isna(val) or str(val).strip() == '':
                return ''
            try:
                return str(int(float(val)))
            except Exception:
                return str(val).strip()

        def _clean_decimal_str(val):
            if pd.isna(val) or str(val).strip() == '':
                return '0.00'
            try:
                return "{:.2f}".format(float(val))
            except Exception:
                return str(val).strip()

        hash_source = (
            df['recap'].apply(_clean_int_str) +
            df['referencia'].apply(_clean_int_str) +
            df['fecha_transaccion'].astype(str).str.strip().replace('NaT', '').replace('nan', '') +
            df['comercio_descripcion'].astype(str).str.strip().fillna('') +
            df['tarjeta'].astype(str).str.strip().replace('nan', '') +
            df['a_pagar'].apply(_clean_decimal_str) +
            df['fecha_liquida'].astype(str).str.strip().replace('NaT', '').replace('nan', '')
        )

        df['hash_id'] = hash_source.apply(
            lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest()
        )
        return df


