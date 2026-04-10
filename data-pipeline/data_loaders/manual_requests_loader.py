"""
===============================================================================
Project: PACIOLI
Module: data_loaders.manual_requests_loader
===============================================================================

Description:
    Implements ManualRequestsLoader for ingesting operator-provided manual
    reconciliation requests. The target table is fully refreshed on each
    run, replicating a snapshot semantics for manual overrides.

Responsibilities:
    - Read the manual request Excel file as strings.
    - Truncate the target table with RESTART IDENTITY CASCADE before
      inserting new records.
    - Parse dates and currency values, sanitize the bank reference column.
    - Generate a deterministic SHA-256 hash id per request.

Key Components:
    - ManualRequestsLoader: Concrete BaseLoader implementation with
      snapshot semantics.

Notes:
    - The YAML 'target_table' must be schema-qualified
      (e.g. biq_raw.raw_manual_requests).
    - openpyxl style warnings emitted while reading the file are silenced.

Dependencies:
    - pandas, hashlib, warnings, sqlalchemy.text
    - data_loaders.base_loader (BaseLoader)
    - utils.parsers (parse_to_sql_date, parse_currency)

===============================================================================
"""

import pandas as pd
import hashlib
import warnings
from sqlalchemy import text
from .base_loader import BaseLoader
from utils.parsers import parse_to_sql_date, parse_currency


class ManualRequestsLoader(BaseLoader):
    """
    Loader for operator-provided manual reconciliation requests.

    Purpose:
        Ingest manual override files with snapshot semantics: the target
        table is fully refreshed on each run.

    Responsibilities:
        - TRUNCATE the target table before loading.
        - Normalize dates, amounts and the bank reference column.
        - Generate a deterministic hash id per request row.
    """

    def read_file(self, file_path):
        header_row = self.config.get('header_row', 0)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_excel(file_path, header=header_row, dtype=str)
        return df

    def specific_business_rules(self, df):

        target_table = self.config.get('target_table')
        if not target_table:
            raise ValueError("'target_table' no está definido en el YAML.")

        with self.engine.begin() as conn:
            conn.execute(
                text(f"TRUNCATE TABLE {target_table} RESTART IDENTITY CASCADE")
            )

        if 'fecha' in df.columns:
            df['fecha'] = df['fecha'].apply(parse_to_sql_date)

        if 'valor' in df.columns:
            df['valor'] = df['valor'].apply(parse_currency)

        if 'ref_banco' in df.columns:
            df['ref_banco'] = df['ref_banco'].astype(str).str.strip()
            df.loc[df['ref_banco'].str.lower() == 'nan', 'ref_banco'] = None

        return df

    def generate_hash_id(self, df):

        df['hash_source'] = (
            df['fecha'].astype(str).fillna('') +
            df['cod_cliente'].astype(str).fillna('') +
            df['valor'].astype(str).fillna('0') +
            df['ref_banco'].astype(str).fillna('')
        )

        df['hash_id'] = df['hash_source'].apply(
            lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest()
        )
        return df.drop(columns=['hash_source'])


