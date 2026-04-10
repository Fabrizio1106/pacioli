"""
===============================================================================
Project: PACIOLI
Module: data_loaders.webpos_loader
===============================================================================

Description:
    Implements WebposLoader for ingesting Web Point-of-Sale transaction
    exports. Enforces presence of the two key columns (date and access
    key) and standardizes the transaction date and total amount.

Responsibilities:
    - Read the WebPOS Excel file as strings.
    - Drop rows missing the mandatory 'fecha' and 'clave_de_acceso'.
    - Parse the transaction date and the total amount.
    - Generate a SHA-256 hash id from payment-identifying columns.

Key Components:
    - WebposLoader: Concrete BaseLoader implementation.

Dependencies:
    - pandas, hashlib, sqlalchemy.text
    - data_loaders.base_loader (BaseLoader)
    - utils.parsers (parse_to_sql_date, parse_currency)

===============================================================================
"""

import pandas as pd
import hashlib
from sqlalchemy import text
from .base_loader import BaseLoader
from utils.parsers import parse_to_sql_date, parse_currency


class WebposLoader(BaseLoader):
    """
    Loader for Web Point-of-Sale transaction exports.

    Purpose:
        Ingest WebPOS transactional files into the bronze layer ensuring
        that each record has a valid date and access key.

    Responsibilities:
        - Drop rows missing 'fecha' or 'clave_de_acceso'.
        - Normalize 'fecha' via parse_to_sql_date and 'total' via
          parse_currency.
        - Generate a SHA-256 hash id using payment identifiers.
    """

    def read_file(self, file_path):
        header_row = self.config.get('header_row', 0)
        df         = pd.read_excel(file_path, header=header_row, dtype=str)
        return df

    def specific_business_rules(self, df):

        required_cols = ["fecha", "clave_de_acceso"]

        for col in required_cols:
            if col in df.columns:
                df[col] = (
                    df[col].astype(str).str.strip()
                    .replace({"": None, "nan": None})
                )

        existing_required = [c for c in required_cols if c in df.columns]
        if existing_required:
            df = df.dropna(subset=existing_required)

        if 'fecha' in df.columns:
            df['fecha'] = df['fecha'].apply(parse_to_sql_date)

        if 'total' in df.columns:
            df['total'] = df['total'].apply(parse_currency)

        return df

    def generate_hash_id(self, df):

        df['hash_source'] = (
            df['tipo_pago'].astype(str)    +
            df['factura'].astype(str)      +
            df['ruc_cliente'].astype(str)  +
            df['autorizacion'].astype(str) +
            df['total'].astype(str)
        )

        df['hash_id'] = df['hash_source'].apply(
            lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest()
        )

        return df.drop(columns=['hash_source'])