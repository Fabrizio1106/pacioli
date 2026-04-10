"""
===============================================================================
Project: PACIOLI
Module: data_loaders.databalance_loader
===============================================================================

Description:
    Implements DatabalanceLoader for ingesting Databalance settlement files.
    Removes empty subtotal rows, normalizes the multiple date columns of
    the voucher lifecycle, parses the full tax and commission amount set,
    and generates a deterministic hash id per voucher.

Responsibilities:
    - Read Databalance Excel files as strings to preserve raw identifiers.
    - Drop rows without an 'id_databalance' (subtotal/decorative lines).
    - Parse voucher, capture and payment dates; backfill missing capture
      dates from the voucher date.
    - Normalize every monetary column using parse_currency.
    - Build a SHA-256 hash id from voucher-identifying fields.

Key Components:
    - DatabalanceLoader: Concrete BaseLoader implementation.
    - read_file: Excel reader with string dtypes.
    - specific_business_rules: Row filtering, date parsing and money cleanup.
    - generate_hash_id: SHA-256 hash over voucher-identifying attributes.

Notes:
    - 'fecha_captura' defaults to 'fecha_voucher' when missing.
    - Rows with missing voucher or capture dates are dropped.

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

class DatabalanceLoader(BaseLoader):
    """
    Loader for Databalance settlement Excel files.

    Purpose:
        Ingest Databalance voucher files into the bronze layer, enforcing
        ID presence, date completeness and consistent monetary parsing.

    Responsibilities:
        - Remove decorative or subtotal rows missing 'id_databalance'.
        - Normalize voucher, capture and payment dates.
        - Parse every configured monetary column.
        - Generate a SHA-256 hash_id using voucher identifiers.
    """

    def read_file(self, file_path):
        # 1. Read, indicating where the heading is.
        header_row = self.config.get('header_row', 0)
        # dtype=str so that pandas does not infer incorrect types prematurely
        df = pd.read_excel(file_path, header=header_row, dtype=str)
        return df

    def specific_business_rules(self, df):
        # ---------------------------------------------------------------------
        # 1. Cleaning empty rows
        # ---------------------------------------------------------------------
        # We only remove the rows where the key column (id_databalance)
        # is empty or null.
        
        col_id = "id_databalance"
        
        # Convert blank spaces to NaN
        df[col_id] = df[col_id].replace(r'^\s*$', float('nan'), regex=True)
        
        # Remove rows where ID is NaN
        initial_count = len(df)
        df = df.dropna(subset=[col_id])
        
        discarded = initial_count - len(df)
        if discarded > 0:
            self.logger(f"Se eliminaron {discarded} filas vacías (sin ID).", "INFO")

        # ---------------------------------------------------------------------
        # 2. Date parsequences
        # ---------------------------------------------------------------------
        df['fecha_voucher'] = df['fecha_voucher'].apply(parse_to_sql_date)
        df['fecha_captura'] = df['fecha_captura'].apply(parse_to_sql_date)
        df['fecha_pago']    = df['fecha_pago'].apply(parse_to_sql_date)
        
        # Rule: If there is no explicit capture date, we assume it was the voucher date.
        df['fecha_captura'] = df['fecha_captura'].fillna(df['fecha_voucher'])
        
        # Final validation: Critical dates cannot be null
        df = df.dropna(subset=['fecha_voucher', 'fecha_captura'])

        # ---------------------------------------------------------------------
        # 3. Cleaning of amounts
        # ---------------------------------------------------------------------
        money_cols = [
            'valor_total', 'base_0', 'base_imponible', 'iva', 'valor_pagado', 
            'ret_fuente', 'ret_iva', 'comision', 'comision_iva', 'servicio', 
            'propina', 'interes', 'ice', 'otros_impuestos', 'monto_fijo'
        ]
        
        for col in money_cols:
            if col in df.columns:
                df[col] = df[col].apply(parse_currency)

        return df

    def generate_hash_id(self, df):
        """
        Build a unique SHA-256 hash_id per Databalance voucher based on
        id_databalance, voucher date, MID/TID, batch, reference and total.
        """
        df['hash_source'] = (
            df['id_databalance'].astype(str).str.strip() +
            df['fecha_voucher'].astype(str).str.strip().fillna('') +
            df['mid'].astype(str).str.strip().fillna('') +
            df['tid'].astype(str).str.strip().fillna('') +
            df['lote'].astype(str).str.strip().fillna('') +
            df['referencia'].astype(str).str.strip().fillna('') +
            df['valor_total'].astype(str).str.strip().fillna('')
        )
        
        df['hash_id'] = df['hash_source'].apply(
            lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest()
        )
        
        return df.drop(columns=['hash_source'])
