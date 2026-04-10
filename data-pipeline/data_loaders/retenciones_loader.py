"""
===============================================================================
Project: PACIOLI
Module: data_loaders.retenciones_loader
===============================================================================

Description:
    Implements RetencionesLoader for ingesting SRI tax withholding reports
    (Ecuadorian retentions). Removes header repetitions and invalid rows,
    restricts the DataFrame to the configured mapped columns, and
    generates a SHA-256 hash id from the configured hash columns.

Responsibilities:
    - Read the withholdings file from CSV or Excel.
    - Strip header repetition rows (rows where the RUC column contains
      literal header strings such as 'RUC / CI Emisor').
    - Apply the YAML column_mapping and filter out unexpected columns.
    - Parse all configured date columns and monetary fields, coercing
      nulls to zero where appropriate.
    - Normalize identifier strings to drop stray '.0' decimal suffixes.
    - Generate a SHA-256 hash id from the configured hash_columns.

Key Components:
    - RetencionesLoader: Concrete BaseLoader implementation for SRI
      withholding files.

Notes:
    - Rows with a missing 'fecha_emision_ret' are dropped.
    - Hash inputs fall back to a full-row concatenation when no valid
      hash_columns are available in the incoming DataFrame.

Dependencies:
    - pandas, hashlib
    - data_loaders.base_loader (BaseLoader)
    - utils.parsers (parse_to_sql_date, parse_currency)

===============================================================================
"""

import pandas as pd
import hashlib
from .base_loader import BaseLoader
from utils.parsers import parse_to_sql_date, parse_currency


class RetencionesLoader(BaseLoader):
    """
    Loader for SRI tax withholding reports.

    Purpose:
        Ingest SRI (Ecuador) withholding reports into the bronze layer,
        cleaning recurrent header rows and normalizing the monetary and
        date fields involved in the retention computation.

    Responsibilities:
        - Read CSV or Excel withholding files.
        - Discard header repetition rows.
        - Apply column mapping, normalize dates, amounts and identifiers.
        - Generate a SHA-256 hash id from the configured hash_columns.
    """

    def read_file(self, file_path):
        header_row = self.config.get('header_row', 0)
        sheet      = 0

        try:
            if str(file_path).endswith('.csv'):
                df = pd.read_csv(file_path, header=header_row, dtype=str)
            else:
                df = pd.read_excel(file_path, sheet_name=sheet, header=header_row, dtype=str)

            df.columns = df.columns.str.strip()
            return df

        except Exception as e:
            raise ValueError(f"Error leyendo archivo: {e}")

    def specific_business_rules(self, df):

        mapping     = self.config.get('column_mapping', {})
        inv_map     = {v: k for k, v in mapping.items()}
        col_ruc_excel = inv_map.get('ruc_emisor', 'RUC / CI Emisor')

        if col_ruc_excel in df.columns:
            garbage = ['RUC / CI Emisor', 'RUC', 'Razon Social Emisor']
            df = df[~df[col_ruc_excel].isin(garbage)]
            df = df.dropna(subset=[col_ruc_excel])

        df = df.rename(columns=mapping)

        expected_cols = list(mapping.values())
        valid_cols    = [c for c in df.columns if c in expected_cols]
        df            = df[valid_cols]

        date_cols_conf = self.config.get('date_columns', [])
        sql_date_cols  = [
            mapping[d] for d in date_cols_conf if d in mapping
        ]
        for col in sql_date_cols:
            if col in df.columns:
                df[col] = df[col].apply(parse_to_sql_date)

        main_date = 'fecha_emision_ret'
        if main_date in df.columns:
            df = df.dropna(subset=[main_date])

        money_cols = [
            'base_ret_renta', 'valor_ret_renta',
            'base_ret_iva', 'valor_ret_iva',
            'base_ret_isd', 'valor_ret_isd',
            'porcentaje_ret_renta', 'porcentaje_ret_iva',
        ]
        for col in money_cols:
            if col in df.columns:
                df[col] = df[col].apply(parse_currency).fillna(0.0)

        str_cols = [
            'num_comp_sustento', 'clave_acceso', 'num_autorizacion',
            'serie_comprobante_ret', 'periodo_fiscal',
        ]
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.replace('.0', '', regex=False)

        return df

    def generate_hash_id(self, df):

        hash_cols_original = self.config.get('hash_columns', [])
        mapping            = self.config.get('column_mapping', {})
        hash_cols_sql      = [mapping.get(c) for c in hash_cols_original if c in mapping]
        valid_cols         = [c for c in hash_cols_sql if c in df.columns]

        if not valid_cols:
            df['hash_source'] = df.astype(str).sum(axis=1)
        else:
            df['hash_source'] = df[valid_cols].astype(str).fillna('').sum(axis=1)

        df['hash_id'] = df['hash_source'].apply(
            lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest()
        )
        return df.drop(columns=['hash_source'])


