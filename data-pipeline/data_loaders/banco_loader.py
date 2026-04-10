"""
===============================================================================
Project: PACIOLI
Module: data_loaders.banco_loader
===============================================================================

Description:
    Implements the BancoLoader class, responsible for ingesting, sanitizing
    and normalizing bank statement extracts in Excel format. Applies source-
    specific business rules to strip non-operational footers and generates a
    deterministic SHA-256 hash that guarantees idempotency and prevents
    duplication of transactional records.

Responsibilities:
    - Read bank statement Excel files preserving string formatting to keep
      leading zeros and bank reference IDs intact.
    - Truncate footer rows and non-operational summary lines.
    - Normalize transaction dates and monetary amounts.
    - Build a transaction fingerprint and generate a SHA-256 hash id.

Key Components:
    - BancoLoader: Concrete BaseLoader implementation for bank statements.
    - read_file: Excel reader preserving dtype=str to avoid data loss.
    - specific_business_rules: Footer cleanup and type normalization.
    - generate_hash_id: Deterministic SHA-256 fingerprint of each transaction.

Notes:
    - Rows starting from the first empty 'fecha_transaccion' value are
      discarded as bank-generated footers.
    - Monetary parsing supports both Latin and US formats via parse_currency.
    - Duplicate intra-file transactions are disambiguated by a cumulative
      duplicate_rank before hashing.

Dependencies:
    - hashlib, typing
    - pandas
    - data_loaders.base_loader (BaseLoader)
    - utils.parsers (parse_bank_datetime, parse_currency)

Author: Fabricio A. Pilatasig - PACIOLI
Created: 2026
Version: 1.0.0
===============================================================================
"""

import hashlib
import pandas as pd
from typing import Callable

from .base_loader import BaseLoader
from utils.parsers import parse_bank_datetime, parse_currency


class BancoLoader(BaseLoader):
    """
    Loader for bank statement Excel files.

    Purpose:
        Ingest bank transactional extracts, strip non-operational footer
        rows, normalize dates and monetary values, and produce a stable
        SHA-256 hash id for each transaction.

    Responsibilities:
        - Read Excel files with dtype=str to preserve leading zeros.
        - Discard footer rows based on the first empty date cell.
        - Normalize date and currency columns via shared parsers.
        - Generate a deterministic hash_id using a fingerprint of the
          transaction attributes plus an intra-file duplicate rank.
    """

    def read_file(self, file_path: str) -> pd.DataFrame:
        """
        Read a bank Excel file preserving textual formatting.

        Args:
            file_path (str): Absolute or relative path to the Excel file.

        Returns:
            pd.DataFrame: Raw DataFrame with every column loaded as string
                          to avoid losing leading zeros in bank references.
        """
        # The header row is obtained from the configuration (default: 0)
        header_row = self.config.get('header_row', 0)
        
        # dtype=str is critical in finance for preserving IDs and bank references
        df = pd.read_excel(file_path, header=header_row, dtype=str)
        return df

    def specific_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply bank-specific sanitization and type normalization rules.

        Args:
            df (pd.DataFrame): Raw DataFrame returned by read_file.

        Returns:
            pd.DataFrame: Sanitized DataFrame with footer rows stripped and
                          date/currency columns converted to proper types.

        Notes:
            - Rows after the first empty 'fecha_transaccion' are discarded
              as footer/summary lines.
            - Transactions with an invalid date are dropped with a warning.
        """
        # 1. Separate copy to avoid Pandas' 'SettingWithCopyWarning'
        df = df.loc[:, ~df.columns.str.startswith("Unnamed")].copy()
        
        col_fecha = "fecha_transaccion"
        
        # 2. Data sanitization: Removal of footers and bank totals
        if col_fecha in df.columns:
            # Normalize empty or spaced cells to NaN
            df[col_fecha] = df[col_fecha].replace(r'^\s*$', float('nan'), regex=True)
            
            # Truncate the DataFrame starting from the first undated row
            empty_rows = df[df[col_fecha].isna()].index
            if not empty_rows.empty:
                first_empty_idx = empty_rows[0]
                initial_count = len(df)
                df = df.iloc[:first_empty_idx].copy()
                discarded = initial_count - len(df)
                
                if discarded > 0:
                    self.logger(f"Info: Se excluyeron {discarded} filas no operacionales (footers) del final.", "INFO")

        # Data Type Normalization (Dates and Currencies)
        df['fecha_transaccion'] = df['fecha_transaccion'].apply(parse_bank_datetime)
        
        # Strict validation: Remove transactions without a valid date
        n_invalid = df['fecha_transaccion'].isna().sum()
        if n_invalid > 0:
            self.logger(f"Warning: Se descartaron {n_invalid} registros por formato de fecha inválido.", "WARN")
            df = df.dropna(subset=['fecha_transaccion'])

        # Automatic currency format detection and normalization (Latin/US)
        financial_columns = ['valor', 'saldo_contable', 'saldo_disponible']
        for col in financial_columns:
            if col in df.columns:
                df[col] = df[col].apply(parse_currency)

        return df

    def generate_hash_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate a deterministic SHA-256 identifier for each transaction
        based on a fingerprint of its key attributes.

        Args:
            df (pd.DataFrame): Sanitized DataFrame.

        Returns:
            pd.DataFrame: Same DataFrame with a new 'hash_id' column and
                          internal fingerprint columns dropped.
        """

        def _fmt_money(val: any) -> str:
            """
            Strict monetary formatter used to build hash inputs.
            Ensures values such as 100.5 are rendered as '100.50' so that
            logically equal amounts always produce the same hash.
            """
            if pd.isna(val) or val == '':
                return '0.00'
            try:
                return "{:.2f}".format(float(val))
            except (ValueError, TypeError):
                return str(val).strip()

        # 1. Building a Digital Footprint
        df['base_fingerprint'] = (
            df['fecha_transaccion'].astype(str).str.strip().replace('NaT', '') +
            df['referencia'].astype(str).str.strip().fillna('') +
            df['referencia2'].astype(str).str.strip().fillna('') +
            df['valor'].apply(_fmt_money) +
            df['saldo_contable'].apply(_fmt_money) +
            df['cod_transaccion'].astype(str).str.strip().fillna('')
        )
        
        # 2. Intra-archive duplicate ranking for twin transactions
        df['duplicate_rank'] = df.groupby('base_fingerprint').cumcount()
        
        # 3. Generation of the Final SHA-256 Hash
        df['hash_source'] = df['base_fingerprint'] + "_" + df['duplicate_rank'].astype(str)
        
        df['hash_id'] = df['hash_source'].apply(
            lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest()
        )
        
        # Cleaning temporary columns used for hashing
        return df.drop(columns=['base_fingerprint', 'duplicate_rank', 'hash_source'])