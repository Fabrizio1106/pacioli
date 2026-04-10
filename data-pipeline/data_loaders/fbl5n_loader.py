"""
===============================================================================
Project: PACIOLI
Module: data_loaders.fbl5n_loader
===============================================================================

Description:
    Implements FBL5NLoader for ingesting SAP FBL5N customer line-item
    exports. Operates as a full snapshot loader: the target table is
    truncated and replaced atomically with the latest extract on each run.

Responsibilities:
    - Read FBL5N Excel exports with dtype=str to preserve raw values.
    - Drop subtotal rows lacking an account and normalize dates/amounts.
    - Generate a stable MD5 hash id using account, invoice reference and
      general ledger account as the natural composite key.
    - Perform a snapshot load: truncate target and reinsert everything.

Key Components:
    - FBL5NLoader: Concrete BaseLoader implementation.
    - read_file: Excel reader with logging of row counts.
    - specific_business_rules: Column mapping, subtotal removal, type
      normalization and string sanitization.
    - generate_hash_id: MD5 hash over uppercased account/reference/ledger.
    - load_to_sql: Snapshot load via staging table and TRUNCATE + INSERT.

Notes:
    - load_to_sql overrides the base behavior: every run fully replaces
      the target table. Downstream consumers must treat the table as a
      point-in-time snapshot.
    - TRUNCATE uses RESTART IDENTITY CASCADE to reset SERIAL sequences.

Dependencies:
    - pandas, hashlib, datetime, sqlalchemy.text
    - data_loaders.base_loader (BaseLoader)
    - utils.parsers (parse_to_sql_date, parse_currency)

===============================================================================
"""

import pandas as pd
import hashlib
from sqlalchemy import text
from datetime import datetime
from data_loaders.base_loader import BaseLoader
from utils.parsers import parse_to_sql_date, parse_currency


class FBL5NLoader(BaseLoader):
    """
    Loader for SAP FBL5N customer line-item exports.

    Purpose:
        Ingest the accounts receivable snapshot from SAP FBL5N into the
        bronze layer, fully replacing the target table on each run.

    Responsibilities:
        - Read Excel exports preserving string values.
        - Apply column mapping and drop subtotal rows.
        - Normalize dates, amounts and key string columns.
        - Generate an MD5 hash_id as the natural composite key.
        - Atomically refresh the target table via TRUNCATE + staging INSERT.
    """

    def read_file(self, file_path: str) -> pd.DataFrame:
        header_row = self.config.get('header_row', 0)
        try:
            df = pd.read_excel(file_path, header=header_row, dtype=str)
            self.logger(f"   -> Leídas {len(df)} filas del archivo", "INFO")
            return df
        except Exception as e:
            self.logger(f"Error leyendo archivo: {e}", "ERROR")
            raise

    def specific_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:

        mapping = self.config.get('column_mapping', {})
        if mapping:
            df = df.rename(columns=mapping)

        if 'cuenta' in df.columns:
            df['cuenta'] = (
                df['cuenta'].astype(str).str.strip()
                .replace({'nan': None, 'None': None, '': None})
            )
            initial_count = len(df)
            df = df.dropna(subset=['cuenta'])
            discarded = initial_count - len(df)
            if discarded > 0:
                self.logger(
                    f"   -> 🗑️ {discarded} filas de subtotales eliminadas.", "INFO"
                )

        date_cols = ['fecha_documento', 'fecha_de_pago', 'fecha_compensacion']
        for col in date_cols:
            if col in df.columns:
                df[col] = df[col].apply(parse_to_sql_date)
                df[col] = pd.to_datetime(df[col], errors='coerce')

        if 'importe' in df.columns:
            df['importe'] = df['importe'].apply(parse_currency)

        str_cols = [
            'cuenta', 'asignacion', 'n_documento',
            'referencia_a_factura', 'referencia', 'cuenta_de_mayor',
        ]
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace({'nan': '', 'None': ''})

        return df

    def generate_hash_id(self, df: pd.DataFrame) -> pd.DataFrame:

        col_cuenta = 'cuenta'              if 'cuenta'              in df.columns else 'Cuenta'
        col_ref    = 'referencia_a_factura' if 'referencia_a_factura' in df.columns else 'Referencia a factura'
        col_mayor  = 'cuenta_de_mayor'     if 'cuenta_de_mayor'      in df.columns else 'Cuenta de mayor'

        if col_cuenta not in df.columns:
            return df

        df['hash_source'] = (
            df[col_cuenta].astype(str).str.strip().str.upper() + "_" +
            df[col_ref].astype(str).str.strip().str.upper()    + "_" +
            df[col_mayor].astype(str).str.strip().str.upper()
        )

        df['hash_id'] = df['hash_source'].apply(
            lambda x: hashlib.md5(x.encode()).hexdigest()
        )

        return df.drop(columns=['hash_source'])

    def load_to_sql(self, df: pd.DataFrame):
        """
        Snapshot load: truncate the target table and replace its content
        with the provided DataFrame.

        Args:
            df (pd.DataFrame): Transformed DataFrame. Empty DataFrames are
                               ignored to avoid wiping the target table.

        Side Effects:
            - Creates a unique staging table.
            - Executes TRUNCATE TABLE ... RESTART IDENTITY CASCADE.
            - Inserts every row from staging into the target table.
            - Drops the staging table in a finally block.

        Notes:
            PostgreSQL migration:
                1. DROP TEMPORARY TABLE -> DROP TABLE IF EXISTS (ordinary
                   staging table with a unique timestamp suffix).
                2. TRUNCATE TABLE -> TRUNCATE TABLE <t> RESTART IDENTITY.
                   RESTART IDENTITY resets SERIAL sequences in PostgreSQL.
                3. Cleanup in finally to avoid orphan staging tables.
        """

        if df.empty:
            return

        table      = self.config.get('target_table', 'biq_raw.raw_customer_portfolio')
        ts         = datetime.now().strftime('%Y%m%d%H%M%S%f')
        temp_table = f"_staging_{table.replace('.', '_')}_{ts}"

        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                # 1. Create staging table
                df.to_sql(
                    name=temp_table,
                    con=conn,
                    if_exists='replace',
                    index=False,
                )

                # 2. Snapshot: Empty destination table
                self.logger(f"Warning: Vaciando '{table}' para Snapshot.", "INFO")
                # Change: Restart Identity to reset SERIAL in PostgreSQL
                conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))

                # 3. Insert everything from staging
                cols_sql = ", ".join(f'"{c}"' for c in df.columns)
                conn.execute(text(f"""
                    INSERT INTO {table} ({cols_sql})
                    SELECT {cols_sql} FROM "{temp_table}"
                """))

                trans.commit()

                if hasattr(self, 'report'):
                    self.report['loaded'] = len(df)

                self.logger(f"Success: Snapshot FBL5N ({len(df)} filas).", "INFO")

            except Exception as e:
                trans.rollback()
                self.logger(f"Critical Error Snapshot FBL5N: {e}", "CRITICAL")
                raise

            finally:
                try:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{temp_table}"'))
                    conn.commit()
                except Exception:
                    pass


