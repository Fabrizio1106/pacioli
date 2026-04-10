"""
===============================================================================
Project: PACIOLI
Module: data_loaders.sap_239_loader
===============================================================================

Description:
    Implements SapLoader for ingesting SAP account 239 line-item exports.
    Overrides the base load_to_sql with a true UPSERT (INSERT ... ON
    CONFLICT DO UPDATE) because SAP records contain mutable fields
    (status_partida, asignacion, texto, doc_compensacion) that must be
    refreshed while preserving the historical composite key.

Responsibilities:
    - Read SAP extract as strings, preserving leading zeros and codes.
    - Drop rows missing the document number or document class.
    - Normalize the posting date and the amount in local currency.
    - Default 'sociedad' to '8000' when missing (single-company deployment).
    - Truncate 'status_partida' to the first 20 characters.
    - Generate a SHA-256 hash id from the SAP composite key.
    - Upsert into the target table updating mutable fields via EXCLUDED.

Key Components:
    - SapLoader: Concrete BaseLoader implementation with UPSERT semantics.
    - load_to_sql: Overrides the base loader to execute a true upsert.

Notes:
    - 'upsert_conflict_column' in the YAML selects the conflict target;
      defaults to 'hash_id' when not configured.
    - The staging table uses a unique timestamp suffix and is dropped in
      a finally block.
    - 'loaded_at' is refreshed to CURRENT_TIMESTAMP on every matching
      upsert row.

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
from .base_loader import BaseLoader
from utils.parsers import parse_to_sql_date, parse_currency


class SapLoader(BaseLoader):
    """
    Loader for SAP account 239 line-item exports.

    Purpose:
        Ingest SAP FBL3N/FBL5N-style extracts for the transitional bank
        account 239 using UPSERT semantics: immutable keys remain stable
        while mutable SAP attributes are refreshed on every run.

    Responsibilities:
        - Filter out rows without a document number or class.
        - Normalize posting dates and local-currency amounts.
        - Default 'sociedad' to '8000' when the column is absent.
        - Generate a SHA-256 hash from the SAP composite key.
        - UPSERT the DataFrame into the target table updating only the
          configured mutable fields.
    """

    def read_file(self, file_path):
        header_row = self.config.get('header_row', 0)
        return pd.read_excel(file_path, header=header_row, dtype=str)

    def specific_business_rules(self, df):

        df = df[
            df['num_documento'].notna() &
            (df['num_documento'].str.strip() != '')
        ].copy()

        if 'clase_documento' in df.columns:
            df = df[df['clase_documento'].notna()]

        df['fecha_documento'] = df['fecha_documento'].apply(parse_to_sql_date)
        df['importe_ml']      = df['importe_ml'].apply(parse_currency)

        if 'sociedad' not in df.columns:
            df['sociedad'] = '8000'

        if 'status_partida' in df.columns:
            df['status_partida'] = df['status_partida'].astype(str).str.slice(0, 20)

        self.report['rejected'] = self.report['total'] - len(df)
        return df

    def generate_hash_id(self, df):

        df['hash_source'] = (
            df['sociedad'].astype(str).fillna('')        +
            df['num_documento'].astype(str).fillna('')   +
            df['ejercicio'].astype(str).fillna('')       +
            df['posicion'].astype(str).fillna('')        +
            df['clase_documento'].astype(str).fillna('') +
            df['importe_ml'].astype(str).fillna('')
        )

        df['hash_id'] = df['hash_source'].apply(
            lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest()
        )

        return df.drop(columns=['hash_source'])

    def load_to_sql(self, df):
        """
        UPSERT SAP rows inserting new ones and updating mutable fields.

        Args:
            df (pd.DataFrame): Transformed DataFrame ready for load.

        Side Effects:
            - Creates a unique staging table.
            - Executes INSERT ... ON CONFLICT (<key>) DO UPDATE SET ...
              refreshing mutable SAP attributes and 'loaded_at'.
            - Drops the staging table in the finally block.

        Notes:
            PostgreSQL migration:
                MySQL      : ON DUPLICATE KEY UPDATE col = VALUES(col)
                PostgreSQL : ON CONFLICT (conflict_col) DO UPDATE SET
                             col = EXCLUDED.col
            EXCLUDED is PostgreSQL's pseudo-table holding the row that
            generated the conflict. The conflict column is read from the
            YAML as 'upsert_conflict_column'; if absent, 'hash_id' is used.
        """

        table           = self.config['target_table']
        conflict_col    = self.config.get('upsert_conflict_column', 'hash_id')
        ts              = datetime.now().strftime('%Y%m%d%H%M%S%f')
        temp_table      = f"_staging_{table.replace('.', '_')}_{ts}"

        # Columns that are updated in case of conflict (SAP mutable fields)
        update_cols = [
            c for c in ['doc_compensacion', 'status_partida', 'asignacion', 'texto']
            if c in df.columns
        ]

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

                cols_sql = ", ".join(f'"{c}"' for c in df.columns)

                # 2. Construct a SET clause for the DO UPDATE
                set_clause = ", ".join(
                    f'"{c}" = EXCLUDED."{c}"' for c in update_cols
                )
                # We always update loaded_at in the upsert
                if set_clause:
                    set_clause += ', "loaded_at" = CURRENT_TIMESTAMP'
                else:
                    set_clause = '"loaded_at" = CURRENT_TIMESTAMP'

                query = text(f"""
                    INSERT INTO {table} ({cols_sql})
                    SELECT {cols_sql} FROM "{temp_table}"
                    ON CONFLICT ("{conflict_col}") DO UPDATE SET
                        {set_clause}
                """)

                res = conn.execute(query)
                trans.commit()

                self.report['loaded'] = res.rowcount
                self.logger(
                    f"Success: UPSERT SAP 239 completado ({res.rowcount} filas).", "INFO"
                )

            except Exception as e:
                trans.rollback()
                self.logger(f"Critical SQL Error en SAP Upsert: {e}", "CRITICAL")
                raise

            finally:
                try:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{temp_table}"'))
                    conn.commit()
                except Exception:
                    pass


