"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.repositories.withholdings_repository
===============================================================================

Description:
    Repository for managing withholdings in the staging layer. It implements
    a safe row-by-row insertion strategy to handle duplicates and errors
    gracefully during bulk loads.

Responsibilities:
    - Save withholding records from a DataFrame into the staging table.
    - Rename and filter columns to match the target schema.
    - Perform chunked insertion with explicit error and duplicate handling.
    - Provide statistics on inserted, duplicated, and failed records.

Key Components:
    - WithholdingsRepository: Data access class for the stg_withholdings table.

Notes:
    - Target Table: biq_stg.stg_withholdings.
    - Strategy: Row-by-row insertion within chunks to capture duplicate key violations.

Dependencies:
    - pandas
    - sqlalchemy
    - logic.infrastructure.repositories.base_repository

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from logic.infrastructure.repositories.base_repository import BaseRepository
from utils.logger import get_logger


class WithholdingsRepository(BaseRepository):
    """
    Repository for biq_stg.stg_withholdings.
    Strategy: safe row-by-row insertion in chunks of 100.
    """

    _SCHEMA     = "biq_stg"
    _TABLE_NAME = "stg_withholdings"

    VALID_COLUMNS = [
        'hash_id', 'withholding_ref', 'clave_acceso',
        'customer_ruc', 'customer_name_raw', 'customer_name_normalized',
        'invoice_ref_sustento', 'invoice_ref_clean', 'invoice_series',
        'base_ret_renta', 'porcentaje_ret_renta', 'valor_ret_renta',
        'base_ret_iva', 'porcentaje_ret_iva', 'valor_ret_iva',
        'fecha_emision_ret', 'fecha_autorizacion_ret', 'periodo_fiscal',
        'rise', 'rimpe', 'agente_retencion', 'obligado_contabilidad',
        'contribuyente_especial', 'eligibility_status', 'ineligibility_reasons',
        'reconcile_status', 'validation_status', 'validation_errors',
        'is_registrable', 'etl_version', 'created_at',
        'match_confidence', 'source_batch_id',
    ]

    def __init__(self, session: Session):
        self.table_name = f"{self._SCHEMA}.{self._TABLE_NAME}"
        super().__init__(session)
        self.logger = get_logger("WITHHOLDINGS_REPO")

    def _get_table_name(self) -> str:
        return self.table_name

    def _get_primary_key(self) -> str:
        return "stg_id"

    # ─────────────────────────────────────────────────────────────────────────

    def save_withholdings(self, df: pd.DataFrame) -> dict:
        # 1. Validation
        if df.empty:
            self.logger("Empty DataFrame, nothing to save", "WARN")
            return {'inserted': 0, 'duplicates': 0, 'errors': 0, 'total': 0}

        # 2. Data preparation
        self.logger(f"Saving {len(df)} withholdings", "INFO")

        df_renamed  = self._rename_columns(df)
        df_insert   = self._filter_valid_columns(df_renamed)
        df_insert   = df_insert.where(pd.notnull(df_insert), None)
        
        # 3. Data persistence
        stats       = self._insert_in_chunks(df_insert)
        self._report_stats(stats)

        return stats

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # 1. Mapping
        rename_map = {
            'razon_social_emisor': 'customer_name_raw',
            'ruc_emisor':          'customer_ruc',
            'num_comp_sustento':   'invoice_ref_sustento',
            'batch_id':            'source_batch_id',
        }
        return df.rename(columns=rename_map)

    def _filter_valid_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # 1. Filtering
        available  = [c for c in self.VALID_COLUMNS if c in df.columns]
        df_filtered = df[available].copy()
        self.logger(
            f"Valid columns: {len(available)}/{len(self.VALID_COLUMNS)}", "INFO"
        )
        return df_filtered

    def _insert_in_chunks(self, df: pd.DataFrame) -> dict:
        # 1. Chunked processing
        chunk_size       = 100
        total_inserted   = 0
        total_duplicates = 0
        total_errors     = 0

        conn = self.session.connection()

        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size]

            # 2. Row iteration
            for _, row in chunk.iterrows():
                sp = conn.begin_nested()
                try:
                    # CHANGE: separate schema kwarg — cannot pass
                    # "schema.table" as name in to_sql with PostgreSQL.
                    pd.DataFrame([row]).to_sql(
                        name=self._TABLE_NAME,
                        con=conn,
                        schema=self._SCHEMA,
                        if_exists='append',
                        index=False,
                    )
                    sp.commit()
                    total_inserted += 1

                except Exception as e:
                    sp.rollback()
                    err_msg = str(e).lower()

                    # CRITICAL CHANGE: duplicate detection
                    # MySQL:      "duplicate entry"
                    # PostgreSQL: "duplicate key value violates unique constraint"
                    # We use "unique constraint" as substring — covers both
                    # drivers and is less fragile than the full message.
                    if "unique constraint" in err_msg or "duplicate key" in err_msg:
                        total_duplicates += 1
                    else:
                        total_errors += 1
                        if total_errors <= 3:
                            self.logger(
                                f"Insertion error: {str(e)[:100]}", "ERROR"
                            )

            processed = min(i + chunk_size, len(df))
            if processed % 200 == 0 or processed == len(df):
                self.logger(f"   → {total_inserted} inserted...", "INFO")

        return {
            'inserted':   total_inserted,
            'duplicates': total_duplicates,
            'errors':     total_errors,
            'total':      len(df),
        }

    def _report_stats(self, stats: dict):
        # 1. Statistics reporting
        self.logger(f"Inserted: {stats['inserted']}", "SUCCESS")

        if stats['duplicates'] > 0:
            self.logger(f"Duplicates omitted: {stats['duplicates']}", "WARN")

        if stats['errors'] > 0:
            self.logger(f"Errors: {stats['errors']}", "ERROR")
