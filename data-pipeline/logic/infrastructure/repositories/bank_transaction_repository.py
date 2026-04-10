"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.repositories.bank_transaction_repository
===============================================================================

Description:
    Repository for persisting and managing bank transactions within the 
    biq_stg.stg_bank_transactions table. It includes logic for preserving 
    reconciliation states during data refreshes.

Responsibilities:
    - Save bank transaction dataframes while preserving existing reconciliation states.
    - Delete records for specific periods to allow data updates.
    - Prepare and sanitize dataframes for database persistence.
    - Handle schema-specific configurations for PostgreSQL compatibility.

Key Components:
    - BankTransactionRepository: Main class for bank transaction persistence logic.

Notes:
    - Automatically filters columns to match the physical database schema.
    - Uses a list of persistent columns to ensure data integrity.
    - Implements state merging to prevent losing reconciliation progress.

Dependencies:
    - pandas
    - sqlalchemy
    - datetime
    - logic.infrastructure.repositories.base_repository
    - utils.logger

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from datetime import date
from logic.infrastructure.repositories.base_repository import BaseRepository
from utils.logger import get_logger


class BankTransactionRepository(BaseRepository):
    """
    Repository for biq_stg.stg_bank_transactions.
    Automatically filters columns to match the physical DB schema.
    """

    PERSISTENT_COLUMNS = [
        'etl_batch_id', 'source_system', 'doc_date', 'posting_date',
        'doc_number', 'doc_type', 'doc_reference', 'amount_total',
        'amount_sign', 'currency', 'sap_description', 'bank_date',
        'bank_ref_1', 'bank_ref_2', 'bank_description', 'bank_office_id',
        'trans_type', 'global_category', 'brand', 'batch_number',
        'match_hash_key', 'is_compensated_sap', 'is_compensated_intraday',
        'reconcile_status', 'settlement_id', 'establishment_name',
        'count_voucher_bank', 'count_voucher_portfolio', 'final_amount_gross',
        'final_amount_net', 'final_amount_commission', 'final_amount_tax_iva',
        'final_amount_tax_irf', 'diff_adjustment', 'reconcile_reason',
        'enrich_customer_id', 'enrich_customer_name', 'enrich_confidence_score',
        'enrich_inference_method', 'enrich_notes', 'match_confidence_score',
        'match_method', 'alternative_matches'
    ]

    def __init__(self, session_or_engine):
        # 1. Initialization
        super().__init__(session_or_engine)
        self.logger = get_logger("BANK_TX_REPOSITORY")

    def _get_table_name(self) -> str:
        return "biq_stg.stg_bank_transactions"

    def _get_primary_key(self) -> str:
        return "stg_id"

    def save_with_preservation(self, df: pd.DataFrame, start_date: date, end_date: date) -> int:
        """
        Saves bank transaction data while preserving existing reconciliation status for the period.
        """
        # 1. Preparation
        self.logger(f"Starting persistent save: {start_date} -> {end_date}", "INFO")
        df = self._prepare_dataframe(df)

        # 2. Connection Management
        if hasattr(self.session, 'connection'):
            conn = self.session.connection()
            close_conn = False
        else:
            conn = self.session.connect()
            close_conn = True

        # 3. State Preservation and Merge
        try:
            existing_state = self._preserve_reconciliation_state(conn, start_date, end_date)
            self._delete_period(conn, start_date, end_date)
            df = self._merge_preserved_state(df, existing_state)

            # 4. Column Filtering and Sanitization
            valid_columns = [col for col in self.PERSISTENT_COLUMNS if col in df.columns]
            df_to_persist = df[valid_columns].copy()

            # Normalize nulls for PostgreSQL compatibility
            df_to_persist = df_to_persist.where(pd.notnull(df_to_persist), None)

            # 5. Database Persistence
            df_to_persist.to_sql(
                name='stg_bank_transactions',
                con=conn,
                schema='biq_stg',
                if_exists='append',
                index=False,
                chunksize=1000,
            )

            self.logger(f"Saved {len(df_to_persist)} records", "SUCCESS")
            return len(df_to_persist)

        finally:
            if close_conn:
                conn.close()

    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensures mandatory columns exist in the dataframe."""
        if 'reconcile_status' not in df.columns:
            df['reconcile_status'] = 'PENDING'
        return df

    def _preserve_reconciliation_state(self, conn, start, end):
        """Retrieves existing non-pending reconciliation states for a period."""
        query = text("""
            SELECT match_hash_key, reconcile_status, settlement_id, match_method
            FROM biq_stg.stg_bank_transactions
            WHERE doc_date BETWEEN :start AND :end
              AND reconcile_status NOT IN ('PENDING', 'CLOSED_IN_SOURCE_SAP')
        """)
        try:
            return pd.read_sql(query, conn, params={"start": start, "end": end})
        except Exception:
            return pd.DataFrame()

    def _delete_period(self, conn, start, end):
        """Deletes records for a specific period to avoid duplicates."""
        query = text("""
            DELETE FROM biq_stg.stg_bank_transactions
            WHERE doc_date BETWEEN :start AND :end
        """)
        conn.execute(query, {"start": start, "end": end})

    def _merge_preserved_state(self, df, existing):
        """Merges existing reconciliation status back into the new data."""
        if existing.empty:
            return df

        # 1. Column Renaming for Merge
        existing = existing.rename(columns={
            'reconcile_status': 'status_old',
            'settlement_id':    'settlement_old',
            'match_method':     'method_old',
        })

        # 2. Merging Logic
        df = pd.merge(df, existing, on='match_hash_key', how='left')

        mask = df['status_old'].notna()
        df.loc[mask, 'reconcile_status'] = df['status_old']
        df.loc[mask, 'settlement_id']    = df['settlement_old']
        df.loc[mask, 'match_method']     = df['method_old']

        # 3. Cleanup
        cols_to_drop = ['status_old', 'settlement_old', 'method_old']
        df.drop(columns=[c for c in cols_to_drop if c in df.columns], inplace=True)

        return df
