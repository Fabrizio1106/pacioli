"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.customer_portfolio_sync_service
===============================================================================

Description:
    Service for delta synchronization between the raw SAP portfolio snapshot 
    and the incremental staging environment. It ensures that the staging 
    layer reflects current SAP statuses while preserving matching history.

Responsibilities:
    - Detect new documents in SAP and insert them into staging.
    - Identify documents closed in SAP and mark them as such in staging.
    - Synchronize balance changes for existing documents (e.g., partial payments).
    - Provide data cleaning and column mapping for raw SAP data.

Key Components:
    - CustomerPortfolioSyncService: Main logic class for delta synchronization.

Notes:
    - invoice_ref is treated as an immutable identifier for the original invoice.
    - Uses a $0.01 tolerance for balance change detection to handle rounding.

Dependencies:
    - pandas, numpy, utils.logger
===============================================================================
"""

import pandas as pd
import numpy as np
from utils.logger import get_logger


class CustomerPortfolioSyncService:
    """
    Service for delta synchronization of the Customer Portfolio.
    
    This service manages synchronization between the raw snapshot and the
    incremental staging layer using invoice_ref as the primary immutable identifier.
    """

    def __init__(self, config: dict):
        """
        Initializes the sync service.

        Parameters:
        -----------
        config : dict
            Configuration rules from YAML.
        """
        self.config = config
        self.logger = get_logger("PORTFOLIO_SYNC")
        self.column_mapping = config.get('column_mapping', {})

    def sync_with_sap_raw(
        self,
        df_raw: pd.DataFrame,
        df_stg_current: pd.DataFrame,
        repo
    ) -> dict:
        """
        Executes the full delta sync process.

        Algorithm:
        1. Clean and map raw SAP data.
        2. Detect NEW documents (in raw, NOT in stg).
        3. Detect CLOSED documents (in stg, NOT in raw).
        4. Detect BALANCE CHANGES (in both, but different amounts).
        5. Apply changes using the repository.

        Parameters:
        -----------
        df_raw         : DataFrame from biq_raw.raw_customer_portfolio.
        df_stg_current : DataFrame from biq_stg.stg_customer_portfolio (active records).
        repo           : CustomerPortfolioRepository instance.

        Returns:
        --------
        dict containing statistics (new_count, closed_count, updated_count).
        """

        self.logger("Executing SAP Delta Sync...", "INFO")

        # 1. Clean and map RAW data
        df_raw = self._clean_and_map_raw(df_raw)

        # 2. Detect NEW documents
        new_docs = self._detect_new_documents(df_raw, df_stg_current)

        if not new_docs.empty:
            new_count = repo.insert_new_documents(new_docs)
            self.logger(f"   → {new_count} new documents inserted", "INFO")
        else:
            new_count = 0

        # 3. Detect CLOSED documents
        closed_refs = self._detect_closed_documents(df_raw, df_stg_current)

        if closed_refs:
            closed_count = repo.mark_as_closed_sap(closed_refs)
            self.logger(f"   → {closed_count} documents marked as CLOSED_SAP", "INFO")
        else:
            closed_count = 0

        # 4. Detect BALANCE CHANGES
        changed_docs = self._detect_balance_changes(df_raw, df_stg_current)

        if not changed_docs.empty:
            updated_count = repo.update_balances(changed_docs)
            self.logger(f"   → {updated_count} balances updated", "INFO")
        else:
            updated_count = 0

        return {
            'new_count': new_count,
            'closed_count': closed_count,
            'updated_count': updated_count
        }

    def _detect_new_documents(
        self,
        df_raw: pd.DataFrame,
        df_stg: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Identifies documents present in SAP but missing in staging.
        """
        raw_refs = set(df_raw['invoice_ref'].astype(str))
        stg_refs = set(df_stg['invoice_ref'].astype(str))

        new_refs = raw_refs - stg_refs

        if not new_refs:
            return pd.DataFrame()

        df_new = df_raw[
            df_raw['invoice_ref'].astype(str).isin(new_refs)
        ].copy()

        return df_new

    def _detect_closed_documents(
        self,
        df_raw: pd.DataFrame,
        df_stg: pd.DataFrame
    ) -> list:
        """
        Identifies documents in staging that are no longer in the SAP snapshot.
        """
        raw_refs = set(df_raw['invoice_ref'].astype(str))
        stg_refs = set(df_stg['invoice_ref'].astype(str))

        closed_refs = stg_refs - raw_refs

        return list(closed_refs) if closed_refs else []

    def _detect_balance_changes(
        self,
        df_raw: pd.DataFrame,
        df_stg: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Identifies documents where the outstanding balance has changed in SAP.
        """
        # 1. Merge by invoice_ref
        merged = pd.merge(
            df_stg[['invoice_ref', 'amount_outstanding']],
            df_raw[['invoice_ref', 'amount_outstanding']],
            on='invoice_ref',
            how='inner',
            suffixes=('_stg', '_raw')
        )

        # 2. Convert to numeric
        merged['amount_outstanding_stg'] = pd.to_numeric(merged['amount_outstanding_stg'], errors='coerce').fillna(0.0)
        merged['amount_outstanding_raw'] = pd.to_numeric(merged['amount_outstanding_raw'], errors='coerce').fillna(0.0)

        # 3. Detect differences with $0.01 tolerance
        mask_changed = ~np.isclose(
            merged['amount_outstanding_stg'],
            merged['amount_outstanding_raw'],
            atol=0.01
        )

        changed_docs = merged[mask_changed].copy()

        if changed_docs.empty:
            return pd.DataFrame()

        return changed_docs[['invoice_ref', 'amount_outstanding_raw']].rename(
            columns={'amount_outstanding_raw': 'amount_outstanding_new'}
        )

    def _clean_and_map_raw(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans and maps RAW SAP columns to staging format.
        """
        # 1. Cleaning
        df = df_raw.copy()
        df = self._clean_all_strings(df)

        # 2. Column Mapping
        df = df.rename(columns=self.column_mapping)

        # 3. Numeric Conversion
        if 'amount_outstanding' in df.columns:
            df['amount_outstanding'] = pd.to_numeric(df['amount_outstanding'], errors='coerce').fillna(0.0)

        # 4. Derived Columns
        if 'texto' in df_raw.columns and 'sap_text' not in df.columns:
            df['sap_text'] = df_raw['texto']

        if 'cuenta_de_mayor' in df_raw.columns and 'gl_account' not in df.columns:
            df['gl_account'] = df_raw['cuenta_de_mayor']

        if 'referencia' in df_raw.columns and 'internal_ref' not in df.columns:
            df['internal_ref'] = df_raw['referencia']

        return df

    @staticmethod
    def _clean_all_strings(df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes all string columns in the DataFrame."""
        df = df.copy()
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = df[col].astype(str).str.strip().str.upper()
                    df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
                except:
                    pass
        return df
