"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.compensation.sap_compensation_handler
===============================================================================

Description:
    Domain service that assigns reconcile_status to SAP documents based on
    their compensation state. Combines SAP-level compensation (is_compensated_sap)
    and intraday compensation (is_compensated_intraday) flags to produce the
    final status for each document before staging.

Responsibilities:
    - Apply rule-based status assignment to all documents in the DataFrame.
    - Rule 1 (highest priority): is_compensated_sap = True
        -> reconcile_status = 'CLOSED_IN_SOURCE_SAP'
    - Rule 2: is_compensated_intraday = True (and not SAP-compensated)
        -> reconcile_status = 'COMPENSATED_INTRADAY'
    - Rule 3 (default): both flags False
        -> reconcile_status = 'PENDING'
    - Provide filtering (get_by_status) and statistics (get_statistics) helpers.

Key Components:
    - SAPCompensationHandler: Domain service; stateless, no DB access.
      Receives and returns a DataFrame.

Notes:
    - Documents with CLOSED_IN_SOURCE_SAP are stored in staging for hash
      sequence context but are not processed for bank reconciliation.
    - Documents with COMPENSATED_INTRADAY are excluded from reconciliation.
    - Only PENDING documents proceed to bank matching.
    - This service runs after IntradayCompensationDetector in the pipeline.

Dependencies:
    - pandas, typing
    - utils.logger

===============================================================================
"""

import pandas as pd
from typing import Dict, List
from utils.logger import get_logger


class SAPCompensationHandler:
    """
    Domain service for assigning reconcile_status based on SAP compensation flags.

    Status priority:
        1. CLOSED_IN_SOURCE_SAP:  is_compensated_sap = True.
        2. COMPENSATED_INTRADAY:  is_compensated_intraday = True (not SAP-compensated).
        3. PENDING:               both flags False (active, to be reconciled).

    All three categories are written to staging. CLOSED_IN_SOURCE_SAP documents
    provide the historical hash sequence context used by HistoricalContextService.
    """

    def __init__(self):
        self.logger = get_logger("SAP_COMPENSATION_HANDLER")

    def handle(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Assign reconcile_status to each document based on compensation flags.

        Rule priority (applied in order, no overwrite):
            1. is_compensated_sap = True  -> CLOSED_IN_SOURCE_SAP
            2. is_compensated_intraday = True -> COMPENSATED_INTRADAY
            3. Both False (default)        -> PENDING

        Args:
            df: DataFrame with is_compensated_sap (bool) and
                is_compensated_intraday (bool) columns.

        Returns:
            DataFrame with reconcile_status column added or updated.
        """
        self.logger(f"Classifying {len(df)} documents by compensation status", "INFO")

        df = df.copy()

        if 'reconcile_status' not in df.columns:
            df['reconcile_status'] = 'PENDING'

        # Rule 1: SAP-compensated (historical closure, highest priority)
        mask_sap = df['is_compensated_sap'] == True
        df.loc[mask_sap, 'reconcile_status'] = 'CLOSED_IN_SOURCE_SAP'

        # Rule 2: Intraday-compensated (same-day netting, only if not SAP-compensated)
        mask_intraday = (
            (df['is_compensated_sap'] == False) &
            (df['is_compensated_intraday'] == True)
        )
        df.loc[mask_intraday, 'reconcile_status'] = 'COMPENSATED_INTRADAY'

        # Rule 3: PENDING is the default (already initialized above)

        counts = df['reconcile_status'].value_counts()
        self.logger("Classification complete:", "SUCCESS")
        for status, count in counts.items():
            self.logger(f"   {status}: {count} documents", "INFO")

        return df

    def get_by_status(self, df: pd.DataFrame, status: str) -> pd.DataFrame:
        """
        Filter documents by reconcile_status.

        Args:
            df: DataFrame with reconcile_status column.
            status: One of 'CLOSED_IN_SOURCE_SAP', 'COMPENSATED_INTRADAY', 'PENDING'.

        Returns:
            Filtered copy of the DataFrame.
        """
        return df[df['reconcile_status'] == status].copy()

    def get_statistics(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Return classification counts for reporting.

        Returns:
            Dict with keys: total, closed_in_sap, compensated_intraday,
            pending, for_conciliation_count, for_context_count.
        """
        counts = df['reconcile_status'].value_counts()

        closed_sap = counts.get('CLOSED_IN_SOURCE_SAP', 0)
        comp_intraday = counts.get('COMPENSATED_INTRADAY', 0)
        pending = counts.get('PENDING', 0)

        return {
            'total': len(df),
            'closed_in_sap': closed_sap,
            'compensated_intraday': comp_intraday,
            'pending': pending,
            'for_conciliation_count': pending,
            'for_context_count': closed_sap + comp_intraday
        }

    def get_compensated_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Return a per-date, per-status summary for reporting.

        Args:
            df: DataFrame with reconcile_status and doc_date columns.

        Returns:
            DataFrame with columns: doc_date, reconcile_status, count, total_amount.
            Sorted by doc_date and reconcile_status.
        """
        summary = df.groupby(['doc_date', 'reconcile_status']).agg(
            count=('doc_number', 'count'),
            total_amount=('amount_total', 'sum')
        ).reset_index()

        return summary.sort_values(['doc_date', 'reconcile_status'])
