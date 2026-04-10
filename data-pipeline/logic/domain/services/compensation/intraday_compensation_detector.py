"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.compensation.intraday_compensation_detector
===============================================================================

Description:
    Domain service that detects SAP ZR documents that are offset by a
    non-ZR document (DZ, RV, etc.) on the same day with the same bank
    reference and amount. These are "intraday compensations" — movements
    that net to zero within a single day and do not represent real bank flows.

Responsibilities:
    - Separate compensator documents (non-ZR) from the input DataFrame.
    - Build an O(1) lookup set of (bank_ref_1, amount_total, doc_date) keys
      from the compensator documents.
    - Mark ZR documents as is_compensated_intraday = True when their key
      matches a compensator, and they are not already compensated in SAP.
    - Provide a get_compensated_summary() method for reporting.

Key Components:
    - IntradayCompensationDetector: Domain service; stateless, no DB access.
      Receives and returns a DataFrame.

Notes:
    - Only ZR documents are evaluated and potentially marked as compensated.
      Non-ZR documents always receive is_compensated_intraday = False.
    - If a ZR already has is_compensated_sap = True, it is not marked as
      intraday (the compensation is historical, not same-day).
    - Intraday-compensated ZRs are stored in staging for historical context
      but are excluded from active reconciliation.

Dependencies:
    - pandas
    - utils.logger

===============================================================================
"""

import pandas as pd
from utils.logger import get_logger


class IntradayCompensationDetector:
    """
    Domain service for detecting same-day offsetting SAP movements.

    Detection criteria for a ZR to be marked as intraday-compensated:
        1. A non-ZR document (DZ, RV, etc.) exists in the same DataFrame.
        2. It shares the same bank_ref_1.
        3. It shares the same amount_total.
        4. It shares the same doc_date.
        5. The ZR does not already have is_compensated_sap = True.

    Only ZR documents are marked; non-ZR documents always get
    is_compensated_intraday = False (they are filtered out before staging).
    """

    def __init__(self):
        self.logger = get_logger("INTRADAY_DETECTOR")

    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect intraday-compensated documents and add is_compensated_intraday column.

        Steps:
            1. Separate non-ZR documents (potential compensators).
            2. Build a set of (bank_ref_1, amount_total, doc_date) keys from compensators.
            3. For each ZR, check if its key is in the compensator set.
            4. Mark matched ZRs as is_compensated_intraday = True.

        Args:
            df: Transformed SAP DataFrame containing doc_type, bank_ref_1,
                amount_total, doc_date, and is_compensated_sap columns.

        Returns:
            DataFrame with is_compensated_intraday column added.
            Non-ZR records always receive False.
        """
        self.logger(f"Detecting intraday compensations in {len(df)} records", "INFO")

        df = df.copy()

        compensators = df[df['doc_type'] != 'ZR'].copy()

        if compensators.empty:
            self.logger("No compensator documents (DZ, RV, etc.) found", "INFO")
            df['is_compensated_intraday'] = False
            return df

        self.logger(f"Found {len(compensators)} compensator documents", "INFO")

        # O(1) lookup set: (bank_ref_1, amount_total, doc_date)
        compensator_keys = set(
            zip(
                compensators['bank_ref_1'],
                compensators['amount_total'],
                compensators['doc_date']
            )
        )

        self.logger(f"Built {len(compensator_keys)} unique compensator keys", "INFO")

        def check_intraday_match(row):
            # Only ZR documents are evaluated
            if row['doc_type'] != 'ZR':
                return False

            # Already SAP-compensated means it is a historical closure, not intraday
            if row.get('is_compensated_sap', False):
                return False

            key = (row.get('bank_ref_1'), row.get('amount_total'), row.get('doc_date'))
            return key in compensator_keys

        df['is_compensated_intraday'] = df.apply(check_intraday_match, axis=1)

        compensated_count = df['is_compensated_intraday'].sum()
        self.logger(f"Intraday compensations detected: {compensated_count}", "SUCCESS")

        if compensated_count > 0:
            by_type = df[df['is_compensated_intraday']]['doc_type'].value_counts()
            self.logger(f"   By type: {dict(by_type)}", "INFO")

        return df

    def get_compensated_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Return a per-day summary of intraday-compensated records.

        Args:
            df: DataFrame with is_compensated_intraday column already populated.

        Returns:
            DataFrame with columns: doc_date, compensated_count, compensated_amount.
            Empty DataFrame (with correct columns) if no compensations found.
        """
        compensated = df[df['is_compensated_intraday']].copy()

        if compensated.empty:
            return pd.DataFrame(
                columns=['doc_date', 'compensated_count', 'compensated_amount']
            )

        summary = compensated.groupby('doc_date').agg(
            compensated_count=('doc_type', 'count'),
            compensated_amount=('amount_total', 'sum')
        ).reset_index()

        return summary.sort_values('doc_date')
