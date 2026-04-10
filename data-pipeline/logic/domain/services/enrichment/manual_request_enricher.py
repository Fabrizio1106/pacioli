"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.enrichment.manual_request_enricher
===============================================================================

Description:
    Phase 0 enricher (highest priority). Matches bank transactions against
    manually-loaded reconciliation requests from stg_manual_requests using
    exact reference match or suffix match. Manual matches carry 100% confidence
    and are never overwritten by subsequent enrichment phases.

Responsibilities:
    - Load active manual requests from biq_stg.stg_manual_requests.
    - Load pending bank transactions (confidence IS NULL or < 99).
    - Calculate exact and suffix matches between bank_ref_1 and manual bank_ref.
    - Write customer identity and match metadata back to stg_bank_transactions.

Key Components:
    - ManualRequestEnricher: Phase 0 enricher. Runs before all other enrichers.

Notes:
    - Exact match: bank_ref_1 == manual.bank_ref.
    - Suffix match: bank_ref_1.endswith(manual.bank_ref) for bank_ref >= 5 chars.
    - method values: MANUAL_MATCH_EXACT | MANUAL_MATCH_SUFFIX.
    - Duplicate manual references are deduplicated (first occurrence wins).

Dependencies:
    - pandas
    - sqlalchemy
    - utils.logger

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from utils.logger import get_logger


class ManualRequestEnricher:
    """
    Phase 0 enricher: highest-priority match using manually loaded requests.

    Matching strategy:
        1. Exact match: bank_ref_1 == manual.bank_ref.
        2. Suffix match: bank_ref_1.endswith(manual.bank_ref) for refs >= 5 chars.

    Confidence: 100% (operator-provided ground truth).
    Methods: MANUAL_MATCH_EXACT | MANUAL_MATCH_SUFFIX.

    Example:
        bank_ref_1    = "0012345678"
        manual.bank_ref = "2345678"
        → Suffix match accepted.
    """

    def __init__(self, engine_stg: Engine, config: dict):
        self.engine_stg = engine_stg
        self.config = config
        self.logger = get_logger("MANUAL_ENRICHER")

    def enrich(self, engine_stg: Engine) -> int:
        """
        Run Phase 0 enrichment using manual requests.

        Returns:
            Number of transactions updated.
        """

        self.logger("Starting manual request matching...", "INFO")

        # 1. Load manual requests
        df_manual = self._load_manual_requests()

        if df_manual.empty:
            self.logger("No manual requests loaded.", "INFO")
            return 0

        # Deduplicate by bank_ref — safety guard against duplicate manual entries
        if df_manual['bank_ref'].duplicated().any():
            dup_count = df_manual['bank_ref'].duplicated().sum()
            self.logger(
                f"{dup_count} duplicate references in manual requests. Keeping first.",
                "WARN"
            )
            df_manual = df_manual.drop_duplicates(subset=['bank_ref'], keep='first')

        # 2. Load pending bank transactions
        df_bank = self._load_pending_bank_refs()

        if df_bank.empty:
            self.logger("No pending bank transactions found.", "INFO")
            return 0

        # 3. Calculate matches
        updates = self._calculate_matches(df_bank, df_manual)

        if not updates:
            self.logger("No manual matches found.", "INFO")
            return 0

        # 4. Apply updates
        self._apply_updates(updates)
        self.logger(f"{len(updates)} transactions enriched with manual data.", "SUCCESS")

        return len(updates)

    def _load_manual_requests(self) -> pd.DataFrame:
        """Load manual reconciliation requests."""

        query = text("""
            SELECT customer_id, customer_name, bank_ref, details
            FROM biq_stg.stg_manual_requests
        """)

        return pd.read_sql(query, self.engine_stg)

    def _load_pending_bank_refs(self) -> pd.DataFrame:
        """Load bank transactions pending enrichment."""

        query = text("""
            SELECT stg_id, bank_ref_1
            FROM biq_stg.stg_bank_transactions
            WHERE (enrich_confidence_score IS NULL
                   OR enrich_confidence_score < 99)
        """)

        return pd.read_sql(query, self.engine_stg)

    def _calculate_matches(
        self,
        df_bank: pd.DataFrame,
        df_manual: pd.DataFrame
    ) -> list:
        """
        Match bank transactions against manual requests.

        For each bank transaction:
            1. Try exact match on bank_ref_1.
            2. If no exact match, try suffix match for manual refs >= 5 characters.

        Args:
            df_bank: Pending bank transactions with stg_id and bank_ref_1.
            df_manual: Manual requests indexed by bank_ref.

        Returns:
            List of update dicts ready for batch apply.
        """

        manual_dict = df_manual.set_index('bank_ref').to_dict('index')
        updates = []

        for _, row in df_bank.iterrows():
            bank_ref = str(row['bank_ref_1']).strip()

            if len(bank_ref) < 3:
                continue

            match_data = None
            method = None

            # Exact match
            if bank_ref in manual_dict:
                match_data = manual_dict[bank_ref]
                method = 'MANUAL_MATCH_EXACT'

            # Suffix match
            else:
                for manual_ref, data in manual_dict.items():
                    if len(str(manual_ref)) > 4 and bank_ref.endswith(str(manual_ref)):
                        match_data = data
                        method = 'MANUAL_MATCH_SUFFIX'
                        break

            if match_data:
                raw_detail = match_data.get('details') or ''
                note_text = f"Manual Match: {raw_detail}"[:499]

                updates.append({
                    "stg_id": row['stg_id'],
                    "customer_id": match_data['customer_id'],
                    "customer_name": match_data['customer_name'],
                    "confidence": 100,
                    "method": method,
                    "notes": note_text
                })

        return updates

    def _apply_updates(self, updates: list):
        """Apply enrichment updates in batch."""

        query = text("""
            UPDATE biq_stg.stg_bank_transactions
            SET enrich_customer_id = :customer_id,
                enrich_customer_name = :customer_name,
                enrich_confidence_score = :confidence,
                enrich_inference_method = :method,
                enrich_notes = :notes
            WHERE stg_id = :stg_id
        """)

        with self.engine_stg.begin() as conn:
            conn.execute(query, updates)
