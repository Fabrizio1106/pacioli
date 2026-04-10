"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.enrichment.card_enricher
===============================================================================

Description:
    Phase 1 enricher. Assigns customer identity to card settlement transactions
    (trans_type = 'LIQUIDACION TC') using deterministic brand-to-customer rules
    loaded from YAML configuration. Confidence is always 100%.

Responsibilities:
    - Load pending card transactions from biq_stg.stg_bank_transactions.
    - Match each transaction's brand against YAML card_rules.
    - Write enrich_customer_id, enrich_customer_name, enrich_confidence_score,
      enrich_inference_method, and enrich_notes back to the staging table.

Key Components:
    - CardEnricher: Phase 1 enricher; brand-matching only; no fuzzy logic.

Notes:
    - Brand rules example: VISA → 400487, AMEX → 400490, PACIFICARD → 400489.
    - DINERS CLUB supports dual customer IDs (pipe-separated); the first is used.
    - Only targets records with enrich_confidence_score IS NULL or < 99.

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


class CardEnricher:
    """
    Phase 1 enricher: deterministic customer assignment by card brand.

    Brand rules (from YAML card_rules):
        PACIFICARD  → 400489 (MASTERCARD PACIFICO)
        AMEX        → 400490 (AMERICAN EXPRESS)
        VISA        → 400487 (DINERS CLUB ECUADOR)
        DINERS CLUB → 400487 | 400492 (DINERS CLUB ECUADOR)

    Filter: applies only to trans_type = 'LIQUIDACION TC'.
    """

    def __init__(self, engine_stg: Engine, config: dict):
        self.engine_stg = engine_stg
        self.config = config
        self.logger = get_logger("CARD_ENRICHER")

    def enrich(self, engine_stg: Engine) -> int:
        """
        Enrich card transactions by brand rule.

        Returns:
            Number of transactions updated.
        """

        self.logger("Starting card enrichment...", "INFO")

        card_rules = self.config.get('card_rules', {})
        target_type = card_rules.get('target_trans_type', 'LIQUIDACION TC')
        brands = card_rules.get('brands', {})

        if not brands:
            self.logger("No card brand rules found in config.", "WARN")
            return 0

        df_cards = self._load_pending_cards(target_type)

        if df_cards.empty:
            self.logger("No pending card transactions found.", "INFO")
            return 0

        updates = []

        for _, row in df_cards.iterrows():
            brand = str(row.get('brand', '')).strip().upper()

            if brand not in brands:
                continue

            rule = brands[brand]

            # DINERS CLUB may have dual IDs (pipe-separated); use the first
            customer_id = str(rule['customer_id']).split('|')[0]

            updates.append({
                "stg_id": row['stg_id'],
                "customer_id": customer_id,
                "customer_name": rule['customer_name'],
                "confidence": int(rule['confidence']),
                "method": rule['method'],
                "notes": f"Brand rule: {brand}"
            })

        if not updates:
            self.logger("No recognized brands in pending transactions.", "INFO")
            return 0

        self._apply_updates(updates)
        self.logger(f"{len(updates)} card transactions enriched.", "SUCCESS")

        return len(updates)

    def _load_pending_cards(self, target_type: str) -> pd.DataFrame:
        """Load card transactions pending enrichment."""

        query = text("""
            SELECT stg_id, brand, trans_type
            FROM biq_stg.stg_bank_transactions
            WHERE trans_type = :trans_type
              AND (enrich_confidence_score IS NULL
                   OR enrich_confidence_score < 99)
        """)

        return pd.read_sql(query, self.engine_stg, params={"trans_type": target_type})

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
