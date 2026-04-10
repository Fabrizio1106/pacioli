"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.enrichment.specific_text_enricher
===============================================================================

Description:
    Phase 2 enricher. Assigns customer identity using hardcoded text-prefix
    rules loaded from YAML configuration. Each rule maps a known bank_ref_2
    prefix to a specific customer ID and name. Confidence is always 100%.

Responsibilities:
    - Load specific_text_rules from YAML config.
    - Load pending transactions that have a non-null bank_ref_2.
    - Match bank_ref_2 against configured text prefixes (startswith).
    - Write customer identity to stg_bank_transactions in batch.

Key Components:
    - SpecificTextEnricher: Phase 2 enricher; deterministic text-prefix rules.

Notes:
    - Rule example: "DIVISION TARJET" → 400499 (BANCO BOLIVARIANO).
    - Match logic: bank_ref_2.upper().startswith(text_key.upper()).
    - First matching rule wins (order defined in YAML).
    - Confidence: 100%.

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


class SpecificTextEnricher:
    """
    Phase 2 enricher: deterministic customer assignment by text-prefix rules.

    Rules (from YAML specific_text_rules):
        "DIVISION TARJET" → 400499 (BANCO BOLIVARIANO)
        "KLM"             → 400068 (KLM CIA REAL HOLANDESA)
        "BUPA ECUADOR..."  → 402302 (BUPA ECUADOR)
        ...

    Match: bank_ref_2.startswith(text_key) — case-insensitive.
    """

    def __init__(self, engine_stg: Engine, config: dict):
        self.engine_stg = engine_stg
        self.config = config
        self.logger = get_logger("TEXT_ENRICHER")

    def enrich(self, engine_stg: Engine) -> int:
        """
        Enrich transactions using specific text-prefix rules.

        Returns:
            Number of transactions updated.
        """

        self.logger("Starting specific text rule enrichment...", "INFO")

        text_rules = self.config.get('specific_text_rules', {})

        if not text_rules:
            self.logger("No specific text rules found in config.", "INFO")
            return 0

        df_pending = self._load_pending_with_ref2()

        if df_pending.empty:
            self.logger("No pending transactions with bank_ref_2.", "INFO")
            return 0

        updates = []

        for _, row in df_pending.iterrows():
            bank_ref_2 = str(row.get('bank_ref_2', '') or '').strip()

            if not bank_ref_2:
                continue

            bank_ref_upper = bank_ref_2.upper()

            for text_key, rule in text_rules.items():
                if bank_ref_upper.startswith(text_key.upper()):
                    updates.append({
                        "stg_id": row['stg_id'],
                        "customer_id": rule['id'],
                        "customer_name": rule['name'],
                        "confidence": int(rule['confidence']),
                        "method": rule['method'],
                        "notes": f"Text rule: {text_key}"
                    })
                    break  # First matching rule wins

        if not updates:
            self.logger("No text rule matches found.", "INFO")
            return 0

        self._apply_updates(updates)
        self.logger(f"{len(updates)} transactions enriched by text rules.", "SUCCESS")

        return len(updates)

    def _load_pending_with_ref2(self) -> pd.DataFrame:
        """Load pending transactions that have a non-null bank_ref_2."""

        query = text("""
            SELECT stg_id, bank_ref_2
            FROM biq_stg.stg_bank_transactions
            WHERE bank_ref_2 IS NOT NULL
              AND bank_ref_2 != ''
              AND (enrich_confidence_score IS NULL
                   OR enrich_confidence_score < 99)
        """)

        return pd.read_sql(query, self.engine_stg)

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
