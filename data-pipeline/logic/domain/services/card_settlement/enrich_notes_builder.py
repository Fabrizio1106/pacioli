"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.card_settlement.enrich_notes_builder
===============================================================================

Description:
    Pure domain service that generates human-readable audit notes for card
    settlement reconciliation outcomes.

    Encapsulates the message-building logic previously embedded in
    UpdateBankValidationMetricsCommand._generate_enrich_notes_v30, which mixed
    string formatting concerns into the orchestrator.

Responsibilities:
    - Resolve a reason code to its message template via the injected config.
    - Format the template with settlement-specific metrics
      (settlement_id, diff, voucher counts, tolerance).
    - Append contextual suffixes for absorbed diffs and pending suggestions.

Key Components:
    - EnrichNotesBuilder: Stateless service initialized with the reason and
      message dictionaries from reconciliation_config.yaml. build() is a pure
      function over a pd.Series row.

Notes:
    - SQL-free by design.
    - Template keys in card_messages must accept named placeholders:
      {settlement_id}, {diff}, {bank_count}, {port_count}, {tolerance}.
      Missing keys are silently tolerated via try/except KeyError.
    - Suffix for absorbed diff is appended only when 0 < |diff_raw| <= tolerance.

Dependencies:
    - typing
    - pandas

===============================================================================
"""

from typing import Dict
import pandas as pd


class EnrichNotesBuilder:
    """
    Domain service for building reconciliation audit notes.

    Initialized with the reason-to-key map and message templates from YAML
    config so that note text remains configurable without code changes.

    Contains no direct SQL.
    """

    def __init__(self, card_reasons: Dict[str, str],
                 card_messages: Dict[str, str],
                 tolerance: float):
        """
        Args:
            card_reasons:  Mapping of reason keys → canonical reason codes,
                           e.g. {'perfect_match': 'CARD_PERFECT_MATCH', ...}.
            card_messages: Mapping of reason keys → message templates,
                           e.g. {'perfect_match': 'Card: Perfect match', ...}.
            tolerance:     Absorption tolerance; used for the diff-suffix guard.
        """
        self.card_messages = card_messages
        self.tolerance     = tolerance

        # Inverted map: canonical reason code → message key
        self._reason_to_key = {
            v: k for k, v in card_reasons.items() if v is not None
        }

    def build(self, row: pd.Series) -> str:
        """
        Build a human-readable audit note for a single settlement row.

        Steps:
            1. Resolve reason code → message key → template string.
            2. Format template with row metrics.
            3. Append diff-absorbed suffix when applicable.
            4. Append pending-suggestions suffix when applicable.

        Args:
            row: pd.Series with fields: reconcile_reason, settlement_id,
                 diff_raw, count_confirmed, count_suggestions,
                 count_voucher_bank.

        Returns:
            Formatted audit note string.
        """
        reason            = row['reconcile_reason']
        settlement_id     = row.get('settlement_id', 'N/A')
        diff              = abs(row['diff_raw'])
        confirmed_count   = row['count_confirmed']
        suggestions_count = row.get('count_suggestions', 0)
        bank_count        = row['count_voucher_bank']

        message_key = self._reason_to_key.get(reason, 'perfect_match')
        template    = self.card_messages.get(message_key, 'Card: Processed')

        try:
            message = template.format(
                settlement_id=settlement_id,
                diff=f"{diff:.2f}",
                bank_count=bank_count,
                port_count=confirmed_count,
                tolerance=f"{self.tolerance:.2f}",
            )
        except KeyError:
            message = f"Card: {reason} - Settlement {settlement_id}"

        if 0 < abs(row['diff_raw']) <= self.tolerance:
            message += f" - Diff ${row['diff_raw']:.2f} absorbed in commission"

        if suggestions_count > 0:
            message += f" - {suggestions_count} suggestion(s) pending"

        return message
