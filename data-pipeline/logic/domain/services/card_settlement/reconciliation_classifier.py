"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.card_settlement.reconciliation_classifier
===============================================================================

Description:
    Pure domain service that classifies card settlement reconciliation outcomes
    into canonical reason codes and their corresponding statuses.

    Encapsulates two business decision tables that were previously embedded in
    UpdateBankValidationMetricsCommand:
        1. determine_reason(): maps settlement metrics to a reason code.
        2. determine_status(): maps a reason code to MATCHED / PENDING / REVIEW.

Responsibilities:
    - Evaluate diff_raw, voucher counts, establishment type, and split-payment
      flag to produce a single canonical reconciliation reason per settlement.
    - Map each reason to its workflow status without knowing how that status
      will be persisted.

Key Components:
    - ReconciliationClassifier: Stateless service initialized with the reason
      and message dictionaries from reconciliation_config.yaml. Both methods
      are pure functions over a pd.Series row.

Notes:
    - SQL-free by design.
    - Reason key constants (e.g. 'perfect_match') are resolved through the
      injected card_reasons dict, making the service config-driven without
      hardcoding string values.
    - establishment comparison is case-insensitive (uppercased internally).

Dependencies:
    - typing
    - pandas

===============================================================================
"""

from typing import Dict
import pandas as pd


class ReconciliationClassifier:
    """
    Domain service for classifying card settlement reconciliation outcomes.

    Initialized with the reason and status dictionaries from YAML config so
    that canonical reason codes remain configurable without code changes.

    Contains no direct SQL.
    """

    def __init__(self, card_reasons: Dict[str, str], tolerance: float):
        """
        Args:
            card_reasons: Mapping of reason keys to canonical reason codes,
                          e.g. {'perfect_match': 'CARD_PERFECT_MATCH', ...}.
            tolerance:    Maximum absolute diff qualifying for absorption.
        """
        self.card_reasons = card_reasons
        self.tolerance    = tolerance

        self._matched_reasons = {
            card_reasons.get('perfect_match'),
            card_reasons.get('split_payment'),
            card_reasons.get('amount_within_tolerance'),
        }
        self._pending_reasons = {
            card_reasons.get('no_portfolio_data'),
            card_reasons.get('zero_invoices'),
            card_reasons.get('only_suggestions'),
        }

    def determine_reason(self, row: pd.Series) -> str:
        """
        Classify a settlement row into a canonical reconciliation reason code.

        Decision tree (in priority order):
            1. No confirmed invoices + no suggestions → no_portfolio_data
            2. No confirmed invoices + has suggestions → only_suggestions
            3. Has suggestions + diff > tolerance     → has_suggestions
            4. Is split payment + diff <= tolerance   → split_payment
            5. By establishment type and diff bands:
               PARKING / URBAPARKING: perfect_match, within_tolerance,
                                      mismatch_small, mismatch_large
               SALAS VIP / ASISTENCIAS: additionally considers voucher count
               Other: within_tolerance or mismatch_small

        Args:
            row: pd.Series with fields: count_confirmed, total_cartera,
                 count_suggestions, diff_raw, count_voucher_bank,
                 count_confirmed, establishment_name, is_split_payment.

        Returns:
            Canonical reason code string from card_reasons config.
        """
        r = self.card_reasons

        if row['count_confirmed'] == 0 and row['total_cartera'] == 0:
            if row.get('count_suggestions', 0) > 0:
                return r.get('only_suggestions', 'CARD_ONLY_SUGGESTIONS')
            return r.get('no_portfolio_data', 'CARD_NO_PORTFOLIO_DATA')

        if row['count_confirmed'] == 0 and row['count_suggestions'] > 0:
            return r.get('only_suggestions', 'CARD_ONLY_SUGGESTIONS')

        diff              = abs(row['diff_raw'])
        bank_count        = row['count_voucher_bank']
        confirmed_count   = row['count_confirmed']
        suggestions_count = row.get('count_suggestions', 0)
        establishment     = row.get('establishment_name', '').upper()
        is_split          = row.get('is_split_payment', False)

        if suggestions_count > 0 and diff > self.tolerance:
            return r.get('has_suggestions', 'CARD_HAS_SUGGESTIONS')

        if is_split and diff <= self.tolerance:
            return r.get('split_payment', 'CARD_SPLIT_PAYMENT')

        if establishment in ('PARKING', 'URBAPARKING'):
            if diff == 0:
                return r.get('perfect_match',           'CARD_PERFECT_MATCH')
            elif diff <= self.tolerance:
                return r.get('amount_within_tolerance', 'CARD_AMOUNT_WITHIN_TOLERANCE')
            elif diff <= self.tolerance * 2:
                return r.get('amount_mismatch_small',   'CARD_AMOUNT_MISMATCH_SMALL')
            else:
                return r.get('amount_mismatch_large',   'CARD_AMOUNT_MISMATCH_LARGE')

        elif establishment in ('SALAS VIP', 'ASISTENCIAS'):
            if diff == 0 and bank_count == confirmed_count:
                return r.get('perfect_match',            'CARD_PERFECT_MATCH')
            elif diff <= self.tolerance and bank_count != confirmed_count:
                return r.get('voucher_count_mismatch',   'CARD_VOUCHER_COUNT_MISMATCH')
            elif diff <= self.tolerance and bank_count == confirmed_count:
                return r.get('amount_within_tolerance',  'CARD_AMOUNT_WITHIN_TOLERANCE')
            elif diff <= self.tolerance * 2:
                return r.get('amount_mismatch_small',    'CARD_AMOUNT_MISMATCH_SMALL')
            else:
                return r.get('amount_mismatch_large',    'CARD_AMOUNT_MISMATCH_LARGE')

        else:
            if diff <= self.tolerance:
                return r.get('amount_within_tolerance', 'CARD_AMOUNT_WITHIN_TOLERANCE')
            else:
                return r.get('amount_mismatch_small',   'CARD_AMOUNT_MISMATCH_SMALL')

    def determine_status(self, reason: str) -> str:
        """
        Map a canonical reason code to its reconciliation workflow status.

        Returns:
            'MATCHED'  — perfect match, split payment, or within tolerance.
            'PENDING'  — no portfolio data, zero invoices, or suggestions only.
            'REVIEW'   — all other cases (mismatches, unresolved suggestions).
        """
        if reason in self._matched_reasons:
            return 'MATCHED'
        if reason in self._pending_reasons:
            return 'PENDING'
        return 'REVIEW'
