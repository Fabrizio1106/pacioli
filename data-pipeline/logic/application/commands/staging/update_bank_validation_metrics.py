"""
===============================================================================
Project: PACIOLI
Module: logic.application.commands.staging.update_bank_validation_metrics
===============================================================================

Description:
    Command to update validation metrics for bank transactions, specifically
    targeting Card Settlements. Orchestrates the full pipeline: data extraction,
    portfolio aggregation, split-payment proration, Golden Rule absorption,
    classification, note generation, and persistence.

Responsibilities:
    - Coordinate data loading via repositories.
    - Delegate business logic to domain services.
    - Manage batch tracking and transaction lifecycle.

Key Components:
    - UpdateBankValidationMetricsCommand: Pure orchestrator. Contains no SQL
      and no business logic — only sequencing, logging, and UnitOfWork control.

Notes:
    - v4.0: Full hexagonal refactor. SQL moved to repositories; business logic
      moved to card_settlement domain services.
    - v3.1: Removed portfolio status updates (delegated to ValidatePortfolioMatches).
    - Only affects transactions of type 'LIQUIDACION TC'.

Dependencies:
    - yaml, pandas, datetime
    - utils.db_config, utils.logger
    - logic.infrastructure.batch_tracker, logic.infrastructure.unit_of_work
    - logic.domain.services.card_settlement.*

===============================================================================
"""

import yaml
import pandas as pd
from datetime import datetime
from utils.db_config import get_db_engine
from utils.logger import get_logger
from logic.infrastructure.batch_tracker import BatchTracker
from logic.infrastructure.unit_of_work import UnitOfWork
from logic.domain.services.card_settlement import (
    GoldenRuleService,
    SplitPaymentService,
    ReconciliationClassifier,
    EnrichNotesBuilder,
)


class UpdateBankValidationMetricsCommand:

    def __init__(self):
        self.logger        = get_logger("UPDATE_VALIDATION_METRICS")
        self.engine_stg    = get_db_engine('stg')
        self.engine_config = get_db_engine('config')

        self.batch_tracker = BatchTracker(
            self.engine_config, "UPDATE_VALIDATION_METRICS"
        )

        self.config    = self._load_config()
        self.tolerance = self.config.get('card_settlements', {}).get('tolerance', 0.05)

        self.card_reasons  = self.config.get('card_settlement_reasons', {})
        self.card_messages = self.config.get('card_settlement_messages', {})

        if not self.card_reasons:
            self.logger("card_settlement_reasons not found in YAML", "WARN")
            self.card_reasons = self._get_default_reasons()

        if not self.card_messages:
            self.logger("card_settlement_messages not found in YAML", "WARN")
            self.card_messages = self._get_default_messages()

        # Domain services — stateless, initialized once
        self._golden_rule   = GoldenRuleService(self.tolerance)
        self._split_svc     = SplitPaymentService()
        self._classifier    = ReconciliationClassifier(self.card_reasons, self.tolerance)
        self._notes_builder = EnrichNotesBuilder(
            self.card_reasons, self.card_messages, self.tolerance
        )

    # ─────────────────────────────────────────────────────────────────────────

    def _load_config(self) -> dict:
        try:
            with open('config/rules/reconciliation_config.yaml', 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger(f"Error loading config: {e}", "WARN")
            return {}

    def _get_default_reasons(self) -> dict:
        return {
            'perfect_match':           'CARD_PERFECT_MATCH',
            'split_payment':           'CARD_SPLIT_PAYMENT',
            'amount_within_tolerance': 'CARD_AMOUNT_WITHIN_TOLERANCE',
            'voucher_count_mismatch':  'CARD_VOUCHER_COUNT_MISMATCH',
            'amount_mismatch_small':   'CARD_AMOUNT_MISMATCH_SMALL',
            'amount_mismatch_large':   'CARD_AMOUNT_MISMATCH_LARGE',
            'has_suggestions':         'CARD_HAS_SUGGESTIONS',
            'only_suggestions':        'CARD_ONLY_SUGGESTIONS',
            'no_portfolio_data':       'CARD_NO_PORTFOLIO_DATA',
            'zero_invoices':           'CARD_ZERO_INVOICES',
        }

    def _get_default_messages(self) -> dict:
        return {
            'perfect_match':     'Card: Perfect match',
            'split_payment':     'Card: Split payment detected',
            'has_suggestions':   'Card: Pending suggestions',
            'only_suggestions':  'Card: Suggestions only, no confirmed',
            'no_portfolio_data': 'Card: No portfolio data',
        }

    # ─────────────────────────────────────────────────────────────────────────

    def execute(self, force: bool = False, **kwargs) -> bool:
        self.logger("═" * 80, "INFO")
        self.logger("VALIDATION METRICS UPDATE v4.0", "INFO")
        self.logger("═" * 80, "INFO")

        today       = datetime.now().strftime('%Y%m%d')
        fingerprint = BatchTracker.generate_config_fingerprint(
            {'date': today, 'process': 'validation_metrics'}
        )

        if not force and self.batch_tracker.should_skip(fingerprint):
            self.logger("Metrics already updated today", "WARN")
            return True

        batch_id = self.batch_tracker.start_batch(fingerprint, metadata={'date': today})
        self.logger.set_batch_id(batch_id)

        try:
            with UnitOfWork(self.engine_stg) as uow:

                # Step 1: Extract card transactions
                self.logger("Extracting card transactions...", "INFO")
                df_bank = uow.bank_validation.get_card_settlements_pending()

                if df_bank.empty:
                    self.logger("No settlements to process", "INFO")
                    self.batch_tracker.complete_batch(records_processed=0)
                    return True

                self.logger(f"   → {len(df_bank)} settlements to process", "INFO")

                # Step 2: Aggregate portfolio data
                self.logger("Aggregating portfolio data (conciliable_amount)...", "INFO")
                df_confirmed   = uow.customer_portfolio.get_confirmed_aggregated_by_settlement()
                df_suggestions = uow.customer_portfolio.get_suggestions_aggregated_by_settlement()

                self.logger(
                    f"   → {len(df_confirmed)} settlements with confirmed invoices", "INFO"
                )
                if not df_suggestions.empty:
                    self.logger(
                        f"   → {len(df_suggestions)} settlements with suggestions", "WARN"
                    )

                # Step 3: Merge DataFrames
                df_merged = df_bank.merge(df_confirmed,    on='settlement_id', how='left')
                df_merged = df_merged.merge(df_suggestions, on='settlement_id', how='left')

                df_merged['count_confirmed']    = df_merged['count_confirmed'].fillna(0).astype(int)
                df_merged['count_suggestions']  = df_merged['count_suggestions'].fillna(0).astype(int)
                df_merged['count_voucher_bank'] = df_merged['count_voucher_bank'].fillna(0).astype(int)
                df_merged['total_cartera']      = df_merged['total_cartera'].fillna(0).round(2)
                df_merged['voucher_commission'] = df_merged['voucher_commission'].fillna(0).round(2)

                # Step 4: Detect & prorate split payments
                self.logger("Processing split payments...", "INFO")
                split_result = self._split_svc.detect_and_prorate(df_merged)
                df_merged    = split_result.df

                if split_result.split_count > 0:
                    self.logger(
                        f"   → Detected {split_result.split_count} settlements with split payments",
                        "INFO",
                    )

                # Step 5: Apply Golden Rule (absorption)
                self.logger("\nApplying GOLDEN RULE (commission absorption)...", "INFO")
                absorption = self._golden_rule.apply(df_merged)
                df_merged  = absorption.df

                self.logger(
                    f"   Absorption applied: {absorption.absorbed_count} settlements", "SUCCESS"
                )
                if absorption.review_count > 0:
                    self.logger(
                        f"   Diff > tolerance: {absorption.review_count} settlements → REVIEW",
                        "WARN",
                    )

                # Step 6: Determine reason, status, and notes
                df_merged['reconcile_reason'] = df_merged.apply(
                    self._classifier.determine_reason, axis=1
                )
                df_merged['reconcile_status'] = df_merged['reconcile_reason'].apply(
                    self._classifier.determine_status
                )
                df_merged['enrich_notes'] = df_merged.apply(
                    self._notes_builder.build, axis=1
                )

                self.logger(
                    f"   → Metrics calculated for {len(df_merged)} settlements", "INFO"
                )

                # Step 7: Update bank transactions
                self.logger("Updating bank...", "INFO")
                update_count = uow.bank_validation.bulk_update_validation_metrics(df_merged)
                self.logger(f"   → {update_count} transactions updated", "SUCCESS")

                # Step 8: Update card settlements
                self.logger("Updating settlements...", "INFO")
                settlements_updated = 0

                matched_ids = df_merged[
                    df_merged['reconcile_reason'].str.contains(
                        'PERFECT_MATCH|SPLIT_PAYMENT|WITHIN_TOLERANCE', na=False
                    )
                ]['settlement_id'].tolist()

                review_ids = df_merged[
                    df_merged['reconcile_reason'].str.contains(
                        'MISMATCH|DIFF|SUGGESTIONS', na=False
                    )
                ]['settlement_id'].tolist()

                settlements_updated += uow.card_settlements.bulk_update_reconcile_status(
                    matched_ids, 'MATCHED'
                )
                settlements_updated += uow.card_settlements.bulk_update_reconcile_status(
                    review_ids, 'REVIEW'
                )

                # Step 9: Statistics & completion
                stats_reasons = df_merged['reconcile_reason'].value_counts()

                self.logger("\nSTATISTICS BY REASON:", "INFO")
                for reason, cnt in stats_reasons.items():
                    self.logger(f"   - {reason}: {cnt}", "INFO")

                self.logger("\nTOLERANCE ABSORPTION:", "INFO")
                self.logger(f"   - Absorbed in commission: {absorption.absorbed_count}", "INFO")
                self.logger(f"   - Exceed tolerance: {absorption.review_count}", "INFO")

                uow.commit()
                self.batch_tracker.complete_batch(records_processed=update_count)

                self.logger(
                    f"\nUpdate completed: {update_count} bank records, "
                    f"{settlements_updated} settlements",
                    "SUCCESS",
                )
                return True

        except Exception as e:
            self.batch_tracker.fail_batch(str(e))
            self.logger(f"ERROR: {e}", "ERROR")
            raise
