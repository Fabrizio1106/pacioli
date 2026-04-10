"""
===============================================================================
Project: PACIOLI
Module: logic.application.commands.staging.reconcile_bank_transactions
===============================================================================

Description:
    Command to orchestrate the bank reconciliation process. It matches bank
    transactions with pending portfolio invoices using multiple strategies,
    automatic matchers, and special case handlers.

Responsibilities:
    - Load and manage reconciliation business rules and configurations.
    - Orchestrate the reconciliation lifecycle: Extraction, Matching, Residuals, 
      and Persistence.
    - Handle specialized matching logic for Card Settlements, Salas VIP, and 
      Urbaparking.
    - Generate suggestions for manual review when automatic matching fails.
    - Track and report reconciliation metrics.

Key Components:
    - ReconcileBankTransactionsCommand: Main orchestrator for bank reconciliation.

Notes:
    - v2.4.1 includes FIX R1 for customer_code normalization (float to string).
    - Uses ReconciliationMatcherService for the core matching logic.

Dependencies:
    - yaml, pandas, pathlib, sqlalchemy
    - utils.logger
    - logic.infrastructure.extractors.bank_reconciliation_extractor
    - logic.infrastructure.repositories.bank_reconciliation_repository
    - logic.domain.services.reconciliation_matcher_service
    - logic.staging.reconciliation.matchers.card_matcher
    - logic.staging.reconciliation.strategies.residuals_reconciliation_strategy
===============================================================================
"""

import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime, date
from sqlalchemy import text
from utils.logger import get_logger
from logic.infrastructure.extractors.bank_reconciliation_extractor import BankReconciliationExtractor
from logic.infrastructure.repositories.bank_reconciliation_repository import BankReconciliationRepository
from logic.domain.services.reconciliation_matcher_service import ReconciliationMatcherService
from logic.staging.reconciliation.matchers.card_matcher import CardMatcher
from logic.staging.reconciliation.strategies.residuals_reconciliation_strategy import ResidualsReconciliationStrategy


class ReconcileBankTransactionsCommand:

    def __init__(self, uow, config_path: str = None):
        # 1. Initialization
        self.uow    = uow
        self.logger = get_logger("BANK_RECONCILIATION")

        self.config     = self._load_config(config_path)
        self.extractor  = BankReconciliationExtractor(uow.session)
        self.repository = BankReconciliationRepository(uow.session)

        self.matcher_service    = ReconciliationMatcherService(self.config)
        self.card_matcher       = CardMatcher(self.config)
        self.residuals_strategy = ResidualsReconciliationStrategy(self.config)

        self.stats = {
            'total_bank': 0, 'total_portfolio': 0,
            'matched': 0, 'review': 0, 'pending': 0,
            'special_cases': 0, 'cards_processed': 0,
            'residuals_processed': 0, 'errors': 0,
            'suggestions_generated': 0,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # CONFIGURATION
    # ─────────────────────────────────────────────────────────────────────────

    def _load_config(self, config_path: str = None) -> dict:
        if config_path is None:
            project_root = Path(__file__).parent.parent.parent.parent.parent
            config_path  = project_root / 'config' / 'rules' / 'reconciliation_config.yaml'
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            self.logger(f"Configuration loaded from: {config_path}", "INFO")
            return config
        except FileNotFoundError:
            self.logger(f"Configuration not found at {config_path}, using default config", "WARN")
            return self._get_default_config()

    def _get_default_config(self) -> dict:
        return {
            'general': {
                'tolerance_threshold': 0.05,
                'max_invoices_per_match': 15,
                'max_combinations_to_try': 5000,
                'recent_invoices_days': 90,
            },
            'confidence_thresholds': {'auto_match_minimum': 90, 'review_minimum': 60},
            'card_settlements': {
                'target_trans_type': 'LIQUIDACION TC',
                'ignore_voucher_count_for': ['URBAPARKING'],
            },
            'residuals_reconciliation': {
                'enabled': True, 'max_date_window_days': 3, 'max_invoices': 5,
            },
            'suggestions': {
                'max_invoices_to_suggest': 20,
                'min_score_to_suggest': 10,
            },
        }

    # ─────────────────────────────────────────────────────────────────────────
    # FIX R1: CUSTOMER_CODE NORMALIZATION
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _normalize_customer_code(raw_value) -> str:
        """
        Converts a customer_code to a clean string format without .0 suffix.

        PROBLEM:
        ─────────
        enrich_customer_id is stored in PostgreSQL as character varying with 
        value '400678.0' — inherited from a float→str conversion in Python 
        during original insertion. In the portfolio, customer_code is '400678'. 
        The extractor performs WHERE customer_code = :customer_code using the 
        raw bank value, and '400678.0' != '400678' in string comparison.

        EXAMPLES:
        ─────────
        '400678.0'  → '400678'
        '400678'    → '400678'   (no change)
        400678.0    → '400678'   (float)
        400678      → '400678'   (int)
        None / NaN  → None       (remains None for SALAS VIP)
        '999999.0'  → '999999'
        """
        if raw_value is None:
            return None

        import math
        try:
            f = float(raw_value)
            if math.isnan(f):
                return None
            # Convert to int to remove .0, then to string
            return str(int(f))
        except (ValueError, TypeError):
            # Already a clean string without .0
            return str(raw_value).strip()

    # ─────────────────────────────────────────────────────────────────────────
    # EXECUTE
    # ─────────────────────────────────────────────────────────────────────────

    def execute(self, force: bool = False, **kwargs) -> bool:
        # 3. Execution Workflow
        self.logger("═" * 80, "INFO")
        self.logger("STARTING BANK RECONCILIATION v2.4.1", "INFO")
        self.logger("═" * 80, "INFO")

        try:
            # PHASE 0B: CARD SETTLEMENTS
            self.logger("\n" + "═" * 80, "INFO")
            self.logger("PHASE 0B: CARD SETTLEMENTS", "INFO")
            self.logger("═" * 80, "INFO")

            card_stats = self.card_matcher.reconcile_card_settlements(
                engine_stg=self.uow.session.get_bind(),
                payment_date=datetime.now().date(),
            )

            self.stats['cards_processed']  = card_stats['processed']
            self.stats['matched']         += card_stats['matched']
            self.stats['review']          += card_stats['review']
            self.stats['pending']         += card_stats['pending']

            self.logger(
                f"\nCards - Matched: {card_stats['matched']}, "
                f"Review: {card_stats['review']}, Pending: {card_stats['pending']}",
                "INFO",
            )

            # PHASE 1: EXTRACTION
            self.logger("\n" + "═" * 80, "INFO")
            self.logger("PHASE 1: DATA EXTRACTION", "INFO")
            self.logger("═" * 80, "INFO")

            df_bank = self._extract_pending_transactions_excluding_cards()

            if df_bank.empty:
                self.logger("No pending general transactions", "INFO")
                self._generate_final_report()
                return True

            self.stats['total_bank'] = len(df_bank) + card_stats['processed']
            self.logger(f"{len(df_bank)} pending general transactions", "INFO")

            # PHASE 2-5: CUSTOMER MATCHING
            customer_list = df_bank['customer_code'].dropna().unique()
            self.logger(f"{len(customer_list)} customers to process\n", "INFO")

            for idx, customer_code in enumerate(customer_list, 1):
                self.logger(f"\n{'─' * 80}", "INFO")
                self.logger(f"Customer {idx}/{len(customer_list)}: {customer_code}", "INFO")
                self.logger(f"{'─' * 80}", "INFO")

                customer_txs = df_bank[df_bank['customer_code'] == customer_code]

                if customer_code == '999999':
                    invoices = self.extractor.extract_pending_portfolio_invoices(
                        customer_code=None,
                        max_age_days=self.config['general']['recent_invoices_days'],
                    )
                    self.logger(
                        f"   SALAS VIP: Using ALL portfolio ({len(invoices)} invoices)",
                        "INFO",
                    )
                else:
                    invoices = self.extractor.extract_pending_portfolio_invoices(
                        # FIX R1: normalize before passing to extractor
                        customer_code=customer_code,
                        max_age_days=self.config['general']['recent_invoices_days'],
                    )

                if invoices.empty:
                    self.logger(f"Customer {customer_code}: No invoices available", "WARN")
                    self._mark_all_as_no_portfolio(customer_txs)
                    continue

                self.stats['total_portfolio'] += len(invoices)
                self._process_customer(customer_code, customer_txs, invoices)

            # INTERMEDIATE COMMIT
            self.logger("\n" + "═" * 80, "INFO")
            self.logger("INTERMEDIATE COMMIT: Persisting changes before residuals", "INFO")
            self.logger("═" * 80, "INFO")
            self.uow.session.commit()

            # RESIDUALS RECONCILIATION
            if self.config.get('residuals_reconciliation', {}).get('enabled', True):
                self._process_residuals()

            self._generate_final_report()

            self.logger("\n" + "═" * 80, "SUCCESS")
            self.logger("BANK RECONCILIATION COMPLETED v2.4.1", "SUCCESS")
            self.logger("═" * 80, "SUCCESS")
            return True

        except Exception as e:
            self.logger(f"Error in reconciliation: {e}", "ERROR")
            import traceback
            self.logger(traceback.format_exc(), "ERROR")
            return False

    # ─────────────────────────────────────────────────────────────────────────
    # EXTRACTION
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_pending_transactions_excluding_cards(self):
        # 4. Data Extraction logic
        target_trans_type = self.config.get('card_settlements', {}).get(
            'target_trans_type', 'LIQUIDACION TC'
        )

        query = text("""
            SELECT
                stg_id, doc_date, bank_date, amount_total,
                bank_ref_1, bank_ref_2, bank_description,
                enrich_customer_id, enrich_customer_name,
                enrich_confidence_score, trans_type, global_category, brand
            FROM biq_stg.stg_bank_transactions
            WHERE reconcile_status = 'PENDING'
              AND trans_type != :trans_type
              AND is_compensated_sap = FALSE
              AND is_compensated_intraday = FALSE
            ORDER BY bank_date ASC
        """)

        df = pd.read_sql(
            query,
            self.uow.session.connection(),
            params={'trans_type': target_trans_type},
        )

        if 'enrich_customer_id' in df.columns:
            # FIX R1: normalize '400678.0' → '400678' throughout the DataFrame
            df['customer_code'] = df['enrich_customer_id'].apply(
                self._normalize_customer_code
            )

        self.logger(
            f"Extracted {len(df)} transactions (excluding {target_trans_type})",
            "INFO",
        )
        return df

    # ─────────────────────────────────────────────────────────────────────────
    # CUSTOMER PROCESSING
    # ─────────────────────────────────────────────────────────────────────────

    def _process_customer(self, customer_code, bank_txs, invoices):
        # 5. Customer Processing phase
        self.logger(
            f"   {len(bank_txs)} transactions vs {len(invoices)} invoices", "INFO"
        )

        first_tx     = bank_txs.iloc[0]
        special_case = None

        if customer_code == '999999':
            special_case = 'SALAS_VIP'
        elif customer_code == '400419':
            special_case = 'URBAPARKING'
        else:
            special_case = self.matcher_service.detect_special_case(first_tx)

        if special_case:
            self.logger(f"   Special case detected: {special_case}", "INFO")
            self._handle_special_case(
                special_case, bank_txs, invoices, first_tx.get('bank_date')
            )
            self.stats['special_cases'] += len(bank_txs)
            return

        self._standard_matching(bank_txs, invoices)

    def _standard_matching(self, bank_txs, invoices):
        # 6. Standard Matching Logic
        used_invoice_ids = set()

        for idx, bank_tx in bank_txs.iterrows():
            available_invoices = invoices[
                ~invoices['stg_id'].isin(used_invoice_ids)
            ].copy()

            if available_invoices.empty:
                self.logger(f"   TX {bank_tx['stg_id']}: No invoices available", "WARN")
                self.repository.mark_bank_as_no_match(bank_tx['stg_id'], 'NO_PORTFOLIO_DATA')
                self.stats['pending'] += 1
                continue

            result = self.matcher_service.find_best_match(bank_tx, available_invoices)

            if result:
                summary = self.matcher_service.get_match_summary(result)
                self.logger(
                    f"   TX {bank_tx['stg_id']}: {summary['status']} "
                    f"(Score: {summary['confidence']:.1f}%, Method: {summary['method']})",
                    "SUCCESS" if summary['status'] == 'MATCHED' else "INFO",
                )
                self._persist_match_result(bank_tx, result)

                if summary['status'] == 'MATCHED':
                    self.stats['matched'] += 1
                elif summary['status'] == 'REVIEW':
                    self.stats['review'] += 1
                else:
                    self.stats['pending'] += 1

                used_invoice_ids.update(
                    [inv['stg_id'] for inv in result['matched_invoices']]
                )

            else:
                suggestions = self._generate_ranked_suggestions(bank_tx, available_invoices)

                if suggestions:
                    self.logger(
                        f"   TX {bank_tx['stg_id']}: NO_MATCH_FOUND — "
                        f"{len(suggestions)} suggestions generated",
                        "INFO",
                    )
                    self._persist_no_match_with_suggestions(bank_tx, suggestions)
                    self.stats['suggestions_generated'] += len(suggestions)
                else:
                    self.logger(
                        f"   TX {bank_tx['stg_id']}: No match found", "WARN"
                    )
                    self.repository.mark_bank_as_no_match(bank_tx['stg_id'], 'NO_MATCH_FOUND')

                self.stats['pending'] += 1

    # ─────────────────────────────────────────────────────────────────────────
    # RANKED SUGGESTIONS
    # ─────────────────────────────────────────────────────────────────────────

    def _generate_ranked_suggestions(self, bank_tx, invoices) -> list:
        # 7. Suggestions Generation
        cfg_sugg       = self.config.get('suggestions', {})
        max_to_suggest = cfg_sugg.get('max_invoices_to_suggest', 20)
        min_score      = cfg_sugg.get('min_score_to_suggest', 10)

        bank_amount = float(bank_tx.get('amount_total', 0))
        bank_date   = bank_tx.get('bank_date')

        if isinstance(bank_date, str):
            try:
                bank_date = date.fromisoformat(bank_date.split(' ')[0])
            except Exception:
                bank_date = None
        elif hasattr(bank_date, 'date'):
            bank_date = bank_date.date()

        if bank_amount <= 0:
            return []

        scored = []

        for _, inv in invoices.iterrows():
            inv_amount = float(inv.get('conciliable_amount', 0))
            if inv_amount <= 0:
                continue

            date_score = 0
            inv_date   = inv.get('doc_date')

            if bank_date is not None and inv_date is not None:
                if isinstance(inv_date, str):
                    try:
                        inv_date = date.fromisoformat(str(inv_date).split(' ')[0])
                    except Exception:
                        inv_date = None
                elif hasattr(inv_date, 'date'):
                    inv_date = inv_date.date()

                if inv_date is not None:
                    days_diff = abs((bank_date - inv_date).days)
                    if days_diff == 0:
                        date_score = 50
                    elif days_diff <= 7:
                        date_score = 40
                    elif days_diff <= 30:
                        date_score = 25
                    elif days_diff <= 60:
                        date_score = 10

            ratio = inv_amount / bank_amount
            if 0.8 <= ratio <= 1.2:
                amount_score = 50
            elif 0.5 <= ratio <= 1.5:
                amount_score = 30
            elif 0.1 <= ratio <= 2.0:
                amount_score = 10
            else:
                amount_score = 0

            total_score = date_score + amount_score

            if total_score >= min_score:
                scored.append({
                    'stg_id':      int(inv['stg_id']),
                    'score':       total_score,
                    'invoice_ref': inv.get('invoice_ref', ''),
                    'amount':      inv_amount,
                    'doc_date':    str(inv.get('doc_date', '')),
                })

        scored.sort(key=lambda x: (-x['score'], x['doc_date']))
        return scored[:max_to_suggest]

    def _persist_no_match_with_suggestions(self, bank_tx, suggestions) -> None:
        bank_stg_id   = int(bank_tx['stg_id'])
        suggested_ids = [s['stg_id'] for s in suggestions]

        self.repository.mark_bank_as_no_match(bank_stg_id, 'NO_MATCH_FOUND')

        ids_str = ','.join(str(i) for i in suggested_ids[:10])
        notes   = (
            f"{len(suggestions)} suggestions available for manual review. "
            f"Principal IDs: {ids_str}"
            f"{'...' if len(suggested_ids) > 10 else ''}"
        )

        self.uow.session.execute(
            text("""
                UPDATE biq_stg.stg_bank_transactions
                SET enrich_notes          = :notes,
                    matched_portfolio_ids = :port_ids,
                    updated_at            = NOW()
                WHERE stg_id = :stg_id
            """),
            {'stg_id': bank_stg_id, 'notes': notes, 'port_ids': str(suggested_ids)}
        )

        for suggestion in suggestions:
            self.uow.session.execute(
                text("""
                    UPDATE biq_stg.stg_customer_portfolio
                    SET is_suggestion    = TRUE,
                        match_confidence = :score,
                        match_method     = 'SUGGESTION_RANKED',
                        updated_at       = NOW()
                    WHERE stg_id = :stg_id
                      AND reconcile_status IN (
                          'PENDING', 'PARTIAL_MATCH', 'WITHHOLDING_APPLIED', 'ENRICHED'
                      )
                      AND settlement_id IS NULL
                """),
                {'stg_id': suggestion['stg_id'], 'score': str(suggestion['score'])}
            )

    # ─────────────────────────────────────────────────────────────────────────
    # SPECIAL CASES
    # ─────────────────────────────────────────────────────────────────────────

    def _handle_special_case(self, special_case, bank_txs, invoices, payment_date):
        # 8. Special Cases Handling
        if special_case == 'URBAPARKING':
            updates = self.matcher_service.match_urbaparking_batch(
                bank_txs, invoices, payment_date
            )
            self._persist_batch_updates(updates)
        elif special_case == 'SALAS_VIP':
            updates = self.matcher_service.match_salas_vip_batch(
                bank_txs, invoices, payment_date
            )
            self._persist_batch_updates(updates)
        elif special_case == 'EXTERIOR':
            for idx, bank_tx in bank_txs.iterrows():
                result = self.matcher_service.match_exterior_transfer(
                    bank_tx, invoices, payment_date
                )
                if result:
                    self._persist_single_update(result)

    # ─────────────────────────────────────────────────────────────────────────
    # RESIDUALS
    # ─────────────────────────────────────────────────────────────────────────

    def _process_residuals(self):
        # 9. Residuals Reconciliation phase
        self.logger("\n" + "═" * 80, "INFO")
        self.logger("RESIDUALS PHASE: URBAPARKING residuals reconciliation", "INFO")
        self.logger("═" * 80, "INFO")

        query_bank = text("""
            SELECT *
            FROM biq_stg.stg_bank_transactions
            WHERE trans_type = 'DEPOSITO EFECTIVO'
              AND is_compensated_sap = FALSE
              AND is_compensated_intraday = FALSE
            ORDER BY bank_date ASC
        """)

        all_bank_txs = pd.read_sql(
            query_bank, self.uow.session.connection()
        ).to_dict('records')

        self.logger(f"\nBank transactions extracted: {len(all_bank_txs)}", "INFO")

        query_portfolio = text("""
            SELECT *
            FROM biq_stg.stg_customer_portfolio
            WHERE customer_code = '400419'
            ORDER BY doc_date ASC
        """)

        all_invoices = pd.read_sql(
            query_portfolio, self.uow.session.connection()
        ).to_dict('records')

        self.logger(f"Portfolio invoices extracted: {len(all_invoices)}", "INFO")

        residual_updates = self.residuals_strategy.reconcile_residuals(
            bank_transactions=all_bank_txs,
            all_invoices=all_invoices,
        )

        if residual_updates:
            self._persist_batch_updates(residual_updates)
            self.stats['residuals_processed'] = len(residual_updates)
            self.logger(
                f"\nResiduals processed: {len(residual_updates)} transactions",
                "SUCCESS",
            )
        else:
            self.logger("\nNo residuals found to process", "INFO")

    # ─────────────────────────────────────────────────────────────────────────
    # PERSISTENCE
    # ─────────────────────────────────────────────────────────────────────────

    def _persist_match_result(self, bank_tx, match_result):
        # 10. Persistence & Reporting logic
        score_result = match_result['score_result']
        matched_ids  = [inv['stg_id'] for inv in match_result['matched_invoices']]

        self.repository.update_bank_transaction_match(
            stg_id=bank_tx['stg_id'],
            status=score_result['status'],
            reason=score_result['reason'],
            confidence=score_result['total_score'],
            diff=score_result['diff'],
            method=match_result['method'],
            notes=f"{len(matched_ids)} invoices matched",
            port_ids=matched_ids,
            bank_ref_match=bank_tx.get('bank_ref_1'),
        )

        settlement_id = bank_tx.get('bank_ref_1')
        if not settlement_id or str(settlement_id).strip() == '':
            settlement_id = str(bank_tx['stg_id'])

        status        = score_result['status']
        is_suggestion = status != 'MATCHED'

        self.repository.bulk_update_portfolio_with_settlement(
            stg_ids=matched_ids,
            settlement_id=settlement_id,
            status=status,
            method=match_result['method'],
            confidence=int(score_result['total_score']),
            is_suggestion=is_suggestion,
        )

        self.repository.insert_audit_record(
            bank_stg_id=bank_tx['stg_id'],
            portfolio_stg_ids=matched_ids,
            match_method=match_result['method'],
            match_confidence=score_result['total_score'],
            amount_diff=score_result['diff'],
        )

    def _persist_batch_updates(self, updates: list):
        count_bank      = self.repository.bulk_update_bank_transactions(updates)
        count_portfolio = 0

        for update in updates:
            if update.get('status') in ('MATCHED', 'REVIEW'):
                port_ids      = update.get('port_ids', [])
                settlement_id = update.get('bank_ref_match')

                if port_ids and settlement_id:
                    rows = self.repository.bulk_update_portfolio_with_settlement(
                        stg_ids=port_ids,
                        settlement_id=settlement_id,
                        status=update.get('status'),
                        method=update.get('method'),
                        confidence=update.get('confidence'),
                    )
                    count_portfolio += rows

        self.logger(
            f"   Bank: {count_bank} txs | Portfolio: {count_portfolio} invoices updated",
            "SUCCESS",
        )

    def _persist_single_update(self, update: dict):
        self.repository.update_bank_transaction_match(
            stg_id=update['id'],
            status=update['status'],
            reason=update['reason'],
            confidence=update['confidence'],
            diff=update['diff'],
            method=update['method'],
            notes=update['notes'],
            port_ids=update.get('port_ids', []),
            bank_ref_match=update.get('bank_ref_match'),
        )

    def _mark_all_as_no_portfolio(self, bank_txs):
        for idx, tx in bank_txs.iterrows():
            self.repository.mark_bank_as_no_match(tx['stg_id'], 'NO_PORTFOLIO_DATA')
            self.stats['pending'] += 1

    # ─────────────────────────────────────────────────────────────────────────
    # FINAL REPORT
    # ─────────────────────────────────────────────────────────────────────────

    def _generate_final_report(self):
        self.logger("\n" + "═" * 80, "INFO")
        self.logger("RECONCILIATION SUMMARY v2.4.1", "INFO")
        self.logger("═" * 80, "INFO")

        total     = self.stats['total_bank']
        matched   = self.stats['matched']
        review    = self.stats['review']
        pending   = self.stats['pending']
        special   = self.stats['special_cases']
        cards     = self.stats['cards_processed']
        residuals = self.stats['residuals_processed']
        sugg      = self.stats['suggestions_generated']

        if total > 0:
            matched_pct = matched / total * 100
            review_pct  = review  / total * 100
            pending_pct = pending / total * 100

            self.logger(f"\nTotal bank transactions: {total}", "INFO")
            self.logger(f"   Cards (LIQUIDACION TC): {cards}", "INFO")
            self.logger(f"   Transfers/Deposits: {total - cards}", "INFO")
            self.logger(f"Total portfolio invoices: {self.stats['total_portfolio']}", "INFO")
            self.logger(f"\nMATCHED (auto): {matched} ({matched_pct:.1f}%)", "SUCCESS")
            self.logger(f"REVIEW (manual): {review} ({review_pct:.1f}%)", "WARN")
            self.logger(f"PENDING (no match): {pending} ({pending_pct:.1f}%)", "INFO")
            if sugg      > 0: self.logger(f"Suggestions generated: {sugg}", "INFO")
            if special   > 0: self.logger(f"\nSpecial cases: {special}", "INFO")
            if residuals > 0: self.logger(f"Residuals processed: {residuals}", "INFO")
            self.logger(
                f"\nMatch Rate: {matched_pct:.1f}%",
                "SUCCESS" if matched_pct >= 80 else "WARN",
            )
