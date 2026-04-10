"""
===============================================================================
Project: PACIOLI
Module: logic.application.commands.staging.match_withholdings
===============================================================================

Description:
    Orchestration command for matching withholdings with customers and invoices.
    It implements a multi-layer matching strategy for customers and a specific
    matching logic for associated invoices in the staging layer.

Responsibilities:
    - Execute customer matching across multiple layers (Exact RUC, Fuzzy Name, etc.).
    - Perform invoice matching for successfully matched customers.
    - Generate detailed reports and statistics on matching results.
    - Handle exceptions for customers or invoices not found in the system.

Key Components:
    - MatchWithholdingsCommand: Main orchestrator for Phase 2 (Matching).

Notes:
    - Uses CustomerMatcherService and InvoiceMatcherService for domain logic.
    - Reports matching efficiency across different layers.

Dependencies:
    - logic.infrastructure.repositories.withholdings_operations_repository
    - logic.domain.services.customer_matcher_service
    - logic.domain.services.invoice_matcher_service

===============================================================================
"""

from utils.logger import get_logger
from logic.infrastructure.repositories.withholdings_operations_repository import WithholdingsOperationsRepository
from logic.domain.services.customer_matcher_service import CustomerMatcherService
from logic.domain.services.invoice_matcher_service import InvoiceMatcherService


class MatchWithholdingsCommand:

    def __init__(self, uow):
        self.uow    = uow
        self.logger = get_logger("MATCH_WITHHOLDINGS")

        self.stats = {
            'customer': {
                'processed': 0, 'matched': 0, 'not_found': 0,
                'layer1': 0, 'layer2': 0, 'layer3': 0, 'layer4': 0,
            },
            'invoice': {
                'processed': 0, 'matched': 0, 'not_found': 0,
            },
        }

    def execute(self, force: bool = False, **kwargs) -> bool:
        # 1. Process execution
        self.logger("Starting PHASE 2: Withholdings Matching", "INFO")
        self.logger("=" * 70, "INFO")

        try:
            self._execute_customer_matching()
            self._execute_invoice_matching()
            self._generate_final_report()

            self.logger("\n" + "=" * 70, "SUCCESS")
            self.logger("PHASE 2 COMPLETED", "SUCCESS")
            self.logger("=" * 70, "SUCCESS")

            self.total_matched = (
                self.stats['customer']['matched'] +
                self.stats['invoice']['matched']
            )
            return True

        except Exception as e:
            self.logger(f"Matching error: {e}", "ERROR")
            return False

    # ─────────────────────────────────────────────────────────────────────────
    # CUSTOMER MATCHING
    # ─────────────────────────────────────────────────────────────────────────

    def _execute_customer_matching(self):
        # 1. Service initialization
        self.logger("\n" + "=" * 70, "INFO")
        self.logger("STEP 1: CUSTOMER MATCHING", "INFO")
        self.logger("=" * 70, "INFO")

        repo    = WithholdingsOperationsRepository(self.uow.session)
        service = CustomerMatcherService(self.uow.session)

        # 2. Data processing
        df_pending = repo.get_pending_for_customer_matching()

        if df_pending.empty:
            self.logger("No withholdings pending customer matching", "INFO")
            return

        self.logger(f"{len(df_pending)} withholdings to process", "INFO")
        self.stats['customer']['processed'] = len(df_pending)

        for idx, row in df_pending.iterrows():
            result = service.match_multi_layer(row)

            if result.matched:
                repo.update_customer_match(
                    stg_id=row['stg_id'],
                    customer_code=result.customer_code_sap,
                    confidence=result.match_confidence,
                    method=result.match_method,
                )
                self.stats['customer']['matched'] += 1

                if result.match_method == 'RUC_EXACT':
                    self.stats['customer']['layer1'] += 1
                elif result.match_method in ['NAME_SOUNDEX', 'NAME_LIKE']:
                    self.stats['customer']['layer2'] += 1
                elif result.match_method == 'RUC_PARTIAL':
                    self.stats['customer']['layer3'] += 1
            else:
                repo.update_customer_no_match(row['stg_id'])
                repo.create_customer_not_found_exception(row)
                self.stats['customer']['not_found'] += 1
                self.stats['customer']['layer4'] += 1

            if (idx + 1) % 50 == 0:
                self.logger(f"   → Processed {idx + 1}/{len(df_pending)}...", "INFO")

        self._report_customer_stats()

    # ─────────────────────────────────────────────────────────────────────────
    # INVOICE MATCHING
    # ─────────────────────────────────────────────────────────────────────────

    def _execute_invoice_matching(self):
        # 1. Service initialization
        self.logger("\n" + "=" * 70, "INFO")
        self.logger("STEP 2: INVOICE MATCHING", "INFO")
        self.logger("=" * 70, "INFO")

        repo    = WithholdingsOperationsRepository(self.uow.session)
        service = InvoiceMatcherService(repo)

        # 2. Data processing
        df_pending = repo.get_pending_for_invoice_matching()

        if df_pending.empty:
            self.logger("No withholdings pending invoice matching", "INFO")
            return

        self.logger(f"{len(df_pending)} withholdings to process", "INFO")
        self.stats['invoice']['processed'] = len(df_pending)

        for idx, row in df_pending.iterrows():
            result = service.match_invoice(
                customer_code=row['customer_code_sap'],
                invoice_ref=row['invoice_ref_sustento'],
            )

            if result.matched:
                repo.update_invoice_match(
                    stg_id=row['stg_id'],
                    invoice_sap_doc=result.sap_doc_number,
                )
                self.stats['invoice']['matched'] += 1
            else:
                repo.update_invoice_no_match(row['stg_id'])
                repo.create_invoice_not_found_exception(row)
                self.stats['invoice']['not_found'] += 1

            if (idx + 1) % 50 == 0:
                self.logger(f"   → Processed {idx + 1}/{len(df_pending)}...", "INFO")

        self._report_invoice_stats()

    # ─────────────────────────────────────────────────────────────────────────
    # REPORTING
    # ─────────────────────────────────────────────────────────────────────────

    def _report_customer_stats(self):
        # 1. Summary reporting
        stats = self.stats['customer']
        total = stats['processed']
        if total == 0:
            return

        matched_pct = (stats['matched'] / total * 100) if total > 0 else 0

        self.logger("\n" + "=" * 60, "INFO")
        self.logger("CUSTOMER MATCHING COMPLETED", "SUCCESS")
        self.logger("=" * 60, "INFO")
        self.logger(f"\nTotal processed: {total}", "INFO")
        self.logger(f"Successful matches: {stats['matched']} ({matched_pct:.1f}%)", "SUCCESS")
        self.logger(f"Not found: {stats['not_found']} ({100 - matched_pct:.1f}%)", "WARN")
        self.logger(f"\nBreakdown by layer:", "INFO")
        self.logger(f"   Layer 1 (Exact RUC): {stats['layer1']}", "INFO")
        self.logger(f"   Layer 2 (Fuzzy Name): {stats['layer2']}", "INFO")
        self.logger(f"   Layer 3 (Partial RUC): {stats['layer3']}", "INFO")
        self.logger(f"   Layer 4 (No Match): {stats['layer4']}", "WARN")

    def _report_invoice_stats(self):
        # 1. Summary reporting
        stats = self.stats['invoice']
        total = stats['processed']
        if total == 0:
            return

        matched_pct = (stats['matched'] / total * 100) if total > 0 else 0

        self.logger("\n" + "=" * 60, "INFO")
        self.logger("INVOICE MATCHING COMPLETED", "SUCCESS")
        self.logger("=" * 60, "INFO")
        self.logger(f"\nTotal processed: {total}", "INFO")
        self.logger(f"Successful matches: {stats['matched']} ({matched_pct:.1f}%)", "SUCCESS")
        self.logger(f"Not found: {stats['not_found']} ({100 - matched_pct:.1f}%)", "WARN")

    def _generate_final_report(self):
        # 1. Final status query
        from sqlalchemy import text
        import pandas as pd

        # CHANGE: explicit schema + session.connection() instead of session.bind
        query = text("""
            SELECT
                reconcile_status,
                COUNT(*) AS cnt
            FROM biq_stg.stg_withholdings
            WHERE is_registrable = TRUE
            GROUP BY reconcile_status
            ORDER BY
                CASE reconcile_status
                    WHEN 'INVOICE_MATCHED'    THEN 1
                    WHEN 'CUSTOMER_MATCHED'   THEN 2
                    WHEN 'NEW'                THEN 3
                    WHEN 'CUSTOMER_NOT_FOUND' THEN 4
                    WHEN 'INVOICE_NOT_FOUND'  THEN 5
                    ELSE 6
                END
        """)

        df_stats = pd.read_sql(query, self.uow.session.connection())

        # 2. Summary display
        self.logger("\n" + "=" * 70, "INFO")
        self.logger("FINAL SUMMARY - PHASE 2", "INFO")
        self.logger("=" * 70, "INFO")

        total_registrables = df_stats['cnt'].sum()
        self.logger(f"\nTotal registrable withholdings: {total_registrables}", "INFO")
        self.logger(f"\nStatuses:", "INFO")

        for _, row in df_stats.iterrows():
            status = row['reconcile_status']
            count  = row['cnt']
            pct    = (count / total_registrables * 100) if total_registrables > 0 else 0

            if status == 'INVOICE_MATCHED':
                self.logger(f"   ✅ {status}: {count} ({pct:.1f}%)", "SUCCESS")
            elif status in ['CUSTOMER_NOT_FOUND', 'INVOICE_NOT_FOUND']:
                self.logger(f"   ⚠️ {status}: {count} ({pct:.1f}%)", "WARN")
            else:
                self.logger(f"   ℹ️ {status}: {count} ({pct:.1f}%)", "INFO")

        # 3. Open exceptions check
        query_exc = text("""
            SELECT COUNT(*) AS exc_count
            FROM biq_stg.withholdings_exceptions
            WHERE resolution_status = 'OPEN'
        """)

        # CHANGE: positional access [0] — more robust with psycopg2
        exc_result = self.uow.session.execute(query_exc).fetchone()
        exc_count  = exc_result[0] if exc_result else 0

        if exc_count > 0:
            self.logger(f"\nOpen exceptions: {exc_count}", "WARN")
            self.logger("   Check table: biq_stg.withholdings_exceptions", "INFO")
