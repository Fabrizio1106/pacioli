"""
===============================================================================
Project: PACIOLI
Module: logic.application.commands.staging.validate_portfolio_matches
===============================================================================

Description:
    Command to transition portfolio invoices from ENRICHED to MATCHED status 
    and synchronize state changes across card vouchers and settlements. It 
    ensures that automatic matches are confirmed and that related records 
    in stg_card_details and stg_card_settlements are updated in cascade.

Responsibilities:
    - Transition auto-confirmed invoices (ENRICHED -> MATCHED) when the bank 
      transaction is MATCHED.
    - Count and report pending suggestions requiring analyst approval.
    - Synchronize voucher statuses (ASSIGNED -> MATCHED) in stg_card_details.
    - Transition stg_parking_pay_breakdown records to MATCHED.
    - Update stg_card_settlements to MATCHED when all associated records 
      are reconciled.

Key Components:
    - ValidatePortfolioMatchesCommand: Main orchestrator for cascading match 
      validation.

Notes:
    - v1.2: Added Step 5 to synchronize stg_card_settlements when bank and 
      all vouchers are MATCHED.
    - Handles specific matching logic for VIP and Parking card types.

Dependencies:
    - datetime, sqlalchemy
    - utils.db_config, utils.logger, logic.infrastructure.batch_tracker
===============================================================================
"""

from datetime import datetime
from sqlalchemy import text
from utils.db_config import get_db_engine
from utils.logger import get_logger
from logic.infrastructure.batch_tracker import BatchTracker


class ValidatePortfolioMatchesCommand:
    """
    Command to transition invoices from ENRICHED to MATCHED in portfolio
    and synchronize the state in cascade to vouchers and settlements.
    """

    def __init__(self):
        # 1. Initialization
        self.logger        = get_logger("VALIDATE_PORTFOLIO_MATCHES")
        self.engine_stg    = get_db_engine('stg')
        self.engine_config = get_db_engine('config')
        self.batch_tracker = BatchTracker(self.engine_config, "VALIDATE_PORTFOLIO_MATCHES")

    def execute(self, force: bool = False, **kwargs) -> bool:
        self.logger("\n" + "═" * 80, "INFO")
        self.logger("VALIDATE PORTFOLIO MATCHES v1.2", "INFO")
        self.logger("═" * 80, "INFO")

        today       = datetime.now().strftime('%Y%m%d')
        fingerprint = BatchTracker.generate_config_fingerprint({
            'date': today, 'process': 'validate_matches'
        })

        if not force and self.batch_tracker.should_skip(fingerprint):
            self.logger("Already executed today — skipping", "INFO")
            return True

        batch_id = self.batch_tracker.start_batch(fingerprint, metadata={'date': today})
        self.logger.set_batch_id(batch_id)

        try:
            with self.engine_stg.begin() as conn:

                # 2. Step 1: Transition Auto-Confirmed Invoices (ENRICHED → MATCHED)
                # Invoices with is_suggestion=FALSE whose bank transaction is MATCHED.
                # Automatically confirmed by cascade — no human intervention required.
                result_p1 = conn.execute(text("""
                    UPDATE biq_stg.stg_customer_portfolio p
                    SET    reconcile_status = 'MATCHED',
                           reconciled_at   = NOW(),
                           updated_at      = NOW()
                    FROM   biq_stg.stg_bank_transactions b
                    WHERE  p.settlement_id    = b.settlement_id
                      AND  p.reconcile_status = 'ENRICHED'
                      AND  p.is_suggestion    = FALSE
                      AND  b.reconcile_status = 'MATCHED'
                """))
                confirmed_count = result_p1.rowcount
                self.logger(
                    f"   Step 1: {confirmed_count} invoices ENRICHED → MATCHED",
                    "SUCCESS" if confirmed_count > 0 else "INFO"
                )

                # 3. Step 2: Count Pending Suggestions
                result_p2 = conn.execute(text("""
                    SELECT COUNT(*) AS cnt
                    FROM   biq_stg.stg_customer_portfolio
                    WHERE  reconcile_status = 'ENRICHED'
                      AND  is_suggestion    = TRUE
                """))
                suggestion_count = result_p2.fetchone()[0]
                if suggestion_count > 0:
                    self.logger(
                        f"   Step 2: {suggestion_count} invoices in ENRICHED "
                        f"(suggestions — require analyst approval)",
                        "WARN"
                    )

                # 4. Step 3: Log within-tolerance Matches
                result_p3 = conn.execute(text("""
                    SELECT COUNT(*) AS cnt
                    FROM   biq_stg.stg_customer_portfolio
                    WHERE  reconcile_status = 'MATCHED'
                      AND  match_method     = 'VIP_EXACT_BATCH_REF_WRONG_AMT'
                      AND  match_confidence = '95'
                      AND  is_suggestion    = FALSE
                """))
                tolerance_count = result_p3.fetchone()[0]
                if tolerance_count > 0:
                    self.logger(
                        f"   Step 3: {tolerance_count} within-tolerance invoices confirmed as MATCHED",
                        "INFO"
                    )

                # 5. Step 4A: VIP Vouchers (Exact Hash)
                # For normal matches where the hash coincides exactly between portfolio and voucher.
                result_4a = conn.execute(text("""
                    UPDATE biq_stg.stg_card_details
                    SET    reconcile_status = 'MATCHED',
                           updated_at      = NOW()
                    FROM   biq_stg.stg_customer_portfolio p
                    WHERE  biq_stg.stg_card_details.voucher_hash_key = p.match_hash_key
                      AND  p.reconcile_status = 'MATCHED'
                      AND  biq_stg.stg_card_details.reconcile_status = 'ASSIGNED'
                """))
                details_exact = result_4a.rowcount
                if details_exact > 0:
                    self.logger(
                        f"   Step 4A: {details_exact} vouchers ASSIGNED → MATCHED (exact hash)",
                        "INFO"
                    )

                # 6. Step 4B: VIP Vouchers (Within-Tolerance)
                # For vouchers whose amount differs slightly from the invoice.
                # Linked by settlement_id + batch_number + voucher_ref.
                result_4b = conn.execute(text("""
                    UPDATE biq_stg.stg_card_details
                    SET    reconcile_status = 'MATCHED',
                           updated_at      = NOW()
                    FROM   biq_stg.stg_customer_portfolio p
                    WHERE  biq_stg.stg_card_details.settlement_id         = p.settlement_id
                      AND  biq_stg.stg_card_details.batch_number::VARCHAR = p.enrich_batch::VARCHAR
                      AND  biq_stg.stg_card_details.voucher_ref::VARCHAR  = p.enrich_ref::VARCHAR
                      AND  p.reconcile_status  = 'MATCHED'
                      AND  p.match_method      = 'VIP_EXACT_BATCH_REF_WRONG_AMT'
                      AND  p.match_confidence  = '95'
                      AND  p.is_suggestion     = FALSE
                      AND  biq_stg.stg_card_details.reconcile_status = 'ASSIGNED'
                """))
                details_tolerance = result_4b.rowcount
                if details_tolerance > 0:
                    self.logger(
                        f"   Step 4B: {details_tolerance} vouchers ASSIGNED → MATCHED "
                        f"(within-tolerance via batch+ref+settlement)",
                        "INFO"
                    )

                # 7. Step 4C: Parking Vouchers (via Settlement)
                # For PARKING, link via settlement_id and breakdown status.
                result_4c = conn.execute(text("""
                    UPDATE biq_stg.stg_card_details
                    SET    reconcile_status = 'MATCHED',
                           updated_at      = NOW()
                    WHERE  biq_stg.stg_card_details.reconcile_status = 'ASSIGNED'
                      AND  biq_stg.stg_card_details.settlement_id IN (
                               SELECT DISTINCT p.settlement_id
                               FROM   biq_stg.stg_customer_portfolio p
                               INNER JOIN biq_stg.stg_parking_pay_breakdown pb
                                   ON pb.settlement_id = p.settlement_id
                               WHERE  p.reconcile_status  = 'MATCHED'
                                 AND  p.reconcile_group   = 'PARKING_CARD'
                                 AND  pb.reconcile_status = 'ASSIGNED'
                           )
                """))
                details_parking = result_4c.rowcount
                if details_parking > 0:
                    self.logger(
                        f"   Step 4C: {details_parking} PARKING vouchers ASSIGNED → MATCHED "
                        f"(via settlement_id + breakdown)",
                        "INFO"
                    )

                # 8. Step 4D: Parking Breakdown (ASSIGNED → MATCHED)
                # Transition breakdown once vouchers are MATCHED.
                result_4d = conn.execute(text("""
                    UPDATE biq_stg.stg_parking_pay_breakdown pb
                    SET    reconcile_status = 'MATCHED'
                    FROM   biq_stg.stg_customer_portfolio p
                    WHERE  pb.settlement_id    = p.settlement_id
                      AND  p.reconcile_status  = 'MATCHED'
                      AND  p.reconcile_group   = 'PARKING_CARD'
                      AND  pb.reconcile_status = 'ASSIGNED'
                """))
                breakdown_matched = result_4d.rowcount
                if breakdown_matched > 0:
                    self.logger(
                        f"   Step 4D: {breakdown_matched} breakdown batches ASSIGNED → MATCHED",
                        "INFO"
                    )

                details_count = details_exact + details_tolerance + details_parking

                # 9. Step 5: Card Settlements (Sychronize Status)
                # A settlement is MATCHED when bank and all associated vouchers are MATCHED.
                result_p5 = conn.execute(text("""
                    UPDATE biq_stg.stg_card_settlements s
                    SET    reconcile_status = 'MATCHED',
                           updated_at      = NOW()
                    FROM   biq_stg.stg_bank_transactions b
                    WHERE  s.settlement_id    = b.settlement_id
                      AND  b.reconcile_status = 'MATCHED'
                      AND  s.reconcile_status != 'MATCHED'
                      AND  NOT EXISTS (
                               SELECT 1
                               FROM   biq_stg.stg_card_details d
                               WHERE  d.settlement_id    = s.settlement_id
                                 AND  d.reconcile_status != 'MATCHED'
                           )
                """))
                settlements_count = result_p5.rowcount
                if settlements_count > 0:
                    self.logger(
                        f"   Step 5: {settlements_count} settlements → MATCHED "
                        f"(bank + all vouchers MATCHED)",
                        "SUCCESS"
                    )
                else:
                    self.logger(
                        "   Step 5: 0 settlements updated (vouchers still pending or already MATCHED)",
                        "INFO"
                    )

                # 10. Summary & Completion
                self.logger("\n" + "─" * 80, "INFO")
                self.logger("VALIDATION SUMMARY v1.2", "INFO")
                self.logger("─" * 80, "INFO")
                self.logger(f"   Portfolio ENRICHED → MATCHED:          {confirmed_count}", "SUCCESS" if confirmed_count > 0 else "INFO")
                self.logger(f"   Portfolio waiting (analyst):         {suggestion_count}", "WARN" if suggestion_count > 0 else "INFO")
                self.logger(f"   VIP Vouchers ASSIGNED → MATCHED:     {details_exact + details_tolerance}", "INFO")
                self.logger(f"   PARKING Vouchers ASSIGNED → MATCHED: {details_parking}", "INFO")
                self.logger(f"   PARKING Breakdown → MATCHED:         {breakdown_matched}", "INFO")
                self.logger(f"   Settlements → MATCHED:               {settlements_count}", "SUCCESS" if settlements_count > 0 else "INFO")

            total = confirmed_count + details_count + breakdown_matched + settlements_count
            self.batch_tracker.complete_batch(
                records_processed=total,
                result_summary={
                    'portfolio_matched':   confirmed_count,
                    'suggestions':         suggestion_count,
                    'details_matched':     details_count,
                    'breakdown_matched':   breakdown_matched,
                    'settlements_matched': settlements_count,
                }
            )
            self.logger("ValidatePortfolioMatches completed", "SUCCESS")
            return True

        except Exception as e:
            self.batch_tracker.fail_batch(str(e))
            self.logger(f"Error: {e}", "ERROR")
            import traceback
            self.logger(traceback.format_exc(), "ERROR")
            return False
