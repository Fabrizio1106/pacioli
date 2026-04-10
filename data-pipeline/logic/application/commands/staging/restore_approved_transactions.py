"""
===============================================================================
Project: PACIOLI
Module: logic.application.commands.staging.restore_approved_transactions
===============================================================================

Description:
    Safety command executed at the end of the pipeline to restore the approved 
    status of manually reconciled transactions. It uses bank_ref_1 as a 
    permanent identity link between workitems and staging tables.

Responsibilities:
    - Synchronize stg_id in workitems with the new stg_id in the bank table 
      after re-insertion.
    - Restore the reconcile_status to MATCHED_MANUAL in stg_bank_transactions 
      using APPROVED workitems as the source of truth.
    - Ensure that manual approvals are not lost even if match_hash_keys change.

Key Components:
    - RestoreApprovedTransactionsCommand: Command to ensure manual approval 
      persistence.

Notes:
    - Runs at the end of the pipeline (Group 6).
    - Only affects transactions with APPROVED workitems.
    - bank_ref_1 is used as the immutable bridge between layers.

Dependencies:
    - sqlalchemy
    - utils.db_config, utils.logger
===============================================================================
"""

from sqlalchemy import text
from utils.db_config import get_db_engine
from utils.logger import get_logger


class RestoreApprovedTransactionsCommand:
    """
    Command for restoring transactions approved by analysts.

    Ensures that no manual approval is lost between pipeline runs,
    using bank_ref_1 as permanent identity data.
    """

    def __init__(self):
        # 1. Initialization
        self.logger     = get_logger("RESTORE_APPROVED_TXS")
        self.engine_stg = get_db_engine('stg')

    def execute(self, force: bool = False, **kwargs) -> bool:
        self.logger("═" * 80, "INFO")
        self.logger("RESTORE APPROVED TRANSACTIONS v1.0", "INFO")
        self.logger("═" * 80, "INFO")
        self.logger(
            "Purpose: ensure manual approvals survive pipeline re-processing.",
            "INFO"
        )

        try:
            with self.engine_stg.connect() as conn:
                trans = conn.begin()
                try:

                    # 2. Step 1: Synchronize stg_id in workitems
                    # After each DELETE + re-insert, stg_ids change.
                    # The workitem points to the old stg_id — we update it
                    # using bank_ref_1 as a permanent bridge.
                    step1_result = conn.execute(text("""
                        UPDATE biq_auth.transaction_workitems w
                        SET stg_id     = bt.stg_id,
                            updated_at = NOW()
                        FROM biq_stg.stg_bank_transactions bt
                        WHERE bt.bank_ref_1 = w.bank_ref_1
                          AND w.work_status = 'APPROVED'
                          AND w.stg_id IS DISTINCT FROM bt.stg_id
                    """))

                    step1_count = step1_result.rowcount
                    if step1_count > 0:
                        self.logger(
                            f"Step 1: {step1_count} stg_ids updated in workitems",
                            "INFO"
                        )
                    else:
                        self.logger(
                            "Step 1: stg_ids already synchronized — no changes needed",
                            "INFO"
                        )

                    # 3. Step 2: Restore reconcile_status in bank
                    # If _preserve_reconciliation_state did not find the hash_key
                    # (because it changed between runs), the row remains PENDING.
                    # We restore it to MATCHED_MANUAL using the workitem as 
                    # source of truth. bank_ref_1 is the bridge.
                    #
                    # Also restore key fields approved by the analyst:
                    # - matched_portfolio_ids: selected invoices
                    # - reconcile_reason: reason for manual match
                    # - match_method: always MANUAL_MATCH
                    # - reconciled_at: approval timestamp
                    step2_result = conn.execute(text("""
                        UPDATE biq_stg.stg_bank_transactions bt
                        SET reconcile_status       = 'MATCHED_MANUAL',
                            match_method            = 'MANUAL_MATCH',
                            match_confidence_score  = 100,
                            reconcile_reason        = 'MANUAL_MATCH',
                            matched_portfolio_ids   = w.approved_portfolio_ids,
                            reconciled_at           = w.approved_at,
                            updated_at              = NOW()
                        FROM biq_auth.transaction_workitems w
                        WHERE bt.bank_ref_1 = w.bank_ref_1
                          AND w.work_status = 'APPROVED'
                          AND bt.reconcile_status != 'MATCHED_MANUAL'
                    """))

                    step2_count = step2_result.rowcount
                    if step2_count > 0:
                        self.logger(
                            f"Step 2: {step2_count} transactions restored to MATCHED_MANUAL",
                            "WARN"
                        )
                        self.logger(
                            "This indicates that the hash_key changed between runs for those rows. "
                            "State was recovered from workitems.",
                            "WARN"
                        )
                    else:
                        self.logger(
                            "Step 2: All statuses are correct — no restorations needed",
                            "INFO"
                        )

                    trans.commit()

                    # 4. Final Reporting
                    self.logger("", "INFO")
                    self.logger("RESTORATION SUMMARY:", "INFO")
                    self.logger(f"   stg_ids synchronized in workitems : {step1_count}", "INFO")
                    self.logger(f"   Transactions restored in bank  : {step2_count}", "INFO")

                    if step2_count == 0:
                        self.logger(
                            "\nPipeline consistent — no manual approvals were affected by re-processing.",
                            "SUCCESS"
                        )
                    else:
                        self.logger(
                            f"\nDetected and recovered {step2_count} transactions whose status was overwritten by the pipeline.",
                            "WARN"
                        )
                        self.logger(
                            "Probable cause: match_hash_key changed between runs.",
                            "WARN"
                        )
                        self.logger(
                            "Action: check the hash counter for those cases.",
                            "WARN"
                        )

                    self.logger("", "INFO")
                    self.logger("═" * 80, "SUCCESS")
                    self.logger("RESTORATION COMPLETED", "SUCCESS")
                    self.logger("═" * 80, "SUCCESS")

                    return True

                except Exception as e:
                    trans.rollback()
                    raise e

        except Exception as e:
            self.logger(f"Error in restoration: {e}", "ERROR")
            import traceback
            self.logger(traceback.format_exc(), "ERROR")
            return False
