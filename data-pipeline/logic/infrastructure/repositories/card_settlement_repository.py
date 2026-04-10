"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.repositories.card_settlement_repository
===============================================================================

Description:
    Repository for managing card settlement reconciliation statuses in the
    staging environment. Provides targeted bulk-update operations on
    biq_stg.stg_card_settlements, scoped to the validation metrics process.

Responsibilities:
    - Bulk-update reconcile_status to MATCHED for settlements that passed
      the Golden Rule (perfect match, split payment, or within tolerance).
    - Bulk-update reconcile_status to REVIEW for settlements with mismatches
      or unresolved suggestions.
    - Protect already-finalized settlements (MATCHED, MATCHED_MANUAL) from
      being overwritten.

Key Components:
    - CardSettlementRepository: Focused repository for stg_card_settlements.
      Does not inherit BaseRepository — the table has no single-row CRUD
      need here; only bulk status transitions are required.

Notes:
    - Operates exclusively on biq_stg.stg_card_settlements.
    - The reconcile_status guard (NOT IN 'MATCHED', 'MATCHED_MANUAL') is
      intentional: manual overrides must never be clobbered by automation.

Dependencies:
    - typing
    - sqlalchemy
    - utils.logger

===============================================================================
"""

from typing import List
from sqlalchemy import text
from sqlalchemy.orm import Session
from utils.logger import get_logger


class CardSettlementRepository:
    """
    Repository for biq_stg.stg_card_settlements.
    Scoped to bulk reconcile_status transitions driven by validation metrics.
    """

    _TABLE = "biq_stg.stg_card_settlements"

    def __init__(self, session: Session):
        self.session = session
        self.logger  = get_logger("CARD_SETTLEMENT_REPO")

    # ─────────────────────────────────────────────────────────────────────────
    # 1. BULK STATUS UPDATES
    # ─────────────────────────────────────────────────────────────────────────

    def bulk_update_reconcile_status(
        self,
        settlement_ids: List,
        status: str,
    ) -> int:
        """
        Set reconcile_status = status for all given settlement_ids,
        skipping those already in MATCHED or MATCHED_MANUAL.

        Args:
            settlement_ids: List of settlement_id values to update.
            status:         Target status, typically 'MATCHED' or 'REVIEW'.

        Returns:
            Number of rows actually updated.
        """
        if not settlement_ids:
            return 0

        query = text("""
            UPDATE biq_stg.stg_card_settlements
            SET reconcile_status = :status,
                updated_at       = NOW()
            WHERE settlement_id = ANY(:settlement_ids)
              AND reconcile_status NOT IN ('MATCHED', 'MATCHED_MANUAL')
        """)

        result = self.session.execute(
            query,
            {'status': status, 'settlement_ids': settlement_ids},
        )

        self.logger(
            f"{result.rowcount} settlements → {status}", "INFO"
        )
        return result.rowcount
