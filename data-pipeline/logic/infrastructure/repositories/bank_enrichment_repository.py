"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.repositories.bank_enrichment_repository
===============================================================================

Description:
    Repository for updating bank transaction enrichment data. It handles 
    updates for customer matches, settlement IDs, and voucher amounts 
    within the staging environment.

Responsibilities:
    - Update customer match information for bank transactions.
    - Update settlement IDs and establishment names.
    - Synchronize voucher amounts (gross, net, commissions, taxes).
    - Provide data retrieval for pending or unenriched transactions.

Key Components:
    - BankEnrichmentRepository: Main class for bank transaction enrichment updates.

Notes:
    - Operates on the biq_stg.stg_bank_transactions table.
    - Inherits from BaseRepository for common database operations.
    - Uses SQLAlchemy Session for database interactions.

Dependencies:
    - pandas
    - sqlalchemy
    - logic.infrastructure.repositories.base_repository
    - utils.logger

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from logic.infrastructure.repositories.base_repository import BaseRepository
from utils.logger import get_logger


class BankEnrichmentRepository(BaseRepository):
    """
    Update-only repository for biq_stg.stg_bank_transactions.
    Centralizes all enrichment UPDATE operations.
    """

    def __init__(self, session: Session):
        # 1. Initialization
        # Parent initialization must occur before logger assignment
        super().__init__(session)
        self.logger = get_logger("BANK_ENRICHMENT_REPO")

    def _get_table_name(self) -> str:
        return "biq_stg.stg_bank_transactions"

    def _get_primary_key(self) -> str:
        return "stg_id"

    # ─────────────────────────────────────────────────────────────────────────
    # 1. CUSTOMER MATCH UPDATES
    # ─────────────────────────────────────────────────────────────────────────

    def update_customer_match(
        self,
        stg_id: int,
        customer_id: str,
        customer_name: str,
        confidence: int,
        method: str,
        notes: str = None
    ) -> bool:
        """
        Updates customer matching information for a single transaction.
        """

        try:
            # 1. Query Definition
            query = text("""
                UPDATE biq_stg.stg_bank_transactions
                SET enrich_customer_id      = :customer_id,
                    enrich_customer_name    = :customer_name,
                    enrich_confidence_score = :confidence,
                    enrich_inference_method = :method,
                    enrich_notes            = :notes
                WHERE stg_id = :stg_id
            """)

            # 2. Execution
            self.session.execute(query, {
                "stg_id":       stg_id,
                "customer_id":  str(customer_id),
                "customer_name": str(customer_name)[:255],
                "confidence":   int(confidence),
                "method":       str(method)[:100],
                "notes":        str(notes)[:499] if notes else None,
            })
            return True

        except Exception as e:
            self.logger(f"Error update_customer_match stg_id={stg_id}: {e}", "ERROR")
            return False

    def update_customer_match_batch(self, updates: list) -> int:
        """
        Performs batch updates for customer matching.
        """

        if not updates:
            return 0

        # 1. Query Definition
        query = text("""
            UPDATE biq_stg.stg_bank_transactions
            SET enrich_customer_id      = :customer_id,
                enrich_customer_name    = :customer_name,
                enrich_confidence_score = :confidence,
                enrich_inference_method = :method,
                enrich_notes            = :notes
            WHERE stg_id = :stg_id
        """)

        # 2. Data Sanitization
        sanitized = [
            {
                "stg_id":       u["stg_id"],
                "customer_id":  str(u.get("customer_id", ""))[:50],
                "customer_name": str(u.get("customer_name", ""))[:255],
                "confidence":   int(u.get("confidence", 0)),
                "method":       str(u.get("method", ""))[:100],
                "notes":        str(u.get("notes", ""))[:499] if u.get("notes") else None,
            }
            for u in updates
        ]

        # 3. Execution
        self.session.execute(query, sanitized)
        return len(sanitized)

    # ─────────────────────────────────────────────────────────────────────────
    # 2. SETTLEMENT ID UPDATES
    # ─────────────────────────────────────────────────────────────────────────

    def update_settlement_id(
        self,
        stg_id: int,
        settlement_id: str,
        establishment_name: str = None
    ) -> bool:
        """
        Updates settlement ID and establishment name for a transaction.
        """

        try:
            # 1. Query Definition
            query = text("""
                UPDATE biq_stg.stg_bank_transactions
                SET settlement_id      = :settlement_id,
                    establishment_name = COALESCE(:establishment_name, establishment_name)
                WHERE stg_id = :stg_id
            """)

            # 2. Execution
            self.session.execute(query, {
                "stg_id":            stg_id,
                "settlement_id":     str(settlement_id),
                "establishment_name": establishment_name,
            })
            return True

        except Exception as e:
            self.logger(f"Error update_settlement_id stg_id={stg_id}: {e}", "ERROR")
            return False

    # ─────────────────────────────────────────────────────────────────────────
    # 3. VOUCHER AMOUNT UPDATES
    # ─────────────────────────────────────────────────────────────────────────

    def update_voucher_amounts(
        self,
        stg_id: int,
        settlement_id: str,
        establishment_name: str,
        final_gross: float,
        final_net: float,
        final_commission: float,
        final_tax_iva: float,
        final_tax_irf: float
    ) -> bool:
        """
        Updates final amounts related to vouchers and settlements.
        """

        try:
            # 1. Query Definition
            query = text("""
                UPDATE biq_stg.stg_bank_transactions
                SET settlement_id           = :settlement_id,
                    establishment_name      = :establishment_name,
                    final_amount_gross      = :final_gross,
                    final_amount_net        = :final_net,
                    final_amount_commission = :final_commission,
                    final_amount_tax_iva    = :final_tax_iva,
                    final_amount_tax_irf    = :final_tax_irf
                WHERE stg_id = :stg_id
            """)

            # 2. Execution
            self.session.execute(query, {
                "stg_id":            stg_id,
                "settlement_id":     str(settlement_id),
                "establishment_name": str(establishment_name),
                "final_gross":       round(float(final_gross), 2),
                "final_net":         round(float(final_net), 2),
                "final_commission":  round(float(final_commission), 2),
                "final_tax_iva":     round(float(final_tax_iva), 2),
                "final_tax_irf":     round(float(final_tax_irf), 2),
            })
            return True

        except Exception as e:
            self.logger(f"Error update_voucher_amounts stg_id={stg_id}: {e}", "ERROR")
            return False

    # ─────────────────────────────────────────────────────────────────────────
    # 4. UNIVERSAL SETTLEMENT UPDATES
    # ─────────────────────────────────────────────────────────────────────────

    def update_settlement_id_universal(self, trans_type_tc: str = "LIQUIDACION TC") -> int:
        """
        Standardizes settlement IDs across non-credit card transactions.
        """

        # 1. Query Definition
        query = text("""
            UPDATE biq_stg.stg_bank_transactions
            SET settlement_id = bank_ref_1
            WHERE trans_type  != :tc
              AND settlement_id IS NULL
              AND bank_ref_1   IS NOT NULL
        """)

        # 2. Execution
        result = self.session.execute(query, {"tc": trans_type_tc})
        rows   = result.rowcount

        if rows > 0:
            self.logger(f"Universalized settlement IDs for {rows} rows", "INFO")

        return rows

    # ─────────────────────────────────────────────────────────────────────────
    # 5. DATA RETRIEVAL QUERIES
    # ─────────────────────────────────────────────────────────────────────────

    def get_pending_transactions(
        self,
        trans_types: list = None,
        only_unenriched: bool = True
    ) -> pd.DataFrame:
        """
        Retrieves transactions that require enrichment.
        """

        # 1. Condition Building
        conditions = []
        params     = {}

        if only_unenriched:
            conditions.append(
                "(enrich_confidence_score IS NULL OR enrich_confidence_score < 99)"
            )

        if trans_types:
            placeholders = ", ".join([f":type_{i}" for i in range(len(trans_types))])
            conditions.append(f"trans_type IN ({placeholders})")
            for i, t in enumerate(trans_types):
                params[f"type_{i}"] = t

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        # 2. Query Definition
        query = text(f"""
            SELECT * FROM biq_stg.stg_bank_transactions
            {where}
        """)

        # 3. Execution
        return pd.read_sql(query, self.session.connection(), params=params)

    def get_all_bank_refs(self, only_unenriched: bool = True) -> pd.DataFrame:
        """
        Retrieves bank references for matching processes.
        """

        # 1. Condition Building
        condition = (
            "WHERE (enrich_confidence_score IS NULL OR enrich_confidence_score < 99)"
            if only_unenriched else ""
        )

        # 2. Query Definition
        query = text(f"""
            SELECT stg_id, bank_ref_1, bank_ref_2, trans_type, brand
            FROM biq_stg.stg_bank_transactions
            {condition}
        """)

        # 3. Execution
        return pd.read_sql(query, self.session.connection())
