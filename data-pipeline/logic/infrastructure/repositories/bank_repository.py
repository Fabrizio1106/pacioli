"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.repositories.bank_repository
===============================================================================

Description:
    Repository for managing bank transactions in the staging environment. 
    It provides methods for querying transactions by status, document number, 
    customer, and other financial attributes.

Responsibilities:
    - Retrieve pending transactions for matching processes.
    - Query transactions by document number, bank reference, or settlement ID.
    - Update reconciliation status and match metadata.
    - Provide statistical summaries of transactions by status.
    - Handle bulk updates for transaction statuses.

Key Components:
    - BankTransactionRepository: Main class for bank transaction data access.

Notes:
    - Operates on the biq_stg.stg_bank_transactions table.
    - Uses PostgreSQL-specific syntax for date arithmetic and interval calculations.
    - Inherits from BaseRepository for standard CRUD operations.

Dependencies:
    - sqlalchemy
    - typing
    - logic.infrastructure.repositories.base_repository

===============================================================================
"""

from logic.infrastructure.repositories.base_repository import BaseRepository
from typing import List, Dict, Any, Optional
from sqlalchemy import text
import json
import re
import pandas as pd


class BankTransactionRepository(BaseRepository):
    """
    Repository for managing bank transactions.
    TABLE: biq_stg.stg_bank_transactions
    PRIMARY KEY: stg_id
    """

    def _get_table_name(self) -> str:
        return "biq_stg.stg_bank_transactions"

    def _get_primary_key(self) -> str:
        return "stg_id"

    # ─────────────────────────────────────────────────────────────────────────
    # 1. PENDING TRANSACTIONS
    # ─────────────────────────────────────────────────────────────────────────

    def get_pending_for_matching(
        self,
        limit: int = 1000,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieves pending transactions within an optional date range.
        """

        if date_from or date_to:
            # 1. Query Building
            query_str = f"""
                SELECT * FROM {self._get_table_name()}
                WHERE reconcile_status = 'PENDING'
            """
            params = {}

            if date_from:
                query_str += " AND bank_date >= :date_from"
                params["date_from"] = date_from

            if date_to:
                query_str += " AND bank_date <= :date_to"
                params["date_to"] = date_to

            query_str += f" ORDER BY bank_date DESC LIMIT {limit}"

            # 2. Execution
            results = self.session.execute(text(query_str), params).fetchall()
            return [dict(row._mapping) for row in results]

        return self.get_all(
            filters={"reconcile_status": "PENDING"},
            order_by="bank_date DESC",
            limit=limit,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # 2. LOOKUP BY ATTRIBUTES
    # ─────────────────────────────────────────────────────────────────────────

    def get_by_doc_number(self, doc_number: str) -> Optional[Dict[str, Any]]:
        """Retrieves a transaction by its document number."""
        results = self.get_all(filters={"doc_number": doc_number})
        return results[0] if results else None

    def get_by_bank_reference(
        self,
        bank_ref_1: Optional[str] = None,
        bank_ref_2: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieves transactions by bank reference fields."""

        filters = {}
        if bank_ref_1:
            filters["bank_ref_1"] = bank_ref_1
        if bank_ref_2:
            filters["bank_ref_2"] = bank_ref_2
        if not filters:
            return []

        return self.get_all(filters=filters)

    def get_by_customer(
        self,
        customer_id: str,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieves transactions associated with a specific customer."""

        filters = {"enrich_customer_id": customer_id}
        if status:
            filters["reconcile_status"] = status

        return self.get_all(filters=filters, order_by="bank_date DESC")

    def get_by_amount_range(
        self,
        min_amount: float,
        max_amount: float,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieves transactions within a specific amount range."""

        # 1. Query Definition
        query_str = f"""
            SELECT * FROM {self._get_table_name()}
            WHERE amount_total >= :min_amount
              AND amount_total <= :max_amount
        """
        params = {"min_amount": min_amount, "max_amount": max_amount}

        if status:
            query_str += " AND reconcile_status = :status"
            params["status"] = status

        query_str += " ORDER BY bank_date DESC"

        # 2. Execution
        results = self.session.execute(text(query_str), params).fetchall()
        return [dict(row._mapping) for row in results]

    def get_unmatched_last_n_days(self, days: int = 30) -> List[Dict[str, Any]]:
        """
        Retrieves unmatched transactions from the last N days using PostgreSQL intervals.
        """
        # 1. Execution
        query = text(f"""
            SELECT * FROM {self._get_table_name()}
            WHERE reconcile_status = 'PENDING'
              AND bank_date >= CURRENT_DATE - INTERVAL '1 day' * :days
            ORDER BY bank_date DESC
        """)

        results = self.session.execute(query, {"days": days}).fetchall()
        return [dict(row._mapping) for row in results]

    def get_by_settlement_id(self, settlement_id: str) -> List[Dict[str, Any]]:
        """Retrieves transactions by settlement ID."""
        return self.get_all(
            filters={"settlement_id": settlement_id},
            order_by="bank_date ASC",
        )

    def get_by_brand(
        self,
        brand: str,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieves transactions by brand (e.g., VISA, MASTERCARD)."""

        filters = {"brand": brand}
        if status:
            filters["reconcile_status"] = status

        return self.get_all(filters=filters, order_by="bank_date DESC")

    # ─────────────────────────────────────────────────────────────────────────
    # 3. UPDATES
    # ─────────────────────────────────────────────────────────────────────────

    def update_reconcile_status(
        self,
        tx_id: int,
        new_status: str,
        match_method: Optional[str] = None,
        match_confidence: Optional[float] = None,
    ) -> bool:
        """Updates the reconciliation status and metadata for a transaction."""

        updates = {"reconcile_status": new_status}
        if match_method:
            updates["match_method"] = match_method
        if match_confidence is not None:
            updates["match_confidence_score"] = match_confidence

        return self.update_by_id(tx_id, updates)

    def bulk_update_status(self, ids: List[int], new_status: str) -> int:
        """Performs bulk status updates for multiple transaction IDs."""

        if not ids:
            return 0

        ids_str = ",".join(map(str, ids))

        query = text(f"""
            UPDATE {self._get_table_name()}
            SET reconcile_status = :status
            WHERE {self._get_primary_key()} IN ({ids_str})
        """)

        result = self.session.execute(query, {"status": new_status})
        return result.rowcount

    # ─────────────────────────────────────────────────────────────────────────
    # 4. SPECIAL QUERIES & STATISTICS
    # ─────────────────────────────────────────────────────────────────────────

    def get_compensated_transactions(
        self,
        sap_compensated: Optional[bool] = None,
        intraday_compensated: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieves transactions based on compensation flags."""

        filters = {}
        if sap_compensated is not None:
            filters["is_compensated_sap"] = 1 if sap_compensated else 0
        if intraday_compensated is not None:
            filters["is_compensated_intraday"] = 1 if intraday_compensated else 0

        return self.get_all(filters=filters, order_by="bank_date DESC")

    def get_statistics_by_status(self) -> Dict[str, Any]:
        """Calculates transaction statistics grouped by reconciliation status."""

        # 1. Query Definition
        query = text(f"""
            SELECT
                reconcile_status,
                COUNT(*)          AS cnt,
                SUM(amount_total) AS total_amount
            FROM {self._get_table_name()}
            GROUP BY reconcile_status
        """)

        # 2. Execution
        results = self.session.execute(query).fetchall()

        # 3. Data Aggregation
        stats = {
            "count_by_status":  {},
            "amount_by_status": {},
            "total_count":      0,
            "total_amount":     0.0,
        }

        for row in results:
            # Positional access for robustness in PostgreSQL
            status = row[0] or "UNKNOWN"
            count  = row[1]
            amount = float(row[2] or 0)

            stats["count_by_status"][status]  = count
            stats["amount_by_status"][status] = amount
            stats["total_count"]  += count
            stats["total_amount"] += amount

        return stats

    # ─────────────────────────────────────────────────────────────────────────
    # 5. CARD SETTLEMENT QUERIES
    # ─────────────────────────────────────────────────────────────────────────

    def get_card_settlements_pending(self) -> pd.DataFrame:
        """
        Returns all LIQUIDACION TC transactions joined with card settlement
        data, filtered to those not yet compensated in SAP or intraday.
        Primary input for the validation metrics update process.
        """
        query = text("""
            SELECT
                b.stg_id                    AS bank_stg_id,
                b.settlement_id,
                b.establishment_name,
                b.brand,
                b.amount_total              AS banco_net,
                b.reconcile_status          AS banco_status,
                b.final_amount_commission   AS voucher_commission,
                s.count_voucher             AS count_voucher_bank,
                s.amount_gross              AS settlement_gross,
                s.amount_net                AS settlement_net,
                s.amount_commission         AS settlement_commission,
                s.amount_tax_iva            AS settlement_iva,
                s.amount_tax_irf            AS settlement_irf,
                s.reconcile_status          AS settlement_status
            FROM biq_stg.stg_bank_transactions b
            LEFT JOIN biq_stg.stg_card_settlements s
                ON b.settlement_id = s.settlement_id
            WHERE b.trans_type = 'LIQUIDACION TC'
              AND b.settlement_id IS NOT NULL
              AND b.is_compensated_sap = FALSE
              AND b.is_compensated_intraday = FALSE
        """)
        return pd.read_sql(query, self.session.connection())

    def bulk_update_validation_metrics(self, df: pd.DataFrame) -> int:
        """
        Updates validation metrics for a batch of LIQUIDACION TC transactions.
        Handles port_ids serialization internally.
        Returns count of updated rows.
        """
        update_count = 0

        for _, row in df.iterrows():
            raw_ids = None
            for col in ['final_portfolio_ids', 'matched_ids', 'suggested_ids']:
                if (col in row
                        and pd.notna(row[col])
                        and str(row[col]).strip() not in ('nan', 'None', '')):
                    raw_ids = row[col]
                    break

            port_ids_json = None
            if raw_ids is not None:
                str_ids = str(raw_ids).replace('.0', '')
                numeros = re.findall(r'\d+', str_ids)
                if numeros:
                    port_ids_json = json.dumps([int(n) for n in numeros])

            query = text("""
                UPDATE biq_stg.stg_bank_transactions
                SET
                    count_voucher_bank       = :count_voucher_bank,
                    count_voucher_portfolio  = :count_portfolio,
                    final_amount_commission  = :final_commission,
                    diff_adjustment          = :diff_adj,
                    reconcile_reason         = :reason,
                    reconcile_status         = :status,
                    enrich_notes             = :notes,
                    matched_portfolio_ids    = :port_ids,
                    match_method             = 'CARD_SETTLEMENT_MATCH',
                    bank_ref_match           = :settlement_id,
                    reconciled_at            = NOW(),
                    updated_at               = NOW()
                WHERE stg_id = :bank_stg_id
                  AND trans_type = 'LIQUIDACION TC'
            """)

            self.session.execute(query, {
                'count_voucher_bank': int(row['count_voucher_bank']),
                'count_portfolio':    int(row['count_confirmed']),
                'final_commission':   float(row['final_commission_adjusted']),
                'diff_adj':           float(row['diff_adjustment']),
                'reason':             row['reconcile_reason'],
                'status':             row['reconcile_status'],
                'notes':              row['enrich_notes'],
                'port_ids':           port_ids_json,
                'settlement_id':      str(row['settlement_id']),
                'bank_stg_id':        row['bank_stg_id'],
            })
            update_count += 1

        return update_count
