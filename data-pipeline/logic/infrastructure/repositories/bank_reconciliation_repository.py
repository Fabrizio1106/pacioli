"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.repositories.bank_reconciliation_repository
===============================================================================

Description:
    Repository for persisting bank reconciliation results. It handles updates 
    for bank transactions, customer portfolio matches, and audit logging 
    within the staging environment.

Responsibilities:
    - Update bank transaction match status, confidence, and associated portfolio IDs.
    - Update customer portfolio invoice match status and settlement IDs.
    - Insert audit records for reconciliation processes.
    - Provide summaries and reset mechanisms for reconciliation status.

Key Components:
    - BankReconciliationRepository: Main class for reconciliation data persistence.

Notes:
    - Operates on biq_stg.stg_bank_transactions and biq_stg.stg_customer_portfolio.
    - Uses JSON for storing lists of IDs and audit details.
    - Implements positional access for PostgreSQL aggregation results for stability.

Dependencies:
    - json
    - sqlalchemy
    - typing

===============================================================================
"""

import json
from sqlalchemy import text
from typing import Optional, List, Dict


class BankReconciliationRepository:
    """
    Persistence repository for bank reconciliation results.
    Focuses on CRUD operations without business logic.
    """

    def __init__(self, session):
        # 1. Initialization
        self.session = session

    # ─────────────────────────────────────────────────────────────────────────
    # 1. BANK TRANSACTION UPDATES
    # ─────────────────────────────────────────────────────────────────────────

    def update_bank_transaction_match(
        self,
        stg_id: int,
        status: str,
        reason: str,
        confidence: float,
        diff: float,
        method: str,
        notes: str,
        port_ids: List[int],
        bank_ref_match: Optional[str] = None,
        enrich_customer_id: Optional[str] = None,
        enrich_customer_name: Optional[str] = None,
        enrich_confidence_score: Optional[int] = None,
        enrich_inference_method: Optional[str] = None,
    ) -> None:
        """
        Updates a single bank transaction with its reconciliation results.
        """

        # 1. Data Preparation
        port_ids_json = json.dumps(port_ids) if port_ids else None

        set_clauses = [
            "reconcile_status      = :status",
            "reconcile_reason      = :reason",
            "match_confidence_score = :confidence",
            "diff_adjustment       = :diff",
            "match_method          = :method",
            "enrich_notes          = :notes",
            "matched_portfolio_ids = :port_ids",
            "bank_ref_match        = :ref_match",
        ]

        params = {
            "id":        stg_id,
            "status":    status,
            "reason":    reason,
            "confidence": confidence,
            "diff":      diff,
            "method":    method,
            "notes":     notes,
            "port_ids":  port_ids_json,
            "ref_match": bank_ref_match,
        }

        # 2. Dynamic Clause Building
        if enrich_customer_id is not None:
            set_clauses.append("enrich_customer_id = :enrich_customer_id")
            params["enrich_customer_id"] = enrich_customer_id

        if enrich_customer_name is not None:
            set_clauses.append("enrich_customer_name = :enrich_customer_name")
            params["enrich_customer_name"] = enrich_customer_name

        if enrich_confidence_score is not None:
            set_clauses.append("enrich_confidence_score = :enrich_confidence_score")
            params["enrich_confidence_score"] = enrich_confidence_score

        if enrich_inference_method is not None:
            set_clauses.append("enrich_inference_method = :enrich_inference_method")
            params["enrich_inference_method"] = enrich_inference_method

        set_clauses.append("reconciled_at = NOW()")
        set_clauses.append("updated_at    = NOW()")

        set_str = ",\n                    ".join(set_clauses)

        # 3. Execution
        query = text(f"""
            UPDATE biq_stg.stg_bank_transactions
            SET {set_str}
            WHERE stg_id = :id
        """)

        self.session.execute(query, params)

    def bulk_update_bank_transactions(self, updates: List[Dict]) -> int:
        """
        Iteratively performs bulk updates for bank transactions.
        """

        if not updates:
            return 0

        for update in updates:
            self.update_bank_transaction_match(
                stg_id=update['id'],
                status=update['status'],
                reason=update['reason'],
                confidence=update['confidence'],
                diff=update['diff'],
                method=update['method'],
                notes=update['notes'],
                port_ids=update.get('port_ids', []),
                bank_ref_match=update.get('bank_ref_match'),
                enrich_customer_id=update.get('enrich_customer_id'),
                enrich_customer_name=update.get('enrich_customer_name'),
                enrich_confidence_score=update.get('enrich_confidence_score'),
                enrich_inference_method=update.get('enrich_inference_method'),
            )

        return len(updates)

    def mark_bank_as_no_match(
        self,
        stg_id: int,
        reason: str = 'NO_PORTFOLIO_DATA'
    ) -> None:
        """
        Marks a bank transaction as pending due to lack of matching portfolio data.
        """

        query = text("""
            UPDATE biq_stg.stg_bank_transactions
            SET reconcile_status        = 'PENDING',
                reconcile_reason        = :reason,
                match_confidence_score  = 0,
                enrich_notes            = 'No invoices available for matching',
                updated_at              = NOW()
            WHERE stg_id = :id
        """)

        self.session.execute(query, {"id": stg_id, "reason": reason})

    # ─────────────────────────────────────────────────────────────────────────
    # 2. PORTFOLIO UPDATES
    # ─────────────────────────────────────────────────────────────────────────

    def update_portfolio_invoice_match(
        self,
        stg_id: int,
        status: str = 'MATCHED'
    ) -> None:
        """
        Updates the reconciliation status for a single portfolio record.
        """

        query = text("""
            UPDATE biq_stg.stg_customer_portfolio
            SET reconcile_status = :status,
                reconciled_at    = NOW(),
                updated_at       = NOW()
            WHERE stg_id = :id
        """)

        self.session.execute(query, {"id": stg_id, "status": status})

    def bulk_update_portfolio_invoices(
        self,
        stg_ids: List[int],
        status: str = 'MATCHED'
    ) -> int:
        """
        Updates reconciliation status for multiple portfolio records.
        """

        if not stg_ids:
            return 0

        ids_str = ','.join(str(i) for i in stg_ids)

        query = text(f"""
            UPDATE biq_stg.stg_customer_portfolio
            SET reconcile_status = :status,
                reconciled_at    = NOW(),
                updated_at       = NOW()
            WHERE stg_id IN ({ids_str})
        """)

        result = self.session.execute(query, {"status": status})
        return result.rowcount

    # ─────────────────────────────────────────────────────────────────────────
    # 3. SETTLEMENT-BASED PORTFOLIO UPDATES
    # ─────────────────────────────────────────────────────────────────────────

    def bulk_update_portfolio_with_settlement(
        self,
        stg_ids: List[int],
        settlement_id: str,
        status: str = 'MATCHED',
        method: Optional[str] = None,
        confidence: Optional[int] = None,
        is_suggestion: Optional[int] = None,
    ) -> int:
        """
        Updates portfolio records with settlement details and match metadata.
        """

        if not stg_ids:
            return 0

        # 1. Clause Building
        set_clauses = [
            "settlement_id    = :settlement_id",
            "reconcile_status = :status",
            "reconciled_at    = NOW()",
            "updated_at       = NOW()",
        ]

        params = {"settlement_id": settlement_id, "status": status}

        if method is not None:
            set_clauses.append("match_method = :method")
            params["method"] = method

        if confidence is not None:
            set_clauses.append("match_confidence = :confidence")
            params["confidence"] = confidence

        if is_suggestion is not None:
            set_clauses.append("is_suggestion = :is_suggestion")
            params["is_suggestion"] = bool(is_suggestion)

        ids_str       = ','.join(str(i) for i in stg_ids)
        set_clause_str = ", ".join(set_clauses)

        # 2. Execution
        query = text(f"""
            UPDATE biq_stg.stg_customer_portfolio
            SET {set_clause_str}
            WHERE stg_id IN ({ids_str})
        """)

        result = self.session.execute(query, params)
        return result.rowcount

    # ─────────────────────────────────────────────────────────────────────────
    # 4. AUDIT LOGGING
    # ─────────────────────────────────────────────────────────────────────────

    def insert_audit_record(
        self,
        bank_stg_id: int,
        portfolio_stg_ids: List[int],
        match_method: str,
        match_confidence: float,
        amount_diff: float,
        details: Optional[Dict] = None,
    ) -> None:
        """
        Inserts an audit record documenting the reconciliation event.
        """

        # 1. Query Definition
        query = text("""
            INSERT INTO biq_stg.audit_bank_reconciliation
                (bank_stg_id, portfolio_stg_ids, match_method,
                 match_confidence, amount_diff, details, created_at)
            VALUES
                (:bank_id, :port_ids, :method,
                 :confidence, :diff, CAST(:details AS jsonb), NOW())
        """)

        # 2. Execution
        self.session.execute(query, {
            "bank_id":    bank_stg_id,
            "port_ids":   json.dumps(portfolio_stg_ids),
            "method":     match_method,
            "confidence": match_confidence,
            "diff":       amount_diff,
            "details":    json.dumps(details) if details else None,
        })

    # ─────────────────────────────────────────────────────────────────────────
    # 5. SUMMARY & HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    def get_match_summary(self) -> Dict:
        """
        Retrieves a summary of reconciliation counts and average confidence.
        """

        # 1. Query Definition
        query = text("""
            SELECT
                SUM(CASE WHEN reconcile_status = 'MATCHED' THEN 1 ELSE 0 END) AS matched,
                SUM(CASE WHEN reconcile_status = 'REVIEW'  THEN 1 ELSE 0 END) AS review,
                SUM(CASE WHEN reconcile_status = 'PENDING' THEN 1 ELSE 0 END) AS pending,
                AVG(CASE WHEN match_confidence_score > 0
                    THEN match_confidence_score ELSE NULL END)                 AS avg_confidence
            FROM biq_stg.stg_bank_transactions
            WHERE reconciled_at IS NOT NULL
        """)

        # 2. Execution
        result = self.session.execute(query).fetchone()

        # 3. Mapping
        if result:
            return {
                'matched_count':  int(result[0] or 0),
                'review_count':   int(result[1] or 0),
                'pending_count':  int(result[2] or 0),
                'avg_confidence': float(result[3] or 0),
            }

        return {
            'matched_count':  0,
            'review_count':   0,
            'pending_count':  0,
            'avg_confidence': 0.0,
        }

    def reset_reconciliation_status(self, customer_code: Optional[str] = None) -> int:
        """
        Resets reconciliation status for bank and portfolio records.
        """

        if customer_code:
            # 1. Targeted Reset
            q_bank = text("""
                UPDATE biq_stg.stg_bank_transactions
                SET reconcile_status      = 'PENDING',
                    reconcile_reason      = NULL,
                    match_confidence_score = 0,
                    matched_portfolio_ids = NULL,
                    reconciled_at         = NULL
                WHERE enrich_customer_id = :cust
            """)

            q_port = text("""
                UPDATE biq_stg.stg_customer_portfolio
                SET reconcile_status = 'PENDING',
                    settlement_id    = NULL,
                    match_method     = NULL,
                    match_confidence = NULL,
                    reconciled_at    = NULL
                WHERE customer_code = :cust
            """)

            r1 = self.session.execute(q_bank, {"cust": customer_code})
            r2 = self.session.execute(q_port, {"cust": customer_code})
            return r1.rowcount + r2.rowcount

        else:
            # 2. Global Reset
            q_bank = text("""
                UPDATE biq_stg.stg_bank_transactions
                SET reconcile_status      = 'PENDING',
                    reconcile_reason      = NULL,
                    match_confidence_score = 0,
                    matched_portfolio_ids = NULL,
                    reconciled_at         = NULL
            """)

            q_port = text("""
                UPDATE biq_stg.stg_customer_portfolio
                SET reconcile_status = 'PENDING',
                    settlement_id    = NULL,
                    match_method     = NULL,
                    match_confidence = NULL,
                    reconciled_at    = NULL
            """)

            r1 = self.session.execute(q_bank)
            r2 = self.session.execute(q_port)
            return r1.rowcount + r2.rowcount
