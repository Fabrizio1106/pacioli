"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.repositories.withholdings_operations_repository
===============================================================================

Description:
    Repository for matching and applying withholdings to customer invoices. 
    It interacts with multiple staging tables to manage the withholding lifecycle.

Responsibilities:
    - Retrieve withholdings pending customer or invoice matching.
    - Update matching results for customers and invoices.
    - Mark withholdings as applied or ineligible with specific reasons.
    - Create exceptions for failed matching or validation processes.
    - Update the customer portfolio by applying withholding amounts.

Key Components:
    - WithholdingsOperationsRepository: Specialized repository for withholding operations.

Notes:
    - Target Tables: biq_stg.stg_withholdings, biq_stg.stg_customer_portfolio, 
      biq_stg.withholdings_exceptions.
    - Uses PostgreSQL-specific features like JSONB for ineligibility reasons.

Dependencies:
    - pandas
    - sqlalchemy
    - json

===============================================================================
"""

import json
import pandas as pd
from sqlalchemy import text
from typing import Optional, List, Dict


class WithholdingsOperationsRepository:
    """
    Repository for matching and applying withholdings.
    TABLES: biq_stg.stg_withholdings, biq_stg.stg_customer_portfolio,
            biq_stg.withholdings_exceptions
    """

    def __init__(self, session):
        self.session = session

    # ─────────────────────────────────────────────────────────────────────────
    # READ — MATCHING
    # ─────────────────────────────────────────────────────────────────────────

    def get_pending_for_customer_matching(self) -> pd.DataFrame:
        # 1. Query preparation
        query = text("""
            SELECT
                stg_id,
                customer_ruc,
                customer_name_raw,
                customer_name_normalized,
                withholding_ref
            FROM biq_stg.stg_withholdings
            WHERE reconcile_status = 'NEW'
              AND is_registrable    = TRUE
              AND customer_code_sap IS NULL
            ORDER BY stg_id
        """)

        # 2. Data retrieval
        # CHANGE: self.session.bind deprecated → self.session.connection()
        return pd.read_sql(query, self.session.connection())

    def get_pending_for_invoice_matching(self) -> pd.DataFrame:
        # 1. Query preparation
        query = text("""
            SELECT
                stg_id,
                customer_code_sap,
                invoice_ref_clean,
                invoice_ref_sustento,
                invoice_assignment,
                withholding_ref,
                valor_ret_iva
            FROM biq_stg.stg_withholdings
            WHERE reconcile_status = 'CUSTOMER_MATCHED'
              AND is_registrable    = TRUE
              AND invoice_sap_doc   IS NULL
            ORDER BY stg_id
        """)

        # 2. Data retrieval
        return pd.read_sql(query, self.session.connection())

    # ─────────────────────────────────────────────────────────────────────────
    # READ — APPLICATION
    # ─────────────────────────────────────────────────────────────────────────

    def get_pending_for_application(self) -> pd.DataFrame:
        # 1. Query preparation
        query = text("""
            SELECT
                stg_id,
                withholding_ref,
                customer_code_sap,
                invoice_sap_doc,
                invoice_assignment,
                invoice_ref_sustento,
                valor_ret_iva,
                valor_ret_renta,
                porcentaje_ret_iva,
                base_ret_iva,
                reconcile_status,
                eligibility_status
            FROM biq_stg.stg_withholdings
            WHERE reconcile_status  = 'INVOICE_MATCHED'
              AND is_registrable     = TRUE
              AND eligibility_status IN ('PENDING', 'ELIGIBLE')
            ORDER BY stg_id
        """)

        # 2. Data retrieval
        # CHANGE: self.session.bind → self.session.connection()
        return pd.read_sql(query, self.session.connection())

    # ─────────────────────────────────────────────────────────────────────────
    # UPDATES — CUSTOMER MATCHING
    # ─────────────────────────────────────────────────────────────────────────

    def update_customer_match(
        self,
        stg_id: int,
        customer_code: str,
        confidence: str,
        method: str,
    ) -> None:
        # 1. Query execution
        query = text("""
            UPDATE biq_stg.stg_withholdings
            SET customer_code_sap = :code,
                match_confidence  = :confidence,
                reconcile_status  = 'CUSTOMER_MATCHED',
                updated_at        = NOW()
            WHERE stg_id = :id
        """)

        self.session.execute(query, {
            "id":         stg_id,
            "code":       customer_code,
            "confidence": confidence,
        })

    def update_customer_no_match(self, stg_id: int) -> None:
        # 1. Query execution
        query = text("""
            UPDATE biq_stg.stg_withholdings
            SET reconcile_status = 'CUSTOMER_NOT_FOUND',
                match_confidence = 'UNMATCHED',
                updated_at       = NOW()
            WHERE stg_id = :id
        """)

        self.session.execute(query, {"id": stg_id})

    # ─────────────────────────────────────────────────────────────────────────
    # UPDATES — INVOICE MATCHING
    # ─────────────────────────────────────────────────────────────────────────

    def update_invoice_match(self, stg_id: int, invoice_sap_doc: str) -> None:
        # 1. Query execution
        query = text("""
            UPDATE biq_stg.stg_withholdings
            SET invoice_sap_doc  = :doc,
                reconcile_status = 'INVOICE_MATCHED',
                updated_at       = NOW()
            WHERE stg_id = :id
        """)

        self.session.execute(query, {"id": stg_id, "doc": invoice_sap_doc})

    def update_invoice_no_match(self, stg_id: int) -> None:
        # 1. Query execution
        query = text("""
            UPDATE biq_stg.stg_withholdings
            SET reconcile_status = 'INVOICE_NOT_FOUND',
                updated_at       = NOW()
            WHERE stg_id = :id
        """)

        self.session.execute(query, {"id": stg_id})

    # ─────────────────────────────────────────────────────────────────────────
    # UPDATES — APPLICATION
    # ─────────────────────────────────────────────────────────────────────────

    def mark_as_applied(self, stg_id: int) -> None:
        # 1. Query execution
        query = text("""
            UPDATE biq_stg.stg_withholdings
            SET reconcile_status  = 'APPLIED',
                eligibility_status = 'ELIGIBLE',
                processed_at       = NOW(),
                updated_at         = NOW()
            WHERE stg_id = :id
        """)

        self.session.execute(query, {"id": stg_id})

    def mark_as_ineligible(self, stg_id: int, reasons: List[str]) -> None:
        # 1. Query execution
        query = text("""
            UPDATE biq_stg.stg_withholdings
            SET eligibility_status    = 'INELIGIBLE',
                ineligibility_reasons = :reasons,
                reconcile_status      = 'VALIDATION_FAILED',
                updated_at            = NOW()
            WHERE stg_id = :id
        """)

        self.session.execute(query, {
            "id":      stg_id,
            # CHANGE: explicit cast to jsonb — ineligibility_reasons is JSONB in PG
            "reasons": json.dumps(reasons),
        })

    # ─────────────────────────────────────────────────────────────────────────
    # EXCEPTIONS
    # ─────────────────────────────────────────────────────────────────────────

    def create_customer_not_found_exception(self, withholding_row: pd.Series) -> None:

        message = (
            f"RUC {withholding_row['customer_ruc']} - "
            f"{withholding_row['customer_name_raw']} "
            f"not found in dim_customers"
        )

        self._insert_exception(
            withholding_id=withholding_row['stg_id'],
            exception_type='CUSTOMER_NOT_FOUND',
            message=message,
        )

    def create_invoice_not_found_exception(self, withholding_row: pd.Series) -> None:

        message = (
            f"Invoice {withholding_row['invoice_ref_sustento']} "
            f"(Customer {withholding_row['customer_code_sap']}) "
            f"not found in portfolio"
        )

        self._insert_exception(
            withholding_id=withholding_row['stg_id'],
            exception_type='INVOICE_NOT_FOUND',
            message=message,
        )

    def create_validation_exception(
        self,
        stg_id: int,
        exception_type: str,
        reasons: List[str],
        withholding_row: pd.Series,
    ) -> None:

        message = (
            f"Withholding {withholding_row['withholding_ref']} "
            f"for Invoice {withholding_row['invoice_ref_sustento']} "
            f"(Customer {withholding_row['customer_code_sap']}): "
            f"{', '.join(reasons)}"
        )

        self._insert_exception(
            withholding_id=stg_id,
            exception_type=exception_type,
            message=message,
        )

    def _insert_exception(
        self,
        withholding_id: int,
        exception_type: str,
        message: str,
    ) -> None:
        # 1. Exception insertion
        query = text("""
            INSERT INTO biq_stg.withholdings_exceptions
                (withholding_id, exception_type, exception_message,
                 resolution_status, created_at)
            VALUES (:id, :type, :msg, 'OPEN', NOW())
        """)

        self.session.execute(query, {
            "id":   withholding_id,
            "type": exception_type,
            "msg":  message,
        })

    # ─────────────────────────────────────────────────────────────────────────
    # AUDIT
    # ─────────────────────────────────────────────────────────────────────────

    def insert_audit_record(
        self,
        withholding_id: int,
        invoice_sap_doc: str,
        amount_before: float,
        amount_after: float,
        applied_val: float,
    ) -> None:
        """
        INSERT ... SELECT ... LIMIT 1 is valid in PostgreSQL.
        Only table names are updated.
        """
        # 1. Audit insertion
        query = text("""
            INSERT INTO biq_stg.audit_withholdings_applied
                (withholding_id, invoice_stg_id, amount_before,
                 amount_after, withholding_applied)
            SELECT
                :wh_id,
                stg_id,
                :before,
                :after,
                :applied
            FROM biq_stg.stg_customer_portfolio
            WHERE sap_doc_number = :doc
            LIMIT 1
        """)

        self.session.execute(query, {
            "wh_id":   withholding_id,
            "doc":     invoice_sap_doc,
            "before":  amount_before,
            "after":   amount_after,
            "applied": applied_val,
        })

    # ─────────────────────────────────────────────────────────────────────────
    # HELPERS — PORTFOLIO QUERIES
    # ─────────────────────────────────────────────────────────────────────────

    def find_invoice_by_ref(
        self,
        customer_code: str,
        invoice_ref: str,
    ) -> Optional[Dict]:
        # 1. Invoice lookup
        query = text("""
            SELECT
                sap_doc_number,
                conciliable_amount
            FROM biq_stg.stg_customer_portfolio
            WHERE customer_code    = :customer
              AND assignment        = :ref
              AND reconcile_status IN ('PENDING', 'MATCH_DATOS')
            LIMIT 1
        """)

        result = self.session.execute(query, {
            "customer": customer_code,
            "ref":      invoice_ref,
        }).fetchone()

        if result:
            # CHANGE: positional access [0], [1] instead of result.sap_doc_number
            # In PostgreSQL, attributes by name in fetchone() are not always
            # available outside of a context with clear column aliases.
            return {
                'sap_doc_number':    result[0],
                'conciliable_amount': float(result[1]),
            }

        return None

    def get_portfolio_amount(self, invoice_sap_doc: str) -> Optional[float]:
        # 1. Amount retrieval
        query = text("""
            SELECT conciliable_amount
            FROM biq_stg.stg_customer_portfolio
            WHERE sap_doc_number = :doc
            LIMIT 1
        """)

        result = self.session.execute(query, {"doc": invoice_sap_doc}).fetchone()

        # CHANGE: result[0] instead of result.conciliable_amount
        return float(result[0]) if result else None

    def is_already_applied(self, invoice_sap_doc: str) -> bool:
        # 1. Check application status
        query = text("""
            SELECT COUNT(*) AS cnt
            FROM biq_stg.stg_customer_portfolio
            WHERE sap_doc_number  = :doc
              AND financial_tax_iva IS NOT NULL
              AND financial_tax_iva > 0
        """)

        result = self.session.execute(query, {"doc": invoice_sap_doc}).fetchone()

        # CHANGE: result[0] instead of result.count
        # 'count' is a Python function and may collide; positional is safer.
        return result[0] > 0

    def apply_withholding_to_portfolio(
        self,
        invoice_sap_doc: str,
        valor_ret_iva: float,
    ) -> int:
        # 1. Portfolio update
        query = text("""
            UPDATE biq_stg.stg_customer_portfolio
            SET conciliable_amount = conciliable_amount - :ret_iva,
                financial_tax_iva  = :ret_iva,
                reconcile_status   = 'WITHHOLDING_APPLIED',
                updated_at         = NOW()
            WHERE sap_doc_number      = :doc
              AND conciliable_amount  >= :ret_iva
        """)

        result = self.session.execute(query, {
            "doc":     invoice_sap_doc,
            "ret_iva": valor_ret_iva,
        })

        if result.rowcount == 0:
            raise Exception(
                f"Could not apply withholding to invoice {invoice_sap_doc}. "
                f"Possible insufficient balance."
            )

        return result.rowcount
