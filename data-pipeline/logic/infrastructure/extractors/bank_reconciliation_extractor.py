"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.extractors.bank_reconciliation_extractor
===============================================================================

Description:
    Specialized extractor for bank reconciliation operations. It retrieves 
    pending portfolio invoices and bank transactions from the staging layer, 
    applying business filters to identify items eligible for matching.

Responsibilities:
    - Extract pending portfolio invoices with specific status filters 
      (PENDING, PARTIAL_MATCH, WITHHOLDING_APPLIED).
    - Extract pending bank transactions for reconciliation.
    - Ensure compatibility with SQLAlchemy 2.x and PostgreSQL by using 
      session connections for data extraction.

Key Components:
    - BankReconciliationExtractor: Orchestrates complex queries for the 
      reconciliation engine.

Notes:
    - Fix 1.1: Uses session.connection() instead of session.bind for SQLA 2.x.
    - Fix 1.2: Includes WITHHOLDING_APPLIED status in eligible invoices.

Dependencies:
    - pandas
    - sqlalchemy
    - sqlalchemy.orm
    - typing

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import Optional


class BankReconciliationExtractor:
    """
    Extractor for bank reconciliation queries.
    """

    def __init__(self, session: Session):
        """
        Initializes the extractor with a database session.
        """
        self.session = session

    def extract_pending_portfolio_invoices(
        self,
        customer_code: Optional[str] = None,
        max_age_days: int = 90
    ) -> pd.DataFrame:
        """
        Extracts reconcilable invoices from the portfolio.

        Eligible Statuses (v2.2):
        -------------------------
        - PENDING: No active process started.
        - PARTIAL_MATCH: Previous partial reconciliation.
        - WITHHOLDING_APPLIED: Tax withholding applied, but net amount 
          is still pending bank reconciliation.
        """

        # 1. Initialization: Build filter conditions
        conditions = [
            "reconcile_status IN ('PENDING', 'PARTIAL_MATCH', 'WITHHOLDING_APPLIED')",
            "conciliable_amount > 0",
        ]

        if customer_code is not None:
            conditions.append("customer_code = :customer_code")

        # 2. Processing: Build and execute the query
        query = text(f"""
            SELECT
                stg_id,
                sap_doc_number,
                accounting_doc,
                customer_code,
                customer_name,
                assignment,
                invoice_ref,
                doc_date,
                due_date,
                amount_outstanding,
                conciliable_amount,
                currency,
                reconcile_status,
                enrich_batch,
                enrich_ref,
                enrich_brand,
                enrich_user,
                enrich_source,
                reconcile_group,
                match_hash_key,
                settlement_id,
                financial_amount_gross,
                financial_amount_net,
                financial_commission,
                financial_tax_iva,
                financial_tax_irf,
                match_method,
                match_confidence,
                sap_text,
                gl_account,
                partial_payment_flag
            FROM biq_stg.stg_customer_portfolio
            WHERE {' AND '.join(conditions)}
            ORDER BY doc_date ASC
        """)

        params = {}
        if customer_code is not None:
            params['customer_code'] = customer_code

        # Fix 1.1: Use session.connection() for SQLAlchemy 2.x compatibility
        df = pd.read_sql(query, self.session.connection(), params=params)

        return df

    def extract_pending_bank_transactions(
        self,
        exclude_trans_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Extracts pending bank transactions.

        Parameters:
        -----------
        exclude_trans_type : str | None
            Transaction type to exclude (e.g., 'LIQUIDACION TC').

        Returns:
        --------
        pd.DataFrame with pending transactions.
        """

        # 1. Initialization: Build filter conditions
        conditions = [
            "reconcile_status = 'PENDING'",
            "is_compensated_sap = FALSE",
            "is_compensated_intraday = FALSE",
        ]

        if exclude_trans_type:
            conditions.append("trans_type != :exclude_type")

        # 2. Processing: Build and execute the query
        query = text(f"""
            SELECT
                stg_id,
                doc_date,
                bank_date,
                amount_total,
                bank_ref_1,
                bank_ref_2,
                bank_description,
                enrich_customer_id,
                enrich_customer_name,
                enrich_confidence_score,
                trans_type,
                global_category,
                brand,
                settlement_id,
                establishment_name
            FROM biq_stg.stg_bank_transactions
            WHERE {' AND '.join(conditions)}
            ORDER BY bank_date ASC
        """)

        params = {}
        if exclude_trans_type:
            params['exclude_type'] = exclude_trans_type

        # Fix 1.1: Use session.connection() for SQLAlchemy 2.x compatibility
        df = pd.read_sql(query, self.session.connection(), params=params)

        # 3. Post-processing: Normalize customer code column
        if 'enrich_customer_id' in df.columns:
            df['customer_code'] = df['enrich_customer_id']

        return df
