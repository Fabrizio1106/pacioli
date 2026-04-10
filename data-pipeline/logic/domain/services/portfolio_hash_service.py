"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.portfolio_hash_service
===============================================================================

Description:
    Domain service acting as the single source of truth for portfolio etl_hash 
    calculations. It ensures consistency across different components that 
    need to verify or generate row hashes for stg_customer_portfolio.

Responsibilities:
    - Provide a unified formula for calculating portfolio row hashes.
    - Handle hash generation for single rows and complete DataFrames.
    - Ensure hash invariance based on SAP-controlled fields only.

Key Components:
    - PortfolioHashService: Main class for etl_hash computation.

Notes:
    - Hash formula: MD5(invoice_ref | amount_outstanding | due_date).
    - Prevents false CDC positives by excluding pipeline-managed fields.

Dependencies:
    - hashlib
    - math
    - pandas
    - typing

===============================================================================
"""

import hashlib
import math
import pandas as pd
from typing import Union


class PortfolioHashService:
    """
    Domain Service for etl_hash calculation in stg_customer_portfolio.

    Pattern: Domain Service (stateless, no infrastructure dependencies).
    """

    # 1. Constants and Sentinel Values
    _SEP = '|'  # Separator to prevent concatenation collisions
    
    # Sentinel values for null fields to ensure hash uniqueness
    _NULL_INVOICE_REF    = 'NO_INV_REF'
    _NULL_AMOUNT         = '0.00'
    _NULL_DUE_DATE       = 'NO_DUE_DATE'
    _NULL_ACCOUNTING_DOC = 'NO_ACCT_DOC'

    @classmethod
    def compute(cls, row: Union[dict, pd.Series]) -> str:
        """
        Calculates the etl_hash for a single row.

        Formula: invoice_ref | amount_outstanding | due_date
        ------------------------------------------------------
        Only 3 fields are used because they are the only ones with 
        identical values in raw_customer_portfolio and stg_customer_portfolio.
        """
        # 1. Field Extraction
        invoice_ref = cls._extract_invoice_ref(row)
        amount      = cls._extract_amount(row)
        due_date    = cls._extract_due_date(row)

        # 2. Hash Generation
        raw = cls._SEP.join([invoice_ref, amount, due_date])
        return hashlib.md5(raw.encode('utf-8')).hexdigest()

    @classmethod
    def compute_dataframe(cls, df: pd.DataFrame) -> pd.Series:
        """
        Calculates the etl_hash for all rows in a DataFrame.

        Vectorized version using pandas operations for performance.
        """
        # 1. Column Detection (handles both stg and raw naming)
        inv_col    = 'invoice_ref'    if 'invoice_ref'    in df.columns else 'referencia_a_factura'
        amount_col = 'amount_outstanding' if 'amount_outstanding' in df.columns else 'importe'
        date_col   = 'due_date'       if 'due_date'       in df.columns else 'fecha_de_pago'
        doc_col    = 'accounting_doc' if 'accounting_doc' in df.columns else 'n_documento'

        # 2. Field Normalization
        col_inv = df[inv_col].fillna(cls._NULL_INVOICE_REF).astype(str).str.strip()
        col_amt = df[amount_col].apply(cls._normalize_amount_series)

        col_date = df[date_col].fillna(cls._NULL_DUE_DATE).astype(str).str.strip()
        # Normalize Timestamp dates to date part only
        col_date = col_date.str.split(' ').str[0].str.split('T').str[0]
        col_date = col_date.replace({'nan': cls._NULL_DUE_DATE, 'NaT': cls._NULL_DUE_DATE, '': cls._NULL_DUE_DATE})

        col_doc = df[doc_col].fillna(cls._NULL_ACCOUNTING_DOC).astype(str).str.strip()

        # 3. Concatenation and Hashing
        raw_series = (
            col_inv  + cls._SEP +
            col_amt  + cls._SEP +
            col_date
        )

        return raw_series.apply(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())

    # 4. Private Helper Methods

    @classmethod
    def _extract_invoice_ref(cls, row: Union[dict, pd.Series]) -> str:
        """Extracts invoice_ref with fallback to raw column names."""
        val = (
            row.get('invoice_ref') or
            row.get('referencia_a_factura') or
            cls._NULL_INVOICE_REF
        )
        return cls._safe_str(val, cls._NULL_INVOICE_REF)

    @classmethod
    def _extract_amount(cls, row: Union[dict, pd.Series]) -> str:
        """Extracts amount_outstanding with fallback to raw column names."""
        val = (
            row.get('amount_outstanding') or
            row.get('importe') or
            0.0
        )
        return cls._normalize_amount(val)

    @classmethod
    def _extract_due_date(cls, row: Union[dict, pd.Series]) -> str:
        """Extracts due_date with fallback to raw column names."""
        val = (
            row.get('due_date') or
            row.get('fecha_de_pago') or
            cls._NULL_DUE_DATE
        )
        s = cls._safe_str(val, cls._NULL_DUE_DATE)
        # Normalize timestamps to date only
        return s.split(' ')[0].split('T')[0] if s else cls._NULL_DUE_DATE

    @classmethod
    def _extract_accounting_doc(cls, row: Union[dict, pd.Series]) -> str:
        """Extracts accounting_doc with fallback to raw column names."""
        val = (
            row.get('accounting_doc') or
            row.get('n_documento') or
            cls._NULL_ACCOUNTING_DOC
        )
        return cls._safe_str(val, cls._NULL_ACCOUNTING_DOC)

    @staticmethod
    def _normalize_amount(value) -> str:
        """Converts numeric value to string with 2 fixed decimals."""
        try:
            f = float(value)
            if math.isnan(f) or math.isinf(f):
                return '0.00'
            return f'{f:.2f}'
        except (TypeError, ValueError):
            return '0.00'

    @staticmethod
    def _normalize_amount_series(value) -> str:
        """Version for use in apply() over pd.Series."""
        try:
            f = float(value)
            if math.isnan(f) or math.isinf(f):
                return '0.00'
            return f'{f:.2f}'
        except (TypeError, ValueError):
            return '0.00'

    @staticmethod
    def _safe_str(value, default: str) -> str:
        """Converts to a clean string, returns default if null."""
        if value is None:
            return default
        try:
            if isinstance(value, float) and math.isnan(value):
                return default
        except Exception:
            pass
        s = str(value).strip()
        return s if s and s.lower() not in ('nan', 'none', 'nat', '') else default
