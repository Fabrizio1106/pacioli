"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.sap_period_closure_validator
===============================================================================

Description:
    Validates whether an accounting period is fully compensated in SAP and
    therefore safe to close. Queries raw_sap_cta_239 directly to evaluate
    net balance and pending compensation entries.

Responsibilities:
    - Check if the net importe_ml sum for a period is zero (within tolerance).
    - Verify that no open entries (missing doc_compensacion) remain.
    - Return structured validation results for use by SmartPeriodClosure.
    - Support bulk validation across multiple periods.

Key Components:
    - SAPPeriodClosureValidator: Validates individual periods against SAP source
      data using two criteria: zero net balance and full compensation coverage.

Notes:
    - Version 1.3:
      FIX #1: Removed double schema prefix `biq_raw.biq_raw.raw_sap_cta_239`.
              The Session is already bound to the biq_raw engine, so the table
              reference must be just `raw_sap_cta_239` (no schema prefix).
      FIX #3: _get_period_stats no longer silently returns zeros on DB error.
              It now raises the exception so the caller can distinguish between
              "no data in SAP" and "could not reach SAP". This prevents a DB
              failure from being mistaken for a clean period ready to close.

Dependencies:
    - sqlalchemy
    - utils.logger

===============================================================================
"""

from sqlalchemy import text
from typing import Dict

from utils.logger import get_logger


class SAPPeriodClosureValidator:
    """
    Validates whether an accounting period is ready to close in SAP.

    Version 1.3 — fixes applied:
        - Schema reference corrected: raw_sap_cta_239 (no biq_raw. prefix).
        - DB errors in _get_period_stats now propagate instead of returning
          zeros, preventing a query failure from silently triggering closure.

    SAP closure rules:
        1. SUM(importe_ml) = 0  → All entries are compensated.
        2. All rows have doc_compensacion → No pending entries remain.
    """

    def __init__(self, session):
        """
        Args:
            session: SQLAlchemy Session connected to biq_raw engine.
                     Do NOT pass a session from stg or config — the table
                     raw_sap_cta_239 lives in biq_raw.
        """
        self.session = session
        self.logger = get_logger("SAP_PERIOD_VALIDATOR")

    # =========================================================================
    # PUBLIC: Period readiness check
    # =========================================================================

    def is_period_ready_to_close(self, period_month: str) -> Dict:
        """
        Validate whether a given accounting period is safe to close.

        Args:
            period_month: Period string in 'YYYY-MM' format.

        Returns:
            Dict with keys: ready (bool), reason (str), sum_importe (float),
            total_rows (int), pending_comp (int), details (str).

        Raises:
            Exception: If the underlying DB query fails. The caller
                       (SmartPeriodClosure) must handle this explicitly so that
                       a DB error never triggers an accidental period closure.
        """
        self.logger(f"Validating period {period_month}...", "INFO")

        # NOTE: _get_period_stats will raise if the query fails.
        # Do NOT wrap this in try/except here — let it propagate.
        stats = self._get_period_stats(period_month)

        total_rows    = stats['total_rows']
        sum_importe   = stats['sum_importe']
        pending_comp  = stats['pending_compensation']

        # ── No data found ────────────────────────────────────────────────────
        # A period with zero rows means the RAW table has no SAP entries for
        # that month. This is only valid as a closure reason when the caller
        # has confirmed that data was actually expected and processed. The
        # validator reports it truthfully; the decision is left to the caller.
        if total_rows == 0:
            return {
                'ready': True,
                'reason': 'PERIOD_WITHOUT_DATA',
                'sum_importe': 0,
                'total_rows': 0,
                'pending_comp': 0,
                'details': f'Period {period_month} has no SAP rows in raw table.'
            }

        # ── Closure conditions ───────────────────────────────────────────────
        importe_ok       = abs(sum_importe) < 0.01   # one-cent tolerance
        compensacion_ok  = pending_comp == 0

        if importe_ok and compensacion_ok:
            return {
                'ready': True,
                'reason': 'SAP_FULLY_COMPENSATED',
                'sum_importe': sum_importe,
                'total_rows': total_rows,
                'pending_comp': pending_comp,
                'details': (
                    f'Period {period_month} fully compensated. '
                    f'Rows: {total_rows}, net: {sum_importe:.2f}, pending: {pending_comp}'
                )
            }

        # ── Not ready ────────────────────────────────────────────────────────
        reasons = []
        if not importe_ok:
            reasons.append(f'Net amount: {sum_importe:.2f} (expected 0)')
        if not compensacion_ok:
            reasons.append(f'{pending_comp} entries without compensation doc')

        return {
            'ready': False,
            'reason': 'SAP_NOT_FULLY_COMPENSATED',
            'sum_importe': sum_importe,
            'total_rows': total_rows,
            'pending_comp': pending_comp,
            'details': f"Period {period_month} still open: {', '.join(reasons)}"
        }

    # =========================================================================
    # PUBLIC: Detail rows for pending entries
    # =========================================================================

    def get_pending_compensation_details(
        self,
        period_month: str,
        limit: int = 10
    ) -> list:
        """
        Retrieve detail rows for entries still pending compensation.

        Returns an empty list on error (non-critical, used for reporting only).
        """
        # FIX #1: removed biq_raw. prefix — session is already on biq_raw engine
        query = text("""
            SELECT
                fecha_documento,
                num_documento,
                importe_ml,
                asignacion AS reference,
                texto
            FROM raw_sap_cta_239
            WHERE TO_CHAR(fecha_documento, 'YYYY-MM') = :period_month
              AND (doc_compensacion IS NULL OR doc_compensacion = '')
            ORDER BY ABS(importe_ml) DESC
            LIMIT :limit
        """)

        try:
            result = self.session.execute(
                query, {'period_month': period_month, 'limit': limit}
            )
            return [
                {
                    'fecha_documento': row[0],
                    'num_documento':   row[1],
                    'importe_ml':      row[2],
                    'reference':       row[3],
                    'texto':           row[4],
                }
                for row in result
            ]
        except Exception as e:
            self.logger(f"Error retrieving pending details: {e}", "ERROR")
            return []

    # =========================================================================
    # PUBLIC: Bulk validation
    # =========================================================================

    def validate_multiple_periods(self, period_months: list) -> Dict:
        """Validate multiple periods and return a dict keyed by period string."""
        return {p: self.is_period_ready_to_close(p) for p in period_months}

    # =========================================================================
    # PRIVATE: DB query
    # =========================================================================

    def _get_period_stats(self, period_month: str) -> Dict:
        """
        Query aggregate statistics for the given period from raw SAP data.

        FIX #1: Table reference is now just `raw_sap_cta_239`.
                The Session is bound to the biq_raw engine, so PostgreSQL
                resolves the table within that database's search_path.
                The previous `biq_raw.biq_raw.raw_sap_cta_239` caused:
                  "cross-database references are not implemented"
                because PostgreSQL treated the first segment as a database name.

        FIX #3: This method NO LONGER catches exceptions. Any DB error is
                allowed to propagate to `is_period_ready_to_close`, which
                propagates it further to `SmartPeriodClosure.auto_close_periods_if_ready`.
                This ensures a broken query never returns zeros that look like
                an empty (closeable) period.
        """
        query = text("""
            SELECT
                COUNT(*) AS total_rows,
                COALESCE(SUM(importe_ml), 0) AS sum_importe,
                COUNT(
                    CASE
                        WHEN doc_compensacion IS NULL
                          OR doc_compensacion = ''
                        THEN 1
                    END
                ) AS pending_compensation
            FROM raw_sap_cta_239
            WHERE TO_CHAR(fecha_documento, 'YYYY-MM') = :period_month
        """)

        # No try/except — intentional. See FIX #3 in module docstring.
        result = self.session.execute(query, {'period_month': period_month}).fetchone()

        return {
            'total_rows':           result[0],
            'sum_importe':          float(result[1]),
            'pending_compensation': result[2],
        }