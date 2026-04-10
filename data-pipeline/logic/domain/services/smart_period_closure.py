"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.smart_period_closure
===============================================================================

Description:
    Orchestrates dual-lock period closure decisions for transactional processes.
    A period is eligible to close only when both a calendar gate (time barrier)
    and a SAP compensation gate (business validation) are satisfied.

Responsibilities:
    - Evaluate whether a bank (SAP_TRANSACTIONS) period passes the calendar gate.
    - Delegate SAP compensation validation to SAPPeriodClosureValidator.
    - Evaluate whether card process periods pass their own cutoff date rule (day 15).
    - Auto-close all PENDING periods in biq_config.etl_process_windows when ready.
    - Auto-open the next period windows after a closure.
    - Guarantee that at least one PENDING window exists before RAW loading
      (ensure_periods_exist — pre-check, never closes).

Key Components:
    - SmartPeriodClosure.ensure_periods_exist(): PRE-CHECK — only creates
      windows if none exist. Never evaluates closure. Called BEFORE Phase 0.
    - SmartPeriodClosure.auto_close_periods_if_ready(): POST-CHECK — evaluates
      both locks with real RAW data already loaded. Called AFTER staging.
    - SmartPeriodClosure.open_next_periods(): Opens next month after a closure.

Notes:
    - VERSION 2.7:
      NEW: ensure_periods_exist() separates pre-check from closure logic.
           Pre-check only creates — never closes. This fixes the race condition
           where the closure validator ran against an empty RAW table (before
           Phase 0 loaded the files) and incorrectly closed the period.
      The dual-lock closure (calendar gate + SAP gate + bank cross-check)
      is unchanged and runs exclusively in the post-check phase.

Dependencies:
    - sqlalchemy
    - utils.logger, utils.db_config
    - logic.domain.services.sap_period_closure_validator

===============================================================================
"""

from datetime import datetime, date, timedelta
from typing import List, Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from utils.logger import get_logger
from utils.db_config import get_db_engine
from logic.domain.services.sap_period_closure_validator import SAPPeriodClosureValidator


class SmartPeriodClosure:
    """
    Orchestrates dual-lock period closure for PACIOLI transactional processes.

    Two-phase design (v2.7):
    ─────────────────────────
    PRE-CHECK  (before RAW load) → ensure_periods_exist()
        Only guarantees that at least one PENDING window exists so staging
        has something to work with. Never closes anything.

    POST-CHECK (after staging)   → auto_close_periods_if_ready()
        Evaluates both locks using real RAW data that was just loaded.
        Closes periods that pass and opens the next month's windows.
    """

    _TRANSACTIONAL_PROCESSES = [
        'SAP_TRANSACTIONS',
        'DINERS_CARDS',
        'GUAYAQUIL_CARDS',
        'PACIFICARD_CARDS',
        'PARKING_BREAKDOWN',
    ]

    def __init__(self, engine_stg: Engine, engine_config: Engine):
        self.engine_stg    = engine_stg
        self.engine_config = engine_config
        self.logger        = get_logger("SMART_CLOSURE")

        self.engine_raw    = get_db_engine('raw')
        self.session_raw   = Session(bind=self.engine_raw)
        self.sap_validator = SAPPeriodClosureValidator(self.session_raw)

    # =========================================================================
    # PRE-CHECK — called BEFORE Phase 0 (RAW loading)
    # =========================================================================

    def ensure_periods_exist(self) -> None:
        """
        Guarantee that at least one PENDING or RUNNING transactional window
        exists so the staging layer has a period to work with.

        Rules:
        - If any PENDING or RUNNING window already exists → do nothing.
          The pipeline is already configured for a period.
        - If ALL windows are COMPLETED or the table is empty → create windows
          for the current calendar month (date.today()).
        - NEVER evaluates closure here. Closure is exclusively post-check.

        This method is idempotent and safe to call on every pipeline run.
        """
        self.logger("🔍 PRE-CHECK: Verifying period windows...", "INFO")

        active_count = self._count_active_windows()

        if active_count > 0:
            self.logger(
                f"   ✅ {active_count} active window(s) found — no action needed.",
                "INFO",
            )
            return

        # No active windows → create for current month
        today      = date.today()
        periodo    = today.strftime('%Y-%m')
        start_str, end_str = self._calculate_period_dates(periodo)

        self.logger(
            f"   ⚠️ No active windows found. "
            f"Creating windows for current period: {periodo}",
            "WARN",
        )
        self._create_period_windows(periodo, start_str, end_str)
        self.logger(f"   ✅ Windows created for {periodo} ({start_str} → {end_str})", "SUCCESS")

    # =========================================================================
    # POST-CHECK — called AFTER staging (auto_close_periods_if_ready)
    # =========================================================================

    def auto_close_periods_if_ready(self) -> dict:
        """
        Evaluate all PENDING transactional windows against both closure locks.

        Lock 1 — Calendar Gate: period month must have ended.
        Lock 2 — SAP Gate:      raw_sap_cta_239 fully compensated,
                                 cross-checked against raw_banco_239.

        Called AFTER Phase 0 (RAW loading) so both RAW tables have real data.

        Returns:
            dict with keys: closed (list), not_ready (list), errors (list).
        """
        self.logger("Checking periods for automatic closure...", "INFO")

        closed, not_ready, errors = [], [], []

        query = text("""
            SELECT DISTINCT process_name, periodo_mes
            FROM biq_config.etl_process_windows
            WHERE status       = 'PENDING'
              AND process_type = 'TRANSACTIONAL'
              AND periodo_mes  IS NOT NULL
        """)

        try:
            with self.engine_config.connect() as conn:
                results = conn.execute(query).fetchall()

                for row in results:
                    process_name = row[0]
                    periodo      = row[1]

                    check = (
                        self.check_banco_ready_to_close(periodo)
                        if process_name == 'SAP_TRANSACTIONS'
                        else self.check_cards_ready_to_close(periodo)
                    )

                    if check['ready']:
                        if self._close_process_period(process_name, periodo):
                            closed.append({
                                'process': process_name,
                                'periodo': periodo,
                                'reason':  check['reason'],
                            })
                            self.logger(f"{process_name} {periodo} CLOSED", "SUCCESS")
                        else:
                            errors.append({
                                'process': process_name,
                                'periodo': periodo,
                                'error':   'Close UPDATE failed',
                            })
                    else:
                        not_ready.append({
                            'process':    process_name,
                            'periodo':    periodo,
                            'reason':     check.get('reason', 'Not ready'),
                            'status':     'waiting',
                            'percentage': check.get('pct_compensated', 0.0),
                        })

        except Exception as e:
            self.logger(f"Error in auto-close: {e}", "ERROR")
            errors.append({'error': str(e)})

        return {'closed': closed, 'not_ready': not_ready, 'errors': errors}

    # =========================================================================
    # CLOSURE RULES
    # =========================================================================

    def check_banco_ready_to_close(self, periodo: str) -> dict:
        """
        Dual-lock check for SAP_TRANSACTIONS periods.

        Lock 1 — Calendar Gate: today >= first day of the following month.
        Lock 2 — SAP Gate:      validator result cross-checked with bank raw.
        """
        year, month = map(int, periodo.split('-'))
        cutoff_date = (
            date(year + 1, 1, 1) if month == 12
            else date(year, month + 1, 1)
        )
        today = date.today()

        self.logger(f"Evaluating period {periodo}:", "INFO")
        self.logger(f"   Cutoff: {cutoff_date} | Today: {today}", "INFO")

        # Lock 1
        if today < cutoff_date:
            days_remaining = (cutoff_date - today).days
            self.logger(
                f"   Period still in progress ({days_remaining} days until cutoff)", "WARN"
            )
            return {
                'ready': False,
                'reason': f'Period in progress (closure blocked until {cutoff_date})',
                'cutoff_date': str(cutoff_date),
                'days_remaining': days_remaining,
            }

        # Lock 2 — SAP Gate
        self.logger("   Calendar gate passed. Validating SAP compensation status...", "INFO")

        try:
            sap_result = self.sap_validator.is_period_ready_to_close(periodo)
        except Exception as e:
            self.logger(f"   SAP validator error — blocking closure: {e}", "ERROR")
            return {
                'ready': False,
                'reason': 'SAP_VALIDATION_ERROR',
                'cutoff_date': str(cutoff_date),
                'error': str(e),
            }

        # Special case: SAP has zero rows
        if sap_result.get('reason') == 'PERIOD_WITHOUT_DATA':
            bank_has_data = self._bank_raw_has_data_for_period(periodo)
            if bank_has_data:
                self.logger(
                    "   SAP: No rows — but bank raw HAS data. "
                    "SAP file not loaded yet. Blocking closure.", "WARN"
                )
                return {
                    'ready': False,
                    'reason': 'SAP_DATA_NOT_LOADED_YET',
                    'cutoff_date': str(cutoff_date),
                    'details': (
                        f'Period {periodo}: bank raw has transactions but '
                        f'raw_sap_cta_239 has zero rows.'
                    ),
                }
            # Both SAP and bank empty → genuinely empty period
            self.logger(
                "   SAP and bank both empty — genuinely empty period. Allowing closure.",
                "INFO",
            )
            return {
                'ready': True,
                'reason': 'PERIOD_WITHOUT_DATA',
                'cutoff_date': str(cutoff_date),
            }

        # Normal SAP result
        if sap_result['ready']:
            self.logger("   SAP: Period ready to close", "SUCCESS")
            self.logger(f"      Reason: {sap_result.get('reason', 'N/A')}", "INFO")
            if 'pct_compensated' in sap_result:
                self.logger(f"      Compensated: {sap_result['pct_compensated']:.1f}%", "INFO")
            return {
                'ready': True,
                'reason': sap_result.get('reason', 'SAP validated'),
                'pct_compensated': sap_result.get('pct_compensated', 100.0),
                'total_open': sap_result.get('total_open', 0),
                'cutoff_date': str(cutoff_date),
            }

        self.logger("   SAP: Period not ready to close", "WARN")
        self.logger(f"      Reason: {sap_result.get('reason', 'N/A')}", "WARN")
        if 'pct_compensated' in sap_result:
            self.logger(f"      Compensated: {sap_result['pct_compensated']:.1f}%", "WARN")
        if 'total_open' in sap_result:
            self.logger(f"      Open entries: {sap_result['total_open']}", "WARN")
        return {
            'ready': False,
            'reason': sap_result.get('reason', 'SAP not fully compensated'),
            'pct_compensated': sap_result.get('pct_compensated', 0.0),
            'total_open': sap_result.get('total_open', 0),
            'cutoff_date': str(cutoff_date),
        }

    def check_cards_ready_to_close(self, periodo: str) -> dict:
        """Calendar-gate check (day 15) for card process periods."""
        year, month = map(int, periodo.split('-'))
        cutoff_date = (
            date(year + 1, 1, 15) if month == 12
            else date(year, month + 1, 15)
        )
        today      = date.today()
        ready      = today >= cutoff_date
        days_until = 0 if ready else (cutoff_date - today).days
        return {
            'ready': ready,
            'current_date': today,
            'cutoff_date': cutoff_date,
            'days_until_closure': days_until,
        }

    def get_banco_pending_details(self, periodo: str, limit: int = 10) -> list:
        """Return SAP entries still pending compensation for the given period."""
        return self.sap_validator.get_pending_compensation_details(
            period_month=periodo, limit=limit
        )

    # =========================================================================
    # PERIOD OPENING (called after auto_close_periods_if_ready)
    # =========================================================================

    def open_next_periods(self, closed_periods: List[dict]) -> None:
        """
        Open the next calendar month's windows for each period that just closed.

        Args:
            closed_periods: List of dicts from auto_close_periods_if_ready,
                            each containing at least a 'periodo' key ('YYYY-MM').
        """
        self.logger("\n🔓 Checking auto-open for next periods...", "INFO")

        unique_periods = {item['periodo'] for item in closed_periods}

        for periodo in unique_periods:
            next_periodo = self._get_next_period(periodo)

            if self._period_exists(next_periodo):
                self.logger(f"ℹ️ Period {next_periodo} already exists", "INFO")
            else:
                start_date, end_date = self._calculate_period_dates(next_periodo)
                self._create_period_windows(next_periodo, start_date, end_date)
                self.logger(f"✅ Period {next_periodo} opened automatically", "SUCCESS")

    # =========================================================================
    # PRIVATE — period existence and creation
    # =========================================================================

    def _count_active_windows(self) -> int:
        """
        Count PENDING or RUNNING transactional windows.

        Returns 0 when the table is empty or all windows are COMPLETED/FAILED,
        which signals that new windows must be created before the pipeline runs.
        """
        query = text("""
            SELECT COUNT(*) AS cnt
            FROM biq_config.etl_process_windows
            WHERE status       IN ('PENDING', 'RUNNING')
              AND process_type  = 'TRANSACTIONAL'
        """)
        try:
            with self.engine_config.connect() as conn:
                result = conn.execute(query).fetchone()
                return result.cnt if result else 0
        except Exception as e:
            self.logger(f"Could not count active windows: {e}", "WARN")
            return 0

    def _period_exists(self, periodo: str) -> bool:
        """Return True if any TRANSACTIONAL window already exists for `periodo`."""
        query = text("""
            SELECT COUNT(*) AS cnt
            FROM biq_config.etl_process_windows
            WHERE periodo_mes  = :periodo
              AND process_type = 'TRANSACTIONAL'
        """)
        try:
            with self.engine_config.connect() as conn:
                result = conn.execute(query, {'periodo': periodo}).fetchone()
                return result.cnt > 0
        except Exception:
            return False

    def _get_next_period(self, periodo: str) -> str:
        """Return 'YYYY-MM' for the month following `periodo`."""
        year, month = map(int, periodo.split('-'))
        return f"{year + 1}-01" if month == 12 else f"{year}-{month + 1:02d}"

    def _calculate_period_dates(self, periodo: str) -> tuple:
        """Return (start_date, end_date) as 'YYYY-MM-DD' strings."""
        year, month = map(int, periodo.split('-'))
        start = datetime(year, month, 1)
        end   = (
            datetime(year + 1, 1, 1) - timedelta(days=1)
            if month == 12
            else datetime(year, month + 1, 1) - timedelta(days=1)
        )
        return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')

    def _create_period_windows(
        self, periodo: str, start_date: str, end_date: str
    ) -> None:
        """Insert PENDING windows for all transactional processes for a new period."""
        rows = ", ".join(
            f"('{p}', 'TRANSACTIONAL', :start, :end, :periodo, 'PENDING', "
            f"'Auto-created by SmartPeriodClosure')"
            for p in self._TRANSACTIONAL_PROCESSES
        )
        query = text(f"""
            INSERT INTO biq_config.etl_process_windows
                (process_name, process_type, window_start, window_end,
                 periodo_mes, status, notes)
            VALUES {rows}
        """)
        try:
            with self.engine_config.connect() as conn:
                conn.execute(query, {
                    'periodo': periodo,
                    'start':   start_date,
                    'end':     end_date,
                })
                conn.commit()
        except Exception as e:
            self.logger(f"Failed to create windows for {periodo}: {e}", "ERROR")

    def _close_process_period(self, process_name: str, periodo: str) -> bool:
        """Mark a process window as COMPLETED."""
        query = text("""
            UPDATE biq_config.etl_process_windows
            SET status       = 'COMPLETED',
                completed_at = NOW()
            WHERE process_name = :process_name
              AND periodo_mes  = :periodo
              AND status       = 'PENDING'
        """)
        try:
            with self.engine_config.connect() as conn:
                conn.execute(query, {'process_name': process_name, 'periodo': periodo})
                conn.commit()
                return True
        except Exception as e:
            self.logger(f"Error closing {process_name} {periodo}: {e}", "ERROR")
            return False

    # =========================================================================
    # PRIVATE — SAP / Bank cross-check
    # =========================================================================

    def _bank_raw_has_data_for_period(self, periodo: str) -> bool:
        """
        Return True if raw_banco_239 has any transactions for the given month.
        Returns True on query error (fail-safe: block closure when uncertain).
        """
        query = text("""
            SELECT 1
            FROM raw_banco_239
            WHERE TO_CHAR(fecha_transaccion, 'YYYY-MM') = :periodo
            LIMIT 1
        """)
        try:
            with self.engine_raw.connect() as conn:
                return conn.execute(query, {'periodo': periodo}).fetchone() is not None
        except Exception as e:
            self.logger(f"Could not verify bank raw for {periodo}: {e}", "WARN")
            return True   # fail-safe