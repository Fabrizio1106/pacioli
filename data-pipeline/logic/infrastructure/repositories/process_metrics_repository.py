"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.repositories.process_metrics_repository
===============================================================================

Description:
    Repository for managing ETL process metrics and execution windows.
    It tracks the status, execution time, and record counts for various
    processes in biq_config.etl_process_windows.

Responsibilities:
    - Retrieve the current status of a specific ETL process.
    - Update execution metrics including record counts, time, and errors.
    - Fetch execution history for a given process.
    - Save the batch_id produced by BatchTracker into the process window
      (previously embedded in the orchestrator as _save_batch_id).
    - Return a today-summary of completed/failed processes for the final
      pipeline report (previously embedded as _show_process_metrics_summary).

Key Components:
    - ProcessMetricsRepository: Data access class for etl_process_windows.

Notes:
    - VERSION 1.1 — additions from v1:
        * save_batch_id(): persists the batch_id into etl_process_windows.notes
          for the most recently completed window of a process/period pair.
          Extracted from main_silver_orchestrator._save_batch_id.
        * get_todays_summary(): returns rows for the final pipeline report.
          Extracted from main_silver_orchestrator._show_process_metrics_summary.

    - Target Table: biq_config.etl_process_windows
    - Primary Key:  id

Dependencies:
    - sqlalchemy
    - logic.domain.services.process_metrics_tracker
    - utils.logger

===============================================================================
"""

from typing import Optional, List
from sqlalchemy import text
from sqlalchemy.engine import Engine

from logic.domain.services.process_metrics_tracker import ProcessExecution
from utils.logger import get_logger


class ProcessMetricsRepository:
    """
    Repository for ETL process metrics.
    TABLE: biq_config.etl_process_windows
    """

    _PK_COLUMN = "id"

    def __init__(self, engine: Engine):
        self.engine = engine
        self.logger = get_logger("METRICS_REPO")

    # =========================================================================
    # GET STATUS
    # =========================================================================

    def get_process_status(
        self,
        process_name: str,
        periodo: Optional[str] = None,
    ) -> Optional[str]:
        """Return the most recent status for a process, optionally filtered by period."""

        if periodo:
            query = text("""
                SELECT status
                FROM biq_config.etl_process_windows
                WHERE process_name = :process_name
                  AND periodo_mes  = :periodo
                ORDER BY created_at DESC
                LIMIT 1
            """)
            params = {'process_name': process_name, 'periodo': periodo}
        else:
            query = text("""
                SELECT status
                FROM biq_config.etl_process_windows
                WHERE process_name = :process_name
                ORDER BY created_at DESC
                LIMIT 1
            """)
            params = {'process_name': process_name}

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, params).fetchone()
                return result[0] if result else None
        except Exception as e:
            self.logger(f"Error obtaining status for {process_name}: {e}", "ERROR")
            return None

    # =========================================================================
    # UPDATE METRICS
    # =========================================================================

    def update_metrics(self, execution: ProcessExecution) -> bool:
        """
        Persist a ProcessExecution value object into etl_process_windows.

        Handles two cases:
          - RUNNING:             updates status, run_id, started_at.
          - COMPLETED / FAILED:  updates all metric columns.
        """

        # ── CASE 1: RUNNING ───────────────────────────────────────────────────
        if execution.status == 'RUNNING':
            query = text(f"""
                UPDATE biq_config.etl_process_windows
                SET status     = 'RUNNING',
                    run_id     = :run_id,
                    started_at = COALESCE(started_at, NOW())
                WHERE {self._PK_COLUMN} = (
                    SELECT {self._PK_COLUMN}
                    FROM   biq_config.etl_process_windows
                    WHERE  process_name = :process_name
                      AND  status       = 'PENDING'
                    ORDER BY created_at DESC
                    LIMIT 1
                )
            """)
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(query, {
                        'process_name': execution.process_name,
                        'run_id':       execution.run_id,
                    })
                    conn.commit()
                    if result.rowcount > 0:
                        self.logger(
                            f"{execution.process_name} marked as RUNNING", "INFO"
                        )
                        return True
                    else:
                        self.logger(
                            f"No PENDING window found for {execution.process_name}",
                            "WARN",
                        )
                        return False
            except Exception as e:
                self.logger(f"Error marking as RUNNING: {e}", "ERROR")
                return False

        # ── CASE 2: COMPLETED / FAILED ────────────────────────────────────────
        query = text(f"""
            UPDATE biq_config.etl_process_windows
            SET status                 = :status,
                run_id                 = :run_id,
                records_processed      = :records_processed,
                records_failed         = :records_failed,
                execution_time_seconds = :execution_time,
                error_message          = :error_message,
                started_at             = COALESCE(started_at, :started_at),
                completed_at           = :completed_at
            WHERE {self._PK_COLUMN} = (
                SELECT {self._PK_COLUMN}
                FROM   biq_config.etl_process_windows
                WHERE  process_name = :process_name
                  AND  status IN ('PENDING', 'RUNNING')
                ORDER BY created_at DESC
                LIMIT 1
            )
        """)

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {
                    'process_name':      execution.process_name,
                    'status':            execution.status,
                    'run_id':            execution.run_id,
                    'records_processed': execution.records_processed,
                    'records_failed':    execution.records_failed,
                    'execution_time':    round(execution.execution_time_seconds, 2),
                    'error_message':     execution.error_message,
                    'started_at':        execution.started_at,
                    'completed_at':      execution.completed_at,
                })
                conn.commit()
                if result.rowcount > 0:
                    self.logger(
                        f"{execution.process_name}: "
                        f"{execution.records_processed} records, "
                        f"{execution.execution_time_seconds:.2f}s",
                        "SUCCESS",
                    )
                    return True
                else:
                    self.logger(
                        f"No window updated for {execution.process_name}", "WARN"
                    )
                    return False
        except Exception as e:
            self.logger(f"Error updating metrics: {e}", "ERROR")
            return False

    # =========================================================================
    # BATCH ID PERSISTENCE
    # Extracted from main_silver_orchestrator._save_batch_id (v2.3)
    # =========================================================================

    def save_batch_id(
        self,
        process_name: str,
        period_start: Optional[str],
        batch_id: str,
    ) -> None:
        """
        Persist the BatchTracker batch_id into etl_process_windows.notes
        for the most recently completed window of the given process/period.

        Args:
            process_name: e.g. 'SAP_TRANSACTIONS'
            period_start: 'YYYY-MM-DD' — the first day of the processing window.
                          The period (YYYY-MM) is derived from this date.
                          Pass None to skip silently.
            batch_id:     The batch_id string produced by BatchTracker.

        Notes:
            Failures are swallowed — this is an audit convenience, not critical.
        """
        if not period_start:
            return

        periodo = period_start[:7]   # 'YYYY-MM-DD' → 'YYYY-MM'

        # NOTE: PostgreSQL does not support ORDER BY / LIMIT in UPDATE statements
        # (that is MySQL syntax). The correct pattern is a subquery on the PK.
        query = text("""
            UPDATE biq_config.etl_process_windows
            SET notes = :batch_id
            WHERE id = (
                SELECT id
                FROM biq_config.etl_process_windows
                WHERE process_name = :process_name
                  AND periodo_mes  = :periodo
                  AND status       = 'COMPLETED'
                ORDER BY completed_at DESC
                LIMIT 1
            )
        """)

        try:
            with self.engine.connect() as conn:
                conn.execute(query, {
                    'batch_id':     batch_id,
                    'process_name': process_name,
                    'periodo':      periodo,
                })
                conn.commit()
        except Exception as e:
            self.logger(
                f"Could not save batch_id for {process_name}/{periodo}: {e}", "WARN"
            )

    # =========================================================================
    # TODAY'S SUMMARY (for final pipeline report)
    # Extracted from main_silver_orchestrator._show_process_metrics_summary (v2.3)
    # =========================================================================

    def get_todays_summary(self, limit: int = 20) -> List[dict]:
        """
        Return today's completed/failed process executions with record counts.

        Used by the orchestrator to print the final pipeline report without
        embedding a raw SQL query in the main module.

        Args:
            limit: Maximum number of rows to return (most recent first).

        Returns:
            List of dicts with keys: process_name, status, records_processed,
            execution_time_seconds, completed_at.
            Returns an empty list on error.
        """
        query = text("""
            SELECT
                process_name,
                status,
                records_processed,
                execution_time_seconds,
                completed_at
            FROM biq_config.etl_process_windows
            WHERE DATE(created_at) = CURRENT_DATE
              AND status IN ('COMPLETED', 'FAILED')
              AND records_processed > 0
            ORDER BY completed_at DESC
            LIMIT :limit
        """)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(query, {'limit': limit}).fetchall()
                return [dict(row._mapping) for row in rows]
        except Exception as e:
            self.logger(f"Could not load today's summary: {e}", "WARN")
            return []

    # =========================================================================
    # GET HISTORY
    # =========================================================================

    def get_process_history(
        self,
        process_name: str,
        limit: int = 10,
    ) -> list:
        """Return execution history for a process (most recent first)."""
        query = text("""
            SELECT
                periodo_mes,
                status,
                records_processed,
                records_failed,
                execution_time_seconds,
                completed_at,
                error_message
            FROM biq_config.etl_process_windows
            WHERE process_name = :process_name
            ORDER BY created_at DESC
            LIMIT :limit
        """)

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {
                    'process_name': process_name,
                    'limit':        limit,
                })
                return [dict(row._mapping) for row in result]
        except Exception as e:
            self.logger(f"Error obtaining history for {process_name}: {e}", "ERROR")
            return []