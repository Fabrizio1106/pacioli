"""
===============================================================================
Project: PACIOLI
Module: logic.application.commands.staging.process_portfolio_cdc
===============================================================================

Description:
    Standalone command that orchestrates the Change Data Capture (CDC) sync
    from raw_customer_portfolio to stg_customer_portfolio.

    Previously this logic lived as a private method (_run_portfolio_cdc) in
    the main orchestrator. Extracting it into its own command follows the
    hexagonal architecture principle: the orchestrator should only direct
    traffic, not implement domain logic.

Responsibilities:
    - Read raw_customer_portfolio from the RAW engine (filtering invalid refs).
    - Delegate the actual CDC diff to ProcessPortfolioLoadCommand.
    - Return True/False so the orchestrator can decide whether to abort.

Key Components:
    - ProcessPortfolioCDCCommand: Self-contained CDC runner. Instantiate
      once and call execute(force). No UnitOfWork required externally —
      it manages its own session internally.

Notes:
    - Non-critical failure mode: if the CDC fails, the pipeline continues
      with the portfolio already in staging. A stale portfolio is better
      than a full abort. The method logs a WARNING and returns True.
    - Idempotency is handled inside ProcessPortfolioLoadCommand via its
      content fingerprint check. If the FBL5N file has not changed since
      the last run, the CDC exits immediately with skipped=True.

Dependencies:
    - sqlalchemy
    - pandas
    - utils.db_config
    - utils.logger
    - logic.infrastructure.unit_of_work
    - logic.application.commands.staging.process_portfolio_load

===============================================================================
"""

import traceback
import pandas as pd
from sqlalchemy import text

from utils.db_config import get_db_engine
from utils.logger import get_logger
from logic.infrastructure.unit_of_work import UnitOfWork
from logic.application.commands.staging.process_portfolio_load import (
    ProcessPortfolioLoadCommand,
)


class ProcessPortfolioCDCCommand:
    """
    CDC command: synchronizes raw_customer_portfolio → stg_customer_portfolio.

    Usage (from orchestrator):
        cdc = ProcessPortfolioCDCCommand()
        ok  = cdc.execute(force=False)
    """

    # SQL to pull valid rows from the RAW portfolio snapshot
    _RAW_QUERY = text("""
        SELECT
            n_documento,
            referencia_a_factura,
            cuenta,
            cliente,
            asignacion,
            referencia,
            fecha_documento,
            fecha_de_pago,
            importe,
            moneda_local,
            texto,
            cuenta_de_mayor,
            hash_id,
            loaded_at
        FROM raw_customer_portfolio
        WHERE referencia_a_factura IS NOT NULL
          AND referencia_a_factura != ''
          AND referencia_a_factura != 'nan'
    """)

    def __init__(self):
        self.logger        = get_logger("PORTFOLIO_CDC_CMD")
        self.engine_raw    = get_db_engine('raw')
        self.engine_stg    = get_db_engine('stg')
        self.engine_config = get_db_engine('config')

        # Expose stats for the orchestrator metrics extractor
        self.stats: dict = {}

    def execute(self, force: bool = False) -> bool:
        """
        Run the CDC sync.

        Args:
            force: Passed through to the orchestrator pattern; not used
                   internally because ProcessPortfolioLoadCommand handles
                   idempotency via content fingerprint, not force flag.

        Returns:
            True always — CDC failure is non-critical (pipeline continues).
        """
        self.logger("─" * 80, "INFO")
        self.logger("📥 CDC CARTERA: Synchronizing raw → stg...", "INFO")
        self.logger("─" * 80, "INFO")

        try:
            # ── 1. Read RAW portfolio ─────────────────────────────────────────
            with self.engine_raw.connect() as conn:
                df_raw = pd.read_sql(self._RAW_QUERY, conn)

            if df_raw.empty:
                self.logger("ℹ️ raw_customer_portfolio is empty — skipping CDC", "INFO")
                self.stats = {'skipped': True}
                return True

            self.logger(f"   → {len(df_raw)} rows in raw_customer_portfolio", "INFO")

            # ── 2. Run CDC inside its own UnitOfWork ──────────────────────────
            with UnitOfWork(self.engine_stg) as uow:
                cdc   = ProcessPortfolioLoadCommand(uow.session, self.engine_config)
                stats = cdc.execute(df_raw)

            self.stats = stats

            # ── 3. Report result ──────────────────────────────────────────────
            if stats.get('skipped'):
                self.logger(
                    "⏭️ CDC: Same FBL5N batch — no changes to apply", "INFO"
                )
                return True

            self.logger(
                f"   ✅ CDC completed — "
                f"Insert: {stats['inserted']} | "
                f"Update: {stats['updated']} | "
                f"Close:  {stats['closed']} | "
                f"Equal:  {stats['unchanged']}",
                "SUCCESS",
            )
            return True

        except Exception as e:
            # Non-critical: log and continue so the pipeline is not aborted
            self.logger(f"⚠️ Portfolio CDC failed (non-critical): {e}", "WARN")
            self.logger(traceback.format_exc(), "WARN")
            self.stats = {'error': str(e)}
            return True   # intentionally True — do not abort the pipeline