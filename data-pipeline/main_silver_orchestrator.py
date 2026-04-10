"""
===============================================================================
Project: PACIOLI
Module: main_silver_orchestrator
===============================================================================

Description:
    Entry point and execution director for the PACIOLI Silver Layer pipeline.
    Responsible ONLY for orchestration: what runs, in what order, and what to
    do when something fails.

Architecture:
    PRE-CHECK:  Guarantee at least one PENDING window exists (never closes).
    PHASE 0:    RAW loaders — always run, load whatever files are in folder.
    PHASES 1-6: STAGING — controlled by etl_process_windows period windows.
    POST-CHECK: Evaluate period closure with real RAW data already loaded.

    Group 1 — Foundations:        SAP staging + Portfolio CDC + Portfolio Ph1
    Group 2 — Cards:              Diners, Guayaquil, Pacificard
    Group 3 — Derived:            Parking breakdown
    Group 4 — Withholdings:       Process, Match, Apply
    Group 5 — Advanced Portfolio: Manual requests, Bank enrichment, Ph2/Ph3
    Group 6 — Final:              Reconciliation, Validation, Portfolio matches,
                                  Restore approved

VERSION: 3.1
Changes from 3.0:
    - _run_period_maintenance('pre') now calls ensure_periods_exist() only —
      never evaluates closure before RAW is loaded.
    - _execute_command saves batch_id to etl_process_windows.notes after
      COMPLETED so NewDataDetector can compare batches on subsequent runs.
    - NewDataDetector schema bug fix propagated (table names without prefix).

Dependencies:
    - All staging commands in logic.application.commands.staging
    - logic.domain.services.smart_period_closure
    - logic.infrastructure.repositories.process_metrics_repository
    - logic.domain.services.process_metrics_tracker
    - logic.domain.services.new_data_detector
    - utils.metrics_helpers
    - utils.logger, utils.db_config

===============================================================================
"""

import sys
import time
from datetime import datetime
from typing import Optional

import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

from sqlalchemy import text

from utils.logger import get_logger
from utils.db_config import get_db_engine
from utils.metrics_helpers import get_records_count

from logic.infrastructure.unit_of_work import UnitOfWork
from logic.domain.services.process_metrics_tracker import ProcessMetricsTracker, ProcessExecution
from logic.domain.services.smart_period_closure import SmartPeriodClosure
from logic.domain.services.new_data_detector import NewDataDetector
from logic.infrastructure.repositories.process_metrics_repository import ProcessMetricsRepository

# Staging commands — Group 1
from logic.application.commands.staging.process_sap_staging import ProcessSAPStagingCommand
from logic.application.commands.staging.process_portfolio_cdc import ProcessPortfolioCDCCommand
from logic.application.commands.staging.process_customer_portfolio import ProcessCustomerPortfolioCommand

# Staging commands — Group 2
from logic.application.commands.staging.process_diners_staging import ProcessDinersCommand
from logic.application.commands.staging.process_guayaquil_staging import ProcessGuayaquilCommand
from logic.application.commands.staging.process_pacificard_staging import ProcessPacificardCommand

# Staging commands — Group 3
from logic.application.commands.staging.process_parking_breakdown import ProcessParkingBreakdownCommand

# Staging commands — Group 4
from logic.application.commands.staging.process_withholdings import ProcessWithholdingsCommand
from logic.application.commands.staging.match_withholdings import MatchWithholdingsCommand
from logic.application.commands.staging.apply_withholdings import ApplyWithholdingsCommand

# Staging commands — Group 5
from logic.application.commands.staging.process_manual_requests import ProcessManualRequestsCommand
from logic.application.commands.staging.process_bank_enrichment import ProcessBankEnrichmentCommand

# Staging commands — Group 6
from logic.application.commands.staging.reconcile_bank_transactions import ReconcileBankTransactionsCommand
from logic.application.commands.staging.update_bank_validation_metrics import UpdateBankValidationMetricsCommand
from logic.application.commands.staging.validate_portfolio_matches import ValidatePortfolioMatchesCommand
from logic.application.commands.staging.restore_approved_transactions import RestoreApprovedTransactionsCommand

# RAW Loaders
from data_loaders.sap_239_loader import SapLoader
from data_loaders.banco_loader import BancoLoader
from data_loaders.databalance_loader import DatabalanceLoader
from data_loaders.diners_club_loader import DinersClubLoader
from data_loaders.guayaquil_loader import GuayaquilLoader
from data_loaders.pacificard_loader import PacificardLoader
from data_loaders.fbl5n_loader import FBL5NLoader
from data_loaders.webpos_loader import WebposLoader
from data_loaders.retenciones_loader import RetencionesLoader
from data_loaders.manual_requests_loader import ManualRequestsLoader

from pathlib import Path


# =============================================================================
# PROCESS NAME MAPPING
# =============================================================================

PROCESS_NAME_MAPPING = {
    'ProcessSAPStaging':             'SAP_TRANSACTIONS',
    'ProcessDinersStaging':          'DINERS_CARDS',
    'ProcessGuayaquilStaging':       'GUAYAQUIL_CARDS',
    'ProcessPacificardStaging':      'PACIFICARD_CARDS',
    'ProcessParkingBreakdown':       'PARKING_BREAKDOWN',
    'ProcessCustomerPortfolio-F1':   'CUSTOMER_PORTFOLIO_PHASE1',
    'ProcessCustomerPortfolio-F2':   'CUSTOMER_PORTFOLIO_PHASE2',
    'ProcessCustomerPortfolio-F3':   'CUSTOMER_PORTFOLIO_PHASE3',
    'ProcessWithholdings':           'WITHHOLDINGS_PROCESS',
    'MatchWithholdings':             'WITHHOLDINGS_MATCH',
    'ApplyWithholdings':             'WITHHOLDINGS_APPLY',
    'ProcessManualRequests':         'MANUAL_REQUESTS',
    'ProcessBankEnrichment':         'BANK_ENRICHMENT',
    'ReconcileBankTransactions':     'BANK_RECONCILIATION',
    'UpdateBankValidationMetrics':   'BANK_VALIDATION_METRICS',
    'ValidatePortfolioMatches':      'VALIDATE_PORTFOLIO_MATCHES',
    'RestoreApprovedTransactions':   'RESTORE_APPROVED',
}


# =============================================================================
# ORCHESTRATOR
# =============================================================================

class SilverLayerOrchestrator:
    """
    Pipeline director for the PACIOLI Silver Layer.

    Pre/post-check separation (v3.1):
        PRE  → ensure_periods_exist(): only creates windows if none active.
        POST → auto_close_periods_if_ready(): closes with real RAW data.
    """

    def __init__(self):
        self.logger         = get_logger("SILVER_ORCHESTRATOR")
        self.engine_raw     = get_db_engine('raw')
        self.engine_stg     = get_db_engine('stg')
        self.engine_config  = get_db_engine('config')

        self.metrics_tracker   = ProcessMetricsTracker()
        self.metrics_repo      = ProcessMetricsRepository(self.engine_config)
        self.period_closure    = SmartPeriodClosure(self.engine_stg, self.engine_config)
        self.new_data_detector = NewDataDetector(self.engine_raw, self.engine_config)

        self._stats = {
            'total': 0, 'successful': 0,
            'failed': 0, 'skipped': 0,
            'start_time': None, 'end_time': None,
        }

    # =========================================================================
    # MAIN ENTRY POINT
    # =========================================================================

    def execute_full_pipeline(
        self,
        force: bool = False,
        auto_close_periods: bool = True,
    ) -> bool:
        """Run the complete Silver Layer pipeline."""

        self._stats['start_time'] = datetime.now()
        self._print_header(force)

        try:
            # ── PRE-CHECK: guarantee windows exist (never closes) ─────────────
            self.logger("\n" + "═" * 80, "INFO")
            self.logger("PASO 0A: VERIFICACIÓN DE VENTANAS DE PROCESO", "INFO")
            self.logger("═" * 80, "INFO")
            self._pre_check()

            # ── Phase 0: RAW ──────────────────────────────────────────────────
            self.logger("\n" + "═" * 80, "INFO")
            self.logger("FASE 0: RAW LAYER", "INFO")
            self.logger("═" * 80, "INFO")
            if not self._execute_raw_loading():
                self.logger("RAW Layer failed — aborting pipeline", "ERROR")
                return False

            # ── Phases 1-6: STAGING ───────────────────────────────────────────
            self.logger("\n" + "═" * 80, "INFO")
            self.logger("INICIANDO STAGING LAYER (Fases 1-6)", "INFO")
            self.logger("═" * 80, "INFO")

            if not self._execute_group1_foundations(force): return False
            if not self._execute_group2_cards(force):       return False
            if not self._execute_group3_derived(force):     return False
            if not self._execute_group4_withholdings(force): return False
            if not self._execute_group5_advanced_portfolio(force): return False
            if not self._execute_group6_final_reconciliation(force): return False

            # ── POST-CHECK: evaluate closure with real RAW data ───────────────
            if auto_close_periods:
                self._post_check()

            # ── Final report ──────────────────────────────────────────────────
            self._stats['end_time'] = datetime.now()
            self.new_data_detector.clear_cache()
            self._generate_final_report()

            self.logger("\n" + "═" * 80, "SUCCESS")
            self.logger("PIPELINE COMPLETO FINALIZADO EXITOSAMENTE", "SUCCESS")
            self.logger("═" * 80, "SUCCESS")
            return True

        except Exception as e:
            self.logger(f"Critical error in orchestrator: {e}", "ERROR")
            import traceback
            self.logger(traceback.format_exc(), "ERROR")
            return False

    # =========================================================================
    # PRE / POST CHECKS
    # =========================================================================

    def _pre_check(self) -> None:
        """
        Guarantee at least one active PENDING window before RAW loading.

        Delegates entirely to SmartPeriodClosure.ensure_periods_exist().
        NEVER evaluates closure — RAW tables may be empty at this point.
        """
        try:
            self.period_closure.ensure_periods_exist()
        except Exception as e:
            # Cold-start: tables may not exist yet, pipeline will create them
            self.logger(
                f"Pre-check warning (cold-start?): {e}", "WARN"
            )
            self.logger(
                "Continuing — tables will be initialized in Phase 0/1.", "INFO"
            )

    def _post_check(self) -> None:
        """
        Evaluate period closure AFTER staging, with real RAW data loaded.

        Delegates to SmartPeriodClosure.auto_close_periods_if_ready().
        If any periods close, immediately opens the next month's windows.
        """
        self.logger("\n" + "═" * 80, "INFO")
        self.logger("POST-CHECK: VERIFICACIÓN DE CIERRE DE PERIODOS", "INFO")
        self.logger("═" * 80, "INFO")
        self.logger("Candado 1: Barrera de Tiempo (Calendar Gate)", "INFO")
        self.logger("Candado 2: Validación SAP (Business Gate)", "INFO")
        self.logger("", "INFO")

        try:
            result = self.period_closure.auto_close_periods_if_ready()

            if result['closed']:
                self.logger(
                    f"Periodos cerrados: {len(result['closed'])}", "SUCCESS"
                )
                for item in result['closed']:
                    self.logger(
                        f"   • {item['process']}: {item['periodo']} "
                        f"(Razón: {item.get('reason', 'N/A')})",
                        "INFO",
                    )
                self.period_closure.open_next_periods(result['closed'])

            if result['not_ready']:
                self.logger(
                    f"Periodos pendientes de cierre: {len(result['not_ready'])}",
                    "INFO",
                )
                for item in result['not_ready']:
                    self.logger(
                        f"   • {item['process']}: {item['periodo']} — {item['reason']}",
                        "INFO",
                    )

            if not result['closed'] and not result['not_ready']:
                self.logger("No hay periodos transaccionales para evaluar", "INFO")

        except Exception as e:
            self.logger(f"Post-check warning: {e}", "WARN")

    # =========================================================================
    # PHASE 0: RAW LOADING
    # =========================================================================

    def _execute_raw_loading(self) -> bool:
        """
        Run all RAW loaders. Each loader checks its own input folder and
        skips silently if no files are pending. The pipeline continues
        regardless of which loaders find files.
        """
        self.logger("Executing RAW Loaders...", "INFO")
        self.logger("─" * 80, "INFO")

        try:
            BASE_DIR = Path(__file__).resolve().parent
        except NameError:
            BASE_DIR = Path.cwd()

        CONFIG_DIR = BASE_DIR / "config" / "schemas"
        self.logger(f"Config folder: {CONFIG_DIR}", "INFO")
        self.logger("", "INFO")

        loaders = [
            ("SAP 239",         SapLoader,           "sap_239_loader.yaml"),
            ("FBL5N",           FBL5NLoader,          "fbl5n_loader.yaml"),
            ("Webpos",          WebposLoader,         "webpos_loader.yaml"),
            ("Banco",           BancoLoader,          "banco_239_loader.yaml"),
            ("Diners Club",     DinersClubLoader,     "diners_club_loader.yaml"),
            ("Guayaquil",       GuayaquilLoader,      "guayaquil_loader.yaml"),
            ("Pacificard",      PacificardLoader,     "pacificard_loader.yaml"),
            ("Databalance",     DatabalanceLoader,    "databalance_loader.yaml"),
            ("Retenciones",     RetencionesLoader,    "retenciones_sri_loader.yaml"),
            ("Manual Requests", ManualRequestsLoader, "manual_requests_loader.yaml"),
        ]

        for name, LoaderClass, config_file in loaders:
            yaml_path = CONFIG_DIR / config_file
            if not yaml_path.exists():
                self.logger(f"No pending files for {name}", "INFO")
                continue
            try:
                self.logger(f"Loading {name}...", "INFO")
                loader = LoaderClass(config_yaml_path=str(yaml_path))
                loader.load()
                self.logger(f"{name} loaded", "SUCCESS")
            except Exception as e:
                self.logger(f"Error loading {name}: {e}", "ERROR")
                # Non-fatal: continue with remaining loaders

        self.logger("", "INFO")
        self.logger("RAW Layer completed", "SUCCESS")
        return True

    # =========================================================================
    # STAGING GROUPS
    # =========================================================================

    def _execute_group1_foundations(self, force: bool) -> bool:
        self.logger("\n" + "═" * 80, "INFO")
        self.logger("GRUPO 1: FUNDACIONES", "INFO")
        self.logger("═" * 80, "INFO")

        start_sap, end_sap = self._get_process_window('SAP_TRANSACTIONS')

        if start_sap:
            if not self._run_process(
                "ProcessSAPStaging", ProcessSAPStagingCommand, force,
                use_strict_dates=True,
                domain_start_date=start_sap, domain_end_date=end_sap,
            ):
                return False

        cdc = ProcessPortfolioCDCCommand()
        if not self._run_process_instance("ProcessPortfolioCDC", cdc, force):
            return False

        if not self._run_process(
            "ProcessCustomerPortfolio-F1", ProcessCustomerPortfolioCommand, force,
            use_strict_dates=False,
            domain_start_date=start_sap, domain_end_date=end_sap,
            window_start=start_sap, window_end=end_sap,
            phase=1,
        ):
            return False

        return True

    def _execute_group2_cards(self, force: bool) -> bool:
        self.logger("\n" + "═" * 80, "INFO")
        self.logger("GRUPO 2: TARJETAS", "INFO")
        self.logger("═" * 80, "INFO")

        start_d, end_d = self._get_process_window('DINERS_CARDS')
        start_g, end_g = self._get_process_window('GUAYAQUIL_CARDS')
        start_p, end_p = self._get_process_window('PACIFICARD_CARDS')

        if start_d:
            if not self._run_process(
                "ProcessDinersStaging", ProcessDinersCommand, force,
                use_strict_dates=True, domain_start_date=start_d, domain_end_date=end_d,
            ): return False
        if start_g:
            if not self._run_process(
                "ProcessGuayaquilStaging", ProcessGuayaquilCommand, force,
                use_strict_dates=True, domain_start_date=start_g, domain_end_date=end_g,
            ): return False
        if start_p:
            if not self._run_process(
                "ProcessPacificardStaging", ProcessPacificardCommand, force,
                use_strict_dates=True, domain_start_date=start_p, domain_end_date=end_p,
            ): return False
        return True

    def _execute_group3_derived(self, force: bool) -> bool:
        self.logger("\n" + "═" * 80, "INFO")
        self.logger("GRUPO 3: DERIVADOS", "INFO")
        self.logger("═" * 80, "INFO")

        start_park, end_park = self._get_process_window('PARKING_BREAKDOWN')
        if start_park:
            if not self._run_process(
                "ProcessParkingBreakdown", ProcessParkingBreakdownCommand, force,
                use_strict_dates=True,
                domain_start_date=start_park, domain_end_date=end_park,
            ): return False
        return True

    def _execute_group4_withholdings(self, force: bool) -> bool:
        self.logger("\n" + "═" * 80, "INFO")
        self.logger("GRUPO 4: RETENCIONES", "INFO")
        self.logger("═" * 80, "INFO")

        cmd = ProcessWithholdingsCommand()
        if not self._run_process_instance("ProcessWithholdings", cmd, force): return False

        with UnitOfWork(self.engine_stg) as uow_match:
            cmd = MatchWithholdingsCommand(uow_match)
            if not self._run_process_instance("MatchWithholdings", cmd, force): return False

        with UnitOfWork(self.engine_stg) as uow_apply:
            cmd = ApplyWithholdingsCommand(uow_apply)
            if not self._run_process_instance("ApplyWithholdings", cmd, force): return False

        return True

    def _execute_group5_advanced_portfolio(self, force: bool) -> bool:
        self.logger("\n" + "═" * 80, "INFO")
        self.logger("GRUPO 5: CARTERA AVANZADA", "INFO")
        self.logger("═" * 80, "INFO")

        if not self._run_process("ProcessManualRequests",       ProcessManualRequestsCommand,   force, use_strict_dates=False): return False
        if not self._run_process("ProcessBankEnrichment",       ProcessBankEnrichmentCommand,   force, use_strict_dates=False): return False
        if not self._run_process("ProcessCustomerPortfolio-F2", ProcessCustomerPortfolioCommand, force, use_strict_dates=False, phase=2): return False
        if not self._run_process("ProcessCustomerPortfolio-F3", ProcessCustomerPortfolioCommand, force, use_strict_dates=False, phase=3): return False
        return True

    def _execute_group6_final_reconciliation(self, force: bool) -> bool:
        self.logger("\n" + "═" * 80, "INFO")
        self.logger("GRUPO 6: CONCILIACIÓN FINAL", "INFO")
        self.logger("═" * 80, "INFO")

        with UnitOfWork(self.engine_stg) as uow_final:
            cmd = ReconcileBankTransactionsCommand(uow_final)
            if not self._run_process_instance("ReconcileBankTransactions", cmd, force): return False

        if not self._run_process("UpdateBankValidationMetrics", UpdateBankValidationMetricsCommand, force, use_strict_dates=False): return False
        if not self._run_process("ValidatePortfolioMatches",    ValidatePortfolioMatchesCommand,    force, use_strict_dates=False): return False

        restore_cmd = RestoreApprovedTransactionsCommand()
        if not self._run_process_instance("RestoreApprovedTransactions", restore_cmd, force): return False

        return True

    # =========================================================================
    # EXECUTION HELPERS
    # =========================================================================

    def _get_process_window(self, process_name: str) -> tuple:
        """Read the active PENDING window dates for a transactional process."""
        query = text("""
            SELECT window_start, window_end, periodo_mes
            FROM biq_config.etl_process_windows
            WHERE process_name = :process_name
              AND process_type = 'TRANSACTIONAL'
              AND status       = 'PENDING'
            ORDER BY created_at DESC
            LIMIT 1
        """)
        try:
            with self.engine_config.connect() as conn:
                result = conn.execute(query, {"process_name": process_name}).fetchone()
                if result and result.window_start and result.window_end:
                    self.logger(
                        f"   📅 Window: {result.window_start} → {result.window_end} "
                        f"(Period: {result.periodo_mes})",
                        "INFO",
                    )
                    return (
                        result.window_start.strftime('%Y-%m-%d'),
                        result.window_end.strftime('%Y-%m-%d'),
                    )
                self.logger(f"   No PENDING window for {process_name}", "WARN")
        except Exception as e:
            self.logger(f"   Error reading window: {e}", "ERROR")
        return None, None

    def _run_process(
        self,
        name: str,
        CommandClass,
        force: bool,
        use_strict_dates: bool = True,
        domain_start_date: Optional[str] = None,
        domain_end_date:   Optional[str] = None,
        **kwargs,
    ) -> bool:
        """Instantiate a command class and run it through the standard gate."""
        self._stats['total'] += 1
        process_name = PROCESS_NAME_MAPPING.get(name)

        if not force and process_name:
            current_status = self.metrics_repo.get_process_status(process_name)
            if current_status == 'COMPLETED':
                detection = self.new_data_detector.has_new_data(
                    process_name,
                    period_start=domain_start_date,
                    period_end=domain_end_date,
                )
                if not detection['has_new_data']:
                    self.logger(f"{name}: {detection['reason']} — skipping", "INFO")
                    self._stats['skipped'] += 1
                    return True
                self.logger(f"{name}: {detection['reason']} — re-running", "SUCCESS")
            else:
                self.logger(f"{name}: status={current_status} — running", "INFO")

        return self._execute_command(
            name, CommandClass(), process_name,
            use_strict_dates, domain_start_date, domain_end_date, **kwargs,
        )

    def _run_process_instance(
        self, name: str, command_instance, force: bool
    ) -> bool:
        """Run a pre-instantiated command (UoW passed at construction time)."""
        self._stats['total'] += 1
        process_name = PROCESS_NAME_MAPPING.get(name)

        if not force and process_name:
            current_status = self.metrics_repo.get_process_status(process_name)
            if self.metrics_tracker.should_skip_process(process_name, None, current_status):
                self.logger(f"{name}: already COMPLETED — skipping", "INFO")
                self._stats['skipped'] += 1
                return True

        return self._execute_command(
            name, command_instance, process_name,
            use_strict_dates=False,
            domain_start_date=None, domain_end_date=None,
        )

    def _execute_command(
        self,
        name: str,
        command,
        process_name: Optional[str],
        use_strict_dates: bool,
        domain_start_date: Optional[str],
        domain_end_date:   Optional[str],
        **kwargs,
    ) -> bool:
        """
        Execute a command with full metrics tracking.

        After a successful COMPLETED transition, persists the batch_id
        (if extractable) into etl_process_windows.notes so that
        NewDataDetector can compare batches on the next pipeline run.
        """
        start_time = time.time()
        execution  = None

        if process_name:
            execution = ProcessExecution.create_running(process_name)
            self.metrics_repo.update_metrics(execution)

        try:
            self.logger(f"\nExecuting: {name}...", "INFO")

            if use_strict_dates and domain_start_date and domain_end_date:
                kwargs['force_start_date'] = domain_start_date
                kwargs['force_end_date']   = domain_end_date
                self.logger(
                    f"   Window: {domain_start_date} → {domain_end_date}", "INFO"
                )
            elif not use_strict_dates:
                self.logger("   Stateful mode: searching PENDING events.", "INFO")

            result         = command.execute(**kwargs)
            execution_time = time.time() - start_time

            if result:
                self.logger(f"{name} completed", "SUCCESS")
                self._stats['successful'] += 1
                records = get_records_count(command)

                if process_name and execution:
                    execution = execution.mark_completed(
                        records_processed=records,
                        execution_time=execution_time,
                    )
                    self.metrics_repo.update_metrics(execution)

                    # ── Persist batch_id so NewDataDetector works correctly ──
                    # Extract batch_id from the command's BatchTracker if available.
                    # Stored in etl_process_windows.notes for comparison next run.
                    batch_id = self._extract_batch_id_from_command(command)
                    if batch_id and domain_start_date:
                        self.metrics_repo.save_batch_id(
                            process_name, domain_start_date, batch_id
                        )

                return True

            self.logger(f"{name} returned False", "WARN")
            self._stats['failed'] += 1
            if process_name and execution:
                execution = execution.mark_failed(
                    error_message="Process returned False",
                    execution_time=execution_time,
                )
                self.metrics_repo.update_metrics(execution)
            return False

        except Exception as e:
            execution_time = time.time() - start_time
            self.logger(f"Error in {name}: {e}", "ERROR")
            self._stats['failed'] += 1
            if process_name and execution:
                execution = execution.mark_failed(
                    error_message=str(e), execution_time=execution_time
                )
                self.metrics_repo.update_metrics(execution)
            return False

    @staticmethod
    def _extract_batch_id_from_command(command) -> Optional[str]:
        """
        Try to read the batch_id from a command's BatchTracker.

        Returns None if the command has no tracker or the tracker has no
        current_batch_id — the save is then silently skipped.
        """
        tracker = getattr(command, 'batch_tracker', None)
        if tracker is None:
            return None
        return getattr(tracker, 'current_batch_id', None)

    # =========================================================================
    # REPORTING
    # =========================================================================

    def _print_header(self, force: bool) -> None:
        self.logger("═" * 80, "INFO")
        self.logger("INICIANDO PIPELINE COMPLETO - PACIOLI SILVER LAYER", "INFO")
        self.logger("═" * 80, "INFO")
        self.logger("", "INFO")
        self.logger("ARQUITECTURA DE CAPAS:", "INFO")
        self.logger("   • FASE 0 (RAW):       Independiente — procesa todo lo que encuentre", "INFO")
        self.logger("   • FASE 1-6 (STAGING): Controlado por periodos", "INFO")
        self.logger("", "INFO")
        mode = (
            "FORCE MODE: Re-ejecutará procesos STAGING completados"
            if force else
            "IDEMPOTENT MODE: Saltará procesos STAGING completados"
        )
        self.logger(mode, "WARN" if force else "INFO")

    def _generate_final_report(self) -> None:
        s        = self._stats
        duration = (s['end_time'] - s['start_time']).total_seconds()
        rate     = (s['successful'] / s['total'] * 100) if s['total'] > 0 else 0

        self.logger("\n" + "═" * 80, "INFO")
        self.logger("REPORTE FINAL", "INFO")
        self.logger("═" * 80, "INFO")
        self.logger(f"\nDuration:      {duration:.2f}s", "INFO")
        self.logger(f"Total:         {s['total']}", "INFO")
        self.logger(f"Successful:    {s['successful']}", "SUCCESS")
        self.logger(f"Skipped:       {s['skipped']}", "INFO")
        self.logger(
            f"Failed:        {s['failed']}",
            "ERROR" if s['failed'] > 0 else "INFO",
        )
        self.logger(f"Success rate:  {rate:.1f}%", "INFO")

        rows = self.metrics_repo.get_todays_summary()
        if rows:
            self.logger("\nToday's process metrics:", "INFO")
            self.logger("─" * 80, "INFO")
            for row in rows:
                icon = "T" if row['status'] == 'COMPLETED' else "F"
                self.logger(
                    f"{icon} {row['process_name']}: "
                    f"{row['records_processed']} records in "
                    f"{row['execution_time_seconds']:.2f}s",
                    "INFO",
                )


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    global_start = time.time()

    orchestrator = SilverLayerOrchestrator()
    success      = orchestrator.execute_full_pipeline(
        force=False,
        auto_close_periods=True,
    )

    total_seconds  = time.time() - global_start
    mins, secs     = divmod(total_seconds, 60)
    hours, mins    = divmod(mins, 60)
    time_formatted = f"{int(hours):02d}:{int(mins):02d}:{secs:.2f}"

    orchestrator.logger("\n" + "═" * 80, "SUCCESS")
    orchestrator.logger(
        f"TIEMPO TOTAL ABSOLUTO: {time_formatted} (HH:MM:SS)", "SUCCESS"
    )
    orchestrator.logger("═" * 80, "SUCCESS")

    sys.exit(0 if success else 1)