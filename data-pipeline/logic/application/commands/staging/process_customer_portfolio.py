"""
===============================================================================
Project: PACIOLI
Module: logic.application.commands.staging.process_customer_portfolio
===============================================================================

Description:
    Processes the customer portfolio by synchronizing with SAP raw data and 
    enriching it with Webpos, Parking, and VIP data across three independent 
    phases.

Responsibilities:
    - PHASE1: Netting, Webpos enrichment, and Hash generation.
    - PHASE2: Iterative Parking Transactional enrichment for convergence.
    - PHASE3: VIP Cascade enrichment (vouchers and settlements).
    - Delta synchronization with SAP raw data.

Key Components:
    - ProcessCustomerPortfolioCommand: Orchestrates the multi-phase processing.

Notes:
    - Uses independent batch tracking for each phase to ensure idempotency.
    - Phase 2 employs an iterative loop to handle residuals and reach convergence.

Dependencies:
    - yaml, datetime, utils.db_config, utils.logger
    - logic.infrastructure.batch_tracker, logic.infrastructure.unit_of_work
    - logic.infrastructure.extractors.customer_portfolio_extractor
    - logic.domain.services.customer_portfolio_sync_service
    - logic.domain.services.customer_portfolio_enricher_service

===============================================================================
"""

import yaml
from datetime import datetime, timedelta
from utils.db_config import get_db_engine
from utils.logger import get_logger
from logic.infrastructure.batch_tracker import BatchTracker, BatchStatus
from logic.infrastructure.unit_of_work import UnitOfWork

# Extractors
from logic.infrastructure.extractors.customer_portfolio_extractor import (
    CustomerPortfolioExtractor
)

# Domain Services
from logic.domain.services.customer_portfolio_sync_service import (
    CustomerPortfolioSyncService
)
from logic.domain.services.customer_portfolio_enricher_service import (
    CustomerPortfolioEnricherService
)


class ProcessCustomerPortfolioCommand:
    """
    Command to process Customer Portfolio.
    
    Phases:
    -------
    PHASE 1: Netting + Webpos + Hashes
    PHASE 2: PARKING Transactional
    PHASE 3: VIP Cascade
    
    Usage:
    ------
    # Execute all phases
    command.execute()
    
    # Execute only PHASE 2
    command.execute(phase=2)
    
    # Force re-execution of PHASE 3
    command.execute(phase=3, force=True)
    """

    def __init__(self):
        # 1. Initialization
        self.logger = get_logger("PORTFOLIO_COMMAND")
        
        # Engines
        self.engine_raw = get_db_engine('raw')
        self.engine_stg = get_db_engine('stg')
        self.engine_config = get_db_engine('config')
        
        # Config
        self.config = self._load_config()
        
        # Phase-specific batch trackers
        self.batch_tracker_p1 = BatchTracker(
            self.engine_config,
            "CUSTOMER_PORTFOLIO_PHASE1"
        )
        self.batch_tracker_p2 = BatchTracker(
            self.engine_config,
            "CUSTOMER_PORTFOLIO_PHASE2"
        )
        self.batch_tracker_p3 = BatchTracker(
            self.engine_config,
            "CUSTOMER_PORTFOLIO_PHASE3"
        )
        
        self.current_batch_id = None

    def _load_config(self) -> dict:
        try:
            with open(
                'config/rules/staging_customer_portfolio_rules.yaml',
                'r',
                encoding='utf-8'
            ) as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger(f"Error loading config: {str(e)}", "WARN")
            return {}

    def execute(self, force: bool = False, phase: int = None, **kwargs) -> bool:
        """
        Executes full processing or specific phases.
        """
        self.logger("Starting Customer Portfolio Processing...", "INFO")
        
        try:
            total_processed = 0
            
            # Capture injected dates from orchestrator
            window_start = kwargs.get('window_start')
            window_end = kwargs.get('window_end')
            
            # Determine which phases to run
            if phase is None:
                phases_to_run = [1, 2, 3]
            else:
                phases_to_run = [phase]
            
            # 2. Phase Execution
            for p in phases_to_run:
                if p == 1:
                    count = self._execute_phase1(force, window_start, window_end)
                    total_processed += count
                elif p == 2:
                    count = self._execute_phase2(force)
                    total_processed += count
                elif p == 3:
                    count = self._execute_phase3(force)
                    total_processed += count
            
            self.logger(
                f"Process completed: {total_processed} documents processed",
                "SUCCESS"
            )
            
            self.documents_processed = total_processed
            return True
            
        except Exception as e:
            self.logger(f"ERROR: {str(e)}", "ERROR")
            raise

    def _execute_phase1(self, force: bool, window_start: str = None, window_end: str = None) -> int:
        """
        PHASE 1: Netting + Webpos + Hashes
        
        Returns:
        --------
        int : Processed documents count
        """
        
        self.logger("\n" + "=" * 80, "INFO")
        self.logger("PHASE 1: NETTING + WEBPOS + HASHES", "INFO")
        self.logger("=" * 80, "INFO")
        
        # Idempotency check
        today = datetime.now().strftime('%Y%m%d')
        fingerprint = BatchTracker.generate_config_fingerprint({
            'date': today,
            'phase': 1
        })
        
        if not force and self.batch_tracker_p1.should_skip(fingerprint):
            self.logger("PHASE 1 already executed today", "WARN")
            return 0
        
        self.batch_tracker_p1.start_batch(
            fingerprint,
            metadata={'date': today, 'phase': 1}
        )
        
        try:
            # 1. Initialization
            extractor = CustomerPortfolioExtractor(self.engine_raw, self.engine_stg)
            sync_service = CustomerPortfolioSyncService(self.config)
            enricher_service = CustomerPortfolioEnricherService(self.config)
            
            # 2. Dynamic Webpos Date Calculation (15-day Lookback)
            if window_start and window_end:
                try:
                    start_dt = datetime.strptime(window_start, '%Y-%m-%d')
                    # Lookback 15 days from window start to catch lagging items
                    webpos_start = (start_dt - timedelta(days=15)).strftime('%Y-%m-%d')
                    webpos_end = window_end
                    self.logger(f"Webpos window (15d lookback): {webpos_start} to {webpos_end}", "INFO")
                except ValueError:
                    webpos_start = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
                    webpos_end = datetime.now().strftime('%Y-%m-%d')
                    self.logger(f"Format error. Fallback Webpos: {webpos_start} to {webpos_end}", "WARN")
            else:
                # Security fallback for standalone execution
                webpos_start = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
                webpos_end = datetime.now().strftime('%Y-%m-%d')
                self.logger(f"Standalone execution. Fallback Webpos: {webpos_start} to {webpos_end}", "INFO")
            
            # 3. Extraction
            df_sap_raw = extractor.extract_sap_snapshot()
            df_webpos = extractor.extract_webpos(
                start_date=webpos_start,
                end_date=webpos_end
            )
            
            if df_sap_raw.empty:
                self.logger("SAP snapshot is empty", "WARN")
                self.batch_tracker_p1.complete_batch(records_processed=0)
                return 0
            
            with UnitOfWork(self.engine_stg) as uow:
                repo = uow.customer_portfolio
                
                # 4. Delta Sync
                df_stg_current = extractor.get_active_portfolio()
                sync_stats = sync_service.sync_with_sap_raw(
                    df_raw=df_sap_raw,
                    df_stg_current=df_stg_current,
                    repo=repo
                )
                
                self.logger(
                    f"   New: {sync_stats['new_count']} | "
                    f"Closed: {sync_stats['closed_count']} | "
                    f"Updated: {sync_stats['updated_count']}",
                    "INFO"
                )
                
                # Reload portfolio
                df_portfolio = extractor.get_active_portfolio()
                
                # 5. Enrichment Phase 1
                df_portfolio = enricher_service.process_internal_netting(df_portfolio)
                df_portfolio = enricher_service.enrich_with_webpos(df_portfolio, df_webpos)
                df_portfolio = enricher_service.generate_final_hashes(df_portfolio)
                df_portfolio = enricher_service.generate_etl_hash(df_portfolio)
                
                # 6. Save changes
                saved_count = repo.save_portfolio_changes(df_portfolio)
                
                # Feedback loop to source tables
                feedback_count = repo.update_source_tables(df_portfolio)
            
            self.batch_tracker_p1.complete_batch(records_processed=saved_count)
            self.logger(f"PHASE 1 completed: {saved_count} documents", "SUCCESS")
            return saved_count
            
        except Exception as e:
            self.batch_tracker_p1.fail_batch(str(e))
            raise

    def _execute_phase2(self, force: bool) -> int:
        """
        PHASE 2: PARKING Transactional (Iterative v3.6)
        
        Convergence Logic:
        -----------------
        - Executes multiple passes until convergence.
        - Each pass processes residuals created in the previous pass.
        - Convergence reached when PENDING status count remains unchanged.
        
        Returns:
        --------
        int : Total processed documents (all iterations)
        """
        
        self.logger("\n" + "=" * 80, "INFO")
        self.logger("PHASE 2: PARKING TRANSACTIONAL (Iterative v3.6)", "INFO")
        self.logger("=" * 80, "INFO")
        
        # Idempotency check
        today = datetime.now().strftime('%Y%m%d')
        fingerprint = BatchTracker.generate_config_fingerprint({
            'date': today,
            'phase': 2
        })
        
        if not force and self.batch_tracker_p2.should_skip(fingerprint):
            self.logger("PHASE 2 already executed today", "WARN")
            return 0
        
        self.batch_tracker_p2.start_batch(
            fingerprint,
            metadata={'date': today, 'phase': 2}
        )
        
        try:
            # 1. Initialization
            extractor = CustomerPortfolioExtractor(self.engine_raw, self.engine_stg)
            enricher_service = CustomerPortfolioEnricherService(self.config)
            
            # Extract sources for PARKING (ONCE)
            df_bank_parking = extractor.extract_bank_parking_pending()
            df_breakdown = extractor.extract_parking_breakdown()
            
            with UnitOfWork(self.engine_stg) as uow:
                repo = uow.customer_portfolio
                
                # 2. Iterative Convergence Loop
                max_iterations = 10  # Security limit
                iteration = 0
                total_saved = 0
                
                # Tracking PENDING between iterations
                previous_pending = None
                
                while iteration < max_iterations:
                    iteration += 1
                    
                    self.logger(
                        f"\nIteration {iteration}: Processing portfolio...",
                        "INFO"
                    )
                    
                    # Load CURRENT portfolio (includes residuals from previous iteration)
                    df_portfolio = extractor.get_active_portfolio()
                    
                    # Count PENDING PARKING before processing
                    pending_before = len(df_portfolio[
                        (df_portfolio['reconcile_status'] == 'PENDING') &
                        (df_portfolio['reconcile_group'] == 'PARKING_CARD')
                    ])
                    
                    # 3. Processing: Parking transactional
                    df_portfolio = enricher_service.enrich_parking_transactional(
                        df_portfolio, df_bank_parking, df_breakdown
                    )
                    
                    df_portfolio = enricher_service.generate_etl_hash(df_portfolio)
                    
                    # 4. Save Changes
                    saved_count = repo.save_portfolio_changes(df_portfolio)
                    total_saved += saved_count
                    
                    self.logger(
                        f"   -> Saved: {saved_count} records",
                        "INFO"
                    )
                    
                    # Reload to count PENDING after
                    df_portfolio_updated = extractor.get_active_portfolio()
                    pending_after = len(df_portfolio_updated[
                        (df_portfolio_updated['reconcile_status'] == 'PENDING') &
                        (df_portfolio_updated['reconcile_group'] == 'PARKING_CARD')
                    ])
                    
                    # 5. Convergence Logic
                    # CASE 1: First iteration
                    if previous_pending is None:
                        previous_pending = pending_after
                        self.logger(
                            f"   PENDING PARKING_CARD: {pending_after} "
                            f"(first iteration, continuing)",
                            "INFO"
                        )
                        continue
                    
                    # CASE 2: No changes between iterations -> Convergence reached
                    if pending_after == previous_pending:
                        self.logger(
                            f"\nConvergence reached in iteration {iteration}",
                            "SUCCESS"
                        )
                        self.logger(
                            f"   PENDING PARKING_CARD: {previous_pending} -> {pending_after} (no changes)",
                            "INFO"
                        )
                        break
                    
                    # CASE 3: PENDING decreased -> Progress, continuing
                    if pending_after < previous_pending:
                        self.logger(
                            f"   PENDING PARKING_CARD: {previous_pending} -> {pending_after} "
                            f"(decreasing, continuing)",
                            "INFO"
                        )
                        previous_pending = pending_after
                        continue
                    
                    # CASE 4: PENDING increased -> Residuals created, continuing to process them
                    if pending_after > previous_pending:
                        self.logger(
                            f"   PENDING PARKING_CARD: {previous_pending} -> {pending_after} "
                            f"(increasing, residuals created, continuing)",
                            "INFO"
                        )
                        previous_pending = pending_after
                        continue
                
                # 6. Final Feedback Loop
                df_portfolio_final = extractor.get_active_portfolio()
                feedback_count = repo.update_source_tables(df_portfolio_final)
            
            self.batch_tracker_p2.complete_batch(records_processed=total_saved)
            self.logger(
                f"\nPHASE 2 completed: {total_saved} documents in {iteration} iterations",
                "SUCCESS"
            )
            return total_saved
            
        except Exception as e:
            self.batch_tracker_p2.fail_batch(str(e))
            raise

    def _execute_phase3(self, force: bool) -> int:
        """
        PHASE 3: VIP Cascade
        
        Responsibility:
        ---------------
        Enriches portfolio with VIP vouchers by matching with settlement details.
        
        Returns:
        --------
        int : Processed documents count
        """
        
        self.logger("\n" + "=" * 80, "INFO")
        self.logger("PHASE 3: VIP CASCADE", "INFO")
        self.logger("=" * 80, "INFO")
        
        # Idempotency check
        today = datetime.now().strftime('%Y%m%d')
        fingerprint = BatchTracker.generate_config_fingerprint({
            'date': today,
            'phase': 3
        })
        
        if not force and self.batch_tracker_p3.should_skip(fingerprint):
            self.logger("PHASE 3 already executed today", "WARN")
            return 0
        
        self.batch_tracker_p3.start_batch(
            fingerprint,
            metadata={'date': today, 'phase': 3}
        )
        
        try:
            # 1. Initialization
            extractor = CustomerPortfolioExtractor(self.engine_raw, self.engine_stg)
            enricher_service = CustomerPortfolioEnricherService(self.config)
            
            # Extract sources for VIP
            df_card_details = extractor.extract_card_details_vip()
            
            with UnitOfWork(self.engine_stg) as uow:
                repo = uow.customer_portfolio
                
                # 2. Loading and Processing
                # Phase 3 only needs VIP-scoped documents (gl_account 1120114035).
                # get_vip_portfolio() loads ~200 rows instead of the full 3,000+
                # that get_active_portfolio() returns. save_portfolio_changes and
                # update_source_tables are both row-scoped (WHERE stg_id = :id
                # and WHERE hash = ANY(:h)) so loading a subset is safe — only
                # the rows in this DataFrame are touched in the DB.
                df_portfolio = extractor.get_vip_portfolio()
                
                # VIP cascade matching
                df_portfolio = enricher_service.enrich_vip_cascade(
                    df_portfolio, df_card_details
                )
                
                df_portfolio = enricher_service.generate_etl_hash(df_portfolio)
                
                # 3. Save Changes
                saved_count = repo.save_portfolio_changes(df_portfolio)
                
                # Feedback loop to source tables
                feedback_count = repo.update_source_tables(df_portfolio)
            
            self.batch_tracker_p3.complete_batch(records_processed=saved_count)
            self.logger(f"PHASE 3 completed: {saved_count} documents", "SUCCESS")
            return saved_count
            
        except Exception as e:
            self.batch_tracker_p3.fail_batch(str(e))
            raise



# ══════════════════════════════════════════════════════════════════════════════
# EJEMPLO DE USO
# ══════════════════════════════════════════════════════════════════════════════

"""
ANTES (PROBLEMA):
────────────────
command = ProcessCustomerPortfolioCommand()
command.execute()

# PHASE1 se ejecuta 
# Marca batch como COMPLETED
# PHASE2 NO se ejecuta (batch ya completed)
# PHASE3 NO se ejecuta (batch ya completed)

Resultado: Portfolio sin enriquecimiento de PARKING y VIP

DESPUÉS (SOLUCIÓN):
──────────────────
command = ProcessCustomerPortfolioCommand()

# Opción 1: Ejecutar todas las fases
command.execute()  # Ejecuta PHASE1, PHASE2, PHASE3

# Opción 2: Ejecutar solo una fase
command.execute(phase=1)  # Solo PHASE1
command.execute(phase=2)  # Solo PHASE2
command.execute(phase=3)  # Solo PHASE3

# Opción 3: Forzar re-ejecución
command.execute(phase=2, force=True)

Resultado: Portfolio completamente enriquecido
"""