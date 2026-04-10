"""
===============================================================================
Project: PACIOLI
Module: logic.application.commands.staging.process_bank_enrichment
===============================================================================

Description:
    Orchestration command for bank transaction enrichment. It manages six 
    phases of enrichment for staging bank transactions, including manual 
    requests, card-based matching, text rules, heuristics, and settlements.

Responsibilities:
    - Orchestrate the sequential execution of various enrichment phases.
    - Initialize and configure specialized enrichers for each phase.
    - Manage idempotency using BatchTracker to avoid redundant processing.
    - Universalize settlement IDs for non-credit card transactions.

Key Components:
    - ProcessBankEnrichmentCommand: Main orchestrator for the enrichment pipeline.

Notes:
    - Does not insert new rows; only updates existing transactions in stg_bank_transactions.
    - Order of phases is critical for data integrity and priority handling.

Dependencies:
    - logic.infrastructure.batch_tracker
    - logic.infrastructure.unit_of_work
    - logic.domain.services.enrichment.*

===============================================================================
"""

import yaml
from pathlib import Path
from datetime import datetime

# Infrastructure
from utils.db_config import get_db_engine
from utils.logger import get_logger
from logic.infrastructure.batch_tracker import BatchTracker
from logic.infrastructure.unit_of_work import UnitOfWork

# Enrichers (imported as they are created)
from logic.domain.services.enrichment.manual_request_enricher import ManualRequestEnricher
from logic.domain.services.enrichment.card_enricher import CardEnricher
from logic.domain.services.enrichment.specific_text_enricher import SpecificTextEnricher
from logic.domain.services.enrichment.smart_heuristic_enricher import SmartHeuristicEnricher
from logic.domain.services.enrichment.cash_deposit_enricher import CashDepositEnricher
from logic.domain.services.enrichment.settlement_enricher import SettlementEnricher


class ProcessBankEnrichmentCommand:
    """
    Command for bank enrichment.

    PATTERN: Command (Application Layer)

    RESPONSIBILITY:
    ---------------
    Orchestrate the 6 enrichment phases for bank transactions.

    FEATURES:
    ---------------
    ✅ Idempotent (BatchTracker)
    ✅ Transactional (UnitOfWork)
    ✅ Complete logging for each phase
    ✅ UPDATE-ONLY (no new rows inserted)
    ✅ Phase order preserved

    IMPORTANT NOTE:
    ---------------
    This command uses UnitOfWork in a SPECIAL way.
    Each phase performs its own commits to avoid
    very long transactions with massive UPDATES.
    """

    def __init__(self):
        self.logger = get_logger("BANK_ENRICHMENT_CMD")

        # Engines
        self.engine_stg = get_db_engine('stg')
        self.engine_raw = get_db_engine('raw')
        self.engine_config = get_db_engine('config')

        # Configuration
        self.config_enrich = self._load_config(
            "config/rules/staging_bank_enrichment_rules.yaml"
        )
        self.config_recon = self._load_config(
            "config/rules/staging_bank_reconciliation_rules.yaml"
        )

        # BatchTracker
        self.batch_tracker = BatchTracker(
            self.engine_config,
            process_name="BANK_ENRICHMENT_STAGING"
        )

        # Initialize Enrichers
        self._initialize_enrichers()

    def _load_config(self, path: str) -> dict:
        """Safely loads YAML configuration."""
        try:
            with open(Path(path), 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            self.logger(f"Error loading {path}: {str(e)}", "WARN")
            return {}

    def _initialize_enrichers(self):
        """
        Initializes the 6 enrichers.

        Each enricher receives:
        - engine_stg: To read and perform UPDATES
        - engine_raw: For additional sources
        - config: Its specific configuration
        """

        self.manual_enricher = ManualRequestEnricher(
            engine_stg=self.engine_stg,
            config=self.config_enrich
        )

        self.card_enricher = CardEnricher(
            engine_stg=self.engine_stg,
            config=self.config_enrich
        )

        self.text_enricher = SpecificTextEnricher(
            engine_stg=self.engine_stg,
            config=self.config_enrich
        )

        self.heuristic_enricher = SmartHeuristicEnricher(
            engine_stg=self.engine_stg,
            engine_raw=self.engine_raw,
            config=self.config_enrich
        )

        self.cash_enricher = CashDepositEnricher(
            engine_stg=self.engine_stg,
            config=self.config_enrich
        )

        self.settlement_enricher = SettlementEnricher(
            engine_stg=self.engine_stg,
            config=self.config_recon
        )

    def execute(self, force: bool = False, **kwargs) -> bool:
        """
        Executes the enrichment command.

        Parameters:
        -----------
        force : bool
            If True, ignores idempotency verification

        Returns:
        --------
        bool : True if successful
        """

        self.logger(
            "Starting Bank Enrichment (6 phases)",
            "INFO"
        )

        # 1. Idempotency check
        today = datetime.now().strftime('%Y%m%d')

        fingerprint = self.batch_tracker.generate_config_fingerprint({
            "date": today,
            "version": "2.0_clean_arch"
        })

        if not force and self.batch_tracker.should_skip(fingerprint):
            self.logger("Batch already processed today", "WARN")
            return True

        # 2. Batch initialization
        batch_id = self.batch_tracker.start_batch(
            fingerprint,
            metadata={"date": today, "phases": 6}
        )
        self.logger.set_batch_id(batch_id)

        try:
            # 3. Pipeline execution
            total_enriched = self._run_pipeline()

            # 4. Batch completion
            self.batch_tracker.complete_batch(
                records_processed=total_enriched
            )

            self.logger(
                f"Process completed: {total_enriched} transactions enriched",
                "SUCCESS"
            )
            self.total_enriched = total_enriched
            return True

        except Exception as e:
            self.logger(f"Process error: {str(e)}", "ERROR")
            self.batch_tracker.fail_batch(error_message=str(e))
            raise

    def _run_pipeline(self) -> int:
        """
        Executes the 6 phases in the correct order.

        CRITICAL ORDER:
        ─────────────────────────────────────────────────────────
        PHASE 0: Manual (100% confidence - priority maximum)
        PHASE 1: Cards (deterministic by brand)
        PHASE 2: Specific Text (YAML rules)
        PHASE 3: Smart Heuristic (rapidfuzz)
        PHASE 4: Cash (temporal patterns)
        PHASE 5: Settlements (shadow + split batch)
        PHASE 6: Universalize settlement_id

        Each phase works on those NOT processed by the previous one.

        Returns:
        --------
        int : Total enriched transactions
        """

        total = 0
        separator = "─" * 60

        # 1. Phase 0: Manual Requests
        self.logger(f"\n{separator}", "INFO")
        self.logger("PHASE 0: Manual Requests (Maximum Priority)", "INFO")
        self.logger(separator, "INFO")

        count_0 = self.manual_enricher.enrich(self.engine_stg)
        total += count_0
        self.logger(f"   → {count_0} transactions enriched", "SUCCESS")

        # 2. Phase 1: Cards
        self.logger(f"\n{separator}", "INFO")
        self.logger("PHASE 1: Cards (Deterministic by Brand)", "INFO")
        self.logger(separator, "INFO")

        count_1 = self.card_enricher.enrich(self.engine_stg)
        total += count_1
        self.logger(f"   → {count_1} transactions enriched", "SUCCESS")

        # 3. Phase 2: Specific Text
        self.logger(f"\n{separator}", "INFO")
        self.logger("PHASE 2: Specific Text (YAML Rules)", "INFO")
        self.logger(separator, "INFO")

        count_2 = self.text_enricher.enrich(self.engine_stg)
        total += count_2
        self.logger(f"   → {count_2} transactions enriched", "SUCCESS")

        # 4. Phase 3: Smart Heuristic
        self.logger(f"\n{separator}", "INFO")
        self.logger("PHASE 3: Smart Heuristic (Fuzzy Matching)", "INFO")
        self.logger(separator, "INFO")

        count_3 = self.heuristic_enricher.enrich(self.engine_stg)
        total += count_3
        self.logger(f"   → {count_3} transactions enriched", "SUCCESS")

        # 5. Phase 4: Cash Deposits
        self.logger(f"\n{separator}", "INFO")
        self.logger("PHASE 4: Cash Deposits (Parking/VIP Lounges)", "INFO")
        self.logger(separator, "INFO")

        count_4 = self.cash_enricher.enrich(self.engine_stg)
        total += count_4
        self.logger(f"   → {count_4} transactions enriched", "SUCCESS")

        # 6. Phase 5: Settlements
        self.logger(f"\n{separator}", "INFO")
        self.logger("PHASE 5: Settlements (Shadow Match + Split Batch)", "INFO")
        self.logger(separator, "INFO")

        count_5 = self.settlement_enricher.enrich(self.engine_stg)
        total += count_5
        self.logger(f"   → {count_5} transactions enriched", "SUCCESS")

        # 7. Phase 6: Universalize IDs
        self.logger(f"\n{separator}", "INFO")
        self.logger("PHASE 6: Universalize Settlement IDs", "INFO")
        self.logger(separator, "INFO")

        count_6 = self._universalize_settlement_ids()
        self.logger(f"   → {count_6} settlement IDs universalized", "INFO")

        # 8. Summary
        self.logger(f"\n{'═' * 60}", "INFO")
        self.logger("ENRICHMENT SUMMARY", "INFO")
        self.logger('═' * 60, "INFO")
        self.logger(f"   Phase 0 (Manual):       {count_0}", "INFO")
        self.logger(f"   Phase 1 (Cards):        {count_1}", "INFO")
        self.logger(f"   Phase 2 (Text):         {count_2}", "INFO")
        self.logger(f"   Phase 3 (Heuristic):    {count_3}", "INFO")
        self.logger(f"   Phase 4 (Cash):         {count_4}", "INFO")
        self.logger(f"   Phase 5 (Settlements):  {count_5}", "INFO")
        self.logger(f"   Phase 6 (Settlement ID):{count_6}", "INFO")
        self.logger(f"   ─────────────────────────────", "INFO")
        self.logger(f"   TOTAL ENRICHED:         {total}", "SUCCESS")
        self.logger('═' * 60, "INFO")

        return total

    def _universalize_settlement_ids(self) -> int:
        """
        Phase 6: Copies bank_ref_1 → settlement_id
        for everything that is NOT a credit card.

        Executed directly here because it doesn't need
        a complex Enricher; it's a simple UPDATE.
        """
        # 1. Configuration retrieval
        trans_type_tc = self.config_recon.get(
            'target_trans_type', 'LIQUIDACION TC'
        )

        # 2. Update execution
        with UnitOfWork(self.engine_stg) as uow:
            count = uow.bank_enrichment.update_settlement_id_universal(
                trans_type_tc=trans_type_tc
            )

        return count
