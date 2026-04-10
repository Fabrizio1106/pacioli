"""
===============================================================================
Project: PACIOLI
Module: logic.application.commands.staging.process_withholdings
===============================================================================

Description:
    Command to process withholdings from the raw layer to the staging layer.
    It handles extraction, transformation, and persistence of withholding data.

Responsibilities:
    - Load and manage configuration for withholdings processing.
    - Track batch execution and ensure idempotency.
    - Orchestrate the extraction, transformation, and saving pipeline.

Key Components:
    - ProcessWithholdingsCommand: Main orchestrator for the withholdings staging process.

Notes:
    - Uses BatchTracker for process monitoring.
    - Delegates data persistence to the WithholdingsRepository via UnitOfWork.

Dependencies:
    - yaml, pathlib
    - utils.db_config, utils.logger
    - logic.infrastructure.batch_tracker
    - logic.infrastructure.unit_of_work
    - logic.infrastructure.extractors.withholdings_extractor
    - logic.domain.services.transformation.withholdings_transformer
===============================================================================
"""

from datetime import datetime
from typing import Optional
import yaml
from pathlib import Path

from utils.db_config import get_db_engine
from utils.logger import get_logger
from logic.infrastructure.batch_tracker import BatchTracker
from logic.infrastructure.unit_of_work import UnitOfWork
from logic.infrastructure.extractors.withholdings_extractor import WithholdingsExtractor
from logic.domain.services.transformation.withholdings_transformer import WithholdingsTransformer


class ProcessWithholdingsCommand:

    def __init__(self):
        # 1. Initialization
        self.logger = get_logger("WITHHOLDINGS_CMD")

        self.engine_raw    = get_db_engine('raw')
        self.engine_stg    = get_db_engine('stg')
        self.engine_config = get_db_engine('config')

        self.config = self._load_config()

        self.batch_tracker = BatchTracker(
            self.engine_config,
            process_name="WITHHOLDINGS_STAGING",
        )

        self._initialize_components()

    def _load_config(self) -> dict:
        try:
            config_path = Path("config/rules/staging_withholdings_rules.yaml")
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger(f"Error loading configuration: {e}. Using defaults.", "WARN")
            return {}

    def _initialize_components(self):
        self.lookback_days = self.config.get('extraction', {}).get('lookback_days', 90)
        self.extractor     = WithholdingsExtractor(self.engine_raw, self.engine_stg)
        self.transformer   = WithholdingsTransformer(self.config)

    def execute(self, force: bool = False) -> bool:
        # 2. Execution logic
        self.logger(
            f"Starting Withholdings Staging PHASE 1 (lookback: {self.lookback_days} days)",
            "INFO",
        )

        today       = datetime.now().strftime('%Y%m%d')
        fingerprint = self.batch_tracker.generate_config_fingerprint({
            "date":         today,
            "lookback_days": self.lookback_days,
            "version":      "2.0_clean_arch",
        })

        if not force and self.batch_tracker.should_skip(fingerprint):
            self.logger("Batch already processed today (idempotency)", "WARN")
            return True

        batch_id = self.batch_tracker.start_batch(
            fingerprint,
            metadata={"lookback_days": self.lookback_days, "date": today},
        )
        self.logger.set_batch_id(batch_id)

        try:
            stats = self._run_pipeline()

            self.batch_tracker.complete_batch(records_processed=stats['inserted'])

            self.logger(
                f"Process completed: {stats['inserted']} withholdings inserted",
                "SUCCESS",
            )

            if stats['total'] == 0:
                self.logger("No new withholdings to process", "INFO")

            self.records_processed = stats['inserted']
            self.stats = stats
            return True

        except Exception as e:
            self.logger(f"Error in process: {e}", "ERROR")
            self.batch_tracker.fail_batch(error_message=str(e))
            raise

    def _run_pipeline(self) -> dict:
        # 3. Pipeline Run phases

        # Phase 1: Extraction
        self.logger(
            f"Extracting new withholdings ({self.lookback_days} days)...",
            "INFO",
        )

        df_raw = self.extractor.extract_new_withholdings(lookback_days=self.lookback_days)

        if df_raw.empty:
            self.logger("No new withholdings to process", "INFO")
            return {'inserted': 0, 'duplicates': 0, 'errors': 0, 'total': 0}

        # Phase 2: Transformation
        self.logger(f"Transforming {len(df_raw)} withholdings...", "INFO")

        df_clean = self.transformer.transform(df_raw)

        if df_clean.empty:
            self.logger("No data after transformation", "WARN")
            return {'inserted': 0, 'duplicates': 0, 'errors': 0, 'total': 0}

        # Phase 3: Persistence
        # PG CHANGE: updated log — table is now biq_stg.stg_withholdings
        self.logger("Saving to biq_stg.stg_withholdings...", "INFO")

        with UnitOfWork(self.engine_stg) as uow:
            stats = uow.withholdings.save_withholdings(df_clean)

        return stats
