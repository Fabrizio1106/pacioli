"""
===============================================================================
Project: PACIOLI
Module: logic.application.commands.staging.process_diners_staging
===============================================================================

Description:
    Orchestrates the staging process for Diners Club card data. This includes 
    extraction from raw sources, transformation, aggregation into settlements 
    and detailed records, and final persistence into the staging area.

Responsibilities:
    - Manage and retrieve active accounting periods for Diners staging.
    - Extract raw Diners Club data using an extended lookback window.
    - Transform raw data and aggregate into settlements and details.
    - Persist processed records using the Unit of Work pattern.

Key Components:
    - ProcessDinersCommand: Main orchestrator for the Diners staging pipeline.

Notes:
    - Uses a dynamic lookback window to ensure data completeness.
    - PostgreSQL migration: Uses explicit schemas and positional result access.

Dependencies:
    - pandas, yaml, sqlalchemy, datetime
    - logic.infrastructure.batch_tracker, logic.infrastructure.unit_of_work
    - logic.infrastructure.extractors.diners_extractor
    - logic.domain.services.transformation.diners_transformer
    - logic.domain.services.aggregation.card_aggregator
    - logic.domain.services.hashing.historical_context_service
    - logic.domain.services.card_window_calculator

===============================================================================
"""

import pandas as pd
from datetime import date
from typing import Optional, Tuple
import yaml
from pathlib import Path
from sqlalchemy import text

from utils.db_config import get_db_engine
from utils.logger import get_logger
from logic.infrastructure.batch_tracker import BatchTracker
from logic.infrastructure.unit_of_work import UnitOfWork

from logic.infrastructure.extractors.diners_extractor import DinersExtractor
from logic.domain.services.transformation.diners_transformer import DinersTransformer
from logic.domain.services.aggregation.card_aggregator import CardAggregator
from logic.domain.services.hashing.historical_context_service import HistoricalContextService
from logic.domain.services.card_window_calculator import CardWindowCalculator


class ProcessDinersCommand:

    def __init__(self):
        # 1. Initialization
        self.logger = get_logger("DINERS_CMD")

        self.engine_raw    = get_db_engine('raw')
        self.engine_stg    = get_db_engine('stg')
        self.engine_config = get_db_engine('config')

        self.config = self._load_config()

        self.batch_tracker = BatchTracker(
            self.engine_config,
            process_name="DINERS_STAGING",
        )

        self._initialize_components()

    def _load_config(self) -> dict:
        try:
            config_path = Path("config/rules/staging_diners_rules.yaml")
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger(f"Error loading configuration: {e}", "WARN")
            return {}

    def _initialize_components(self):
        self.extractor       = DinersExtractor(self.engine_raw)
        self.transformer     = DinersTransformer(self.config)
        self.context_service = HistoricalContextService()
        self.aggregator      = CardAggregator(context_service=self.context_service)

    def execute(
        self,
        force_start_date: Optional[date] = None,
        force_end_date:   Optional[date] = None,
        force: bool = False,
    ) -> bool:
        """
        Executes the Diners staging pipeline.
        """
        # 1. Period determination
        start_date, end_date = self._get_period(force_start_date, force_end_date)

        if not start_date:
            self.logger("No active period found", "WARN")
            return False

        self.logger(f"Starting Diners Staging: {start_date} -> {end_date}", "INFO")

        # 2. Idempotency check
        fingerprint = self.batch_tracker.generate_config_fingerprint({
            "start": str(start_date),
            "end":   str(end_date),
            "version": "2.4_extended_window",
        })

        if not force and self.batch_tracker.should_skip(fingerprint):
            self.logger("Batch already processed (idempotency)", "WARN")
            return True

        batch_id = self.batch_tracker.start_batch(
            fingerprint,
            metadata={"range": f"{start_date}-{end_date}"},
        )
        self.logger.set_batch_id(batch_id)

        try:
            # 3. Pipeline execution
            settlements_saved, details_saved = self._run_pipeline(start_date, end_date)

            self.batch_tracker.complete_batch(
                records_processed=settlements_saved + details_saved
            )

            self.logger(
                f"Process completed: {settlements_saved} settlements, {details_saved} details",
                "SUCCESS",
            )

            self.total_settlements = settlements_saved
            self.total_details     = details_saved
            return True

        except Exception as e:
            self.logger(f"Error in process: {e}", "ERROR")
            self.batch_tracker.fail_batch(error_message=str(e))
            raise

    def _get_period(self, force_start, force_end):
        if force_start and force_end:
            return (force_start, force_end)
        return self._get_active_period()

    def _get_active_period(self) -> Tuple[Optional[date], Optional[date]]:
        """
        Retrieves the currently open period for Diners Staging.
        """
        try:
            # PostgreSQL migration: explicit schema biq_config
            query = text("""
                SELECT start_date, end_date
                FROM biq_config.etl_periodos_control
                WHERE process_name = 'DINERS_STAGING'
                  AND status = 'ABIERTO'
                LIMIT 1
            """)

            with self.engine_config.connect() as conn:
                result = conn.execute(query).fetchone()

                if result:
                    # Positional access [0], [1] for robustness
                    return (result[0], result[1])
                return (None, None)

        except Exception as e:
            self.logger(f"Error querying period: {e}", "WARN")
            return (None, None)

    def _run_pipeline(self, start_date: date, end_date: date) -> Tuple[int, int]:
        """
        Runs the extraction, transformation, and aggregation steps.
        """
        # 1. Window Calculation
        extended_start, extended_end = CardWindowCalculator.calculate_window(
            start_date, end_date
        )

        self.logger(f"Accounting period: {start_date} -> {end_date}", "INFO")
        self.logger(
            f"Extended window: {extended_start} -> {extended_end} "
            f"(+{CardWindowCalculator.LOOKBACK_DAYS} days lookback)",
            "INFO",
        )

        # 2. Extraction
        self.logger("1. Extracting raw data...", "INFO")
        df_raw = self.extractor.extract(extended_start, extended_end)

        if df_raw.empty:
            self.logger("No data found in biq_raw.raw_diners_club", "WARN")
            return (0, 0)

        self.logger(
            f"Extracted {len(df_raw)} Diners vouchers (window: {extended_start} -> {extended_end})",
            "SUCCESS",
        )

        # 3. Transformation
        self.logger("2. Transforming data...", "INFO")
        df_clean = self.transformer.transform(df_raw)

        if df_clean.empty:
            self.logger("No data found after filtering", "WARN")
            return (0, 0)

        self.logger(f"Transformation: {len(df_clean)} vouchers", "SUCCESS")

        # 4. Aggregation
        self.logger("3. Aggregating settlements...", "INFO")
        df_settlements, df_details = self.aggregator.aggregate(
            df_vouchers=df_clean, brand='DINERS CLUB'
        )

        self.logger(
            f"Aggregation: {len(df_settlements)} settlements, {len(df_details)} details",
            "SUCCESS",
        )

        # 5. Persistence
        self.logger("4. Saving to database...", "INFO")
        self.logger(f"   DELETE will use accounting period: {start_date} -> {end_date}", "INFO")
        self.logger(f"   INSERT includes vouchers from: {extended_start}", "INFO")

        with UnitOfWork(self.engine_stg) as uow:
            settlements_saved = uow.cards.save_settlements(df_settlements)
            details_saved     = uow.cards.save_details(df_details)

        self.logger(
            f"Saved: {settlements_saved} settlements, {details_saved} details",
            "SUCCESS",
        )

        return (settlements_saved, details_saved)



