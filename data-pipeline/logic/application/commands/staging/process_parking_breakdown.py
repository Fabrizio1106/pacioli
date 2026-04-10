"""
===============================================================================
Project: PACIOLI
Module: logic.application.commands.staging.process_parking_breakdown
===============================================================================

Description:
    Orchestrates the generation and storage of parking breakdown data. It 
    aggregates parking transactions into batches (lotes) for a given accounting 
    period and manages their persistence into the staging area.

Responsibilities:
    - Manage and retrieve active accounting periods for parking breakdown.
    - Generate parking breakdown batches using an extended lookback window.
    - Persist aggregated breakdown data using the Unit of Work pattern.

Key Components:
    - ProcessParkingBreakdownCommand: Main orchestrator for the breakdown pipeline.

Notes:
    - Uses a lookback window for transaction completeness.
    - PostgreSQL migration: Uses explicit schemas and positional result access.

Dependencies:
    - pandas, yaml, sqlalchemy, datetime, pathlib
    - utils.db_config, utils.logger
    - logic.infrastructure.batch_tracker, logic.infrastructure.unit_of_work
    - logic.domain.services.aggregation.parking_breakdown_service
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

from logic.domain.services.aggregation.parking_breakdown_service import ParkingBreakdownService
from logic.domain.services.card_window_calculator import CardWindowCalculator


class ProcessParkingBreakdownCommand:

    def __init__(self):
        # 1. Initialization
        self.logger = get_logger("PARKING_BREAKDOWN_CMD")

        self.engine_stg    = get_db_engine('stg')
        self.engine_config = get_db_engine('config')

        self.config = self._load_config()

        self.batch_tracker = BatchTracker(
            self.engine_config,
            process_name="PARKING_BREAKDOWN_STAGING",
        )

        self._initialize_components()

    def _load_config(self) -> dict:
        try:
            config_path = Path("config/rules/staging_parking_breakdown_rules.yaml")
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger(f"Error loading configuration: {e}", "WARN")
            return {}

    def _initialize_components(self):
        self.service = ParkingBreakdownService(self.engine_stg, self.config)

    def execute(
        self,
        force_start_date: Optional[date] = None,
        force_end_date:   Optional[date] = None,
        force: bool = False,
    ) -> bool:
        """
        Executes the parking breakdown pipeline.
        """
        # 1. Period determination
        start_date, end_date = self._get_period(force_start_date, force_end_date)

        if not start_date:
            self.logger("No active period found", "WARN")
            return False

        self.logger(f"Starting Parking Breakdown: {start_date} -> {end_date}", "INFO")

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
            rows_saved = self._run_pipeline(start_date, end_date)

            self.batch_tracker.complete_batch(records_processed=rows_saved)

            self.logger(f"Process completed: {rows_saved} batches inserted", "SUCCESS")

            self.total_lotes = rows_saved
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
        Retrieves the currently open period for Parking Breakdown Staging.
        """
        try:
            # PostgreSQL migration: explicit schema + positional access
            query = text("""
                SELECT start_date, end_date
                FROM biq_config.etl_periodos_control
                WHERE process_name = 'PARKING_BREAKDOWN_STAGING'
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

    def _run_pipeline(self, start_date: date, end_date: date) -> int:
        """
        Runs the breakdown generation and persistence steps.
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

        # 2. Breakdown Generation
        self.logger("1. Generating PARKING breakdown...", "INFO")

        df_breakdown = self.service.generate_breakdown(extended_start, extended_end)

        if df_breakdown.empty:
            self.logger("No PARKING data found to generate breakdown", "WARN")
            return 0

        self.logger(
            f"Breakdown generated: {len(df_breakdown)} batches "
            f"(window: {extended_start} -> {extended_end})",
            "INFO",
        )

        # 3. Persistence
        self.logger("2. Saving breakdown to database...", "INFO")
        self.logger(f"   DELETE will use accounting period: {start_date} -> {end_date}", "INFO")
        self.logger(f"   INSERT includes batches from: {extended_start}", "INFO")

        with UnitOfWork(self.engine_stg) as uow:
            rows_saved = uow.parking_breakdown.save_breakdown(
                df_breakdown,
                start_date,
                end_date,
            )

        self.logger(
            f"Saved {rows_saved} batches for accounting period "
            f"{start_date} -> {end_date}",
            "SUCCESS",
        )

        return rows_saved



