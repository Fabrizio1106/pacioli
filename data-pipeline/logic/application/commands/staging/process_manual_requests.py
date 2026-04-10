"""
===============================================================================
Project: PACIOLI
Module: logic.application.commands.staging.process_manual_requests
===============================================================================

Description:
    Orchestrates the processing of manual requests. Unlike standard card staging, 
    it processes all records (no date filtering) using a TRUNCATE + INSERT 
    strategy and performs reference explosion for multiple entries.

Responsibilities:
    - Orchestrate the full manual requests processing pipeline.
    - Extract all manual requests from raw sources without date filters.
    - Transform records, including reference explosion logic.
    - Persist results using a full replacement (TRUNCATE + INSERT) strategy.

Key Components:
    - ProcessManualRequestsCommand: Command class for manual requests orchestration.

Notes:
    - Processes the entire dataset every time (no incremental loading).
    - Uses TRUNCATE + INSERT for persistence instead of append-only.
    - Reference explosion handles multiple references within a single request.

Dependencies:
    - pandas, yaml, pathlib, datetime
    - utils.db_config, utils.logger
    - logic.infrastructure.batch_tracker, logic.infrastructure.unit_of_work
    - logic.infrastructure.extractors.manual_requests_extractor
    - logic.domain.services.transformation.manual_requests_transformer

===============================================================================
"""

import pandas as pd
from datetime import date
from typing import Optional
import yaml
from pathlib import Path

# Infrastructure
from utils.db_config import get_db_engine
from utils.logger import get_logger
from logic.infrastructure.batch_tracker import BatchTracker
from logic.infrastructure.unit_of_work import UnitOfWork

# Extractors
from logic.infrastructure.extractors.manual_requests_extractor import ManualRequestsExtractor

# Domain Services
from logic.domain.services.transformation.manual_requests_transformer import ManualRequestsTransformer


class ProcessManualRequestsCommand:
    """
    Command to process manual requests.
    
    Responsibilities:
    -----------------
    Orchestrates manual request processing including full extraction, 
    reference explosion, and full table replacement.
    """
    
    def __init__(self):
        # 1. Initialization
        self.logger = get_logger("MANUAL_REQUESTS_CMD")
        
        # Engines
        self.engine_raw = get_db_engine('raw')
        self.engine_stg = get_db_engine('stg')
        self.engine_config = get_db_engine('config')
        
        # Config
        self.config = self._load_config()
        
        # Batch Tracker
        self.batch_tracker = BatchTracker(
            self.engine_config,
            process_name="MANUAL_REQUESTS_STAGING"
        )
        
        # Components
        self._initialize_components()
    
    def _load_config(self) -> dict:
        """Loads configuration from YAML."""
        try:
            config_path = Path("config/rules/staging_manual_requests_rules.yaml")
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger(
                f"Error loading configuration: {str(e)}",
                "WARN"
            )
            return {}
    
    def _initialize_components(self):
        """Initializes pipeline components."""
        self.extractor = ManualRequestsExtractor(self.engine_raw)
        self.transformer = ManualRequestsTransformer(self.config)
    
    def execute(self, force: bool = False, **kwargs) -> bool:
        """
        Executes the command.
        
        Note: This command does not take date parameters as it always 
        processes the entire dataset.
        
        Returns:
            bool: True if successful.
        """
        
        self.logger(
            "Starting Manual Requests Staging",
            "INFO"
        )
        
        # 2. Idempotency Check
        fingerprint = self.batch_tracker.generate_config_fingerprint({
            "mode": "full_replace",
            "version": "2.0_clean_arch"
        })
        
        if not force and self.batch_tracker.should_skip(fingerprint):
            self.logger("Batch already processed (idempotency)", "WARN")
            return True
        
        # 3. Start Batch
        batch_id = self.batch_tracker.start_batch(
            fingerprint,
            metadata={"mode": "full_replace"}
        )
        self.logger.set_batch_id(batch_id)

        try:
            # 4. Pipeline Execution
            rows_saved = self._run_pipeline()
            
            # 5. Complete Batch
            self.batch_tracker.complete_batch(
                records_processed=rows_saved
            )
            
            self.logger(
                f"Process completed: {rows_saved} requests processed",
                "SUCCESS"
            )
            
            # Metrics tracking for orchestrator
            self.total_requests = rows_saved
            return True
            
        except Exception as e:
            self.logger(
                f"Error in process: {str(e)}",
                "ERROR"
            )
            
            self.batch_tracker.fail_batch(error_message=str(e))
            raise
    
    def _run_pipeline(self) -> int:
        """
        Executes the full processing pipeline.
        
        Returns:
            int: Number of records saved.
        """
        
        # Phase 1: Full Extraction
        self.logger("1. Extracting manual requests...", "INFO")
        
        df_raw = self.extractor.extract_all()
        
        if df_raw.empty:
            self.logger(
                "No requests found in biq_raw.raw_manual_requests",
                "WARN"
            )
            return 0
        
        # Phase 2: Transformation (with reference explosion)
        self.logger("2. Transforming (with reference explosion)...", "INFO")
        
        df_clean = self.transformer.transform(df_raw)
        
        if df_clean.empty:
            self.logger(
                "No data found after transformation",
                "WARN"
            )
            return 0
        
        # Phase 3: Persistence (TRUNCATE + INSERT)
        self.logger("3. Saving (TRUNCATE + INSERT)...", "INFO")
        
        with UnitOfWork(self.engine_stg) as uow:
            rows_saved = uow.manual_requests.replace_all(df_clean)
        
        return rows_saved
