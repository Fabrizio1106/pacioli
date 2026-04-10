"""
===============================================================================
Project: PACIOLI
Module: logic.application.commands.staging.process_sap_staging
===============================================================================

Description:
    Orchestrates the staging process for SAP and bank transactions. It handles
    extraction, transformation, enrichment with bank data, classification,
    compensation detection, and hash generation with historical context.

Responsibilities:
    - Manage and retrieve active accounting periods for SAP staging.
    - Extract raw SAP and bank transaction data.
    - Enrich SAP records with bank details and classify transactions.
    - Generate unique match hashes using historical context and caching.
    - Detect intraday compensations and handle SAP compensation logic.
    - Propagate reconciliation statuses and synchronize reconciliation reasons.

Key Components:
    - ProcessSAPStagingCommand: Main orchestrator for the SAP staging pipeline.

Notes:
    - Version 4.1:
      FIX #2: `complete_batch` is called WITHOUT passing batch_id as a
              positional argument. BatchTracker stores the current batch ID
              internally in `self.current_batch_id`. Passing it again as a
              positional arg caused "multiple values for argument records_processed"
              because `complete_batch(batch_id, records_processed=0)` was sending
              batch_id into the first positional param (records_processed) while
              also supplying records_processed as a keyword.
              Correct call: `self.batch_tracker.complete_batch(records_processed=0)`

Dependencies:
    - pandas, yaml, pathlib, sqlalchemy, datetime
    - utils.db_config, utils.logger
    - logic.infrastructure.batch_tracker, logic.infrastructure.unit_of_work
    - logic.infrastructure.extractors, logic.domain.services

===============================================================================
"""

import pandas as pd
from datetime import datetime, date
import yaml
from pathlib import Path
from sqlalchemy import text

from utils.db_config import get_db_engine
from utils.logger import get_logger
from logic.infrastructure.batch_tracker import BatchTracker
from logic.infrastructure.unit_of_work import UnitOfWork

from logic.infrastructure.extractors import SAPExtractor, BankExtractor
from logic.domain.services.transformation.sap_transformer import SAPTransformer
from logic.domain.services.compensation.intraday_compensation_detector import IntradayCompensationDetector
from logic.domain.services.enrichment.bank_enricher import BankEnricher
from logic.domain.services.compensation.sap_compensation_handler import SAPCompensationHandler
from logic.domain.services.classification.transaction_classifier import TransactionClassifier
from logic.domain.services.hashing.hash_generator import HashGenerator
from logic.domain.services.hashing.historical_context_service import HistoricalContextService
from logic.domain.services.reconciliation_propagation_service import ReconciliationPropagationService
from logic.domain.services.reconciliation_reason_synchronizer import ReconciliationReasonSynchronizer
from logic.domain.services.hash_counter_cache_manager import HashCounterCacheManager


class ProcessSAPStagingCommand:

    def __init__(self):
        self.logger = get_logger("SAP_CMD_V4")

        self.engine_raw    = get_db_engine('raw')
        self.engine_stg    = get_db_engine('stg')
        self.engine_config = get_db_engine('config')

        self.config        = self._load_config()
        self.batch_tracker = BatchTracker(self.engine_config, process_name="SAP_STAGING")

        self.sap_extractor  = SAPExtractor(self.engine_raw)
        self.bank_extractor = BankExtractor(self.engine_raw)

        mapping_rules = self.config.get('sql_mapping', {})
        if not mapping_rules:
            self.logger("No 'sql_mapping' found in YAML configuration", "WARN")

        self.transformer     = SAPTransformer(column_mapping=mapping_rules)
        self.history_service = HistoricalContextService()
        self.hash_generator  = HashGenerator(context_service=self.history_service)

        self.intraday_detector = IntradayCompensationDetector()
        self.enricher          = BankEnricher()
        self.sap_handler       = SAPCompensationHandler()

        tagging_rules   = self.config.get('tagging_rules', [])
        self.classifier = TransactionClassifier(tagging_rules)

        self.propagation_service = ReconciliationPropagationService()
        self.reason_sync         = ReconciliationReasonSynchronizer()

    def execute(self, force_start_date=None, force_end_date=None, force=False) -> bool:
        """
        Executes the SAP staging pipeline.
        """
        # ── 1. Period determination ───────────────────────────────────────────
        if force_start_date and force_end_date:
            start_date, end_date = force_start_date, force_end_date
        else:
            start_date, end_date = self._get_active_period()
            if not start_date:
                self.logger("No OPEN period found.", "WARN")
                return False

        self.logger(f"Starting SAP Pipeline V4: {start_date} to {end_date}", "INFO")

        # ── 2. Idempotency check ──────────────────────────────────────────────
        fingerprint = self.batch_tracker.generate_config_fingerprint({
            "start": str(start_date),
            "end":   str(end_date),
            "v":     "4.0_cache",
        })

        if not force and self.batch_tracker.should_skip(fingerprint):
            self.logger("Batch already processed.", "WARN")
            return True

        batch_id = self.batch_tracker.start_batch(
            fingerprint,
            metadata={"range": f"{start_date}-{end_date}"},
        )
        self.logger.set_batch_id(batch_id)

        try:
            # ── 3. Extraction ─────────────────────────────────────────────────
            self.logger("1. Extracting data...", "INFO")
            df_sap  = self.sap_extractor.extract(start_date, end_date)
            df_bank = self.bank_extractor.extract(start_date, end_date)

            if df_sap.empty:
                self.logger("No SAP data found.", "WARN")
                # FIX #2: complete_batch does NOT receive batch_id as a positional
                # argument. BatchTracker already holds it in self.current_batch_id.
                # Previous broken call: complete_batch(batch_id, records_processed=0)
                # → "multiple values for argument 'records_processed'"
                self.batch_tracker.complete_batch(records_processed=0)
                return True

            # ── 4. Processing and Enrichment ──────────────────────────────────
            self.logger("2. Processing transactions...", "INFO")

            df = self.transformer.transform(df_sap)
            df = self.enricher.enrich(df, df_bank)
            df = self.classifier.classify(df)

            with UnitOfWork(self.engine_stg) as uow:
                self.history_service.session = uow.session

                mask_banco = df['doc_type'] == 'ZR'

                if mask_banco.any():
                    df_banco = self.history_service.build_context(
                        df[mask_banco].copy(),
                        start_date,
                        end_date,
                        use_cache=True,
                    )

                    if self.history_service.was_cache_used():
                        self.logger("Historical context loaded from cache", "SUCCESS")
                    else:
                        self.logger("Cache unavailable, using direct query", "WARN")

                    df_banco = self.hash_generator.generate(df_banco)

                    df.loc[mask_banco, 'match_hash_key']     = df_banco['match_hash_key']
                    df.loc[mask_banco, '_historical_counter'] = df_banco['_historical_counter']

                    validation = self.history_service.validate_continuity(df_banco)
                    if not validation['is_continuous']:
                        self.logger(
                            f"Sequence gaps: {len(validation['gaps'])} detected", "WARN"
                        )
                else:
                    self.logger("No ZR transactions found to process", "WARN")

                df.loc[~mask_banco, 'match_hash_key'] = None

            df = self.intraday_detector.detect(df)
            df = self.sap_handler.handle(df)

            # ── 5. Persistence ────────────────────────────────────────────────
            self.logger("3. Saving records...", "INFO")

            with UnitOfWork(self.engine_stg) as uow:
                saved_count = uow.bank_transactions.save_with_preservation(
                    df=df,
                    start_date=start_date,
                    end_date=end_date,
                )

                # ── 6. Cache Management ───────────────────────────────────────
                self.logger("4. Updating counter cache...", "INFO")

                cache_mgr    = HashCounterCacheManager(uow.session)
                cache_result = cache_mgr.update_cache_for_period(
                    start_date=start_date,
                    end_date=end_date,
                )
                self.logger(
                    f"   Cache updated: {cache_result['groups_updated']} groups",
                    "SUCCESS",
                )

                # ── 7. Post-processing ────────────────────────────────────────
                self.logger("5. Post-processing...", "INFO")

                self.reason_sync.session         = uow.session
                self.propagation_service.session = uow.session

                reason_result = self.reason_sync.sync_reasons()
                self.logger(
                    f"   Reasons: {reason_result['sap_synced']} SAP, "
                    f"{reason_result['intraday_synced']} intraday",
                    "INFO",
                )

                propagation_result = self.propagation_service.propagate_closures()
                self.logger(
                    f"   Propagation: {propagation_result['settlements_closed']} settlements, "
                    f"{propagation_result['details_closed']} details",
                    "INFO",
                )

            # FIX #2: same correction — no batch_id passed positionally
            self.batch_tracker.complete_batch(records_processed=saved_count)

            self.stats = {'total': saved_count}

            self.logger("=" * 80, "SUCCESS")
            self.logger(f"Pipeline completed. Records: {saved_count}", "SUCCESS")
            self.logger(
                f"   - Cache: {cache_result['groups_updated']} groups updated", "SUCCESS"
            )
            self.logger(
                f"   - Reasons synced: "
                f"{reason_result['sap_synced'] + reason_result['intraday_synced']}",
                "SUCCESS",
            )
            self.logger(
                f"   - Propagation: "
                f"{propagation_result['settlements_closed'] + propagation_result['details_closed']}",
                "SUCCESS",
            )
            self.logger("=" * 80, "SUCCESS")

            return True

        except Exception as e:
            self.logger(f"Error: {e}", "ERROR")
            if batch_id:
                self.batch_tracker.fail_batch(error_message=str(e))
            raise

    def _get_active_period(self):
        """Retrieves OPEN period from biq_config.etl_periodos_control."""
        try:
            query = text("""
                SELECT start_date, end_date
                FROM biq_config.etl_periodos_control
                WHERE process_name = 'SAP_STAGING'
                  AND status = 'ABIERTO'
                LIMIT 1
            """)
            with self.engine_config.connect() as conn:
                result = conn.execute(query).fetchone()
                if result:
                    return result[0], result[1]
            return None, None
        except Exception:
            return None, None

    def _load_config(self) -> dict:
        try:
            config_path = Path("config/rules/staging_sap_rules.yaml")
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception:
            return {}