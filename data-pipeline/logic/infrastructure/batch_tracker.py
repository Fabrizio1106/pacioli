"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.batch_tracker
===============================================================================

Description:
    Manages the execution lifecycle of ETL batches, providing idempotency, 
    tracking status, and logging progress in the database. It acts as an 
    audit trail for all data pipeline processes.

Responsibilities:
    - Initialize, complete, or fail batches.
    - Detect duplicate executions using configuration fingerprints (idempotency).
    - Track record counts (processed/failed) and execution durations.
    - Provide real-time progress updates for long-running processes.

Key Components:
    - BatchStatus: Enum defining the lifecycle states of a batch process.
    - BatchTracker: Main class for managing batch tracking and database synchronization.

Notes:
    - Idempotency is achieved by hashing the configuration (fingerprint).
    - Prevents concurrent execution of identical batches.

Dependencies:
    - sqlalchemy
    - datetime
    - typing
    - enum
    - hashlib
    - json
    - utils.logger

===============================================================================
"""

from sqlalchemy import text
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
import hashlib
import json
from utils.logger import get_logger


# 1. Lifecycle Status Definitions

class BatchStatus(Enum):
    """
    Lifecycle states for a batch process.
    
    Status mapping:
    - PENDING: Batch created but not yet started.
    - RUNNING: Batch is currently being processed.
    - COMPLETED: Batch finished successfully and balanced.
    - FAILED: Batch encountered errors and requires review.
    - ROLLED_BACK: Batch was cancelled or reverted.
    """
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ROLLED_BACK = "ROLLED_BACK"


# 2. Main BatchTracker Class

class BatchTracker:
    """
    Manager for ETL process executions.
    
    Main Functions:
    1. Start a batch (audit trail initialization).
    2. Complete a batch (successful closure).
    3. Mark as failed (error tracking).
    4. Detect duplicates (idempotency).
    
    Usage Example:
    --------------
    tracker = BatchTracker(engine, "INVOICE_PROCESSOR")
    
    try:
        # Start batch
        batch_id = tracker.start_batch(config={"month": "january"})
        
        # Process data...
        processed_records = process_invoices()
        
        # Complete successfully
        tracker.complete_batch(records_processed=len(processed_records))
        
    except Exception as e:
        # Mark as failed on error
        tracker.fail_batch(str(e))
    """
    
    def __init__(self, engine, process_name: str):
        """
        Initializes the BatchTracker.
        
        Parameters:
        -----------
        engine : SQLAlchemy engine
            Database connection for audit logging.
        
        process_name : str
            Name of the process (e.g., "SAP_STAGING", "DINERS_PROCESSOR").
        """
        self.engine = engine
        self.process_name = process_name
        self.logger = get_logger(f"BATCH_TRACKER_{process_name}")
        self.current_batch_id: Optional[str] = None
    
    # 3. Batch Lifecycle Methods
    
    def start_batch(
        self, 
        config_fingerprint: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Starts a new processing batch.
        
        Parameters:
        -----------
        config_fingerprint : str (optional)
            Hash of the configuration to detect duplicate executions.
        
        metadata : dict (optional)
            Configuration details (dates, filters, etc.).
        
        Returns:
        --------
        str : The unique batch ID created.
        """
        
        # 1. Initialization: Generate unique ID with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.current_batch_id = f"{self.process_name}_{timestamp}"
        
        # 2. Validation: Check for recent identical batches
        if config_fingerprint:
            existing = self._find_recent_identical_batch(config_fingerprint)
            
            if existing:
                if existing['status'] == BatchStatus.COMPLETED.value:
                    # Idempotency check: Already processed successfully
                    self.logger(
                        f"IDEMPOTENCY: Identical batch already completed: {existing['batch_id']}", 
                        "WARN"
                    )
                    self.logger(
                        f"Config: {metadata}", 
                        "INFO"
                    )
                    return existing['batch_id']
                
                elif existing['status'] == BatchStatus.RUNNING.value:
                    # Prevent concurrent execution of the same batch
                    self.logger(
                        f"ERROR: Duplicate batch already running: {existing['batch_id']}", 
                        "ERROR"
                    )
                    raise RuntimeError(
                        f"An identical batch is already running. "
                        f"Wait for it to finish or cancel it manually."
                    )
        
        # 3. Processing: Create database record
        query = text("""
            INSERT INTO biq_config.etl_batch_executions 
            (batch_id, process_name, status, config_fingerprint, metadata, 
             started_at, records_processed, records_failed)
            VALUES 
            (:batch_id, :process, :status, :fingerprint, :metadata, 
             NOW(), 0, 0)
        """)
        
        with self.engine.begin() as conn:
            conn.execute(query, {
                "batch_id": self.current_batch_id,
                "process": self.process_name,
                "status": BatchStatus.RUNNING.value,
                "fingerprint": config_fingerprint,
                "metadata": json.dumps(metadata) if metadata else None
            })
        
        self.logger(
            f"Batch started: {self.current_batch_id}", 
            "SUCCESS"
        )
        
        return self.current_batch_id
    
    def complete_batch(
        self, 
        records_processed: int = 0,
        records_failed: int = 0,
        result_summary: Optional[Dict] = None
    ):
        """
        Marks the batch as successfully completed.
        
        Parameters:
        -----------
        records_processed : int
            Count of successfully processed records.
        
        records_failed : int
            Count of failed records.
        
        result_summary : dict (optional)
            Audit summary of results.
        """
        
        query = text("""
            UPDATE biq_config.etl_batch_executions
            SET status = :status,
                records_processed = :processed,
                records_failed = :failed,
                result_summary = :summary,
                completed_at = NOW(),
                duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))
            WHERE batch_id = :batch_id
        """)
        
        with self.engine.begin() as conn:
            conn.execute(query, {
                "batch_id": self.current_batch_id,
                "status": BatchStatus.COMPLETED.value,
                "processed": records_processed,
                "failed": records_failed,
                "summary": json.dumps(result_summary) if result_summary else None
            })
        
        self.logger(
            f"Batch completed successfully", 
            "SUCCESS"
        )
        self.logger(
            f"Processed: {records_processed} | Failed: {records_failed}",
            "INFO"
        )
    
    def fail_batch(self, error_message: str):
        """
        Marks the batch as failed.
        
        Parameters:
        -----------
        error_message : str
            Description of the error.
        """
        
        query = text("""
            UPDATE biq_config.etl_batch_executions
            SET status = :status,
                error_message = :error,
                completed_at = NOW(),
                duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))
            WHERE batch_id = :batch_id
        """)
        
        with self.engine.begin() as conn:
            conn.execute(query, {
                "batch_id": self.current_batch_id,
                "status": BatchStatus.FAILED.value,
                "error": error_message[:500]
            })
        
        self.logger(
            f"Batch failed: {error_message}", 
            "ERROR"
        )
    
    def update_progress(self, records_processed: int):
        """
        Updates the progress counter for long-running processes.
        """
        
        query = text("""
            UPDATE biq_config.etl_batch_executions
            SET records_processed = :processed,
                last_heartbeat = NOW()
            WHERE batch_id = :batch_id
        """)
        
        with self.engine.begin() as conn:
            conn.execute(query, {
                "batch_id": self.current_batch_id,
                "processed": records_processed
            })
    
    # 4. Helper and Utility Methods
    
    def _find_recent_identical_batch(
        self, 
        config_fingerprint: str,
        hours_back: int = 24
    ) -> Optional[Dict]:
        """
        Searches for an identical batch execution within a given timeframe.
        """
        
        query = text("""
            SELECT batch_id, status, started_at
            FROM biq_config.etl_batch_executions
            WHERE process_name = :process
              AND config_fingerprint = :fingerprint
              AND started_at >= NOW() - INTERVAL '1 hour' * :hours
            ORDER BY started_at DESC
            LIMIT 1
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {
                "process": self.process_name,
                "fingerprint": config_fingerprint,
                "hours": hours_back
            }).fetchone()
        
        if result:
            return {
                "batch_id": result[0],
                "status": result[1],
                "started_at": result[2]
            }
        
        return None
    
    @staticmethod
    def generate_config_fingerprint(config: Dict) -> str:
        """
        Generates a configuration fingerprint (hash) for idempotency tracking.
        """
        
        # Sort keys to ensure consistent hashing for identical dictionaries
        config_str = json.dumps(config, sort_keys=True)
        return hashlib.md5(config_str.encode()).hexdigest()

    def should_skip(self, config_fingerprint: str) -> bool:
        """
        Determines if a process should be skipped based on previous success.
        """
        return self.is_already_completed(config_fingerprint)

    def is_already_completed(self, config_fingerprint: str) -> bool:
        """
        Checks if an identical fingerprint exists with a 'COMPLETED' status.
        """
        if not config_fingerprint:
            return False
            
        existing = self._find_recent_identical_batch(config_fingerprint)
        
        if existing and existing['status'] == BatchStatus.COMPLETED.value:
            return True
            
        return False
