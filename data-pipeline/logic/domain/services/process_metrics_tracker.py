"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.process_metrics_tracker
===============================================================================

Description:
    Domain service for tracking process execution metrics. It manages the 
    lifecycle of process executions and calculates key performance indicators 
    without infrastructure dependencies.

Responsibilities:
    - Track process execution status and statistics.
    - Implement business rules for process skipping and idempotency.
    - Calculate success rates and SLA compliance.

Key Components:
    - ProcessExecution: Value object representing a single process run.
    - ProcessMetricsTracker: Service for managing process metrics logic.

Notes:
    - Follows DDD patterns (Value Objects and Domain Services).
    - Decoupled from persistence concerns.

Dependencies:
    - dataclasses
    - datetime
    - typing
    - uuid

===============================================================================
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import uuid


@dataclass
class ProcessExecution:
    """
    Value Object representing a process execution (DDD Pattern).
    
    Attributes:
    -----------
    process_name : str
        Name of the executed process.
    status : str
        Execution status: 'PENDING', 'RUNNING', 'COMPLETED', 'FAILED'.
    records_processed : int
        Number of successfully processed records.
    records_failed : int
        Number of failed records.
    execution_time_seconds : float
        Total execution time in seconds.
    error_message : Optional[str]
        Error message if the execution failed.
    run_id : str
        Unique UUID for this execution.
    started_at : Optional[datetime]
        Start timestamp.
    completed_at : Optional[datetime]
        Completion timestamp.
    """
    
    process_name: str
    status: str
    records_processed: int = 0
    records_failed: int = 0
    execution_time_seconds: float = 0.0
    error_message: Optional[str] = None
    run_id: str = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Generates a unique run_id if not provided."""
        if self.run_id is None:
            self.run_id = str(uuid.uuid4())
    
    @staticmethod
    def create_running(process_name: str) -> 'ProcessExecution':
        """
        Factory method to create a new execution in RUNNING state.
        """
        return ProcessExecution(
            process_name=process_name,
            status='RUNNING',
            started_at=datetime.now()
        )
    
    def mark_completed(
        self, 
        records_processed: int,
        execution_time: float
    ) -> 'ProcessExecution':
        """
        Marks the execution as completed and returns a new immutable instance.
        """
        return ProcessExecution(
            process_name=self.process_name,
            status='COMPLETED',
            records_processed=records_processed,
            execution_time_seconds=execution_time,
            run_id=self.run_id,
            started_at=self.started_at,
            completed_at=datetime.now()
        )
    
    def mark_failed(
        self, 
        error_message: str,
        execution_time: float
    ) -> 'ProcessExecution':
        """
        Marks the execution as failed and returns a new immutable instance.
        """
        return ProcessExecution(
            process_name=self.process_name,
            status='FAILED',
            error_message=error_message,
            execution_time_seconds=execution_time,
            run_id=self.run_id,
            started_at=self.started_at,
            completed_at=datetime.now()
        )


class ProcessMetricsTracker:
    """
    Domain Service for tracking process metrics.
    
    Pattern: Domain Service (DDD).
    """
    
    def __init__(self):
        # 1. Initialization
        pass
    
    def should_skip_process(
        self, 
        process_name: str,
        periodo: str,
        existing_status: Optional[str]
    ) -> bool:
        """
        Business rule to determine if a process should be skipped.
        
        IDEMPOTENCY:
        ------------
        If the period is already marked as COMPLETED, do not re-execute.
        """
        # 1. Status Validation
        if existing_status == 'COMPLETED':
            return True
        
        return False
    
    def calculate_success_rate(
        self,
        records_processed: int,
        records_failed: int
    ) -> float:
        """
        Calculates the success rate as a percentage (0-100).
        """
        # 1. Rate Calculation
        total = records_processed + records_failed
        
        if total == 0:
            return 100.0
        
        return (records_processed / total) * 100
    
    def is_within_sla(
        self,
        execution_time: float,
        sla_minutes: Optional[int]
    ) -> bool:
        """
        Validates if the execution time is within the defined SLA.
        """
        # 1. SLA Validation
        if sla_minutes is None:
            return True
        
        execution_minutes = execution_time / 60
        
        return execution_minutes <= sla_minutes
