"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.reconciliation_reason_synchronizer
===============================================================================

Description:
    Synchronizes the reconcile_reason field on bank transactions based on their
    compensation flags, ensuring consistency between is_compensated_* booleans
    and the human-readable reason codes stored in the staging table.

Responsibilities:
    - Assign 'CLOSED_IN_SOURCE_SAP' to records with is_compensated_sap = TRUE.
    - Assign 'AUTO_OFFSETTING_ENTRY' to records with is_compensated_intraday = TRUE.
    - Support full-table sync and date-range-scoped sync.
    - Detect and report unsynced records for diagnostic purposes.

Key Components:
    - ReconciliationReasonSynchronizer: Updates reconcile_reason on
      biq_stg.stg_bank_transactions to reflect compensation flags.

Notes:
    - Requires a SQLAlchemy session injected at runtime via service.session.

Dependencies:
    - sqlalchemy
    - typing

===============================================================================
"""

from sqlalchemy import text
from typing import Dict


class ReconciliationReasonSynchronizer:
    """
    Synchronizes reconcile_reason codes with compensation flags.

    Business rules applied:
        IF is_compensated_sap = TRUE  → reconcile_reason = 'CLOSED_IN_SOURCE_SAP'
        IF is_compensated_intraday = TRUE → reconcile_reason = 'AUTO_OFFSETTING_ENTRY'

    Targets records where reconcile_reason is NULL or does not match the
    expected value for the active compensation flag.
    """
    
    def __init__(self, session=None):
        """
        Args:
            session: SQLAlchemy Session injected at runtime.
        """
        self.session = session
    
    # ═════════════════════════════════════════════════════════════════════════
    # MÉTODO PRINCIPAL
    # ═════════════════════════════════════════════════════════════════════════
    
    def sync_reasons(self) -> Dict[str, int]:
        """
        Synchronize reconcile_reason for all compensated transactions.

        Execution order:
            1. SAP compensations
            2. Intraday compensations

        Returns:
            Dict with keys: sap_synced, intraday_synced, total.

        Raises:
            RuntimeError: If session has not been injected before calling this method.
        """

        if self.session is None:
            raise RuntimeError(
                "ReconciliationReasonSynchronizer requires an injected session. "
                "Use: service.session = uow.session"
            )

        # 1. Sync SAP compensations
        sap_count = self._sync_sap_compensations()

        # 2. Sync intraday compensations
        intraday_count = self._sync_intraday_compensations()
        
        return {
            'sap_synced': sap_count,
            'intraday_synced': intraday_count,
            'total': sap_count + intraday_count
        }
    
    # ═════════════════════════════════════════════════════════════════════════
    # SINCRONIZACIÓN: is_compensated_sap
    # ═════════════════════════════════════════════════════════════════════════
    
    def _sync_sap_compensations(self) -> int:
        """Set reconcile_reason = 'CLOSED_IN_SOURCE_SAP' for SAP-compensated records."""
        
        query = text("""
            UPDATE biq_stg.stg_bank_transactions -- CAMBIO PG: Esquema explícito y sin prefijo v2_
            SET reconcile_reason = 'CLOSED_IN_SOURCE_SAP',
                updated_at = NOW()
            WHERE is_compensated_sap = TRUE
              AND (reconcile_reason IS NULL 
                   OR reconcile_reason != 'CLOSED_IN_SOURCE_SAP')
        """)
        
        result = self.session.execute(query)
        return result.rowcount
    
    # ═════════════════════════════════════════════════════════════════════════
    # SINCRONIZACIÓN: is_compensated_intraday
    # ═════════════════════════════════════════════════════════════════════════
    
    def _sync_intraday_compensations(self) -> int:
        """Set reconcile_reason = 'AUTO_OFFSETTING_ENTRY' for intraday-compensated records."""
        
        query = text("""
            UPDATE biq_stg.stg_bank_transactions -- CAMBIO PG: Esquema explícito y sin prefijo v2_
            SET reconcile_reason = 'AUTO_OFFSETTING_ENTRY',
                updated_at = NOW()
            WHERE is_compensated_intraday = TRUE
              AND (reconcile_reason IS NULL 
                   OR reconcile_reason != 'AUTO_OFFSETTING_ENTRY')
        """)
        
        result = self.session.execute(query)
        return result.rowcount
    
    # ═════════════════════════════════════════════════════════════════════════
    # SINCRONIZACIÓN SELECTIVA
    # ═════════════════════════════════════════════════════════════════════════
    
    def sync_for_date_range(self, start_date, end_date) -> Dict[str, int]:
        """Synchronize reason codes for compensated records within a date range."""

        # SAP compensations in range
        query_sap = text("""
            UPDATE biq_stg.stg_bank_transactions -- CAMBIO PG: Esquema explícito y sin prefijo v2_
            SET reconcile_reason = 'CLOSED_IN_SOURCE_SAP',
                updated_at = NOW()
            WHERE is_compensated_sap = TRUE
              AND doc_date BETWEEN :start_date AND :end_date
              AND (reconcile_reason IS NULL 
                   OR reconcile_reason != 'CLOSED_IN_SOURCE_SAP')
        """)
        
        result_sap = self.session.execute(
            query_sap,
            {"start_date": start_date, "end_date": end_date}
        )
        
        # Intraday compensations in range
        query_intraday = text("""
            UPDATE biq_stg.stg_bank_transactions -- CAMBIO PG: Esquema explícito y sin prefijo v2_
            SET reconcile_reason = 'AUTO_OFFSETTING_ENTRY',
                updated_at = NOW()
            WHERE is_compensated_intraday = TRUE
              AND doc_date BETWEEN :start_date AND :end_date
              AND (reconcile_reason IS NULL 
                   OR reconcile_reason != 'AUTO_OFFSETTING_ENTRY')
        """)
        
        result_intraday = self.session.execute(
            query_intraday,
            {"start_date": start_date, "end_date": end_date}
        )
        
        return {
            'sap_synced': result_sap.rowcount,
            'intraday_synced': result_intraday.rowcount,
            'total': result_sap.rowcount + result_intraday.rowcount
        }
    
    # ═════════════════════════════════════════════════════════════════════════
    # VALIDACIÓN Y DIAGNÓSTICO
    # ═════════════════════════════════════════════════════════════════════════
    
    def get_unsynced_records(self) -> Dict:
        """Return records that have compensation flags but incorrect or missing reconcile_reason."""

        # SAP-compensated records without the correct reason
        query_sap = text("""
            SELECT 
                stg_id,
                doc_date,
                amount_total,
                is_compensated_sap,
                reconcile_reason
            FROM biq_stg.stg_bank_transactions -- CAMBIO PG: Esquema explícito y sin prefijo v2_
            WHERE is_compensated_sap = TRUE
              AND (reconcile_reason IS NULL 
                   OR reconcile_reason != 'CLOSED_IN_SOURCE_SAP')
            LIMIT 100
        """)
        
        result_sap = self.session.execute(query_sap)
        unsynced_sap = [{
            'stg_id': row[0],
            'doc_date': row[1],
            'amount_total': row[2],
            'is_compensated_sap': row[3],
            'reconcile_reason': row[4]
        } for row in result_sap]
        
        # Intraday-compensated records without the correct reason
        query_intraday = text("""
            SELECT 
                stg_id,
                doc_date,
                amount_total,
                is_compensated_intraday,
                reconcile_reason
            FROM biq_stg.stg_bank_transactions -- CAMBIO PG: Esquema explícito y sin prefijo v2_
            WHERE is_compensated_intraday = TRUE
              AND (reconcile_reason IS NULL 
                   OR reconcile_reason != 'AUTO_OFFSETTING_ENTRY')
            LIMIT 100
        """)
        
        result_intraday = self.session.execute(query_intraday)
        unsynced_intraday = [{
            'stg_id': row[0],
            'doc_date': row[1],
            'amount_total': row[2],
            'is_compensated_intraday': row[3],
            'reconcile_reason': row[4]
        } for row in result_intraday]
        
        return {
            'unsynced_sap': unsynced_sap,
            'unsynced_intraday': unsynced_intraday,
            'total_unsynced': len(unsynced_sap) + len(unsynced_intraday)
        }
    
    def validate_synchronization(self) -> bool:
        """Return True if no unsynced records exist."""
        
        unsynced = self.get_unsynced_records()
        return unsynced['total_unsynced'] == 0