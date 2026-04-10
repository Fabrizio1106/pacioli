"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.reconciliation_propagation_service
===============================================================================

Description:
    Propagates reconciliation closure states in cascade from bank transactions
    to their related card settlements and card details, maintaining referential
    integrity across the staging layer.

Responsibilities:
    - Close card settlements linked to compensated bank transactions.
    - Close card details linked to closed settlements.
    - Support selective propagation by settlement ID or date range.
    - Detect and report propagation integrity gaps.

Key Components:
    - ReconciliationPropagationService: Cascades is_compensated flags
      (SAP or intraday) from stg_bank_transactions down to stg_card_settlements
      and stg_card_details.

Notes:
    - Requires a SQLAlchemy session injected at runtime via service.session.
    - Operates exclusively on biq_stg schema tables.

Dependencies:
    - sqlalchemy
    - typing

===============================================================================
"""

from sqlalchemy import text
from typing import Dict


class ReconciliationPropagationService:
    """
    Propagates reconciliation closure states from bank transactions to child tables.

    When a bank transaction is marked as compensated (SAP or intraday), the child
    tables are not updated automatically. This service cascades the CLOSED status:

        stg_bank_transactions (is_compensated_sap=TRUE | is_compensated_intraday=TRUE)
             ↓ (settlement_id)
        stg_card_settlements (reconcile_status='CLOSED')
             ↓ (settlement_id)
        stg_card_details (reconcile_status='CLOSED')

    Each closed bank transaction propagates its status to all related rows,
    preserving referential integrity across the staging schema.
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
    
    def propagate_closures(self) -> Dict[str, int]:
        """
        Propagate closures from bank transactions to child tables.

        Execution order:
            1. Bank transactions → Settlements
            2. Settlements → Details

        Returns:
            Dict with keys: settlements_closed, details_closed, total.

        Raises:
            RuntimeError: If session has not been injected before calling this method.
        """

        if self.session is None:
            raise RuntimeError(
                "ReconciliationPropagationService requires an injected session. "
                "Use: service.session = uow.session"
            )
        
        # Paso 1: Banco → Settlements
        settlements_count = self._close_settlements_from_bank()
        
        # Paso 2: Settlements → Details
        details_count = self._close_details_from_settlements()
        
        return {
            'settlements_closed': settlements_count,
            'details_closed': details_count,
            'total': settlements_count + details_count
        }
    
    # ═════════════════════════════════════════════════════════════════════════
    # PROPAGACIÓN: BANCO → SETTLEMENTS
    # ═════════════════════════════════════════════════════════════════════════
    
    def _close_settlements_from_bank(self) -> int:
        """
        Close settlements whose linked bank transaction is compensated.

        Rule:
            IF bank.is_compensated_sap = TRUE OR bank.is_compensated_intraday = TRUE
            THEN settlement.reconcile_status = 'CLOSED'
        """
        
        # CAMBIO PG: UPDATE ... FROM y tablas sin prefijo v2_
        query = text("""
            UPDATE biq_stg.stg_card_settlements s 
            SET reconcile_status = 'CLOSED',
                updated_at = NOW()
            FROM biq_stg.stg_bank_transactions b 
            WHERE b.settlement_id = s.settlement_id
              AND (b.is_compensated_sap = TRUE OR b.is_compensated_intraday = TRUE)
              AND s.reconcile_status != 'CLOSED'
        """)
        
        result = self.session.execute(query)
        return result.rowcount
    
    # ═════════════════════════════════════════════════════════════════════════
    # PROPAGACIÓN: SETTLEMENTS → DETAILS
    # ═════════════════════════════════════════════════════════════════════════
    
    def _close_details_from_settlements(self) -> int:
        """
        Close details whose parent settlement is already closed.

        Rule:
            IF settlement.reconcile_status = 'CLOSED'
            THEN detail.reconcile_status = 'CLOSED'
        """
        
        # CAMBIO PG: UPDATE ... FROM y tablas sin prefijo v2_
        query = text("""
            UPDATE biq_stg.stg_card_details d
            SET reconcile_status = 'CLOSED',
                updated_at = NOW()
            FROM biq_stg.stg_card_settlements s 
            WHERE d.settlement_id = s.settlement_id
              AND s.reconcile_status = 'CLOSED'
              AND d.reconcile_status != 'CLOSED'
        """)
        
        result = self.session.execute(query)
        return result.rowcount
    
    # ═════════════════════════════════════════════════════════════════════════
    # PROPAGACIÓN SELECTIVA
    # ═════════════════════════════════════════════════════════════════════════
    
    def propagate_for_settlement(self, settlement_id: str) -> int:
        """Propagate closure to all details of a specific settlement."""
        
        query = text("""
            UPDATE biq_stg.stg_card_details -- CAMBIO PG: Esquema explícito y sin prefijo v2_
            SET reconcile_status = 'CLOSED',
                updated_at = NOW()
            WHERE settlement_id = :settlement_id
              AND reconcile_status != 'CLOSED'
        """)
        
        result = self.session.execute(query, {"settlement_id": settlement_id})
        return result.rowcount
    
    def propagate_for_date_range(self, start_date, end_date) -> Dict[str, int]:
        """Propagate closures for bank transactions within a specific date range."""
        
        # Close settlements within the date range
        query_settlements = text("""
            UPDATE biq_stg.stg_card_settlements s
            SET reconcile_status = 'CLOSED',
                updated_at = NOW()
            FROM biq_stg.stg_bank_transactions b 
            WHERE b.settlement_id = s.settlement_id
              AND (b.is_compensated_sap = TRUE OR b.is_compensated_intraday = TRUE)
              AND b.doc_date BETWEEN :start_date AND :end_date
              AND s.reconcile_status != 'CLOSED'
        """)
        
        result_settlements = self.session.execute(
            query_settlements,
            {"start_date": start_date, "end_date": end_date}
        )
        
        # Details relacionados
        details_count = self._close_details_from_settlements()
        
        return {
            'settlements_closed': result_settlements.rowcount,
            'details_closed': details_count,
            'total': result_settlements.rowcount + details_count
        }
    
    # ═════════════════════════════════════════════════════════════════════════
    # VALIDACIÓN Y DIAGNÓSTICO
    # ═════════════════════════════════════════════════════════════════════════
    
    def get_propagation_gaps(self) -> Dict:
        """Detect propagation gaps — records that should be closed but are not."""
        
        # Gap 1: Compensated bank transaction with a non-closed settlement
        query_gap1 = text("""
            SELECT 
                b.stg_id as bank_id,
                b.settlement_id,
                b.is_compensated_sap,
                b.is_compensated_intraday,
                s.stg_id as settlement_stg_id,
                s.reconcile_status as settlement_status
            FROM biq_stg.stg_bank_transactions b -- CAMBIO PG: Esquemas explícitos y sin prefijo v2_
            INNER JOIN biq_stg.stg_card_settlements s 
                ON b.settlement_id = s.settlement_id
            WHERE (b.is_compensated_sap = TRUE OR b.is_compensated_intraday = TRUE)
              AND s.reconcile_status != 'CLOSED'
            LIMIT 100
        """)
        
        result_gap1 = self.session.execute(query_gap1)
        bank_settlement_gaps = [{
            'bank_id': row[0],
            'settlement_id': row[1],
            'is_compensated_sap': row[2],
            'is_compensated_intraday': row[3],
            'settlement_stg_id': row[4],
            'settlement_status': row[5]
        } for row in result_gap1]
        
        # Gap 2: Closed settlement with non-closed detail rows
        query_gap2 = text("""
            SELECT 
                s.stg_id as settlement_stg_id,
                s.settlement_id,
                COUNT(d.stg_id) as open_details_count
            FROM biq_stg.stg_card_settlements s -- CAMBIO PG: Esquemas explícitos y sin prefijo v2_
            INNER JOIN biq_stg.stg_card_details d 
                ON s.settlement_id = d.settlement_id
            WHERE s.reconcile_status = 'CLOSED'
              AND d.reconcile_status != 'CLOSED'
            GROUP BY s.stg_id, s.settlement_id
            LIMIT 100
        """)
        
        result_gap2 = self.session.execute(query_gap2)
        settlement_detail_gaps = [{
            'settlement_stg_id': row[0],
            'settlement_id': row[1],
            'open_details_count': row[2]
        } for row in result_gap2]
        
        return {
            'bank_settlement_gaps': bank_settlement_gaps,
            'settlement_detail_gaps': settlement_detail_gaps,
            'total_gaps': len(bank_settlement_gaps) + len(settlement_detail_gaps)
        }
    
    def validate_propagation_integrity(self) -> bool:
        """Return True if no propagation gaps exist."""
        gaps = self.get_propagation_gaps()
        return gaps['total_gaps'] == 0