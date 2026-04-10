"""
===============================================================================
Project: PACIOLI
Module: logic.staging.reconciliation.matchers.special_cases_matcher
===============================================================================

Description:
    Matcher for handling business-specific special cases that require 
    specialized reconciliation logic. This includes Urbaparking cash 
    deposits, Salas VIP deposits, and foreign transfers.

Responsibilities:
    - Detect special cases based on customer ID or transaction type strings.
    - Execute batch matching for Urbaparking (exact match one-to-one and 
      residual sum comparison).
    - Delegate Salas VIP complex matching to the SalasVIPStrategy.
    - Handle foreign transfers with expanded tolerance for bank fees.

Key Components:
    - SpecialCasesMatcher: Orchestrator for specialized reconciliation rules.

Notes:
    - Urbaparking requires exact matches (0.00 tolerance) for primary matching.
    - Foreign transfers (EXTERIOR) typically require manual review due to fees.

Dependencies:
    - typing, datetime, sys, pathlib
    - logic.staging.reconciliation.strategies.salas_vip_strategy
    - logic.staging.reconciliation.matchers.scoring_engine
    - logic.staging.reconciliation.utils.amount_helpers
===============================================================================
"""

from typing import List, Dict, Optional, Union
from datetime import date, datetime
import sys
from pathlib import Path
from logic.staging.reconciliation.strategies.salas_vip_strategy import SalasVIPStrategy

project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from logic.staging.reconciliation.matchers.scoring_engine import ScoringEngine
from logic.staging.reconciliation.utils.amount_helpers import (
    is_within_tolerance,
    sum_amounts
)


class SpecialCasesMatcher:
    """Matcher for specialized business cases."""
    
    # Configurable special customer IDs
    SPECIAL_CUSTOMER_IDS = {
        'URBAPARKING': '400419',
        'SALAS_VIP': '999999',
        'EXTERIOR': 'EXTERIOR'  # Flag for foreign transfers
    }
    
    def __init__(self, config: Optional[dict] = None):
        # 1. Initialization
        self.config = config or {}
        self.tolerance = self.config.get('general', {}).get('tolerance_threshold', 0.05)
        self.scoring_engine = ScoringEngine()
        
        self.parking_tolerance = 0.00
        self.vip_strategy = SalasVIPStrategy(self.config)
    
    def detect_special_case(
        self,
        bank_transaction: dict,
        customer_id: str
    ) -> Optional[str]:
        """
        Detects if a transaction belongs to a special case.
        
        Returns:
            'URBAPARKING' | 'SALAS_VIP' | 'EXTERIOR' | None
        """
        # 2. Special Case Detection
        # Normalize customer_id
        cust_id = str(customer_id).strip()
        
        # URBAPARKING
        if cust_id == self.SPECIAL_CUSTOMER_IDS['URBAPARKING']:
            return 'URBAPARKING'
        
        # SALAS VIP
        if cust_id == self.SPECIAL_CUSTOMER_IDS['SALAS_VIP']:
            return 'SALAS_VIP'
        
        # Foreign transfers (Exterior)
        trans_type = bank_transaction.get('trans_type', '')
        if 'EXTERIOR' in trans_type.upper():
            return 'EXTERIOR'
        
        return None
    
    def match_urbaparking_batch(
        self,
        bank_transactions: List[dict],
        invoices: List[dict],
        payment_date: Union[date, datetime, str]
    ) -> List[Dict]:
        """
        URBAPARKING Batch Matching - Exact matching without tolerance.
        
        Strategy:
        1. Sort payments and invoices by amount (descending)
        2. One-to-one exact match
        3. Residuals: compare total sums
        """
        # 3. Urbaparking Batch Matching
        updates = []
        used_invoice_indices = set()
        
        # STEP 1: Sort both by amount (highest to lowest)
        sorted_bank = sorted(
            enumerate(bank_transactions),
            key=lambda x: float(x[1].get('amount_total', 0)),
            reverse=True
        )
        
        sorted_invoices = sorted(
            enumerate(invoices),
            key=lambda x: float(x[1].get('conciliable_amount', 0)),
            reverse=True
        )
        
        # STEP 2: One-to-one exact match
        for bank_idx, bank_tx in sorted_bank:
            bank_amount = float(bank_tx.get('amount_total', 0))
            bank_ref = bank_tx.get('bank_ref_1')
            stg_id = bank_tx.get('stg_id')
            
            # Look for exact invoice
            matched = False
            for inv_idx, invoice in sorted_invoices:
                if inv_idx in used_invoice_indices:
                    continue
                
                inv_amount = float(invoice.get('conciliable_amount', 0))
                
                # URBAPARKING: No tolerance, must be EXACT
                if abs(bank_amount - inv_amount) <= self.parking_tolerance:
                    # Match found
                    used_invoice_indices.add(inv_idx)
                    
                    score_result = self.scoring_engine.calculate_match_score(
                        bank_amount=bank_amount,
                        invoice_amounts=[inv_amount],
                        invoice_dates=[invoice.get('doc_date')],
                        payment_date=payment_date,
                        invoice_indices=[inv_idx],
                        tolerance=self.parking_tolerance
                    )
                    
                    updates.append({
                        'id': stg_id,
                        'status': 'MATCHED',
                        'reason': 'PARKING_EXACT_MATCH',
                        'diff': 0,
                        'confidence': score_result['total_score'],
                        'method': 'PARKING_EXACT',
                        'notes': f"URBAPARKING: {invoice.get('invoice_ref', 'N/A')}",
                        'port_ids': [invoice.get('stg_id')],
                        'bank_ref_match': bank_ref
                    })
                    matched = True
                    break
            
            if not matched:
                # Mark for residual processing
                bank_transactions[bank_idx]['_unmatched'] = True
        
        # STEP 3: Process residuals (if any)
        unmatched_bank = [tx for tx in bank_transactions if tx.get('_unmatched')]
        unmatched_invoices = [
            invoices[i] for i in range(len(invoices))
            if i not in used_invoice_indices
        ]
        
        if unmatched_bank and unmatched_invoices:
            residual_updates = self._match_parking_residuals(
                unmatched_bank,
                unmatched_invoices,
                payment_date
            )
            updates.extend(residual_updates)
        elif unmatched_bank:
            # No invoices for these payments
            for tx in unmatched_bank:
                updates.append({
                    'id': tx.get('stg_id'),
                    'status': 'REVIEW',
                    'reason': 'PARKING_NO_MATCH',
                    'diff': 0,
                    'confidence': 0,
                    'method': 'PARKING_NO_MATCH',
                    'notes': 'URBAPARKING: No corresponding invoice'
                })
        
        return updates
    
    def _match_parking_residuals(
        self,
        bank_transactions: List[dict],
        invoices: List[dict],
        payment_date: Union[date, datetime, str]
    ) -> List[Dict]:
        """
        Processes URBAPARKING residuals by comparing total sum.
        """
        # 4. Urbaparking Residuals Processing
        updates = []
        
        total_bank = sum(float(tx.get('amount_total', 0)) for tx in bank_transactions)
        total_invoices = sum(float(inv.get('conciliable_amount', 0)) for inv in invoices)
        
        diff = abs(total_bank - total_invoices)
        
        # If total matches (expanded tolerance for batches)
        if diff <= 1.00:
            # Assign all invoices to all payments (batch match)
            invoice_ids = [inv.get('stg_id') for inv in invoices]
            invoice_refs = [inv.get('invoice_ref', 'N/A') for inv in invoices]
            
            for tx in bank_transactions:
                updates.append({
                    'id': tx.get('stg_id'),
                    'status': 'MATCHED',
                    'reason': 'PARKING_BATCH_MATCH',
                    'diff': 0,
                    'confidence': 85,  # High confidence but not perfect
                    'method': 'PARKING_BATCH',
                    'notes': f"URBAPARKING Batch: {len(invoice_refs)} invoices | Diff: ${diff:.2f}",
                    'port_ids': invoice_ids,
                    'bank_ref_match': tx.get('bank_ref_1')
                })
        else:
            # Total does not match
            for tx in bank_transactions:
                updates.append({
                    'id': tx.get('stg_id'),
                    'status': 'REVIEW',
                    'reason': 'PARKING_AMOUNT_MISMATCH',
                    'diff': diff,
                    'confidence': 0,
                    'method': 'PARKING_MISMATCH',
                    'notes': f"URBAPARKING: Batch mismatch (Bank: ${total_bank:.2f} vs Port: ${total_invoices:.2f})"
                })
        
        return updates
    
    def match_salas_vip_batch(
        self,
        bank_transactions: List[dict],
        invoices: List[dict],
        payment_date: Union[date, datetime, str]
    ) -> List[Dict]:
        """
        SALAS VIP Batch Matching.
        Delegates processing to the specialized VIP Strategy.
        """
        # 5. Salas VIP Batch Matching
        decisiones = self.vip_strategy.match_by_user_groups(
            bank_transactions=bank_transactions,
            all_invoices=invoices,
            payment_date=payment_date
        )
        
        return decisiones
    
    def match_exterior_transfer(
        self,
        bank_transaction: dict,
        invoices: List[dict],
        payment_date: Union[date, datetime, str]
    ) -> Optional[Dict]:
        """
        Match for foreign transfers (EXTERIOR).
        
        Features:
        - May include bank fees.
        - Expanded tolerance.
        - Usually requires manual validation.
        """
        # 6. Foreign Transfers Matching
        bank_amount = float(bank_transaction.get('amount_total', 0))
        bank_ref = bank_transaction.get('bank_ref_1')
        stg_id = bank_transaction.get('stg_id')
        
        # Expanded tolerance for exterior transfers (fees may apply)
        exterior_tolerance = self.tolerance * 3  # 0.15 instead of 0.05
        
        # Look for exact match first
        for invoice in invoices:
            inv_amount = float(invoice.get('conciliable_amount', 0))
            
            if is_within_tolerance(bank_amount, inv_amount, exterior_tolerance):
                score_result = self.scoring_engine.calculate_match_score(
                    bank_amount=bank_amount,
                    invoice_amounts=[inv_amount],
                    invoice_dates=[invoice.get('doc_date')],
                    payment_date=payment_date,
                    invoice_indices=[0],
                    tolerance=exterior_tolerance
                )
                
                # Exterior always requires review due to potential fees
                return {
                    'id': stg_id,
                    'status': 'REVIEW',
                    'reason': 'EXTERIOR_TRANSFER_MATCH',
                    'diff': score_result['diff'],
                    'confidence': score_result['total_score'],
                    'method': 'EXTERIOR_MATCH',
                    'notes': f"Exterior Transfer: {invoice.get('invoice_ref', 'N/A')} | Validate bank fees",
                    'port_ids': [invoice.get('stg_id')],
                    'bank_ref_match': bank_ref
                }
        
        # No match found
        return {
            'id': stg_id,
            'status': 'REVIEW',
            'reason': 'EXTERIOR_NO_MATCH',
            'diff': 0,
            'confidence': 0,
            'method': 'EXTERIOR_NO_MATCH',
            'notes': 'Exterior Transfer: No corresponding invoice found (validate manually)'
        }
