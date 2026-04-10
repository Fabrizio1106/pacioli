"""
===============================================================================
Project: PACIOLI
Module: logic.staging.reconciliation.strategies.urbaparking_strategy
===============================================================================

Description:
    Specialized matching strategy for URBAPARKING cash deposits. This strategy 
    prioritizes exact 1-to-1 matching and handles residuals using a subset sum 
    approach.

Responsibilities:
    - Perform exact 1-to-1 matching without tolerance.
    - Process residual bank deposits and invoices using subset sum algorithms.
    - Calculate final match scores and confidence levels.
    - Validate total balances for auditing purposes.

Key Components:
    - UrbaParkingStrategy: Main class for the URBAPARKING matching logic.

Notes:
    - URBAPARKING transactions require exact matches (zero tolerance).
    - High confidence is only assigned if the totals match perfectly.

Dependencies:
    - typing, datetime, sys, pathlib
    - logic.staging.reconciliation.strategies.subset_sum_solver
    - logic.staging.reconciliation.matchers.scoring_engine
===============================================================================
"""

from typing import List, Dict, Optional, Tuple
from datetime import date, datetime
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from logic.staging.reconciliation.strategies.subset_sum_solver import SubsetSumSolver
from logic.staging.reconciliation.matchers.scoring_engine import ScoringEngine


class UrbaParkingStrategy:
    """Optimized strategy for URBAPARKING matching."""
    
    def __init__(self, config: Optional[dict] = None):
        self.config = config or {}
        self.tolerance = 0.00  # URBAPARKING has NO tolerance
        self.subset_solver = SubsetSumSolver(config)
        self.scoring_engine = ScoringEngine()
    
    def match_with_residuals(
        self,
        bank_transactions: List[dict],
        invoices: List[dict],
        payment_date
    ) -> List[Dict]:
        """
        Complete strategy including residual processing.
        
        Process:
        1. Sort both by amount (descending).
        2. Perform exact 1:1 matching.
        3. Match bank residuals vs portfolio residuals using subset sum.
        4. Calculate final score.
        
        Args:
            bank_transactions: Cash deposits.
            invoices: Available invoices.
            payment_date: Reference date.
        
        Returns:
            List of matching decision updates.
        """
        updates = []
        
        # 1. Exact Matching Phase
        exact_matches, bank_residuals, invoice_residuals = self._exact_matching(
            bank_transactions, invoices
        )
        
        # 2. Process Exact Matches
        for bank_tx, invoice in exact_matches:
            score_result = self.scoring_engine.calculate_match_score(
                bank_amount=float(bank_tx.get('amount_total', 0)),
                invoice_amounts=[float(invoice.get('conciliable_amount', 0))],
                invoice_dates=[invoice.get('doc_date')],
                payment_date=payment_date,
                invoice_indices=[0],
                tolerance=self.tolerance
            )
            
            updates.append({
                'id': bank_tx.get('stg_id'),
                'status': 'MATCHED',
                'reason': 'PARKING_EXACT_MATCH',
                'diff': 0,
                'confidence': score_result['total_score'],
                'method': 'PARKING_EXACT',
                'notes': f"URBAPARKING Exact: {invoice.get('invoice_ref', 'N/A')}",
                'port_ids': [invoice.get('stg_id')],
                'bank_ref_match': bank_tx.get('bank_ref_1')
            })
        
        # 3. Residual Matching Phase
        if bank_residuals and invoice_residuals:
            residual_updates = self._match_residuals(
                bank_residuals,
                invoice_residuals,
                payment_date
            )
            updates.extend(residual_updates)
        elif bank_residuals:
            # Handle deposits without corresponding invoices
            for tx in bank_residuals:
                updates.append({
                    'id': tx.get('stg_id'),
                    'status': 'REVIEW',
                    'reason': 'PARKING_NO_INVOICE',
                    'diff': 0,
                    'confidence': 0,
                    'method': 'PARKING_NO_MATCH',
                    'notes': f"URBAPARKING: No invoice for ${tx.get('amount_total')}"
                })
        
        return updates
    
    def _exact_matching(
        self,
        bank_transactions: List[dict],
        invoices: List[dict]
    ) -> Tuple[List[Tuple], List[dict], List[dict]]:
        """
        Performs exact 1:1 matching without tolerance.
        
        Returns:
            (matches, bank_residuals, invoice_residuals)
        """
        # 1. Sort by amount descending
        sorted_bank = sorted(
            bank_transactions,
            key=lambda x: float(x.get('amount_total', 0)),
            reverse=True
        )
        
        sorted_invoices = sorted(
            invoices,
            key=lambda x: float(x.get('conciliable_amount', 0)),
            reverse=True
        )
        
        exact_matches = []
        used_invoice_ids = set()
        used_bank_ids = set()
        
        # 2. Match 1:1
        for bank_tx in sorted_bank:
            bank_amount = float(bank_tx.get('amount_total', 0))
            
            for invoice in sorted_invoices:
                if invoice.get('stg_id') in used_invoice_ids:
                    continue
                
                inv_amount = float(invoice.get('conciliable_amount', 0))
                
                # URBAPARKING constraint: must be EXACT (no tolerance)
                if abs(bank_amount - inv_amount) <= self.tolerance:
                    exact_matches.append((bank_tx, invoice))
                    used_invoice_ids.add(invoice.get('stg_id'))
                    used_bank_ids.add(bank_tx.get('stg_id'))
                    break
        
        # 3. Identify residuals
        bank_residuals = [
            tx for tx in sorted_bank
            if tx.get('stg_id') not in used_bank_ids
        ]
        
        invoice_residuals = [
            inv for inv in sorted_invoices
            if inv.get('stg_id') not in used_invoice_ids
        ]
        
        return exact_matches, bank_residuals, invoice_residuals
    
    def _match_residuals(
        self,
        bank_residuals: List[dict],
        invoice_residuals: List[dict],
        payment_date
    ) -> List[Dict]:
        """
        Matches residuals using the subset sum algorithm.
        
        Example:
        Bank: [114]
        Portfolio: [100, 14]
        → 100 + 14 = 114 ✅
        """
        updates = []
        
        for bank_tx in bank_residuals:
            bank_amount = float(bank_tx.get('amount_total', 0))
            
            # 1. Try subset sum on residual invoices
            amounts = [float(inv.get('conciliable_amount', 0)) for inv in invoice_residuals]
            
            result = self.subset_solver.find_contiguous_sum(
                target=bank_amount,
                amounts=amounts,
                tolerance=self.tolerance
            )
            
            if result:
                # 2. Process Successful Combination
                indices, total_sum = result
                matched_invoices = [invoice_residuals[i] for i in indices]
                
                score_result = self.scoring_engine.calculate_match_score(
                    bank_amount=bank_amount,
                    invoice_amounts=[float(inv.get('conciliable_amount', 0)) for inv in matched_invoices],
                    invoice_dates=[inv.get('doc_date') for inv in matched_invoices],
                    payment_date=payment_date,
                    invoice_indices=indices,
                    tolerance=self.tolerance
                )
                
                invoice_refs = [inv.get('invoice_ref', 'N/A') for inv in matched_invoices]
                invoice_ids = [inv.get('stg_id') for inv in matched_invoices]
                
                updates.append({
                    'id': bank_tx.get('stg_id'),
                    'status': score_result['status'],
                    'reason': 'PARKING_RESIDUAL_MATCH',
                    'diff': score_result['diff'],
                    'confidence': score_result['total_score'],
                    'method': 'PARKING_RESIDUAL',
                    'notes': f"URBAPARKING Residual ({len(invoice_refs)}): {','.join(invoice_refs[:3])}{'...' if len(invoice_refs) > 3 else ''}",
                    'port_ids': invoice_ids,
                    'bank_ref_match': bank_tx.get('bank_ref_1')
                })
                
                # 3. Remove used invoices from available pool
                used_ids = set(invoice_ids)
                invoice_residuals = [
                    inv for inv in invoice_residuals
                    if inv.get('stg_id') not in used_ids
                ]
            else:
                # 4. Handle failed combinations
                updates.append({
                    'id': bank_tx.get('stg_id'),
                    'status': 'REVIEW',
                    'reason': 'PARKING_RESIDUAL_MISMATCH',
                    'diff': 0,
                    'confidence': 0,
                    'method': 'PARKING_NO_MATCH',
                    'notes': f"URBAPARKING: No combination found for ${bank_amount}"
                })
        
        return updates
    
    def validate_total_balance(
        self,
        bank_transactions: List[dict],
        invoices: List[dict]
    ) -> Dict:
        """
        Validates that the total amounts match for auditing purposes.
        
        Returns:
            {
                'bank_total': float,
                'invoice_total': float,
                'diff': float,
                'balanced': bool
            }
        """
        bank_total = sum(float(tx.get('amount_total', 0)) for tx in bank_transactions)
        invoice_total = sum(float(inv.get('conciliable_amount', 0)) for inv in invoices)
        diff = abs(bank_total - invoice_total)
        
        return {
            'bank_total': bank_total,
            'invoice_total': invoice_total,
            'diff': diff,
            'balanced': diff <= 0.01  # Minimum tolerance for rounding differences
        }
