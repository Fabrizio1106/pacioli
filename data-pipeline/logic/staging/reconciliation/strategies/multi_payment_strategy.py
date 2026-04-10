"""
===============================================================================
Project: PACIOLI
Module: logic.staging.reconciliation.strategies.multi_payment_strategy
===============================================================================

Description:
    Strategy for handling cases where a customer makes multiple payments within 
     a short timeframe (e.g., the same day). It finds the optimal assignment 
     of these multiple payments to a set of pending invoices using various 
     algorithms like permutation or greedy matching with backtracking.

Responsibilities:
    - Detect multi-payment groups based on customer ID and date proximity.
    - Implement optimal assignment algorithms (Permutation for small sets, 
      Greedy with backtracking for larger ones).
    - Evaluate assignment quality using the ScoringEngine.
    - Convert assignment results into standardized updates for the staging bank layer.

Key Components:
    - MultiPaymentStrategy: Orchestrator for multiple payment assignments.

Notes:
    - Handles complex cases like Ecuacentair with many payments/invoices in a day.
    - Includes safety limits for payments and invoices to prevent performance issues.

Dependencies:
    - typing, datetime, collections, itertools, sys, pathlib
    - logic.staging.reconciliation.matchers.scoring_engine
    - logic.staging.reconciliation.strategies.subset_sum_solver
    - logic.staging.reconciliation.utils.amount_helpers
    - logic.staging.reconciliation.utils.date_helpers
===============================================================================
"""

from typing import List, Dict, Optional, Tuple, Union
from datetime import date, datetime
from collections import defaultdict
import itertools
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from logic.staging.reconciliation.matchers.scoring_engine import ScoringEngine
from logic.staging.reconciliation.strategies.subset_sum_solver import SubsetSumSolver
from logic.staging.reconciliation.utils.amount_helpers import (
    is_within_tolerance,
    sum_amounts
)
from logic.staging.reconciliation.utils.date_helpers import parse_date


class MultiPaymentStrategy:
    """Strategy for assigning multiple payments to multiple invoices."""
    
    def __init__(self, config: Optional[dict] = None):
        # 1. Initialization
        self.config = config or {}
        self.tolerance = self.config.get('general', {}).get('tolerance_threshold', 0.05)
        self.scoring_engine = ScoringEngine()
        self.subset_solver = SubsetSumSolver(config)
        
        # Safety limits
        self.max_payments = 10  # Max payments in a group
        self.max_invoices = 50  # Max invoices to consider
        self.max_permutations = 1000  # Max permutations to evaluate
    
    def detect_multi_payments(
        self,
        bank_transactions: List[dict],
        time_window_hours: int = 24
    ) -> Dict[str, List[dict]]:
        """
        Detects groups of multiple payments from the same customer within a time window.
        
        Args:
            bank_transactions: List of bank transactions.
            time_window_hours: Time window in hours (default 24h).
        
        Returns:
            Dict with groups: {customer_id: [list of transactions]}
        """
        # 2. Multi-Payment Detection
        # Group by customer
        by_customer = defaultdict(list)
        
        for tx in bank_transactions:
            cust_id = tx.get('enrich_customer_id')
            if cust_id:
                by_customer[cust_id].append(tx)
        
        # Detect multi-payments
        multi_payment_groups = {}
        
        for cust_id, transactions in by_customer.items():
            if len(transactions) < 2:
                continue
            
            # Group by close dates
            groups = self._group_by_date_proximity(transactions, time_window_hours)
            
            for group in groups:
                if len(group) >= 2:
                    key = f"{cust_id}_{group[0].get('bank_date')}"
                    multi_payment_groups[key] = group
        
        return multi_payment_groups
    
    def _group_by_date_proximity(
        self,
        transactions: List[dict],
        time_window_hours: int
    ) -> List[List[dict]]:
        """Groups transactions by temporal proximity."""
        # 3. Proximity Grouping Logic
        # Sort by date
        sorted_txs = sorted(
            transactions,
            key=lambda x: parse_date(x.get('bank_date')) or date.min
        )
        
        groups = []
        current_group = []
        
        for tx in sorted_txs:
            if not current_group:
                current_group.append(tx)
                continue
            
            # Compare with the first of the current group
            first_date = parse_date(current_group[0].get('bank_date'))
            current_date = parse_date(tx.get('bank_date'))
            
            if first_date and current_date:
                hours_diff = abs((current_date - first_date).total_seconds() / 3600)
                
                if hours_diff <= time_window_hours:
                    current_group.append(tx)
                else:
                    # New group
                    if len(current_group) >= 2:
                        groups.append(current_group)
                    current_group = [tx]
            else:
                current_group.append(tx)
        
        # Add last group
        if len(current_group) >= 2:
            groups.append(current_group)
        
        return groups
    
    def find_optimal_assignment(
        self,
        payments: List[dict],
        invoices: List[dict],
        payment_date: Union[date, datetime, str]
    ) -> Optional[Dict]:
        """
        Finds the optimal assignment of N payments to M invoices.
        
        Strategy:
        1. If payments <= 3 and invoices <= 20: Try permutations.
        2. Otherwise: Greedy assignment (highest to lowest).
        """
        # 4. Optimal Assignment Orchestration
        # Validate limits
        if len(payments) > self.max_payments:
            return self._greedy_assignment(payments, invoices, payment_date)
        
        if len(invoices) > self.max_invoices:
            invoices = invoices[:self.max_invoices]
        
        # Strategy according to size
        if len(payments) <= 3 and len(invoices) <= 20:
            return self._permutation_assignment(payments, invoices, payment_date)
        else:
            return self._greedy_assignment(payments, invoices, payment_date)
    
    def _permutation_assignment(
        self,
        payments: List[dict],
        invoices: List[dict],
        payment_date: Union[date, datetime, str]
    ) -> Optional[Dict]:
        """
        Assignment by permutations (smart brute force).
        Generates all possible assignments and chooses the best one.
        """
        # 5. Permutation-based Assignment
        best_assignment = None
        best_score = 0
        attempts = 0
        
        # Sort invoices by age (FIFO)
        sorted_invoices = sorted(
            invoices,
            key=lambda x: parse_date(x.get('doc_date')) or date.min
        )
        
        # Try to assign each payment
        for assignment in self._generate_assignments(payments, sorted_invoices):
            attempts += 1
            if attempts > self.max_permutations:
                break
            
            # Evaluate this assignment
            score = self._evaluate_assignment(assignment, payment_date)
            
            if score > best_score:
                best_score = score
                best_assignment = assignment
        
        if best_assignment and best_score >= 60:
            return {
                'assignment': best_assignment,
                'total_score': best_score,
                'method': 'MULTI_PAYMENT_PERMUTATION',
                'attempts': attempts
            }
        
        return None
    
    def _greedy_assignment(
        self,
        payments: List[dict],
        invoices: List[dict],
        payment_date: Union[date, datetime, str]
    ) -> Dict:
        """
        IMPROVED greedy assignment: Two-Pass with Backtracking.
        
        PASS 1: Try normal greedy (highest to lowest).
        PASS 2: If any payment remains unmatched, backtrack and re-assign.
        """
        # 6. Greedy Assignment (with Backtracking)
        # Sort invoices (FIFO)
        sorted_invoices = sorted(
            invoices,
            key=lambda x: parse_date(x.get('doc_date')) or date.min
        )
        
        # PASS 1: Try descending order
        assignment_desc = self._try_greedy_order(
            payments=sorted(payments, key=lambda x: float(x.get('amount_total', 0)), reverse=True),
            invoices=sorted_invoices.copy(),
            payment_date=payment_date
        )
        
        # Check if all payments matched
        unmatched_desc = sum(1 for a in assignment_desc if not a['invoices'])
        
        if unmatched_desc == 0:
            # All matched with descending order
            total_score = sum(a['score'] for a in assignment_desc) / len(assignment_desc)
            return {
                'assignment': assignment_desc,
                'total_score': total_score,
                'method': 'MULTI_PAYMENT_GREEDY'
            }
        
        # PASS 2: Try ascending order (backtracking)
        assignment_asc = self._try_greedy_order(
            payments=sorted(payments, key=lambda x: float(x.get('amount_total', 0))),
            invoices=sorted_invoices.copy(),
            payment_date=payment_date
        )
        
        unmatched_asc = sum(1 for a in assignment_asc if not a['invoices'])
        
        # Choose best result
        if unmatched_asc < unmatched_desc:
            # Ascending order is better
            total_score = sum(a['score'] for a in assignment_asc) / len(assignment_asc)
            return {
                'assignment': assignment_asc,
                'total_score': total_score,
                'method': 'MULTI_PAYMENT_GREEDY_BACKTRACK'
            }
        else:
            # Keep descending
            total_score = sum(a['score'] for a in assignment_desc) / len(assignment_desc)
            return {
                'assignment': assignment_desc,
                'total_score': total_score,
                'method': 'MULTI_PAYMENT_GREEDY'
            }
    
    def _try_greedy_order(
        self,
        payments: List[dict],
        invoices: List[dict],
        payment_date: Union[date, datetime, str]
    ) -> List[Dict]:
        """Attempts greedy assignment in the given order."""
        # 7. Greedy Pass Logic
        assignment = []
        available_invoices = invoices.copy()
        
        for payment in payments:
            payment_amount = float(payment.get('amount_total', 0))
            
            # Try subset sum for this payment
            amounts = [float(inv.get('conciliable_amount', 0)) for inv in available_invoices]
            
            result = self.subset_solver.solve_with_strategy(
                target=payment_amount,
                amounts=amounts,
                tolerance=self.tolerance,
                strategy='auto'
            )
            
            if result:
                # Match found
                matched_invoices = [available_invoices[i] for i in result['indices']]
                
                score_result = self.scoring_engine.calculate_match_score(
                    bank_amount=payment_amount,
                    invoice_amounts=[float(inv.get('conciliable_amount', 0)) for inv in matched_invoices],
                    invoice_dates=[inv.get('doc_date') for inv in matched_invoices],
                    payment_date=payment_date,
                    invoice_indices=result['indices'],
                    tolerance=self.tolerance
                )
                
                assignment.append({
                    'payment': payment,
                    'invoices': matched_invoices,
                    'score': score_result['total_score'],
                    'status': score_result['status'],
                    'method': result['strategy']
                })
                
                # Remove used invoices
                used_ids = {inv.get('stg_id') for inv in matched_invoices}
                available_invoices = [
                    inv for inv in available_invoices
                    if inv.get('stg_id') not in used_ids
                ]
            else:
                # No match for this payment
                assignment.append({
                    'payment': payment,
                    'invoices': [],
                    'score': 0,
                    'status': 'REVIEW',
                    'method': 'NO_MATCH'
                })
        
        return assignment
    
    def _generate_assignments(
        self,
        payments: List[dict],
        invoices: List[dict]
    ):
        """Generates possible assignments of payments to invoices."""
        # 8. Assignment Generation
        # For 2-3 payments, try different combinations of subsets
        amounts = [float(inv.get('conciliable_amount', 0)) for inv in invoices]
        
        for payment in payments:
            payment_amount = float(payment.get('amount_total', 0))
            
            # Try different combinations
            result = self.subset_solver.solve_with_strategy(
                target=payment_amount,
                amounts=amounts,
                tolerance=self.tolerance,
                strategy='auto'
            )
            
            if result:
                yield [{
                    'payment': payment,
                    'invoice_indices': result['indices'],
                    'diff': result['diff']
                }]
    
    def _evaluate_assignment(
        self,
        assignment: List[Dict],
        payment_date: Union[date, datetime, str]
    ) -> float:
        """Evaluates an assignment and returns a total score."""
        # 9. Assignment Evaluation
        scores = []
        
        for a in assignment:
            # Calculate score for this part of the assignment
            scores.append(a.get('score', 0))
        
        return sum(scores) / len(scores) if scores else 0
    
    def convert_to_updates(
        self,
        assignment_result: Dict
    ) -> List[Dict]:
        """Converts assignment results to update format."""
        # 10. Update Conversion
        updates = []
        
        for item in assignment_result.get('assignment', []):
            payment = item['payment']
            invoices = item.get('invoices', [])
            score = item.get('score', 0)
            status = item.get('status', 'REVIEW')
            method = item.get('method', 'MULTI_PAYMENT')
            
            if invoices:
                invoice_ids = [inv.get('stg_id') for inv in invoices]
                invoice_refs = [inv.get('invoice_ref', 'N/A') for inv in invoices]
                
                # Generate note
                if len(invoice_refs) == 1:
                    note = f"Multi-Payment: {invoice_refs[0]}"
                else:
                    note = f"Multi-Payment ({len(invoice_refs)}): {','.join(invoice_refs[:3])}"
                    if len(invoice_refs) > 3:
                        note += f"... +{len(invoice_refs)-3}"
                
                note += f" | Score: {score:.0f}%"
                
                updates.append({
                    'id': payment.get('stg_id'),
                    'status': status,
                    'reason': 'MULTI_PAYMENT_ASSIGNMENT',
                    'diff': 0,
                    'confidence': score,
                    'method': f"MULTI_{method}",
                    'notes': note[:500],
                    'port_ids': invoice_ids,
                    'bank_ref_match': payment.get('bank_ref_1')
                })
            else:
                # No match found
                updates.append({
                    'id': payment.get('stg_id'),
                    'status': 'REVIEW',
                    'reason': 'MULTI_PAYMENT_NO_MATCH',
                    'diff': 0,
                    'confidence': 0,
                    'method': 'MULTI_NO_MATCH',
                    'notes': 'Multi-Payment: No optimal assignment found'
                })
        
        return updates
