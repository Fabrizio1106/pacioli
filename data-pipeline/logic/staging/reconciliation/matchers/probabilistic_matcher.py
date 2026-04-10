"""
===============================================================================
Project: PACIOLI
Module: logic.staging.reconciliation.matchers.probabilistic_matcher
===============================================================================

Description:
    Conservative hybrid probabilistic matcher for the bank reconciliation
    pipeline. Handles cases that cannot be resolved deterministically by
    applying greedy sequential, subset-sum, and best-effort strategies.

Responsibilities:
    - Execute greedy sequential matching (oldest invoices first)
    - Execute subset-sum matching with and without gaps
    - Execute a best-effort approximation as a last resort
    - Apply gap penalties to reduce score for non-contiguous matches

Key Components:
    - ProbabilisticMatcher: Cascades through four strategies and returns the
      first that satisfies the auto-match threshold
    - find_best_effort_match: Always returns REVIEW status regardless of score

Notes:
    - BEST_EFFORT results are always forced to status='REVIEW' to ensure
      human validation for approximate matches.
    - Greedy and Subset strategies can yield MATCHED if the score threshold is met.

Dependencies:
    - logic.staging.reconciliation.strategies.subset_sum_solver
    - logic.staging.reconciliation.matchers.scoring_engine

===============================================================================
"""

from typing import List, Dict, Optional, Union
from datetime import date, datetime
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from logic.staging.reconciliation.strategies.subset_sum_solver import SubsetSumSolver
from logic.staging.reconciliation.matchers.scoring_engine import ScoringEngine


class ProbabilisticMatcher:
    """Probabilistic matcher with a conservative hybrid approach."""
    
    def __init__(self, config: Optional[dict] = None):
        self.config = config or {}
        self.tolerance = self.config.get('general', {}).get('tolerance_threshold', 0.05)
        self.subset_solver = SubsetSumSolver(config)
        self.scoring_engine = ScoringEngine()
    
    def find_greedy_sequential_match(
        self,
        bank_amount: float,
        invoices: List[dict],
        payment_date: Union[date, datetime, str]
    ) -> Optional[Dict]:
        """Strategy 1 — Greedy sequential: accumulate invoices from oldest to newest."""
        amounts = [float(inv.get('conciliable_amount', 0)) for inv in invoices]
        
        result = self.subset_solver.find_contiguous_sum(
            target=bank_amount,
            amounts=amounts,
            tolerance=self.tolerance
        )
        
        if not result:
            return None
        
        indices, total_sum = result
        matched_invoices = [invoices[i] for i in indices]
        
        score_result = self.scoring_engine.calculate_match_score(
            bank_amount=bank_amount,
            invoice_amounts=[float(inv.get('conciliable_amount', 0)) for inv in matched_invoices],
            invoice_dates=[inv.get('doc_date') for inv in matched_invoices],
            payment_date=payment_date,
            invoice_indices=indices,
            tolerance=self.tolerance
        )
        
        return {
            'matched_invoices': matched_invoices,
            'matched_indices': indices,
            'score_result': score_result,
            'method': 'GREEDY_SEQUENTIAL',
            'total_sum': total_sum
        }
    
    def find_subset_sum_match(
        self,
        bank_amount: float,
        invoices: List[dict],
        payment_date: Union[date, datetime, str],
        allow_gaps: bool = True,
        max_gap: int = 3
    ) -> Optional[Dict]:
        """Strategy 2/3 — Subset-sum: find the optimal combination with or without gaps."""
        amounts = [float(inv.get('conciliable_amount', 0)) for inv in invoices]
        
        if allow_gaps:
            result = self.subset_solver.find_subset_with_gaps(
                target=bank_amount,
                amounts=amounts,
                tolerance=self.tolerance,
                max_gap=max_gap
            )
        else:
            result = self.subset_solver.find_contiguous_sum(
                target=bank_amount,
                amounts=amounts,
                tolerance=self.tolerance
            )
        
        if not result:
            return None
        
        if allow_gaps:
            indices, total_sum, gap_count = result
        else:
            indices, total_sum = result
            gap_count = 0
        
        matched_invoices = [invoices[i] for i in indices]
        
        score_result = self.scoring_engine.calculate_match_score(
            bank_amount=bank_amount,
            invoice_amounts=[float(inv.get('conciliable_amount', 0)) for inv in matched_invoices],
            invoice_dates=[inv.get('doc_date') for inv in matched_invoices],
            payment_date=payment_date,
            invoice_indices=indices,
            tolerance=self.tolerance
        )
        
        # Apply gap penalty: each missing position reduces the score by 5, up to 15 points
        if gap_count > 0:
            penalty = min(gap_count * 5, 15)
            score_result['total_score'] = max(score_result['total_score'] - penalty, 0)
            score_result['gap_penalty'] = penalty

            # Re-classify after penalty
            if score_result['total_score'] >= 90:
                score_result['status'] = 'MATCHED'
            elif score_result['total_score'] >= 60:
                score_result['status'] = 'REVIEW'
            else:
                score_result['status'] = 'PENDING'
        
        return {
            'matched_invoices': matched_invoices,
            'matched_indices': indices,
            'score_result': score_result,
            'method': 'SUBSET_SUM_WITH_GAPS' if gap_count > 0 else 'SUBSET_SUM_CONTIGUOUS',
            'total_sum': total_sum,
            'gap_count': gap_count
        }
    
    def find_best_effort_match(
        self,
        bank_amount: float,
        invoices: List[dict],
        payment_date: Union[date, datetime, str]
    ) -> Optional[Dict]:
        """
        Strategy 4 — Best-effort approximation.

        Returns the closest possible match regardless of score.
        Status is always forced to 'REVIEW' to ensure human validation,
        no matter how high the computed score is.
        """
        amounts = [float(inv.get('conciliable_amount', 0)) for inv in invoices]
        
        result = self.subset_solver.find_best_approximation(
            target=bank_amount,
            amounts=amounts
        )
        
        if not result:
            return None
        
        indices, total_sum, diff = result
        matched_invoices = [invoices[i] for i in indices]
        
        score_result = self.scoring_engine.calculate_match_score(
            bank_amount=bank_amount,
            invoice_amounts=[float(inv.get('conciliable_amount', 0)) for inv in matched_invoices],
            invoice_dates=[inv.get('doc_date') for inv in matched_invoices],
            payment_date=payment_date,
            invoice_indices=indices,
            tolerance=self.tolerance
        )
        
        # Force REVIEW status — best-effort matches always require human validation
        score_result['status'] = 'REVIEW'
        score_result['reason'] = 'BEST_EFFORT_MATCH'
        
        return {
            'matched_invoices': matched_invoices,
            'matched_indices': indices,
            'score_result': score_result,
            'method': 'BEST_EFFORT',
            'total_sum': total_sum,
            'difference': diff
        }
    
    def find_any_probabilistic_match(
        self,
        bank_amount: float,
        invoices: List[dict],
        payment_date: Union[date, datetime, str]
    ) -> Optional[Dict]:
        """
        Run probabilistic strategies in cascade and return the best result.

        Execution order:
        1. Greedy Sequential
        2. Subset Sum (contiguous)
        3. Subset Sum (with gaps)
        4. Best Effort (always returns REVIEW)
        """
        # 1. Greedy sequential
        result = self.find_greedy_sequential_match(bank_amount, invoices, payment_date)
        if result and result['score_result']['status'] == 'MATCHED':
            return result

        # 2. Subset sum without gaps
        result = self.find_subset_sum_match(
            bank_amount, invoices, payment_date, allow_gaps=False
        )
        if result and result['score_result']['status'] == 'MATCHED':
            return result

        # 3. Subset sum with gaps (max 3 missing positions)
        result = self.find_subset_sum_match(
            bank_amount, invoices, payment_date, allow_gaps=True, max_gap=3
        )
        if result and result['score_result']['status'] == 'MATCHED':
            return result

        # 4. Best effort — always returns REVIEW
        result = self.find_best_effort_match(bank_amount, invoices, payment_date)
        return result