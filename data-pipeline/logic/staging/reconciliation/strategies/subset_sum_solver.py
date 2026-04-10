"""
===============================================================================
Project: PACIOLI
Module: logic.staging.reconciliation.strategies.subset_sum_solver
===============================================================================

Description:
    Optimized solver for the Subset Sum problem, tailored for bank reconciliation.
    Implements multiple strategies to find combinations of amounts that match 
    a target value.

Responsibilities:
    - Identify contiguous sequences of amounts that match a target.
    - Find non-contiguous combinations (subsets with gaps) within constraints.
    - Provide best-approximation logic for cases where no exact match exists.
    - Automatically select the best strategy based on the input data.

Key Components:
    - SubsetSumSolver: Main class providing the subset sum algorithms.

Notes:
    - Limits complexity for non-contiguous searches to avoid performance issues.
    - Uses specific tolerance levels for matching business rules.

Dependencies:
    - typing, itertools, sys, pathlib
    - logic.staging.reconciliation.utils.amount_helpers
===============================================================================
"""

from typing import List, Dict, Tuple, Optional
import itertools
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from logic.staging.reconciliation.utils.amount_helpers import (
    is_within_tolerance,
    sum_amounts,
    is_exact_match
)


class SubsetSumSolver:
    """Optimized solver for the Subset Sum problem."""
    
    def __init__(self, config: Optional[dict] = None):
        self.config = config or {}
        self.max_combinations = self.config.get('general', {}).get('max_combinations_to_try', 10000)
        self.max_invoices = self.config.get('general', {}).get('max_invoices_per_match', 20)
    
    def find_contiguous_sum(
        self,
        target: float,
        amounts: List[float],
        tolerance: float = 0.05,
        max_items: int = 20
    ) -> Optional[Tuple[List[int], float]]:
        """Finds a CONTIGUOUS combination that sums to the target value."""
        # 1. Initialization
        n = len(amounts)
        
        # 2. Sequential Search
        for i in range(n):
            current_sum = 0.0
            for j in range(i, min(i + max_items, n)):
                current_sum = sum_amounts(amounts[i:j+1])
                
                # 3. Validation
                if is_within_tolerance(target, current_sum, tolerance):
                    return (list(range(i, j+1)), current_sum)
                
                if current_sum > target + tolerance:
                    break
        return None
    
    def find_subset_with_gaps(
        self,
        target: float,
        amounts: List[float],
        tolerance: float = 0.05,
        max_gap: int = 3,
        max_items: int = 20
    ) -> Optional[Tuple[List[int], float, int]]:
        """Finds a combination with GAPS that sums to the target value."""
        # 1. Pre-validation
        n = len(amounts)
        
        if n > 25:
            return None
        
        combinations_tried = 0
        best_match = None
        
        # 2. Iterative Search for Subsets
        for size in range(2, min(max_items + 1, n + 1)):
            if combinations_tried >= self.max_combinations:
                break
            
            for combo in itertools.combinations(range(n), size):
                combinations_tried += 1
                
                if combinations_tried >= self.max_combinations:
                    break
                
                combo_amounts = [amounts[i] for i in combo]
                combo_sum = sum_amounts(combo_amounts)
                
                # 3. Validation and Scoring
                if is_within_tolerance(target, combo_sum, tolerance):
                    gap_count = self._count_gaps(list(combo))
                    
                    if gap_count <= max_gap:
                        if is_exact_match(target, combo_sum):
                            return (list(combo), combo_sum, gap_count)
                        
                        if not best_match or gap_count < best_match[2]:
                            best_match = (list(combo), combo_sum, gap_count)
        
        return best_match
    
    def find_best_approximation(
        self,
        target: float,
        amounts: List[float],
        max_items: int = 20
    ) -> Optional[Tuple[List[int], float, float]]:
        """Finds the BEST possible approximation for the target value."""
        # 1. Pre-validation
        n = len(amounts)
        
        if n > 25:
            return None
        
        best_match = None
        best_diff = float('inf')
        combinations_tried = 0
        
        # 2. Combinatorial Search
        for size in range(1, min(max_items + 1, n + 1)):
            if combinations_tried >= self.max_combinations:
                break
            
            for combo in itertools.combinations(range(n), size):
                combinations_tried += 1
                
                if combinations_tried >= self.max_combinations:
                    break
                
                combo_amounts = [amounts[i] for i in combo]
                combo_sum = sum_amounts(combo_amounts)
                diff = abs(target - combo_sum)
                
                # 3. Update Best Found Match
                if diff < best_diff:
                    best_diff = diff
                    best_match = (list(combo), combo_sum, diff)
                    
                    if diff < 0.10:
                        return best_match
        
        return best_match
    
    def _count_gaps(self, indices: List[int]) -> int:
        """Counts gaps in a sequence of indices."""
        if len(indices) <= 1:
            return 0
        
        sorted_indices = sorted(indices)
        gaps = 0
        
        for i in range(len(sorted_indices) - 1):
            diff = sorted_indices[i+1] - sorted_indices[i]
            if diff > 1:
                gaps += (diff - 1)
        
        return gaps
    
    def solve_with_strategy(
        self,
        target: float,
        amounts: List[float],
        tolerance: float = 0.05,
        strategy: str = 'auto'
    ) -> Optional[Dict]:
        """Resolves using the best strategy automatically or as specified."""
        if strategy == 'auto':
            # Strategy 1: Contiguous Sum
            result = self.find_contiguous_sum(target, amounts, tolerance)
            if result:
                return {
                    'indices': result[0],
                    'sum': result[1],
                    'gap_count': 0,
                    'diff': target - result[1],
                    'strategy': 'CONTIGUOUS'
                }
            
            # Strategy 2: Subset with Gaps
            result = self.find_subset_with_gaps(target, amounts, tolerance, max_gap=3)
            if result:
                return {
                    'indices': result[0],
                    'sum': result[1],
                    'gap_count': result[2],
                    'diff': target - result[1],
                    'strategy': 'WITH_GAPS'
                }
            
            # Strategy 3: Best Approximation
            result = self.find_best_approximation(target, amounts)
            if result:
                return {
                    'indices': result[0],
                    'sum': result[1],
                    'gap_count': self._count_gaps(result[0]),
                    'diff': result[2],
                    'strategy': 'APPROXIMATION'
                }
            
            return None
        
        return None
