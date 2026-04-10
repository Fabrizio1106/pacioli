"""
===============================================================================
Project: PACIOLI
Module: logic.staging.reconciliation.matchers.deterministic_matcher
===============================================================================

Description:
    Implements high-confidence deterministic matching strategies for the bank
    reconciliation pipeline. Handles exact 1:1 matches, tolerance-based 1:1
    matches, and exact 1:N contiguous multi-invoice matches.

Responsibilities:
    - Find a single invoice that exactly equals the bank payment amount
    - Find a single invoice within the configured tolerance threshold
    - Find a contiguous run of invoices whose sum exactly equals the payment

Key Components:
    - DeterministicMatcher: Orchestrates all deterministic matching strategies
      in order of confidence and returns the first match found

Notes:
    - All deterministic matches produce MATCHED status with confidence >= 90%.
    - Invoice lists are expected to be pre-sorted by doc_date (oldest first).
    - For non-contiguous or approximate matches, use probabilistic_matcher.py.

Dependencies:
    - logic.staging.reconciliation.utils.amount_helpers
    - logic.staging.reconciliation.matchers.scoring_engine

===============================================================================
"""

from typing import List, Dict, Optional, Union
from datetime import date, datetime

# Resolve project root for relative imports
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from logic.staging.reconciliation.utils.amount_helpers import (
    is_exact_match,
    is_within_tolerance,
    sum_amounts,
    calculate_diff
)
from logic.staging.reconciliation.matchers.scoring_engine import ScoringEngine


class DeterministicMatcher:
    """
    Matcher for high-confidence deterministic reconciliation cases.

    Executes exact and tolerance-based strategies in order of confidence,
    returning the first successful match.
    """

    def __init__(self, config: Optional[dict] = None):
        """
        Initialize the deterministic matcher.

        Args:
            config: Optional configuration dictionary (loaded from YAML).
        """
        self.config = config or {}
        self.tolerance = self.config.get('general', {}).get('tolerance_threshold', 0.05)
        self.scoring_engine = ScoringEngine()
    
    def find_exact_single_match(
        self,
        bank_amount: float,
        invoices: List[dict],
        payment_date: Union[date, datetime, str]
    ) -> Optional[Dict]:
        """
        Strategy 1 — Exact 1:1 match.

        Find a single invoice whose amount exactly equals the bank payment.

        Args:
            bank_amount: Bank payment amount.
            invoices: Available invoices (each dict must contain
                      'conciliable_amount' and 'doc_date').
            payment_date: Date of the bank payment.

        Returns:
            Match result dict or None:
            {
                'matched_invoices': [invoice],
                'matched_indices': [index],
                'score_result': {...},
                'method': 'EXACT_SINGLE'
            }

        Examples:
            >>> matcher = DeterministicMatcher()
            >>> invoices = [
            ...     {'conciliable_amount': 1000.00, 'doc_date': '2026-01-15'},
            ...     {'conciliable_amount': 500.00, 'doc_date': '2026-01-16'}
            ... ]
            >>> result = matcher.find_exact_single_match(1000.00, invoices, '2026-02-01')
            >>> result['matched_invoices'][0]['conciliable_amount']
            1000.00
        """
        for idx, invoice in enumerate(invoices):
            invoice_amount = float(invoice.get('conciliable_amount', 0))
            
            if is_exact_match(bank_amount, invoice_amount):
                # Calcular score
                score_result = self.scoring_engine.calculate_match_score(
                    bank_amount=bank_amount,
                    invoice_amounts=[invoice_amount],
                    invoice_dates=[invoice.get('doc_date')],
                    payment_date=payment_date,
                    invoice_indices=[idx],
                    tolerance=self.tolerance
                )
                
                return {
                    'matched_invoices': [invoice],
                    'matched_indices': [idx],
                    'score_result': score_result,
                    'method': 'EXACT_SINGLE'
                }
        
        return None
    
    def find_tolerance_single_match(
        self,
        bank_amount: float,
        invoices: List[dict],
        payment_date: Union[date, datetime, str]
    ) -> Optional[Dict]:
        """
        Strategy 1 (variant) — Tolerance-based 1:1 match.

        Find a single invoice within the configured tolerance (±threshold).
        Exact matches are excluded because they are handled by
        find_exact_single_match.

        Args:
            bank_amount: Bank payment amount.
            invoices: Available invoices.
            payment_date: Date of the bank payment.

        Returns:
            Match result dict or None.

        Examples:
            >>> matcher = DeterministicMatcher()
            >>> invoices = [
            ...     {'conciliable_amount': 100.03, 'doc_date': '2026-01-15'}
            ... ]
            >>> result = matcher.find_tolerance_single_match(100.00, invoices, '2026-02-01')
            >>> result['score_result']['reason']
            'PENNY_ADJUSTMENT'
        """
        for idx, invoice in enumerate(invoices):
            invoice_amount = float(invoice.get('conciliable_amount', 0))

            # Within tolerance but not exact — exact matches belong to find_exact_single_match
            if (is_within_tolerance(bank_amount, invoice_amount, self.tolerance) and
                not is_exact_match(bank_amount, invoice_amount)):
                
                score_result = self.scoring_engine.calculate_match_score(
                    bank_amount=bank_amount,
                    invoice_amounts=[invoice_amount],
                    invoice_dates=[invoice.get('doc_date')],
                    payment_date=payment_date,
                    invoice_indices=[idx],
                    tolerance=self.tolerance
                )
                
                return {
                    'matched_invoices': [invoice],
                    'matched_indices': [idx],
                    'score_result': score_result,
                    'method': 'TOLERANCE_SINGLE'
                }
        
        return None
    
    def find_exact_contiguous_multi_match(
        self,
        bank_amount: float,
        invoices: List[dict],
        payment_date: Union[date, datetime, str],
        max_invoices: int = 20
    ) -> Optional[Dict]:
        """
        Strategy 2 — Exact 1:N contiguous multi-invoice match.

        Find N consecutive invoices (sorted by doc_date, oldest first) whose
        amounts sum exactly to the bank payment.

        Args:
            bank_amount: Bank payment amount.
            invoices: Invoice list sorted by doc_date ascending.
            payment_date: Date of the bank payment.
            max_invoices: Maximum number of invoices to combine.

        Returns:
            Match result dict or None.

        Examples:
            >>> # Payment of 1000 = invoice A (500) + invoice B (500)
            >>> matcher = DeterministicMatcher()
            >>> invoices = [
            ...     {'conciliable_amount': 500.00, 'doc_date': '2026-01-10'},
            ...     {'conciliable_amount': 500.00, 'doc_date': '2026-01-11'},
            ...     {'conciliable_amount': 200.00, 'doc_date': '2026-01-12'}
            ... ]
            >>> result = matcher.find_exact_contiguous_multi_match(1000.00, invoices, '2026-02-01')
            >>> len(result['matched_invoices'])
            2
            >>> result['score_result']['reason']
            'PERFECT_MATCH'
        """
        n = len(invoices)

        # Evaluate all contiguous windows
        for i in range(n):
            current_sum = 0.0

            for j in range(i, min(i + max_invoices, n)):
                invoice_amount = float(invoices[j].get('conciliable_amount', 0))
                current_sum = sum_amounts([
                    float(invoices[k].get('conciliable_amount', 0))
                    for k in range(i, j+1)
                ])

                if is_exact_match(bank_amount, current_sum):
                    matched_invoices = invoices[i:j+1]
                    matched_indices = list(range(i, j+1))

                    score_result = self.scoring_engine.calculate_match_score(
                        bank_amount=bank_amount,
                        invoice_amounts=[
                            float(inv.get('conciliable_amount', 0))
                            for inv in matched_invoices
                        ],
                        invoice_dates=[inv.get('doc_date') for inv in matched_invoices],
                        payment_date=payment_date,
                        invoice_indices=matched_indices,
                        tolerance=self.tolerance
                    )
                    
                    return {
                        'matched_invoices': matched_invoices,
                        'matched_indices': matched_indices,
                        'score_result': score_result,
                        'method': 'EXACT_CONTIGUOUS'
                    }
                
                # Prune: stop accumulating once the window exceeds the target
                if current_sum > bank_amount:
                    break
        
        return None
    
    def find_any_deterministic_match(
        self,
        bank_amount: float,
        invoices: List[dict],
        payment_date: Union[date, datetime, str]
    ) -> Optional[Dict]:
        """
        Run all deterministic strategies in order and return the first match.

        Execution order (highest to lowest confidence):
        1. Exact Single
        2. Exact Contiguous Multi
        3. Tolerance Single

        Args:
            bank_amount: Bank payment amount.
            invoices: Available invoices.
            payment_date: Date of the bank payment.

        Returns:
            First match result found, or None.

        Examples:
            >>> matcher = DeterministicMatcher()
            >>> invoices = [...]
            >>> result = matcher.find_any_deterministic_match(1000.00, invoices, '2026-02-01')
            >>> if result:
            ...     print(f"Method: {result['method']}")
            ...     print(f"Score: {result['score_result']['total_score']}")
        """
        # 1. Exact Single
        result = self.find_exact_single_match(bank_amount, invoices, payment_date)
        if result:
            return result

        # 2. Exact Contiguous Multi
        result = self.find_exact_contiguous_multi_match(bank_amount, invoices, payment_date)
        if result:
            return result

        # 3. Tolerance Single
        result = self.find_tolerance_single_match(bank_amount, invoices, payment_date)
        if result:
            return result

        return None
    
    def validate_match_result(self, result: Dict) -> bool:
        """
        Validate that a match result produced by any find_* method is acceptable.

        A deterministic match is valid when all required keys are present,
        at least one invoice was matched, and the total score is >= 85.

        Args:
            result: Result dict from any find_* method.

        Returns:
            True if the result is valid and meets confidence requirements.
        """
        if not result:
            return False

        required_keys = ['matched_invoices', 'matched_indices', 'score_result', 'method']
        if not all(key in result for key in required_keys):
            return False

        if not result['matched_invoices']:
            return False

        # Deterministic matches require a minimum score of 85
        if result['score_result'].get('total_score', 0) < 85:
            return False

        return True