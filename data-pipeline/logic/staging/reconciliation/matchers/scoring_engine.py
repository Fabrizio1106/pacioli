"""
===============================================================================
Project: PACIOLI
Module: logic.staging.reconciliation.matchers.scoring_engine
===============================================================================

Description:
    Conservative hybrid scoring engine for the bank reconciliation pipeline.
    Computes a composite confidence score (0–100) for each candidate match and
    assigns a final status (MATCHED / REVIEW / PENDING) using rule-based logic.

Responsibilities:
    - Calculate weighted sub-scores for amount match, date proximity,
      invoice continuity, invoice count, and reference match
    - Apply hybrid conservative rules to determine final match status
    - Expose score breakdowns and metadata for audit trails

Key Components:
    - ScoringEngine: Main scoring class with configurable weight table
    - calculate_match_score: Entry point that returns a full score result dict

Notes:
    - EXACT_SINGLE with diff <= tolerance always yields MATCHED.
    - EXACT_CONTIGUOUS with score >= 70 and diff <= tolerance yields MATCHED.
    - BEST_EFFORT results are always forced to REVIEW by the caller.
    - Date proximity weight is intentionally reduced (10 points) to avoid
      penalizing old invoices that are legitimately paid late.

Dependencies:
    - logic.staging.reconciliation.utils.amount_helpers
    - logic.staging.reconciliation.utils.date_helpers

===============================================================================
"""

from typing import List, Dict, Optional, Union
from datetime import date, datetime
from decimal import Decimal
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from logic.staging.reconciliation.utils.amount_helpers import (
    is_within_tolerance,
    is_exact_match,
    calculate_diff
)
from logic.staging.reconciliation.utils.date_helpers import (
    calculate_date_proximity_score,
    days_between,
    parse_date
)


class ScoringEngine:
    """
    Conservative hybrid scoring engine for bank reconciliation.

    Computes a weighted composite score and maps it to a status
    (MATCHED / REVIEW / PENDING) using deterministic rules.
    """

    # Weight table — adjusted for conservative hybrid approach
    SCORING_WEIGHTS = {
        'exact_amount_match': 45,   # Perfect amount match
        'tolerance_match': 35,      # Amount within tolerance
        'date_proximity': 10,       # Temporal proximity (intentionally reduced)
        'invoice_continuity': 5,    # Sequential invoice ordering
        'reference_match': 5,       # Bank reference match bonus
    }

    def __init__(self):
        """Initialize the scoring engine with the default weight table."""
        self.weights = self.SCORING_WEIGHTS.copy()
    
    def calculate_match_score(
        self,
        bank_amount: float,
        invoice_amounts: List[float],
        invoice_dates: List[Union[date, datetime, str]],
        payment_date: Union[date, datetime, str],
        invoice_indices: List[int],
        tolerance: float = 0.05,
        has_reference_match: bool = False
    ) -> Dict:
        """
        Compute a composite match score using hybrid conservative logic.

        Returns:
            Dict with the following structure:
            {
                'total_score': float (0–100),
                'status': 'MATCHED' | 'REVIEW' | 'PENDING',
                'reason': str,
                'diff': float,
                'breakdown': {...},
                'metadata': {...}
            }
        """
        # 1. Amount difference
        total_invoice_amount = sum(invoice_amounts)
        diff = calculate_diff(bank_amount, total_invoice_amount)

        # 2. Score components
        amount_score = self._calculate_amount_score(bank_amount, total_invoice_amount, tolerance)
        continuity_score = self._calculate_continuity_score(invoice_indices)
        date_score = self._calculate_date_score(invoice_dates, payment_date)
        quantity_score = self._calculate_quantity_score(len(invoice_amounts))
        reference_score = self.weights['reference_match'] if has_reference_match else 0

        # 3. Composite score
        total_score = (
            amount_score +
            continuity_score +
            date_score +
            quantity_score +
            reference_score
        )
        total_score = min(100.0, round(total_score, 2))

        # 4. Determine status and reason
        status, reason = self._determine_status_and_reason(
            total_score=total_score,
            diff=diff,
            tolerance=tolerance,
            invoice_count=len(invoice_amounts),
            is_contiguous=self._is_contiguous(invoice_indices)
        )

        # 5. Metadata for audit trail
        metadata = {
            'invoice_count': len(invoice_amounts),
            'gap_count': self._count_gaps(invoice_indices),
            'is_contiguous': self._is_contiguous(invoice_indices),
            'amount_category': self._get_amount_category(diff, tolerance),
            'avg_invoice_age_days': self._calculate_avg_age(invoice_dates, payment_date)
        }
        
        return {
            'total_score': total_score,
            'status': status,
            'reason': reason,
            'diff': diff,
            'breakdown': {
                'amount_match': amount_score,
                'continuity': continuity_score,
                'date_proximity': date_score,
                'invoice_quantity': quantity_score,
                'reference_match': reference_score
            },
            'metadata': metadata
        }
    
    def _determine_status_and_reason(
        self,
        total_score: float,
        diff: float,
        tolerance: float,
        invoice_count: int,
        is_contiguous: bool
    ) -> tuple:
        """
        Apply conservative hybrid rules to assign a final status and reason.

        Rules (evaluated in order):
        1. EXACT_SINGLE  — 1 invoice, diff <= tolerance  → MATCHED
        2. EXACT_CONTIGUOUS — N contiguous invoices, diff <= tolerance,
           score >= 70  → MATCHED
        3. Score >= 90 (any method)  → MATCHED
        4. Score 60–89  → REVIEW
        5. Score < 60  → PENDING
        Note: BEST_EFFORT is forced to REVIEW by the caller, not here.
        """
        # Rule 1: Exact single invoice match
        if invoice_count == 1 and abs(diff) <= tolerance:
            return ("MATCHED", "PERFECT_MATCH")

        # Rule 2: Exact contiguous multi-invoice match with sufficient score
        if is_contiguous and abs(diff) <= tolerance and total_score >= 70:
            if invoice_count > 1:
                return ("MATCHED", "SPLIT_BATCH_MATCH")
            else:
                return ("MATCHED", "PERFECT_MATCH")

        # Rule 3: Very high score — auto-approve
        if total_score >= 90:
            return ("MATCHED", "HIGH_CONFIDENCE_MATCH")

        # Rule 4: Medium-high score — requires human review
        if total_score >= 60:
            if abs(diff) <= tolerance:
                return ("REVIEW", "PERFECT_MATCH")
            elif abs(diff) <= tolerance * 2:
                return ("REVIEW", "TOLERANCE_MATCH")
            else:
                return ("REVIEW", "REQUIRES_HUMAN_VALIDATION")

        # Rule 5: Low confidence
        return ("PENDING", "LOW_CONFIDENCE")
    
    def _calculate_amount_score(
        self,
        bank_amount: float,
        invoice_amount: float,
        tolerance: float
    ) -> float:
        """Calculate the amount component of the composite score."""
        diff = abs(bank_amount - invoice_amount)

        if is_exact_match(bank_amount, invoice_amount, precision=2):
            return self.weights['exact_amount_match']

        if diff <= tolerance:
            # Proportional: closer to zero difference = higher score
            ratio = 1 - (diff / tolerance)
            return self.weights['tolerance_match'] * ratio

        # Near tolerance (up to 3x) — partial credit with 50% discount
        if diff <= tolerance * 3:
            max_diff = tolerance * 3
            ratio = 1 - (diff / max_diff)
            return self.weights['tolerance_match'] * ratio * 0.5

        return 0.0

    def _calculate_continuity_score(self, indices: List[int]) -> float:
        """Calculate the invoice sequence continuity component."""
        if len(indices) <= 1:
            return 5.0  # Single invoice is always contiguous

        gaps = self._count_gaps(indices)

        if gaps == 0:
            return 5.0  # Perfect sequence
        elif gaps <= 2:
            return 4.0  # Minor gaps
        elif gaps <= 5:
            return 2.5  # Acceptable
        else:
            return 1.0  # Heavily fragmented

    def _calculate_date_score(
        self,
        invoice_dates: List[Union[date, datetime, str]],
        payment_date: Union[date, datetime, str]
    ) -> float:
        """Calculate the average temporal proximity component (max_days=120)."""
        if not invoice_dates:
            return 0.0

        scores = []
        for inv_date in invoice_dates:
            score = calculate_date_proximity_score(
                invoice_date=inv_date,
                payment_date=payment_date,
                max_days=120
            )
            scores.append(score)

        avg_proximity = sum(scores) / len(scores)
        return (avg_proximity / 100) * self.weights['date_proximity']

    def _calculate_quantity_score(self, count: int) -> float:
        """Calculate the invoice-count component — fewer invoices yields higher confidence."""
        if count == 1:
            return 5.0
        elif count <= 3:
            return 4.0
        elif count <= 5:
            return 3.0
        elif count <= 10:
            return 2.0
        else:
            return 1.0

    def _count_gaps(self, indices: List[int]) -> int:
        """Count the total number of missing positions in an index sequence."""
        if len(indices) <= 1:
            return 0

        sorted_indices = sorted(indices)
        gaps = 0

        for i in range(len(sorted_indices) - 1):
            diff = sorted_indices[i+1] - sorted_indices[i]
            if diff > 1:
                gaps += (diff - 1)

        return gaps

    def _is_contiguous(self, indices: List[int]) -> bool:
        """Return True if the given index list contains no gaps."""
        return self._count_gaps(indices) == 0

    def _get_amount_category(self, diff: float, tolerance: float) -> str:
        """Categorize an amount difference relative to the tolerance threshold."""
        abs_diff = abs(diff)

        if abs_diff <= 0.01:
            return "EXACT"
        elif abs_diff <= tolerance:
            return "TOLERANCE"
        elif abs_diff <= tolerance * 2:
            return "NEAR_TOLERANCE"
        else:
            return "MISMATCH"

    def _calculate_avg_age(
        self,
        invoice_dates: List[Union[date, datetime, str]],
        payment_date: Union[date, datetime, str]
    ) -> int:
        """Calculate the average age in days of the invoices relative to the payment date."""
        if not invoice_dates:
            return 0

        pay_date = parse_date(payment_date)
        if not pay_date:
            return 0

        ages = []
        for inv_date in invoice_dates:
            parsed = parse_date(inv_date)
            if parsed:
                age = (pay_date - parsed).days
                ages.append(age)

        return int(sum(ages) / len(ages)) if ages else 0