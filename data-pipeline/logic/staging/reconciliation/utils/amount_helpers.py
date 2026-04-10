"""
===============================================================================
Project: PACIOLI
Module: logic.staging.reconciliation.utils.amount_helpers
===============================================================================

Description:
    Utility functions for monetary amount handling in the bank reconciliation
    pipeline. Provides comparison, rounding, difference calculation, and
    tolerance-based matching helpers used across all reconciliation matchers.

Responsibilities:
    - Compare two amounts with configurable tolerance thresholds
    - Calculate absolute and percentage differences between amounts
    - Identify exact matches at a given decimal precision
    - Find matching amounts within a list and enumerate subset combinations

Key Components:
    - is_within_tolerance: Check whether two amounts differ by at most a threshold
    - is_exact_match: Strict equality check at a given decimal precision
    - sum_amounts: Decimal-safe summation of a list of floats
    - find_combinations_sum: Enumerate contiguous subsets that sum to a target

Notes:
    - All arithmetic uses Python's Decimal to avoid floating-point rounding errors.
    - For full subset-sum search (non-contiguous), use subset_sum_solver.py.

Dependencies:
    - decimal (stdlib)
    - typing (stdlib)

===============================================================================
"""

from decimal import Decimal
from typing import List, Tuple, Optional


def is_within_tolerance(
    amount1: float,
    amount2: float,
    tolerance: float = 0.05
) -> bool:
    """
    Check whether two amounts are within the specified tolerance.

    Args:
        amount1: First amount to compare.
        amount2: Second amount to compare.
        tolerance: Maximum allowed absolute difference (default 0.05).

    Returns:
        True if abs(amount1 - amount2) <= tolerance.

    Examples:
        >>> is_within_tolerance(100.00, 100.05, 0.05)
        True
        >>> is_within_tolerance(100.00, 100.10, 0.05)
        False
    """
    diff = abs(Decimal(str(amount1)) - Decimal(str(amount2)))
    return diff <= Decimal(str(tolerance))


def calculate_diff(
    amount1: float,
    amount2: float,
    precision: int = 2
) -> float:
    """
    Calculate the signed difference between two amounts (amount1 - amount2).

    Args:
        amount1: Base amount.
        amount2: Amount to subtract.
        precision: Decimal precision for rounding (default 2).

    Returns:
        Signed difference rounded to the given precision.

    Examples:
        >>> calculate_diff(100.00, 99.95)
        0.05
        >>> calculate_diff(99.95, 100.00)
        -0.05
    """
    diff = Decimal(str(amount1)) - Decimal(str(amount2))
    return float(round(diff, precision))


def is_exact_match(
    amount1: float,
    amount2: float,
    precision: int = 2
) -> bool:
    """
    Check whether two amounts are exactly equal at the given decimal precision.

    Args:
        amount1: First amount.
        amount2: Second amount.
        precision: Decimal precision for comparison (default 2).

    Returns:
        True if the amounts are equal when rounded to the given precision.

    Examples:
        >>> is_exact_match(100.00, 100.00)
        True
        >>> is_exact_match(100.00, 100.01)
        False
    """
    d1 = round(Decimal(str(amount1)), precision)
    d2 = round(Decimal(str(amount2)), precision)
    return d1 == d2


def sum_amounts(amounts: List[float], precision: int = 2) -> float:
    """
    Sum a list of amounts using Decimal precision.

    Args:
        amounts: List of float amounts to sum.
        precision: Decimal precision for rounding (default 2).

    Returns:
        Total sum rounded to the given precision.

    Examples:
        >>> sum_amounts([10.50, 20.30, 5.20])
        36.00
    """
    total = sum(Decimal(str(amt)) for amt in amounts)
    return float(round(total, precision))


def find_exact_amount_in_list(
    target: float,
    amounts: List[float],
    tolerance: float = 0.0
) -> Optional[int]:
    """
    Search for an amount (exact or within tolerance) in a list.

    Args:
        target: Target amount to search for.
        amounts: List of amounts to search through.
        tolerance: Allowed tolerance (default 0.0 = exact match).

    Returns:
        Index of the matching amount, or None if not found.

    Examples:
        >>> find_exact_amount_in_list(100.00, [50.00, 100.00, 200.00])
        1
        >>> find_exact_amount_in_list(100.05, [50.00, 100.00, 200.00], 0.05)
        1
    """
    for idx, amount in enumerate(amounts):
        if is_within_tolerance(target, amount, tolerance):
            return idx
    return None


def get_amount_difference_category(diff: float, tolerance: float = 0.05) -> str:
    """
    Categorize the magnitude of an amount difference.

    Args:
        diff: Absolute difference between two amounts.
        tolerance: Tolerance threshold.

    Returns:
        Category string: "EXACT" | "TOLERANCE" | "MISMATCH".

    Examples:
        >>> get_amount_difference_category(0.00)
        'EXACT'
        >>> get_amount_difference_category(0.03)
        'TOLERANCE'
        >>> get_amount_difference_category(1.50)
        'MISMATCH'
    """
    abs_diff = abs(diff)
    
    if abs_diff == 0.0:
        return "EXACT"
    elif abs_diff <= tolerance:
        return "TOLERANCE"
    else:
        return "MISMATCH"


def validate_positive_amount(amount: float) -> bool:
    """
    Validate that an amount is positive and parseable.

    Args:
        amount: Amount to validate.

    Returns:
        True if the amount is greater than zero.
    """
    try:
        return Decimal(str(amount)) > 0
    except (ValueError, TypeError):
        return False


def round_currency(amount: float, precision: int = 2) -> float:
    """
    Round an amount to currency precision.

    Args:
        amount: Amount to round.
        precision: Number of decimal places (default 2).

    Returns:
        Rounded amount as a float.
    """
    return float(round(Decimal(str(amount)), precision))


def find_combinations_sum(
    target: float,
    amounts: List[float],
    tolerance: float = 0.05,
    max_items: int = 20
) -> List[Tuple[List[int], float]]:
    """
    Find all contiguous subsets of amounts that sum to the target.

    Note:
        This is a simplified helper (O(n^2)) that only evaluates contiguous
        combinations. For a full non-contiguous subset-sum search, use
        subset_sum_solver.py.

    Args:
        target: Target amount to match.
        amounts: List of available amounts.
        tolerance: Allowed tolerance for match acceptance.
        max_items: Maximum number of items per combination.

    Returns:
        List of tuples (indices, total_sum) for all matching subsets.
    """
    results = []
    n = len(amounts)

    # Evaluate all contiguous windows of size 1..max_items (O(n^2))
    for i in range(n):
        current_sum = 0.0
        for j in range(i, min(i + max_items, n)):
            current_sum = sum_amounts(amounts[i:j+1])

            if is_within_tolerance(target, current_sum, tolerance):
                indices = list(range(i, j+1))
                results.append((indices, current_sum))

                # Return immediately on an exact match for efficiency
                if is_exact_match(target, current_sum):
                    return [results[-1]]

    return results


def format_amount_for_display(amount: float, currency: str = "USD") -> str:
    """
    Format an amount as a human-readable string for display or logging.

    Args:
        amount: Amount to format.
        currency: ISO currency code (default "USD").

    Returns:
        Formatted string, e.g., '$1,234.56 USD'.

    Examples:
        >>> format_amount_for_display(1234.56)
        '$1,234.56 USD'
    """
    formatted = f"${amount:,.2f}"
    return f"{formatted} {currency}" if currency else formatted


def calculate_percentage_difference(
    amount1: float,
    amount2: float
) -> float:
    """
    Calculate the percentage difference between two amounts relative to amount1.

    Args:
        amount1: Base (reference) amount.
        amount2: Compared amount.

    Returns:
        Percentage difference in the range 0–100.

    Examples:
        >>> calculate_percentage_difference(100.00, 105.00)
        5.0
    """
    if amount1 == 0:
        return 100.0 if amount2 != 0 else 0.0
    
    diff = abs(Decimal(str(amount1)) - Decimal(str(amount2)))
    percentage = (diff / Decimal(str(amount1))) * 100
    return float(round(percentage, 2))