"""
===============================================================================
Project: PACIOLI
Module: logic.staging.reconciliation.utils.date_helpers
===============================================================================

Description:
    Utility functions for date handling in the bank reconciliation pipeline.
    Provides parsing, age calculation, temporal proximity scoring, and
    date-range classification used by reconciliation matchers.

Responsibilities:
    - Parse dates from multiple input types (date, datetime, str, Timestamp)
    - Calculate the number of days between two dates
    - Score temporal proximity between an invoice date and a payment date
    - Categorize invoice age into business-meaningful buckets

Key Components:
    - parse_date: Normalize any date-like input to a Python date object
    - calculate_date_proximity_score: Return a 0–100 score based on date gap
    - get_date_range_category: Classify invoice age as RECENT/MEDIUM/OLD/VERY_OLD
    - sort_by_date: Sort a list of dicts by a date field

Notes:
    - Invoices dated after the payment date are allowed but scored at most 50%.
    - Returns a large sentinel value (999999) for unparseable dates in day-count
      functions to prevent false matches.

Dependencies:
    - datetime (stdlib)
    - typing (stdlib)
    - pandas

===============================================================================
"""

from datetime import datetime, date, timedelta
from typing import List, Optional, Union
import pandas as pd


def parse_date(date_input: Union[date, datetime, str, pd.Timestamp]) -> Optional[date]:
    """
    Normalize a date-like input to a Python date object.

    Handles pandas.Timestamp, datetime, plain date, and common string formats.

    Args:
        date_input: Date value in any supported format.

    Returns:
        A Python date object, or None if parsing fails.
    """
    # 1. Already a plain date (not datetime subclass)
    if isinstance(date_input, date) and not isinstance(date_input, datetime):
        return date_input

    # 2. datetime or pandas.Timestamp — extract the date portion
    if isinstance(date_input, (datetime, pd.Timestamp)):
        return date_input.date()

    # 3. String — try common formats, then fall back to pandas parser
    if isinstance(date_input, str):
        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d %H:%M:%S']:
            try:
                return datetime.strptime(date_input, fmt).date()
            except ValueError:
                continue

        try:
            return pd.to_datetime(date_input).date()
        except Exception:
            pass

    return None


def days_between(
    date1: Union[date, datetime, str],
    date2: Union[date, datetime, str]
) -> int:
    """
    Calculate the absolute number of days between two dates.

    Args:
        date1: First date.
        date2: Second date.

    Returns:
        Absolute number of days. Returns 999999 if either date cannot be parsed,
        ensuring unparseable dates never produce false matches.

    Examples:
        >>> days_between('2026-01-01', '2026-01-10')
        9
    """
    d1 = parse_date(date1)
    d2 = parse_date(date2)

    if not d1 or not d2:
        return 999999

    return abs((d1 - d2).days)


def days_old(invoice_date: Union[date, datetime, str]) -> int:
    """
    Calculate the age of an invoice in days from today.

    Args:
        invoice_date: Invoice date.

    Returns:
        Number of days elapsed from invoice_date to today.

    Examples:
        >>> # If today is 2026-02-04
        >>> days_old('2026-01-01')
        34
    """
    inv_date = parse_date(invoice_date)
    if not inv_date:
        return 0
    
    return (date.today() - inv_date).days


def is_within_date_range(
    target_date: Union[date, datetime, str],
    days_back: int = 90
) -> bool:
    """
    Check whether a date falls within the last N days from today.

    Args:
        target_date: Date to check.
        days_back: Look-back window in days (default 90).

    Returns:
        True if target_date >= today - days_back.

    Examples:
        >>> # If today is 2026-02-04
        >>> is_within_date_range('2026-01-01', 60)
        True
    """
    target = parse_date(target_date)
    if not target:
        return False
    
    cutoff = date.today() - timedelta(days=days_back)
    return target >= cutoff


def calculate_date_proximity_score(
    invoice_date: Union[date, datetime, str],
    payment_date: Union[date, datetime, str],
    max_days: int = 90
) -> float:
    """
    Calculate a temporal proximity score (0–100) between an invoice and a payment.

    Invoices closer to the payment date receive higher scores. Invoices dated
    after the payment (future relative to payment) are permitted but capped at 50.

    Args:
        invoice_date: Invoice date.
        payment_date: Payment date.
        max_days: Maximum day gap to consider; beyond this returns 0.

    Returns:
        Score in the range 0–100.

    Examples:
        >>> calculate_date_proximity_score('2026-01-30', '2026-02-01', 90)
        97.78  # 2-day gap
    """
    inv_date = parse_date(invoice_date)
    pay_date = parse_date(payment_date)

    if not inv_date or not pay_date:
        return 0.0

    days_diff = (pay_date - inv_date).days

    # Invoice dated after payment — unusual but allowed with reduced score
    if days_diff < 0:
        abs_days = abs(days_diff)
        if abs_days > max_days:
            return 0.0
        # Capped at 50 for future-dated invoices
        score = ((max_days - abs_days) / max_days) * 50
        return round(score, 2)

    # Normal case: invoice precedes payment
    if days_diff > max_days:
        return 0.0

    # Inverse linear score: fewer days = higher score
    score = ((max_days - days_diff) / max_days) * 100
    return round(score, 2)


def get_date_range_category(days: int) -> str:
    """
    Categorize invoice age into a business-meaningful bucket.

    Args:
        days: Age of the invoice in days.

    Returns:
        Category: "RECENT" (<=30), "MEDIUM" (<=60), "OLD" (<=90), or "VERY_OLD".
    """
    if days <= 30:
        return "RECENT"
    elif days <= 60:
        return "MEDIUM"
    elif days <= 90:
        return "OLD"
    else:
        return "VERY_OLD"


def sort_by_date(
    items: List[dict],
    date_field: str = 'doc_date',
    ascending: bool = True
) -> List[dict]:
    """
    Sort a list of dicts by a date field.

    Args:
        items: List of dictionaries containing a date field.
        date_field: Name of the date field to sort by (default 'doc_date').
        ascending: True = oldest first, False = newest first.

    Returns:
        Sorted list of dictionaries.
    """
    def get_date_key(item):
        date_val = item.get(date_field)
        parsed = parse_date(date_val)
        return parsed if parsed else date.min if ascending else date.max
    
    return sorted(items, key=get_date_key, reverse=not ascending)


def is_same_day(
    date1: Union[date, datetime, str],
    date2: Union[date, datetime, str]
) -> bool:
    """
    Check whether two dates represent the same calendar day.

    Args:
        date1: First date.
        date2: Second date.

    Returns:
        True if both dates resolve to the same day.
    """
    d1 = parse_date(date1)
    d2 = parse_date(date2)
    
    if not d1 or not d2:
        return False
    
    return d1 == d2


def get_date_range_description(
    start_date: Union[date, datetime, str],
    end_date: Union[date, datetime, str]
) -> str:
    """
    Generate a human-readable description of a date range.

    Args:
        start_date: Start date.
        end_date: End date.

    Returns:
        Formatted range description, e.g., 'Range: 9 days (2026-01-01 to 2026-01-10)'.

    Examples:
        >>> get_date_range_description('2026-01-01', '2026-01-10')
        'Range: 9 days (2026-01-01 to 2026-01-10)'
    """
    d1 = parse_date(start_date)
    d2 = parse_date(end_date)

    if not d1 or not d2:
        return "Invalid date range"

    days = abs((d2 - d1).days)
    return f"Range: {days} days ({d1} to {d2})"