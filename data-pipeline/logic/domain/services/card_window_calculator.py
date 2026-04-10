"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.card_window_calculator
===============================================================================

Description:
    Calculates sliding time windows for credit card processing. This service 
    ensures that late-settling vouchers from the previous month are included 
    in the current processing period.

Responsibilities:
    - Calculate extended lookback windows for card settlements.
    - Provide utility methods for date and string-based window calculations.
    - Offer debugging information regarding lookback logic.

Key Components:
    - CardWindowCalculator: Main service class for window calculations.

Notes:
    - Default lookback is 15 days into the previous month.
    - This lookback addresses the delay between voucher creation and bank settlement.

Dependencies:
    - datetime, typing
===============================================================================
"""

from datetime import date, timedelta
from typing import Tuple


class CardWindowCalculator:
    """
    Calculates time windows for sliding card processing.
    
    BUSINESS RULE:
    ──────────────
    Credit cards have "lagging" vouchers that are settled days after
    the accounting month-end. Therefore, we need a window that
    includes the last 15 days of the previous month.
    
    PROBLEM SOLVED:
    ───────────────
    An accounting period for March (2026-03-01 → 2026-03-31) may have:
    - February 25-28 vouchers that are settled in March 1-3.
    - March settlements that group February vouchers.
    
    If we only process March (03-01 → 03-31), those February vouchers
    are NOT included in the settlements, causing:
    - Settlements without vouchers.
    - Bank transactions without matches.
    - Massive PACIFICARD PENDING issues.
    
    SOLUTION:
    ─────────
    Extended 15-day lookback window:
    - Accounting period: March 2026 (2026-03-01 → 2026-03-31)
    - Extended window: 2026-02-15 → 2026-03-31 (45 days)
    
    WHY 15 DAYS?
    ────────────
    - Month-end vouchers take 3-5 days to settle.
    - Safety buffer for extreme cases.
    - Covers the SAP accounting closure period (days 1-5 of the next month).
    - Day 15 is a clean conceptual midpoint.
    """
    
    LOOKBACK_DAYS = 15
    
    @classmethod
    def calculate_window(
        cls,
        period_start,
        period_end
    ) -> Tuple[date, date]:
        """
        Calculates the extended window for card processing.
        
        Parameters:
        ──────────
        period_start : date or str
            Start of the accounting period (e.g., 2026-03-01)
        
        period_end : date or str
            End of the accounting period (e.g., 2026-03-31)
        
        Returns:
        ───────
        Tuple[date, date]
            (extended_start, period_end)
            
        Algorithm:
        ─────────
        1. Convert strings to date if necessary.
        2. Get the first day of the period's month.
        3. Go back to the previous month.
        4. Adjust to the 15th day of the previous month.
        5. Maintain the original end_date.
        """
        
        # 1. Conversion
        if isinstance(period_start, str):
            period_start = date.fromisoformat(period_start)
        
        if isinstance(period_end, str):
            period_end = date.fromisoformat(period_end)
        
        # 2. Extract Year and Month
        year = period_start.year
        month = period_start.month
        
        # 3. Calculate Previous Month
        if month == 1:
            # Special case: January → December of previous year
            prev_year = year - 1
            prev_month = 12
        else:
            prev_year = year
            prev_month = month - 1
        
        # 4. Set Extended Start (15th of previous month)
        extended_start = date(prev_year, prev_month, 15)
        
        return (extended_start, period_end)
    
    @classmethod
    def calculate_window_str(
        cls,
        period_start: str,
        period_end: str
    ) -> Tuple[str, str]:
        """
        String version of calculate_window, accepting 'YYYY-MM-DD' format.
        
        Parameters:
        ──────────
        period_start : str
            Start date in 'YYYY-MM-DD' format.
        
        period_end : str
            End date in 'YYYY-MM-DD' format.
        
        Returns:
        ───────
        Tuple[str, str]
            (extended_start, period_end) in 'YYYY-MM-DD' format.
        """
        
        start_date = date.fromisoformat(period_start)
        end_date = date.fromisoformat(period_end)
        
        extended_start, extended_end = cls.calculate_window(start_date, end_date)
        
        return (
            extended_start.strftime('%Y-%m-%d'),
            extended_end.strftime('%Y-%m-%d')
        )
    
    @classmethod
    def get_lookback_info(cls, period_start) -> dict:
        """
        Obtains detailed lookback information for debugging purposes.
        
        Parameters:
        ──────────
        period_start : date or str
            Start of the accounting period.
        
        Returns:
        ───────
        dict with:
            - lookback_days: Configured lookback days.
            - original_month: Original month string.
            - lookback_month: Lookback month string.
            - crosses_year: Boolean indicating if it crosses a year boundary.
        """
        
        # 1. Initialization
        if isinstance(period_start, str):
            period_start = date.fromisoformat(period_start)
        
        year = period_start.year
        month = period_start.month
        
        # 2. Logic Calculation
        if month == 1:
            prev_year = year - 1
            prev_month = 12
            crosses_year = True
        else:
            prev_year = year
            prev_month = month - 1
            crosses_year = False
        
        # 3. Result Compilation
        return {
            'lookback_days': cls.LOOKBACK_DAYS,
            'original_month': f"{year:04d}-{month:02d}",
            'lookback_month': f"{prev_year:04d}-{prev_month:02d}",
            'crosses_year': crosses_year
        }


# ══════════════════════════════════════════════════════════════════════════════
# USAGE EXAMPLES
# ══════════════════════════════════════════════════════════════════════════════

# EXAMPLE 1: Basic Usage
# ──────────────────────
# from logic.domain.services.card_window_calculator import CardWindowCalculator
# from datetime import date
#
# # Accounting period: March 2026
# start, end = CardWindowCalculator.calculate_window(
#     date(2026, 3, 1),
#     date(2026, 3, 31)
# )
#
# print(f"Accounting period: 2026-03-01 → 2026-03-31")
# print(f"Extended window: {start} → {end}")
# # Output:
# # Accounting period: 2026-03-01 → 2026-03-31
# # Extended window: 2026-02-15 → 2026-03-31
