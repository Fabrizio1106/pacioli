"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.card_settlement.golden_rule_service
===============================================================================

Description:
    Pure domain service that encapsulates the Golden Rule absorption logic for
    card settlement reconciliation. Determines the final commission by computing
    the arithmetic difference between the portfolio base and the net amounts
    received, absorbing small rounding differences (within tolerance) into the
    commission rather than leaving them as unmatched residuals.

Responsibilities:
    - Compute diff_raw (settlement_gross − total_cartera) for each settlement.
    - For diffs within tolerance: derive final_commission_adjusted using
      total_cartera as the base (falling back to settlement_gross when
      total_cartera is zero), guaranteeing the accounting equation:
          base = banco_net + commission + iva + irf
    - For diffs outside tolerance: preserve voucher_commission unchanged
      and flag the settlement for manual review.

Key Components:
    - AbsorptionResult: Dataclass carrying the enriched DataFrame and counters.
    - GoldenRuleService: Stateless service; apply() operates on a DataFrame
      copy and returns a typed result without side effects.

Notes:
    - SQL-free by design.
    - Uses total_cartera (sum of conciliable_amount of matched invoices) as the
      base, not settlement_gross, so the commission absorbs the rounding cent
      and the SAP journal entry balances exactly.
    - Fallback to settlement_gross only when total_cartera <= 0 (no confirmed
      invoices), which by definition implies diff_raw > tolerance and the
      absorption branch is never reached in practice.

Dependencies:
    - dataclasses
    - pandas

===============================================================================
"""

from dataclasses import dataclass
import pandas as pd


@dataclass
class AbsorptionResult:
    """Result of the Golden Rule absorption pass."""
    df: pd.DataFrame
    absorbed_count: int
    review_count: int


class GoldenRuleService:
    """
    Domain service for absorbing small settlement differences into commission.

    Business rule (Golden Rule):
        If |settlement_gross − total_cartera| <= tolerance:
            commission = total_cartera − banco_net − settlement_iva − settlement_irf
        Else:
            commission = voucher_commission  (unchanged; settlement goes to REVIEW)

    The base for the commission formula is total_cartera, not settlement_gross,
    because the analyst selects the matched invoices (total_cartera) in SAP —
    not the raw bank gross — as the debit side of the journal entry.

    Contains no direct SQL.
    """

    def __init__(self, tolerance: float):
        """
        Args:
            tolerance: Maximum absolute difference (in currency units) that
                       qualifies for automatic absorption. Typically 0.05.
        """
        self.tolerance = tolerance

    def apply(self, df: pd.DataFrame) -> AbsorptionResult:
        """
        Apply the Golden Rule absorption pass to a merged settlements DataFrame.

        Adds or updates the following columns in the returned DataFrame:
            - diff_raw:                  settlement_gross − total_cartera (audit trail)
            - diff_adjustment:           same as diff_raw (persisted to DB as audit)
            - final_commission_adjusted: commission after absorption

        Args:
            df: DataFrame with columns: settlement_gross, total_cartera,
                banco_net, settlement_iva, settlement_irf, voucher_commission.

        Returns:
            AbsorptionResult with the enriched DataFrame copy and counters.
        """
        result = df.copy()

        result['diff_raw'] = (
            result['settlement_gross'] - result['total_cartera']
        ).round(2)

        result['diff_adjustment']           = result['diff_raw']
        result['final_commission_adjusted'] = result['voucher_commission']

        absorbed_count = 0
        review_count   = 0

        for idx in result.index:
            diff         = result.at[idx, 'diff_raw']
            voucher_comm = result.at[idx, 'voucher_commission']

            if abs(diff) <= self.tolerance:
                base = result.at[idx, 'total_cartera']
                if base <= 0:
                    base = result.at[idx, 'settlement_gross']

                banco_net      = result.at[idx, 'banco_net']
                settlement_iva = result.at[idx, 'settlement_iva']
                settlement_irf = result.at[idx, 'settlement_irf']

                result.at[idx, 'final_commission_adjusted'] = round(
                    base - banco_net - settlement_iva - settlement_irf, 2
                )
                absorbed_count += 1
            else:
                result.at[idx, 'final_commission_adjusted'] = voucher_comm
                review_count += 1

        return AbsorptionResult(
            df=result,
            absorbed_count=absorbed_count,
            review_count=review_count,
        )
