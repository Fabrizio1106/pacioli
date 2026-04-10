"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.card_settlement.split_payment_service
===============================================================================

Description:
    Pure domain service that detects split payments in card settlements and
    prorates total_cartera proportionally across each transaction leg.

    A split payment occurs when a single settlement_id has more than one bank
    transaction (e.g., a PARKING establishment paid in two partial amounts).
    In that case, the shared portfolio amount (total_cartera) must be split
    in proportion to each leg's banco_net so that the Golden Rule can evaluate
    each transaction independently.

Responsibilities:
    - Identify settlement_ids with more than one transaction for
      brand=PACIFICARD and establishment=PARKING.
    - Prorate total_cartera per transaction leg: leg_cartera = total_cartera
      × (leg_banco_net / sum_banco_net_for_settlement).
    - Flag each row with is_split_payment for downstream classifiers.

Key Components:
    - SplitPaymentResult: Dataclass carrying the enriched DataFrame and
      the count of detected split settlements.
    - SplitPaymentService: Stateless service; detect_and_prorate() operates
      on a DataFrame copy and returns a typed result without side effects.

Notes:
    - SQL-free by design.
    - Only PACIFICARD / PARKING settlements are subject to split detection;
      all other rows receive is_split_payment = False.
    - Proration uses the original total_cartera of the first row for the
      settlement (all rows in the group share the same aggregated value).

Dependencies:
    - dataclasses
    - pandas

===============================================================================
"""

from dataclasses import dataclass
import pandas as pd


@dataclass
class SplitPaymentResult:
    """Result of the split-payment detection and proration pass."""
    df: pd.DataFrame
    split_count: int


class SplitPaymentService:
    """
    Domain service for detecting and prorating split card-settlement payments.

    A settlement qualifies as a split payment when:
        - brand == 'PACIFICARD'
        - establishment_name == 'PARKING'
        - The settlement_id appears in more than one bank transaction row.

    For qualifying settlements, total_cartera on each row is replaced by:
        leg_cartera = original_total_cartera × (leg_banco_net / sum_banco_net)

    Contains no direct SQL.
    """

    def detect_and_prorate(self, df: pd.DataFrame) -> SplitPaymentResult:
        """
        Detect split payments and prorate total_cartera per transaction leg.

        Adds or updates the following columns in the returned DataFrame:
            - is_split_payment: bool flag per row
            - total_cartera:    prorated value for split rows; unchanged otherwise

        Args:
            df: DataFrame with columns: settlement_id, bank_stg_id, banco_net,
                establishment_name, brand, total_cartera.

        Returns:
            SplitPaymentResult with the enriched DataFrame copy and split_count.
        """
        result = df.copy()

        split_summary = result.groupby('settlement_id').agg(
            tx_count=('bank_stg_id', 'count'),
            total_banco=('banco_net', 'sum'),
            establishment=('establishment_name', 'first'),
            brand=('brand', 'first'),
        ).reset_index()

        splits = split_summary[
            (split_summary['tx_count'] > 1) &
            (split_summary['brand'] == 'PACIFICARD') &
            (split_summary['establishment'] == 'PARKING')
        ]

        result['is_split_payment'] = result['settlement_id'].isin(
            splits['settlement_id']
        )

        for _, split_row in splits.iterrows():
            settlement_id    = split_row['settlement_id']
            total_banco      = split_row['total_banco']
            mask             = result['settlement_id'] == settlement_id
            original_cartera = result.loc[mask, 'total_cartera'].iloc[0]

            for idx in result[mask].index:
                proporcion = result.at[idx, 'banco_net'] / total_banco
                result.at[idx, 'total_cartera'] = round(
                    original_cartera * proporcion, 2
                )

        return SplitPaymentResult(
            df=result,
            split_count=len(splits),
        )
