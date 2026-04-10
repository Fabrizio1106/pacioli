"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.aggregation
===============================================================================

Description:
    Sub-package containing aggregation services. Converts voucher-level detail
    records into settlement-level summaries, and produces derived breakdowns
    for specific business categories such as parking payments.

Responsibilities:
    - Expose aggregation service classes under a single importable namespace.

Key Components:
    - CardAggregator: Groups card vouchers into settlements; generates
      match_hash_base (without counter) for CardRepository to finalize.
    - ParkingBreakdownService: Reads processed card details for PARKING
      establishments and aggregates them by batch into breakdown records.

Notes:
    - CardAggregator operates purely on DataFrames (no DB access).
    - ParkingBreakdownService reads from biq_stg.stg_card_details.

Dependencies:
    - pandas
    - sqlalchemy
    - utils.data_cleaner
    - utils.logger

===============================================================================
"""
