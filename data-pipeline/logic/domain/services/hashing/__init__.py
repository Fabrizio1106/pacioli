"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.hashing
===============================================================================

Description:
    Sub-package containing hash key generation and historical context services.
    Provides unique, sequentially consistent match_hash_key values that allow
    bank transactions and card settlements to be correlated across runs.

Responsibilities:
    - Expose hashing service classes under a single importable namespace.

Key Components:
    - HistoricalContextService: Builds and applies historical hash counters,
      ensuring sequence continuity across daily ETL executions.
    - HashGenerator: Produces brand/batch/amount-based hash keys with
      counter values sourced from HistoricalContextService.

Notes:
    - Counter values are guaranteed >= 1 (v2.1 fix).

Dependencies:
    - pandas
    - sqlalchemy
    - logic.domain.services.hash_counter_cache_manager
    - utils.logger

===============================================================================
"""
