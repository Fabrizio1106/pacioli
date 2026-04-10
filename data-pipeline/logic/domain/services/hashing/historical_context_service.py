"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.hashing.historical_context_service
===============================================================================

Description:
    Domain service that builds the historical counter context used by
    HashGenerator to ensure match_hash_key sequence continuity across ETL runs.
    Reads the last-used counter for each (brand, batch, amount) group from
    the hash counter cache or directly from the staging database.

Responsibilities:
    - Load historical hash counters via HashCounterCacheManager (with DB fallback).
    - Normalize batch_number: None/NULL -> '' (empty string) for consistent
      dict key lookups across SQL and Python.
    - Apply historical counters to the input DataFrame as _historical_counter.
    - Provide continuity validation (sequence gap detection).
    - Report whether cache or direct DB was used.

Key Components:
    - HistoricalContextService: Domain service requiring a SQLAlchemy session.
      build_context() enriches the DataFrame with _historical_counter before
      HashGenerator.generate() is called.

Notes:
    - v2.4 fix: batch_number normalization is applied consistently across the
      cache lookup, DB query, and in-memory grouping. None -> '' prevents
      dict key mismatches between SQL NULL and Python None.
    - NO_BATCH = '' is the single canonical representation for "no batch".
    - The DB query filters to doc_type = 'ZR' and uses REGEXP_REPLACE to
      extract the trailing integer from match_hash_key.
    - _historical_counter is a temporary column; HashGenerator reads it and
      does not persist it to staging.

Dependencies:
    - pandas, sqlalchemy, typing
    - logic.domain.services.hash_counter_cache_manager

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from typing import Dict, Tuple, Optional

from logic.domain.services.hash_counter_cache_manager import HashCounterCacheManager
from utils.logger import get_logger


class HistoricalContextService:
    """
    Domain service that pre-computes hash sequence counters for HashGenerator.

    v2.4 design principle:
        "A single canonical representation for absent batch across the application."
        batch_number absent -> '' (empty string) in all dict keys and comparisons.

    This eliminates mismatches between SQL NULL (returned as None by SQLAlchemy)
    and Python None used as dict keys, which previously caused counters to
    not be found and sequences to restart from 1 incorrectly.

    Counter lookup key: (brand: str, batch: str, amount: float)
        PACIFICARD: batch = actual batch number string
        All others: batch = '' (NO_BATCH constant)
    """

    # Canonical representation for "no batch" across all dict keys and comparisons
    NO_BATCH = ''

    def __init__(self, session=None):
        """
        Args:
            session: SQLAlchemy session for DB queries. Must be injected before
                     calling build_context(). Typically set via
                     service.session = uow.session inside a UnitOfWork.
        """
        self.session = session
        self.context = {}
        self.cache_manager = None
        self.used_cache = False
        self.logger = get_logger("HISTORICAL_CONTEXT")

    def build_context(
        self,
        df: pd.DataFrame,
        start_date,
        end_date,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Build historical counter context and apply it to the input DataFrame.

        Steps:
            1. Load counters from cache (or fall back to direct DB query).
            2. Normalize batch_number in counters: None -> ''.
            3. Apply counters to DataFrame as _historical_counter column.

        Args:
            df: DataFrame to enrich with _historical_counter.
            start_date: Reference start date (used by DB query as before_date).
            end_date: Not used in the query but accepted for interface consistency.
            use_cache: If True, attempt cache first; fall back to DB on failure.

        Returns:
            DataFrame with _historical_counter column added.

        Raises:
            RuntimeError: If session is None (session must be injected first).
        """
        if self.session is None:
            raise RuntimeError(
                "HistoricalContextService requires an injected session. "
                "Set: service.session = uow.session"
            )

        if use_cache:
            historical_counters = self._get_counters_from_cache(start_date)
        else:
            historical_counters = self._get_counters_from_db(start_date)

        df_enriched = self._apply_historical_counters(df, historical_counters)

        self.context = historical_counters

        return df_enriched

    def _get_counters_from_cache(
        self,
        before_date
    ) -> Dict[Tuple[str, str, float], int]:
        """
        Load historical counters from the HashCounterCacheManager.

        Normalizes batch keys: None -> '' before returning, so all downstream
        dict lookups use a consistent key type.

        Falls back to _get_counters_from_db() if cache is empty or raises.

        Returns:
            Dict mapping (brand, batch_or_empty_str, amount) -> last_counter.
            Example: {('VISA', '', 28.39): 3, ('PACIFICARD', '001089', 74.52): 1}
        """
        try:
            if self.cache_manager is None:
                self.cache_manager = HashCounterCacheManager(self.session)

            counters_raw = self.cache_manager.get_last_counters()

            if len(counters_raw) == 0:
                self.logger("Cache empty, falling back to direct DB query", "WARN")
                self.used_cache = False
                return self._get_counters_from_db(before_date)

            # Normalize: None -> '' for consistent key lookups
            counters_normalized = {}
            for (brand, batch, amount), counter in counters_raw.items():
                batch_normalized = batch if batch is not None else self.NO_BATCH
                counters_normalized[(brand, batch_normalized, amount)] = counter

            self.used_cache = True
            return counters_normalized

        except Exception as e:
            self.logger(f"Cache error: {e}. Falling back to direct DB query", "ERROR")
            self.used_cache = False
            return self._get_counters_from_db(before_date)

    def _get_counters_from_db(
        self,
        before_date
    ) -> Dict[Tuple[str, str, float], int]:
        """
        Query biq_stg.stg_bank_transactions directly for the last-used counters.

        Filters to doc_type = 'ZR' and records with match_hash_key ending in
        an integer suffix. Uses PostgreSQL REGEXP_REPLACE and COALESCE to
        normalize NULL batch_number to '' at the SQL level, so returned keys
        are already consistent with NO_BATCH.

        Returns:
            Dict mapping (brand, batch_or_empty_str, amount) -> last_counter.
        """
        query = text("""
            SELECT
                brand,
                COALESCE(
                    CASE
                        WHEN brand = 'PACIFICARD' THEN batch_number
                        ELSE NULL
                    END,
                    ''
                ) as batch_number,
                amount_total,
                MAX(
                    CAST(
                        REGEXP_REPLACE(match_hash_key, '^.*_', '')
                        AS INTEGER
                    )
                ) as last_counter
            FROM biq_stg.stg_bank_transactions
            WHERE doc_date < :before_date
              AND match_hash_key ~ '_[0-9]+$'
              AND brand != 'NA'
              AND brand IS NOT NULL
              AND doc_type = 'ZR'
            GROUP BY
                brand,
                COALESCE(
                    CASE
                        WHEN brand = 'PACIFICARD' THEN batch_number
                        ELSE NULL
                    END,
                    ''
                ),
                amount_total
        """)

        result = self.session.execute(query, {"before_date": before_date})

        counters = {}
        for row in result:
            # batch_number is already '' if NULL (via COALESCE in SQL)
            key = (row.brand, row.batch_number, float(row.amount_total))
            counters[key] = int(row.last_counter)

        return counters

    def _apply_historical_counters(
        self,
        df: pd.DataFrame,
        historical_counters: Dict[Tuple[str, str, float], int]
    ) -> pd.DataFrame:
        """
        Apply historical counters to the DataFrame as _historical_counter.

        For each (brand, batch_for_grouping, amount_total) group, looks up the
        last counter in historical_counters and assigns sequential values starting
        from last_counter + 1. Groups not found in history start from 1.

        batch_for_grouping normalization:
            PACIFICARD with a real batch: uses the batch value.
            All others (including PACIFICARD without batch): uses '' (NO_BATCH).
        """
        df = df.copy()
        df['_historical_counter'] = 0

        # Normalize batch_number column
        if 'batch_number' not in df.columns:
            df['batch_number'] = self.NO_BATCH
        else:
            df['batch_number'] = df['batch_number'].fillna(self.NO_BATCH)

        # Build batch_for_grouping: real batch for PACIFICARD, '' for all others
        df['batch_for_grouping'] = df.apply(
            lambda row: (
                row['batch_number']
                if row['brand'] == 'PACIFICARD' and row['batch_number'] != self.NO_BATCH
                else self.NO_BATCH
            ),
            axis=1
        )

        for (brand, batch, amount), group_df in df.groupby(
            ['brand', 'batch_for_grouping', 'amount_total']
        ):
            # Look up last counter; default to 0 (sequence starts at 1)
            # Key uses '' for batch, matching the normalization applied above
            last_counter = historical_counters.get((brand, batch, amount), 0)

            counters = range(
                last_counter + 1,
                last_counter + len(group_df) + 1
            )

            df.loc[group_df.index, '_historical_counter'] = list(counters)

        df = df.drop(columns=['batch_for_grouping'], errors='ignore')

        return df

    def get_counter_for_key(
        self,
        brand: str,
        batch_number: Optional[str],
        amount: float
    ) -> int:
        """
        Look up the last counter for a specific (brand, batch, amount) key.

        Normalizes batch_number before lookup (None -> '').

        Returns:
            Last counter value, or 0 if not found in context.
        """
        batch_normalized = batch_number if batch_number else self.NO_BATCH
        return self.context.get((brand, batch_normalized, amount), 0)

    def get_context_summary(self) -> pd.DataFrame:
        """
        Return the current context as a readable DataFrame for diagnostics.

        Returns:
            DataFrame with columns: brand, batch_number, amount, last_counter.
            Sorted by last_counter descending.
        """
        if not self.context:
            return pd.DataFrame(
                columns=['brand', 'batch_number', 'amount', 'last_counter']
            )

        data = [
            {
                'brand': brand,
                'batch_number': batch if batch != self.NO_BATCH else None,
                'amount': amount,
                'last_counter': counter
            }
            for (brand, batch, amount), counter in self.context.items()
        ]

        return pd.DataFrame(data).sort_values('last_counter', ascending=False)

    def validate_continuity(self, df: pd.DataFrame) -> Dict:
        """
        Validate that _historical_counter values form continuous sequences per group.

        Returns:
            Dict with keys:
                is_continuous (bool): True if no gaps found.
                gaps (list): List of (brand, batch, amount, prev_counter, next_counter)
                             tuples for each detected gap.
        """
        gaps = []

        if 'batch_number' not in df.columns:
            df['batch_number'] = self.NO_BATCH
        else:
            df['batch_number'] = df['batch_number'].fillna(self.NO_BATCH)

        df = df.copy()
        df['batch_for_grouping'] = df.apply(
            lambda row: (
                row['batch_number']
                if row['brand'] == 'PACIFICARD' and row['batch_number'] != self.NO_BATCH
                else self.NO_BATCH
            ),
            axis=1
        )

        for (brand, batch, amount), group_df in df.groupby(
            ['brand', 'batch_for_grouping', 'amount_total']
        ):
            counters = sorted(group_df['_historical_counter'].tolist())

            for i in range(len(counters) - 1):
                if counters[i + 1] != counters[i] + 1:
                    gaps.append((brand, batch, amount, counters[i], counters[i + 1]))

        return {
            'is_continuous': len(gaps) == 0,
            'gaps': gaps
        }

    def was_cache_used(self) -> bool:
        """Return True if the last build_context() call used the cache."""
        return self.used_cache
