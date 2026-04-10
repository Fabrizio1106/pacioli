"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.hashing.hash_generator
===============================================================================

Description:
    Domain service that generates match_hash_key identifiers for SAP transactions
    using historical sequence counters. Ensures uniqueness across ETL runs by
    reading pre-computed counters from HistoricalContextService and continuing
    sequences from where previous runs left off.

Responsibilities:
    - Sort the DataFrame for deterministic hash generation order.
    - Prepare brand, batch_number, and batch_clean columns.
    - Generate match_hash_key per row using brand-specific strategies.
    - Validate that the sequence counter is always >= 1 (never zero).
    - Log duplicate hash warnings if uniqueness is violated.

Key Components:
    - HashGenerator: Domain service; requires a pre-built HistoricalContextService.
      Returns the DataFrame with match_hash_key added.

Notes:
    - Two hash strategies:
        PACIFICARD (with valid batch): BRAND_BATCH_AMOUNT_SEQ
          Example: PACIFICARD_001032_49.68_1
        All others:                   BRAND_AMOUNT_SEQ
          Example: VISA_23.66_1
    - v2.1 fix: sequence counter is validated and forced to >= 1 to prevent
      match_hash_key values ending in _0 (e.g., DINERS CLUB_141.96_0).
    - Must run AFTER TransactionClassifier (brand column required) and
      AFTER HistoricalContextService.build_context() (counter column required).

Dependencies:
    - pandas, numpy, typing
    - logic.domain.services.hashing.historical_context_service
    - utils.logger

===============================================================================
"""

import pandas as pd
import numpy as np
from typing import Optional
from logic.domain.services.hashing.historical_context_service import HistoricalContextService
from utils.logger import get_logger


class HashGenerator:
    """
    Domain service for generating match_hash_key with historical sequence continuity.

    Hash strategies:
        PACIFICARD (batch present): BRAND_BATCH_AMOUNT_SEQ
            Example: PACIFICARD_001032_49.68_1
        All other brands:          BRAND_AMOUNT_SEQ
            Example: VISA_23.66_1

    Sequence counters are read from _historical_counter (pre-computed by
    HistoricalContextService.build_context()) and validated to be >= 1.

    v2.1 fix: counter values of 0 or negative are forced to 1 to prevent
    hash keys ending in _0 (e.g., DINERS CLUB_141.96_0 -> DINERS CLUB_141.96_1).

    Prerequisite: HistoricalContextService must have already called build_context()
    before HashGenerator.generate() is called.
    """

    def __init__(self, context_service: HistoricalContextService):
        """
        Args:
            context_service: Pre-built HistoricalContextService instance.
                             The DataFrame passed to generate() must already
                             contain _historical_counter from build_context().
        """
        self.context_service = context_service
        self.logger = get_logger("HASH_GENERATOR")

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate match_hash_key for all transactions in the DataFrame.

        Steps:
            1. Sort DataFrame by bank_date and bank_ref_1 for deterministic order.
            2. Prepare brand, batch_number, and batch_clean columns.
            3. Generate hash for each row using the pre-computed counter.
            4. Log duplicate hash warnings if uniqueness is violated.

        Args:
            df: DataFrame with columns: brand, amount_total, batch_number (optional),
                bank_date (optional, for sort), bank_ref_1 (optional, for sort),
                _historical_counter (required, set by HistoricalContextService).

        Returns:
            DataFrame with match_hash_key column added.
        """
        self.logger(f"Generating hashes for {len(df)} transactions", "INFO")

        df = df.copy()

        # 1. Sort for deterministic hash generation
        df = self._sort_for_consistency(df)

        # 2. Prepare columns
        df = self._prepare_columns(df)

        # 3. Generate hashes row by row
        hashes = []
        for idx, row in df.iterrows():
            hash_key = self._generate_hash_for_row(row)
            hashes.append(hash_key)

        df['match_hash_key'] = hashes

        self.logger(f"Hashes generated: {len(hashes)}", "SUCCESS")

        # Warn if any duplicates were produced
        unique_hashes = df['match_hash_key'].nunique()
        if unique_hashes != len(df):
            self.logger(
                f"WARNING: {len(df) - unique_hashes} duplicate hashes detected",
                "WARN"
            )

        return df

    def _sort_for_consistency(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sort the DataFrame to ensure the same input always produces the same hashes.

        Sort order: bank_date (if present), then bank_ref_1 (if present).
        Consistent ordering guarantees idempotent hash generation across runs.
        """
        sort_columns = []

        if 'bank_date' in df.columns:
            sort_columns.append('bank_date')

        if 'bank_ref_1' in df.columns:
            sort_columns.append('bank_ref_1')

        if sort_columns:
            df = df.sort_values(by=sort_columns)
            self.logger(f"DataFrame sorted by: {sort_columns}", "INFO")

        return df

    def _prepare_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare brand, batch_number, and batch_clean columns for hash generation.

        batch_clean: zero-padded to 6 digits, trailing .0 removed.
        brand: defaults to 'GENERIC' if absent or null.
        """
        if 'brand' not in df.columns:
            df['brand'] = 'GENERIC'
        else:
            df['brand'] = df['brand'].fillna('GENERIC').astype(str).str.strip().str.upper()

        if 'batch_number' not in df.columns:
            df['batch_number'] = None

        df['batch_clean'] = (
            df['batch_number']
            .fillna('0')
            .astype(str)
            .str.replace(r'\.0$', '', regex=True)
            .str.zfill(6)
        )

        return df

    def _generate_hash_for_row(self, row: pd.Series) -> str:
        """
        Generate the match_hash_key for a single row.

        Reads _historical_counter from the row (pre-computed by
        HistoricalContextService). Validates the counter is >= 1; forces
        it to 1 if zero or invalid (v2.1 fix).

        Strategies:
            PACIFICARD with valid batch: BRAND_BATCH_AMOUNT_SEQ
            All others:                 BRAND_AMOUNT_SEQ
        """
        brand = row['brand']
        amount = row['amount_total']
        batch_clean = row.get('batch_clean', '0')

        # v2.1 fix: validate counter is always >= 1
        seq_raw = row.get('_historical_counter', 1)
        try:
            seq = int(seq_raw)
            if seq <= 0:
                self.logger(
                    f"Invalid counter ({seq}) for {brand} {amount}, forcing to 1",
                    "WARN"
                )
                seq = 1
        except (ValueError, TypeError):
            self.logger(
                f"Non-numeric counter ({seq_raw}) for {brand} {amount}, using 1",
                "WARN"
            )
            seq = 1

        try:
            amount_str = "{:.2f}".format(float(amount))
        except Exception:
            amount_str = str(amount)

        is_pacificard = 'PACIFICARD' in str(brand).upper() or 'PCF' in str(brand).upper()

        if is_pacificard and batch_clean != '000000':
            return f"{brand}_{batch_clean}_{amount_str}_{seq}"
        else:
            return f"{brand}_{amount_str}_{seq}"
