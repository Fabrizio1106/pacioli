"""
===============================================================================
Project: PACIOLI
Module: utils.data_cleaner
===============================================================================

Description:
    Collection of vectorized pandas helpers used by the bronze-layer
    loaders to normalize string and numeric Series consistently. All
    methods are static and side-effect free, returning new Series.

Responsibilities:
    - Normalize string series for consistent comparison and storage.
    - Parse numeric series coercing errors to zero.
    - Extract the leading token from free-text references.
    - Produce strict two-decimal string formatting for stable hashing.

Key Components:
    - DataCleaner.clean_string: Upper, strip, null-token cleanup.
    - DataCleaner.parse_decimal: Numeric coercion with rounding.
    - DataCleaner.extract_numeric_ref: First whitespace-delimited token.
    - DataCleaner.format_decimal_strict: Fixed two-decimal string format.

Notes:
    - format_decimal_strict is specifically designed to support
      deterministic hash generation across sources.

Dependencies:
    - pandas
    - re

===============================================================================
"""

import pandas as pd
import re

class DataCleaner:
    """
    Vectorized helpers for cleaning pandas Series in the bronze layer.

    Purpose:
        Provide reusable, side-effect free string and numeric helpers so
        that every loader applies the same normalization rules.

    Responsibilities:
        - String normalization (upper, strip, null-token cleanup).
        - Numeric parsing with safe defaults.
        - Reference extraction and strict decimal formatting for hashing.
    """

    @staticmethod
    def clean_string(series: pd.Series) -> pd.Series:
        """
        Normalize a string series: uppercase, trimmed and null-token free.

        Args:
            series (pd.Series): Input string (or stringifiable) series.

        Returns:
            pd.Series: Cleaned string series with 'NAN', 'NONE', 'NAT'
            and 'NULL' placeholders collapsed to ''.
        """
        return (
            series.astype(str)
            .str.strip()
            .str.upper()
            .replace({'NAN': '', 'NONE': '', 'NAT': '', 'NULL': ''}, regex=False)
        )

    @staticmethod
    def parse_decimal(series: pd.Series) -> pd.Series:
        """
        Coerce a series to float rounded to two decimals.

        Args:
            series (pd.Series): Source series.

        Returns:
            pd.Series: Numeric series with parsing errors coerced to 0.0
            and every value rounded to two decimals.
        """
        cleaned = pd.to_numeric(series, errors='coerce').fillna(0.0)
        return cleaned.round(2)
    
    @staticmethod
    def extract_numeric_ref(text_series: pd.Series) -> pd.Series:
        """
        Extract the first whitespace-delimited token from each string.

        Args:
            text_series (pd.Series): Source text series.

        Returns:
            pd.Series: Series of leading tokens, used to recover the
            SAP-Bank link reference.

        Notes:
            Example: '18384658-1427 PAGO...' -> '18384658-1427'.
        """
        return text_series.astype(str).str.extract(r'^(\S+)')[0]

    @staticmethod
    def format_decimal_strict(series: pd.Series) -> pd.Series:
        """
        Format numbers as strings with exactly two decimal digits.

        Args:
            series (pd.Series): Source series.

        Returns:
            pd.Series: String series where values are rendered with two
            fixed decimals (e.g. 7.8 -> '7.80').

        Notes:
            Designed to guarantee hash stability across heterogeneous
            numeric representations of the same value.
        """
        nums = pd.to_numeric(series, errors='coerce').fillna(0.0)
        return nums.apply(lambda x: "{:.2f}".format(x))