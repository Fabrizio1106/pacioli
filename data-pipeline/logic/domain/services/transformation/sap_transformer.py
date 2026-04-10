"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.transformation.sap_transformer
===============================================================================

Description:
    Domain service that transforms raw SAP financial document data into clean,
    normalized records ready for staging and classification. Applies all
    SAP-specific normalization rules via a YAML-configured column mapping.

Responsibilities:
    - Rename columns from raw SAP names to staging schema names (YAML mapping).
    - Apply fallback column renaming for columns not covered by the YAML mapping.
    - Clean and normalize string columns (trim, uppercase, collapse whitespace).
    - Normalize amount columns: separate absolute value from sign.
    - Convert is_compensated_sap to boolean (non-empty value = True).
    - Extract numeric bank reference from sap_description text.
    - Convert date columns to datetime.

Key Components:
    - SAPTransformer: Domain service; receives a raw DataFrame from SAPExtractor
      and returns a clean, typed DataFrame ready for classification and hashing.

Notes:
    - amount_raw preserves the original signed value; amount_total is always
      positive; amount_sign carries '+' or '-' for downstream calculations.
    - bank_ref_1 is extracted from the first long numeric sequence in
      sap_description using DataCleaner.extract_numeric_ref().
    - is_compensated_sap: any non-null, non-empty value in the SAP compensation
      document field is treated as True (compensated).
    - Column mapping is injected at construction time and sourced from YAML;
      no column names are hardcoded in the transformer.

Dependencies:
    - pandas, numpy, typing
    - utils.data_cleaner, utils.logger

===============================================================================
"""

import pandas as pd
import numpy as np
from typing import Dict
from utils.data_cleaner import DataCleaner
from utils.logger import get_logger


class SAPTransformer:
    """
    Domain service for transforming raw SAP financial document data.

    Responsibilities:
        1. Rename columns from raw SAP names to staging schema names.
        2. Apply fallback renaming for columns missing from the YAML mapping.
        3. Clean string columns (trim, uppercase, collapse whitespace).
        4. Normalize amounts: absolute value + sign + raw signed value.
        5. Convert is_compensated_sap to boolean.
        6. Extract bank_ref_1 from sap_description text.
        7. Convert date columns to datetime.

    Out of scope (handled by other services):
        - Business classification (TransactionClassifier).
        - Joins with other tables (enrichers).
        - Hash generation (HashGenerator).

    Column mapping is injected at construction time from the YAML configuration.
    """

    def __init__(self, column_mapping: Dict[str, str]):
        """
        Args:
            column_mapping: Dict mapping raw SAP column names to staging schema
                names. Sourced from YAML (e.g., {'fecha_documento': 'doc_date'}).
                No column names are hardcoded; all mappings must be injected.
        """
        self.column_mapping = column_mapping
        self.logger = get_logger("SAP_TRANSFORMER")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform a raw SAP DataFrame into a clean staging DataFrame.

        Steps:
            1. Rename columns (YAML mapping).
            2. Apply fallback column fixes for critical columns.
            3. Clean string columns.
            4. Normalize amount columns (absolute value + sign).
            5. Convert is_compensated_sap to boolean.
            6. Extract bank_ref_1 from sap_description.
            7. Convert date columns to datetime.

        Args:
            df: Raw DataFrame from SAPExtractor. Expected raw columns include
                fecha_documento, num_documento, clase_documento, asignacion,
                texto, importe_ml, doc_compensacion, moneda_local.

        Returns:
            Clean DataFrame with staging columns including:
                doc_date, doc_number, doc_type, doc_reference, sap_description,
                amount_total (absolute), amount_raw (signed), amount_sign,
                currency, is_compensated_sap (bool), bank_ref_1.
        """
        self.logger("Starting SAP data transformation", "INFO")

        df = df.copy()

        # 1. Rename columns
        df = self._rename_columns(df)

        # 2. Apply fallback column fixes
        df = self._fix_missing_columns(df)

        # 3. Clean strings
        df = self._clean_strings(df)

        # 4. Normalize amounts
        df = self._normalize_amounts(df)

        # 5. Convert is_compensated_sap to boolean
        df = self._convert_compensated_flag(df)

        # 6. Extract bank reference from text
        df = self._extract_bank_reference(df)

        # 7. Convert dates
        df = self._convert_dates(df)

        self.logger(f"Transformation complete: {len(df)} records", "SUCCESS")

        return df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename columns according to the injected YAML column mapping."""
        df = df.rename(columns=self.column_mapping)
        self.logger("Columns renamed per YAML mapping", "INFO")
        return df

    def _fix_missing_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply fallback renaming for critical columns not covered by the YAML mapping.

        Fallback cases:
            1. 'is_compensated_sap' missing, 'doc_compensacion' present.
            2. 'amount_total' missing, 'importe_ml' present.
            3. 'doc_reference' missing, 'asignacion' present.
            4. 'sap_description' missing, 'texto' present.
        """
        if 'is_compensated_sap' not in df.columns and 'doc_compensacion' in df.columns:
            df.rename(columns={'doc_compensacion': 'is_compensated_sap'}, inplace=True)
            self.logger("Fallback: doc_compensacion -> is_compensated_sap", "INFO")

        if 'amount_total' not in df.columns and 'importe_ml' in df.columns:
            df.rename(columns={'importe_ml': 'amount_total'}, inplace=True)
            self.logger("Fallback: importe_ml -> amount_total", "INFO")

        if 'doc_reference' not in df.columns and 'asignacion' in df.columns:
            df.rename(columns={'asignacion': 'doc_reference'}, inplace=True)
            self.logger("Fallback: asignacion -> doc_reference", "INFO")

        if 'sap_description' not in df.columns and 'texto' in df.columns:
            df.rename(columns={'texto': 'sap_description'}, inplace=True)
            self.logger("Fallback: texto -> sap_description", "INFO")

        return df

    def _clean_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean string columns: trim, uppercase, collapse whitespace."""
        cols_to_clean = ['doc_number', 'doc_type', 'doc_reference', 'sap_description']
        for col in cols_to_clean:
            if col in df.columns:
                df[col] = DataCleaner.clean_string(df[col])
        self.logger("Strings cleaned", "INFO")
        return df

    def _normalize_amounts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize amount columns: separate absolute value, raw signed value, and sign.

        Output columns:
            amount_raw:   original signed float (e.g., -69.00, 1000.50).
            amount_total: absolute value rounded to 2 decimals (always positive).
            amount_sign:  '+' or '-' string for downstream calculations.
        """
        if 'amount_total' in df.columns:
            df['amount_raw'] = pd.to_numeric(df['amount_total'], errors='coerce').fillna(0)
            df['amount_total'] = df['amount_raw'].abs().round(2)
            df['amount_sign'] = df['amount_raw'].apply(lambda x: '+' if x >= 0 else '-')
            self.logger(f"Amounts normalized: {len(df)} records", "INFO")
        return df

    def _convert_compensated_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert is_compensated_sap to boolean.

        Any non-null, non-empty value in the SAP compensation document field
        is treated as True (the document has been compensated). Null or empty
        values become False.
        """
        if 'is_compensated_sap' in df.columns:
            df['is_compensated_sap'] = (
                df['is_compensated_sap'].notna() &
                (df['is_compensated_sap'].astype(str).str.strip() != '')
            )
            self.logger("is_compensated_sap converted to boolean", "INFO")
        else:
            df['is_compensated_sap'] = False
            self.logger("is_compensated_sap not found; defaulting to False", "WARN")
        return df

    def _extract_bank_reference(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract numeric bank reference from sap_description text.

        Delegates to DataCleaner.extract_numeric_ref(), which returns the first
        long numeric sequence found in the text field.
        Example: "1538438649 GO RVICIOS RIOS SH..." -> bank_ref_1 = "1538438649".
        """
        if 'sap_description' in df.columns:
            df['bank_ref_1'] = DataCleaner.extract_numeric_ref(df['sap_description'])
            extracted_count = df['bank_ref_1'].notna().sum()
            self.logger(f"Bank references extracted: {extracted_count}/{len(df)}", "INFO")
        else:
            df['bank_ref_1'] = None
            self.logger("sap_description not found; bank_ref_1 set to None", "WARN")
        return df

    def _convert_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert doc_date and posting_date columns to datetime."""
        if 'doc_date' in df.columns:
            df['doc_date'] = pd.to_datetime(df['doc_date'])
            self.logger("doc_date converted to datetime", "INFO")

        if 'posting_date' in df.columns:
            df['posting_date'] = pd.to_datetime(df['posting_date'])
            self.logger("posting_date converted to datetime", "INFO")

        return df
