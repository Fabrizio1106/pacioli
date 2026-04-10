"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.classification.transaction_classifier
===============================================================================

Description:
    Domain service that classifies SAP transactions using YAML-configured
    tagging rules applied against a combined search bag of text fields.
    Classification is ordered and non-overwriting: the first matching rule wins.

Responsibilities:
    - Build a search_bag field combining SAP and bank text columns for pattern
      matching (sap_description, doc_reference, doc_type, bank_description,
      bank_ref_1, bank_ref_2).
    - Initialize classification columns with default values (PENDIENTE / OTROS / NA).
    - Apply YAML tagging rules in order; skip already-classified records.
    - Extract additional metadata (e.g., batch_number) via per-rule regex.
    - Log a classification summary (by global_category / trans_type / brand).

Key Components:
    - TransactionClassifier: Domain service; receives a DataFrame and a list
      of tagging rules, returns the DataFrame with classification columns added.

Notes:
    - Rule priority: rules are applied in YAML order. Once a record is classified
      (global_category != 'PENDIENTE'), subsequent rules do not overwrite it.
    - search_bag includes bank columns (bank_description, bank_ref_1, bank_ref_2)
      when present, enabling rules to match on enriched bank data after the
      BankEnricher has run.
    - For best accuracy, classify AFTER bank enrichment so search_bag contains
      all available text signals.
    - extract_metadata: optional per-rule configuration to capture a regex group
      from search_bag into a target column (e.g., batch_number from LOTE pattern).

Dependencies:
    - pandas, warnings, typing
    - utils.logger

===============================================================================
"""

import pandas as pd
import warnings
from typing import List, Dict, Any
from utils.logger import get_logger


class TransactionClassifier:
    """
    Domain service for classifying transactions using YAML tagging rules.

    Classification strategy:
        1. Build search_bag from all available text columns (SAP + bank).
        2. Initialize classification columns with default values.
        3. Apply YAML rules in order; each rule matches against search_bag.
        4. First matching rule wins — already-classified records are skipped.
        5. Optionally extract metadata (e.g., batch_number) via regex.

    Tagging rule structure (from YAML):
        pattern:          Regex pattern matched against search_bag (case-insensitive).
        category:         Value assigned to global_category on match.
        transaction_type: Value assigned to trans_type on match.
        brand:            Value assigned to brand on match.
        extract_metadata: Optional; contains target_col and regex for metadata capture.
    """

    def __init__(self, tagging_rules: List[Dict[str, Any]]):
        """
        Args:
            tagging_rules: List of rule dicts loaded from the YAML configuration.
                           Each rule must have at least a 'pattern' key.
        """
        self.tagging_rules = tagging_rules
        self.logger = get_logger("TRANSACTION_CLASSIFIER")

    def classify(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Classify all transactions in the DataFrame using YAML tagging rules.

        Steps:
            1. Build search_bag from SAP and bank text columns.
            2. Initialize classification columns.
            3. Apply rules in order (first match wins).
            4. Log classification results.

        Args:
            df: DataFrame with SAP columns (sap_description, doc_reference,
                doc_type) and optionally bank columns (bank_description,
                bank_ref_1, bank_ref_2) after enrichment.

        Returns:
            DataFrame with additional columns: global_category, trans_type,
            brand, batch_number, search_bag.
        """
        self.logger(
            f"Classifying {len(df)} transactions with {len(self.tagging_rules)} rules",
            "INFO"
        )

        df = df.copy()

        # 1. Build search bag from all text columns
        df = self._create_search_bag(df)

        # 2. Initialize classification columns with defaults
        df = self._initialize_columns(df)

        # 3. Apply rules in order
        df = self._apply_rules(df)

        # 4. Log results
        self._log_classification_results(df)

        return df

    def _create_search_bag(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Build the search_bag field by concatenating all relevant text columns.

        Column priority (all uppercased, NaN-safe):
            1. sap_description  — SAP document description text.
            2. doc_reference    — SAP assignment / reference field.
            3. doc_type         — SAP document type code.
            4. bank_description — Bank transaction description (post-enrichment).
            5. bank_ref_1       — Primary bank reference (post-enrichment).
            6. bank_ref_2       — Secondary bank reference (post-enrichment).

        Bank columns are included when present, enabling YAML rules to match on
        bank text signals after BankEnricher has run.
        """
        sap_desc = df.get('sap_description', pd.Series([''] * len(df)))
        doc_ref = df.get('doc_reference', pd.Series([''] * len(df)))
        doc_type = df.get('doc_type', pd.Series([''] * len(df)))

        bank_desc = df.get('bank_description', pd.Series([''] * len(df)))
        bank_ref1 = df.get('bank_ref_1', pd.Series([''] * len(df)))
        bank_ref2 = df.get('bank_ref_2', pd.Series([''] * len(df)))

        df['search_bag'] = (
            sap_desc.fillna('').astype(str) + " " +
            doc_ref.fillna('').astype(str) + " " +
            doc_type.fillna('').astype(str) + " " +
            bank_desc.fillna('').astype(str) + " " +
            bank_ref1.fillna('').astype(str) + " " +
            bank_ref2.fillna('').astype(str)
        ).str.upper()

        has_bank_desc = (bank_desc != '').sum()
        self.logger(
            f"search_bag built: {has_bank_desc}/{len(df)} records with bank_description",
            "INFO"
        )

        return df

    def _initialize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Initialize classification columns with default values."""
        df['global_category'] = 'PENDIENTE'
        df['trans_type'] = 'OTROS'
        df['brand'] = 'NA'
        df['batch_number'] = None
        return df

    def _apply_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply tagging rules in YAML order against the search_bag.

        Rules are evaluated only against records that are still PENDIENTE.
        Once a record is classified, no subsequent rule can overwrite it.
        This ensures rule priority matches declaration order in the YAML.
        """
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)

            for rule in self.tagging_rules:
                pattern = rule.get('pattern')

                if not pattern:
                    continue

                # Only evaluate records not yet classified
                mask_pending = df['global_category'] == 'PENDIENTE'

                matches = df.loc[mask_pending, 'search_bag'].str.contains(
                    pattern,
                    regex=True,
                    na=False
                )

                if matches.any():
                    idx = df.loc[mask_pending][matches].index

                    if 'category' in rule:
                        df.loc[idx, 'global_category'] = str(rule['category']).strip().upper()

                    if 'transaction_type' in rule:
                        df.loc[idx, 'trans_type'] = str(rule['transaction_type']).strip().upper()

                    if 'brand' in rule:
                        df.loc[idx, 'brand'] = str(rule['brand']).strip().upper()

                    if 'extract_metadata' in rule:
                        self._extract_metadata(df, idx, rule['extract_metadata'])

        return df

    def _extract_metadata(
        self,
        df: pd.DataFrame,
        idx: pd.Index,
        metadata_config: Dict[str, str]
    ) -> None:
        r"""
        Extract additional metadata from search_bag using a per-rule regex.

        YAML configuration:
            extract_metadata:
              target_col: "batch_number"
              regex: '(?i)LOTE-?\s*(\d+)'

        Example: "LOTE-123" -> batch_number = "123".
        """
        target_col = metadata_config.get('target_col')
        regex = metadata_config.get('regex')

        if not target_col or not regex:
            return

        extracted = df.loc[idx, 'search_bag'].str.extract(regex, expand=False)

        if not extracted.empty:
            df.loc[idx, target_col] = extracted

    def _log_classification_results(self, df: pd.DataFrame) -> None:
        """Log a summary of classification results by category, type, and brand."""
        classified = (df['global_category'] != 'PENDIENTE').sum()
        pending = (df['global_category'] == 'PENDIENTE').sum()

        self.logger(
            f"Classification complete: {classified} classified, {pending} pending",
            "INFO"
        )

        if classified > 0:
            summary = df[df['global_category'] != 'PENDIENTE'].groupby([
                'global_category',
                'trans_type',
                'brand'
            ]).size()

            self.logger("Distribution by category:", "INFO")
            for (cat, ttype, brand), count in summary.items():
                self.logger(f"  {cat} / {ttype} / {brand}: {count}", "INFO")

    def get_classification_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Return a classification summary DataFrame for analysis.

        Returns:
            DataFrame with columns: global_category, trans_type, brand, count.
            Sorted by count descending.
        """
        count_col = 'match_hash_key' if 'match_hash_key' in df.columns else 'search_bag'

        summary = df.groupby([
            'global_category',
            'trans_type',
            'brand'
        ]).agg(count=(count_col, 'count')).reset_index().sort_values('count', ascending=False)

        return summary
