"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.enrichment.bank_enricher
===============================================================================

Description:
    Domain service that enriches transformed SAP records with bank transaction
    data. Joins on bank_ref_1 using two matching levels: exact reference match
    and smart suffix matching for truncated references.

Responsibilities:
    - Clean bank reference strings before matching.
    - Perform a standard left join on bank_ref_1 = ref_clean.
    - Recover unmatched (orphan) SAP records via suffix-based smart matching.
    - Map raw bank column names to staging column names.
    - Report enrichment statistics (total, enriched, rate).

Key Components:
    - BankEnricher: Stateless domain service that accepts two DataFrames
      (SAP and bank) and returns an enriched SAP DataFrame.

Notes:
    - Domain service — receives DataFrames, has no database or file I/O.
    - Smart matching: if bank_ref_1 has >= 6 characters and no exact match,
      search for bank references that END WITH bank_ref_1. First match wins.
    - Column mapping: fecha_transaccion → bank_date, referencia2 → bank_ref_2,
      descripcion → bank_description, oficina → bank_office_id.

Dependencies:
    - pandas
    - typing
    - utils.data_cleaner
    - utils.logger

===============================================================================
"""

import pandas as pd
from typing import Dict
from utils.data_cleaner import DataCleaner
from utils.logger import get_logger


class BankEnricher:
    """
    Enriches SAP records with bank transaction data.

    Matching algorithm:
        Level 1 — Exact join: bank_ref_1 = ref_clean (direct reference match).
        Level 2 — Smart matching: for orphan SAP records with bank_ref_1 >= 6
            characters, find bank references that end with bank_ref_1.
            Example: SAP "438649" matches bank "1538438649".

    Input DataFrames:
        df_sap  — transformed SAP records; must include bank_ref_1.
        df_bank — raw bank records; must include fecha_transaccion, referencia,
                  referencia2, descripcion, oficina.

    Output: df_sap enriched with bank_date, bank_ref_2, bank_description,
            bank_office_id. Unmatched rows retain None in bank columns.
    """

    def __init__(self):
        self.logger = get_logger("BANK_ENRICHER")

    def enrich(
        self,
        df_sap: pd.DataFrame,
        df_bank: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Enrich SAP records with bank data using a two-level matching strategy.

        Steps:
            1. Clean bank references and deduplicate.
            2. Left join on bank_ref_1 = ref_clean.
            3. Apply smart suffix matching to unmatched rows.
            4. Map bank column names to staging names.

        Args:
            df_sap: Transformed SAP DataFrame. Requires bank_ref_1 column.
            df_bank: Raw bank DataFrame. Requires fecha_transaccion, referencia,
                     referencia2, descripcion, oficina.

        Returns:
            df_sap enriched with bank_date, bank_ref_2, bank_description,
            bank_office_id. Returns df_sap with null bank columns if df_bank
            is empty.
        """

        self.logger(
            f"Enriching {len(df_sap)} SAP records with bank data",
            "INFO"
        )

        # Return SAP unchanged if no bank data is available
        if df_bank.empty:
            self.logger("No bank data available for enrichment", "WARN")

            df_sap['bank_date'] = None
            df_sap['bank_ref_2'] = None
            df_sap['bank_description'] = None
            df_sap['bank_office_id'] = None

            return df_sap

        # 1. Clean bank references
        df_bank_clean = self._clean_bank_references(df_bank)

        # 2. Exact join
        df_merged = self._join_normal(df_sap, df_bank_clean)

        # 3. Smart matching for unmatched rows
        df_final = self._smart_match_orphans(df_merged, df_bank_clean)

        # 4. Map column names to staging schema
        df_final = self._map_bank_columns(df_final)

        enriched_count = df_final['bank_date'].notna().sum()
        self.logger(
            f"Enrichment complete: {enriched_count}/{len(df_final)} records matched",
            "SUCCESS"
        )

        return df_final

    def _clean_bank_references(
        self,
        df_bank: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Normalize bank reference strings and remove duplicates.

        Creates a ref_clean column (trimmed, uppercased) and keeps the first
        occurrence when multiple rows share the same reference.

        Args:
            df_bank: Raw bank DataFrame.

        Returns:
            Copy of df_bank with ref_clean column and duplicates removed.
        """

        df = df_bank.copy()

        df['ref_clean'] = DataCleaner.clean_string(df['referencia'])

        original_count = len(df)
        df = df.drop_duplicates(subset=['ref_clean'], keep='first')

        duplicates_removed = original_count - len(df)
        if duplicates_removed > 0:
            self.logger(f"Removed {duplicates_removed} duplicate bank references", "INFO")

        self.logger(f"Clean bank references: {len(df)}", "INFO")

        return df

    def _join_normal(
        self,
        df_sap: pd.DataFrame,
        df_bank: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Left join SAP records to bank data on bank_ref_1 = ref_clean.

        Args:
            df_sap: Transformed SAP DataFrame.
            df_bank: Cleaned bank DataFrame.

        Returns:
            Merged DataFrame (left join preserving all SAP rows).
        """

        merged = pd.merge(
            df_sap,
            df_bank,
            left_on='bank_ref_1',
            right_on='ref_clean',
            how='left'
        )

        matched_count = merged['ref_clean'].notna().sum()
        self.logger(f"Exact join: {matched_count}/{len(df_sap)} matches", "INFO")

        return merged

    def _smart_match_orphans(
        self,
        df_merged: pd.DataFrame,
        df_bank: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Apply suffix-based matching to SAP records that had no exact match.

        For each unmatched SAP row whose bank_ref_1 has >= 6 characters, search
        for bank references that END WITH bank_ref_1. First match found is used.

        Example:
            SAP bank_ref_1 = "438649"
            Bank ref_clean = "1538438649"  → ends with "438649" → match.

        Args:
            df_merged: Result of the exact join.
            df_bank: Cleaned bank DataFrame used as the search pool.

        Returns:
            df_merged with smart-matched rows filled in.
        """

        mask_orphans = (
            (df_merged['bank_ref_1'].notna()) &
            (df_merged['ref_clean'].isna()) &
            (df_merged['bank_ref_1'].str.len() >= 6)
        )

        if not mask_orphans.any():
            self.logger("No orphan records for smart matching", "INFO")
            return df_merged

        orphan_count = mask_orphans.sum()
        self.logger(f"Attempting smart matching for {orphan_count} orphan records", "INFO")

        orphan_refs = df_merged.loc[mask_orphans, 'bank_ref_1'].unique()
        bank_refs_pool = df_bank['ref_clean'].dropna().astype(str).unique()

        # Build suffix-match mapping: SAP ref → bank ref
        mapping = {}
        for sap_ref in orphan_refs:
            matches = [b_ref for b_ref in bank_refs_pool if b_ref.endswith(sap_ref)]
            if matches:
                mapping[sap_ref] = matches[0]

        if not mapping:
            self.logger("Smart matching: 0 references recovered", "INFO")
            return df_merged

        self.logger(f"Smart matching: {len(mapping)} references recovered", "SUCCESS")

        # Apply matches
        for sap_ref, bank_ref in mapping.items():
            bank_row = df_bank[df_bank['ref_clean'] == bank_ref].iloc[0]
            idx_to_update = df_merged[df_merged['bank_ref_1'] == sap_ref].index

            df_merged.loc[idx_to_update, 'fecha_transaccion'] = bank_row['fecha_transaccion']
            df_merged.loc[idx_to_update, 'referencia2'] = bank_row.get('referencia2')
            df_merged.loc[idx_to_update, 'descripcion'] = bank_row.get('descripcion')
            df_merged.loc[idx_to_update, 'oficina'] = bank_row.get('oficina')
            df_merged.loc[idx_to_update, 'ref_clean'] = bank_ref

        return df_merged

    def _map_bank_columns(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Rename raw bank columns to their staging schema equivalents.

        Mapping:
            fecha_transaccion → bank_date
            referencia2       → bank_ref_2
            descripcion       → bank_description
            oficina           → bank_office_id

        Args:
            df: Merged DataFrame with raw bank column names.

        Returns:
            DataFrame with staging column names; temporary columns removed.
        """

        rename_map = {
            'fecha_transaccion': 'bank_date',
            'referencia2': 'bank_ref_2',
            'descripcion': 'bank_description',
            'oficina': 'bank_office_id'
        }

        df = df.rename(columns=rename_map)
        df = df.drop(columns=['ref_clean', 'referencia'], errors='ignore')

        return df

    def get_enrichment_stats(
        self,
        df: pd.DataFrame
    ) -> Dict[str, int]:
        """
        Return enrichment statistics for a processed DataFrame.

        Args:
            df: Enriched DataFrame with bank_date column.

        Returns:
            Dict with keys: total, enriched, not_enriched, enrichment_rate.
        """

        total = len(df)
        enriched = df['bank_date'].notna().sum()
        not_enriched = total - enriched
        enrichment_rate = (enriched / total * 100) if total > 0 else 0

        return {
            'total': total,
            'enriched': enriched,
            'not_enriched': not_enriched,
            'enrichment_rate': enrichment_rate
        }
