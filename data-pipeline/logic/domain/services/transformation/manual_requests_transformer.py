"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.transformation.manual_requests_transformer
===============================================================================

Description:
    Domain service that transforms manual reconciliation requests, applying
    reference explosion logic to normalize multi-reference entries. A single
    record with multiple bank references (separated by '/') is expanded into
    one record per reference for individual matching.

Responsibilities:
    - Split multi-reference bank_ref fields by configurable separator (default '/').
    - Tag expanded records as EXPLODED for audit traceability.
    - Clean individual references (strip whitespace and hyphens).
    - Map raw column names to staging schema column names.

Key Components:
    - ManualRequestsTransformer: Domain service for manual request normalization.

Notes:
    - Reference explosion: "12345/67890" -> two rows with bank_ref '12345' and '67890'.
    - EXPLODED records indicate a single payment split across multiple transfers.
    - Output may have MORE rows than input (one per reference after explosion).
    - Configuration from staging_manual_requests_rules.yaml:
        split_logic.separator (default '/'), clean_logic.strip_chars (default ' -').

Dependencies:
    - pandas
    - typing
    - utils.logger

===============================================================================
"""

import pandas as pd
from typing import Dict, Any
from utils.logger import get_logger


class ManualRequestsTransformer:
    """
    Domain service for transforming manual reconciliation requests.

    Reference explosion logic:
        Before: ref_banco = "12345/67890"  (1 record)
        After:  bank_ref = "12345" (EXPLODED)
                bank_ref = "67890" (EXPLODED)
                                   (2 records)

    Configuration from staging_manual_requests_rules.yaml:
        split_logic.separator: Reference separator (default '/').
        clean_logic.strip_chars: Characters to strip from each reference (default ' -').
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Args:
            config: Parsed YAML dict from staging_manual_requests_rules.yaml.
        """
        self.config = config
        self.logger = get_logger("MANUAL_REQUESTS_TRANSFORMER")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform manual request records with reference explosion.

        Steps:
            1. Explode multi-reference entries (split by separator).
            2. Tag exploded records (normalization_tag = EXPLODED | NORMAL).
            3. Clean individual references (strip unwanted characters).
            4. Map columns to staging schema names.

        Args:
            df: Raw DataFrame from ManualRequestsExtractor.

        Returns:
            Transformed DataFrame. May have MORE rows than input due to
            reference explosion.
        """

        self.logger(f"Starting transformation of {len(df)} manual requests", "INFO")

        df = df.copy()

        # 1. Explode multi-reference entries
        df_exploded = self._explode_references(df)

        # 2. Clean individual references
        df_exploded = self._clean_references(df_exploded)

        # 3. Map columns to staging names
        df_final = self._map_columns(df_exploded)

        additional = len(df_final) - len(df)
        self.logger(
            f"Transformation complete: {len(df_final)} records ({additional} additional from explosion)",
            "SUCCESS"
        )

        return df_final

    def _explode_references(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Split multi-reference bank_ref fields and expand to one row per reference.

        Records with multiple references are tagged EXPLODED; single-reference
        records are tagged NORMAL. The normalization_tag supports audit tracing.

        Example:
            Before: raw_id=1, ref_banco="12345/67890"
            After:  raw_id=1, bank_ref_list="12345", normalization_tag="EXPLODED"
                    raw_id=1, bank_ref_list="67890", normalization_tag="EXPLODED"
        """
        split_cfg = self.config.get('split_logic', {})
        separator = split_cfg.get('separator', '/')

        self.logger(f"Exploding references with separator: '{separator}'", "INFO")

        df['bank_ref_list'] = (
            df['ref_banco']
            .fillna('')
            .astype(str)
            .str.split(separator)
        )

        # Identify IDs with multiple references for tagging
        ids_multi_ref = df[
            df['ref_banco'].fillna('').str.contains(separator, regex=False)
        ]['raw_id'].values

        df_exploded = df.explode('bank_ref_list').reset_index(drop=True)

        df_exploded['normalization_tag'] = df_exploded['raw_id'].apply(
            lambda x: 'EXPLODED' if x in ids_multi_ref else 'NORMAL'
        )

        count_diff = len(df_exploded) - len(df)
        if count_diff > 0:
            self.logger(f"Multi-references detected: +{count_diff} additional records", "INFO")

        exploded_count = (df_exploded['normalization_tag'] == 'EXPLODED').sum()
        normal_count = (df_exploded['normalization_tag'] == 'NORMAL').sum()
        self.logger(f"   EXPLODED: {exploded_count} | NORMAL: {normal_count}", "INFO")

        return df_exploded

    def _clean_references(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Strip unwanted characters from each reference.

        Configured by clean_logic.strip_chars (default ' -').
        Example: "  12345  " or "-12345-" -> "12345".
        """
        clean_cfg = self.config.get('clean_logic', {})
        strip_chars = clean_cfg.get('strip_chars', ' -')

        self.logger(f"Cleaning references (strip: '{strip_chars}')", "INFO")

        df['bank_ref_clean'] = (
            df['bank_ref_list']
            .astype(str)
            .str.strip(strip_chars)
        )

        return df

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map raw column names to staging schema column names.

        Mapping:
            raw_id             -> raw_id
            fecha              -> request_date
            cod_cliente        -> customer_id
            cliente            -> customer_name
            valor              -> amount
            bank_ref_clean     -> bank_ref
            estado_pago        -> payment_status
            factura            -> invoice_ref
            detalle            -> details
            normalization_tag  -> normalization_tag
        """
        df_final = pd.DataFrame()

        df_final['raw_id'] = df['raw_id']
        df_final['request_date'] = df['fecha']
        df_final['customer_id'] = df['cod_cliente']
        df_final['customer_name'] = df['cliente']
        df_final['amount'] = df['valor']
        df_final['bank_ref'] = df['bank_ref_clean']
        df_final['payment_status'] = df['estado_pago']
        df_final['invoice_ref'] = df['factura']
        df_final['details'] = df['detalle']
        df_final['normalization_tag'] = df['normalization_tag']

        self.logger("Columns mapped to staging schema names", "INFO")

        return df_final
