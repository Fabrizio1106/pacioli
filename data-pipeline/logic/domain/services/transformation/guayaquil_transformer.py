"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.transformation.guayaquil_transformer
===============================================================================

Description:
    Domain service that transforms raw Guayaquil (AMEX) voucher data into clean,
    normalized records ready for staging. Applies Guayaquil-specific business
    rules loaded from YAML configuration.

Responsibilities:
    - Clean and normalize all string columns.
    - Rename columns from raw to staging names (YAML column_mapping).
    - Convert amount columns to float.
    - Compute total commission as comision_base + comision_iva.
    - Map commerce codes to establishment names (YAML commerce_mapping).
    - Generate settlement_id in COMERCIO-MMDD format.
    - Generate double hash identifiers (voucher_hash_key + etl_hash).
    - Add fixed brand (AMEX) and metadata columns.

Key Components:
    - GuayaquilTransformer: Domain service; receives raw DataFrame, returns clean DataFrame.

Notes:
    - Brand is always AMEX (no normalization required).
    - settlement_id format: COMERCIO-MMDD (e.g., 68893-0202).
    - voucher_hash_key prefix: AMX_LOTE_REF_MONTO.
    - etl_hash: MD5(AMEX+LOTE+REF+AUTH+MONTO+YYYYMMDD).
    - All rules come from staging_guayaquil_rules.yaml.

Dependencies:
    - pandas, hashlib, typing
    - utils.data_cleaner, utils.logger

===============================================================================
"""

import pandas as pd
import hashlib
from typing import Dict, Any
from utils.data_cleaner import DataCleaner
from utils.logger import get_logger


class GuayaquilTransformer:
    """
    Domain service for transforming Guayaquil (AMEX) voucher data.

    Differences from other transformers:
        - Brand is always AMEX (fixed, no normalization).
        - Commerce mapping uses numeric codes.
        - settlement_id format: COMERCIO-MMDD.
        - Total commission = comision_base + comision_iva.

    All configuration comes from staging_guayaquil_rules.yaml:
        column_mapping, commerce_mapping.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Args:
            config: Parsed YAML dict from staging_guayaquil_rules.yaml.
        """
        self.config = config
        self.logger = get_logger("GUAYAQUIL_TRANSFORMER")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw Guayaquil data into a clean staging DataFrame.

        Steps:
            1. Clean strings.
            2. Rename columns (YAML mapping).
            3. Ensure required columns exist.
            4. Convert amount columns to float.
            5. Calculate total commission (base + IVA).
            6. Map commerce codes to establishment names.
            7. Generate settlement_id (COMERCIO-MMDD).
            8. Generate double hash IDs (voucher_hash_key + etl_hash).
            9. Add metadata columns.

        Args:
            df: Raw DataFrame from GuayaquilExtractor.

        Returns:
            Clean transformed DataFrame.
        """

        self.logger(f"Starting Guayaquil transformation for {len(df)} vouchers", "INFO")

        df = df.copy()

        # 1. Clean strings
        df = self._clean_strings(df)

        # 2. Rename columns
        df = self._rename_columns(df)

        # 3. Ensure required columns
        df = self._ensure_columns(df)

        # 4. Convert amount columns
        df = self._convert_numeric_columns(df)

        # 5. Calculate total commission
        df = self._calculate_commission(df)

        # 6. Map commerce codes
        df = self._map_commerces(df)

        # 7. Generate settlement_id
        df = self._generate_settlement_id(df)

        # 8. Generate IDs
        df = self._generate_ids(df)

        # 9. Add metadata
        df = self._add_metadata(df)

        self.logger(f"Transformation complete: {len(df)} vouchers", "SUCCESS")

        return df

    def _clean_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean all string columns."""
        self.logger("Cleaning strings", "INFO")
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = DataCleaner.clean_string(df[col])
        return df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename columns according to YAML column_mapping."""
        mapping = self.config.get('column_mapping', {})
        valid_mapping = {k: v for k, v in mapping.items() if k in df.columns}
        df = df.rename(columns=valid_mapping)
        self.logger(f"Columns renamed: {len(valid_mapping)}", "INFO")
        return df

    def _ensure_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure autorizacion column exists; default to 'NO_AUTH' if absent."""
        if 'autorizacion' not in df.columns:
            df['autorizacion'] = 'NO_AUTH'
            self.logger("Column 'autorizacion' created with default value", "INFO")
        return df

    def _convert_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert amount columns to float. NaN values are filled with 0.0."""
        numeric_cols = [
            'comision_base', 'comision_iva', 'amount_gross',
            'amount_net', 'amount_tax_iva', 'amount_tax_irf'
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        self.logger(
            f"Numeric columns converted: {len([c for c in numeric_cols if c in df.columns])}",
            "INFO"
        )
        return df

    def _calculate_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute total commission as comision_base + comision_iva.

        Guayaquil provides commission split into base and VAT portions.
        """
        base = df['comision_base'] if 'comision_base' in df.columns else 0
        iva_com = df['comision_iva'] if 'comision_iva' in df.columns else 0
        df['amount_commission'] = base + iva_com
        self.logger("Total commission calculated (base + IVA)", "INFO")
        return df

    def _map_commerces(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map commerce codes to establishment names.

        YAML commerce_mapping example: 68893 -> SALAS VIP, 68894 -> PARKING.
        Commerce codes may arrive as floats (68893.0); trailing .0 is stripped.
        """
        commerce_map = self.config.get('commerce_mapping', {})

        if 'comercio' in df.columns:
            df['comercio_str'] = (
                df['comercio']
                .astype(str)
                .str.replace(r'\.0$', '', regex=True)
            )
            df['establishment_name'] = df['comercio_str'].map(commerce_map).fillna('OTROS')
            df['establishment_code'] = df['comercio_str']
            self.logger(f"Commerces: {dict(df['establishment_name'].value_counts())}", "INFO")
        else:
            df['establishment_name'] = 'SIN_COMERCIO'
            df['establishment_code'] = '0'

        return df

    def _generate_settlement_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate settlement_id in COMERCIO-MMDD format.

        Example: commerce 68893, payment date 2026-02-02 -> '68893-0202'.
        """
        df['fecha_pago'] = pd.to_datetime(df['fecha_pago'])
        mmdd = df['fecha_pago'].dt.strftime('%m%d')
        df['settlement_id'] = df['comercio_str'] + "-" + mmdd
        self.logger("Settlement IDs generated (COMERCIO-MMDD)", "INFO")
        return df

    def _generate_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate double hash identifiers.

        voucher_hash_key (business matching key):
            Format: AMX_LOTE_REF_MONTO.

        etl_hash (deduplication key):
            Format: MD5(AMEX + LOTE + REF + AUTH + MONTO + YYYYMMDD).
        """
        amount_str = DataCleaner.format_decimal_strict(df['amount_gross'])

        df['voucher_hash_key'] = (
            "AMX_" +
            df['lote'].astype(str) + "_" +
            df['referencia'].astype(str) + "_" +
            amount_str
        )

        date_str = pd.to_datetime(df['fecha_venta']).dt.strftime('%Y%m%d').fillna('00000000')
        raw_str_etl = (
            "AMEX" +
            df['lote'].astype(str) +
            df['referencia'].astype(str) +
            df['autorizacion'].astype(str) +
            amount_str +
            date_str
        )
        df['etl_hash'] = raw_str_etl.apply(lambda x: hashlib.md5(x.encode()).hexdigest())

        self.logger("IDs generated: voucher_hash_key + etl_hash", "INFO")
        return df

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add fixed brand (AMEX), source_file, and reconcile_status columns."""
        df['brand'] = 'AMEX'

        if 'source_file' not in df.columns:
            source_name = self.config.get('general', {}).get('source_name', 'GUAYAQUIL')
            df['source_file'] = source_name

        df['reconcile_status'] = 'PENDING'
        df = df.drop(columns=['comercio_str'], errors='ignore')
        return df
