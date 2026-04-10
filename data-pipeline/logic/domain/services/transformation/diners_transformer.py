"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.transformation.diners_transformer
===============================================================================

Description:
    Domain service that transforms raw Diners Club voucher data into clean,
    normalized records ready for staging. Applies all Diners-specific business
    rules loaded from YAML configuration.

Responsibilities:
    - Clean and normalize all string columns.
    - Filter records to estado_del_vale = 'PAGADO' only.
    - Rename columns from raw to staging names (YAML column_mapping).
    - Convert and normalize amount columns.
    - Normalize brand codes (DC -> DINERS CLUB, MC -> MASTERCARD, etc.).
    - Map establishment codes to business group names (YAML establishment_mapping).
    - Convert date columns to datetime.
    - Generate double hash identifiers (voucher_hash_key + etl_hash).
    - Generate settlement_id from comprobante_pago or lote+MMDD fallback.

Key Components:
    - DinersTransformer: Domain service; receives raw DataFrame, returns clean DataFrame.

Notes:
    - voucher_hash_key format: DVM_LOTE_REF_MONTO (business matching key).
    - etl_hash: MD5(BRAND+LOTE+REF+AUTH+MONTO+YYYYMMDD) (deduplication key).
    - All mapping rules come from staging_diners_rules.yaml.

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


class DinersTransformer:
    """
    Domain service for transforming Diners Club voucher data.

    Applies all Diners-specific business rules:
        - String cleaning and column renaming.
        - Status filtering (PAGADO only).
        - Brand normalization and establishment mapping.
        - Double hash ID generation.

    All configuration comes from staging_diners_rules.yaml:
        column_mapping, filters, brand_normalization, establishment_mapping.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Args:
            config: Parsed YAML dict from staging_diners_rules.yaml.
        """
        self.config = config
        self.logger = get_logger("DINERS_TRANSFORMER")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw Diners Club data into a clean staging DataFrame.

        Steps:
            1. Clean strings.
            2. Filter by status (PAGADO only).
            3. Rename columns (YAML mapping).
            4. Ensure required columns exist.
            5. Convert amount columns to float.
            6. Normalize brand codes.
            7. Map establishment codes to names.
            8. Convert date columns to datetime.
            9. Generate double hash IDs (voucher_hash_key + etl_hash).
            10. Generate settlement_id.
            11. Add metadata columns.

        Args:
            df: Raw DataFrame from DinersExtractor.

        Returns:
            Clean transformed DataFrame.
        """

        self.logger(f"Starting Diners transformation for {len(df)} vouchers", "INFO")

        df = df.copy()

        # 1. Clean strings
        df = self._clean_strings(df)

        # 2. Filter by status
        df = self._filter_by_status(df)

        # 3. Rename columns
        df = self._rename_columns(df)

        # 4. Ensure required columns
        df = self._ensure_columns(df)

        # 5. Convert amount columns
        df = self._convert_numeric_columns(df)

        # 6. Normalize brand
        df = self._normalize_brand(df)

        # 7. Map establishments
        df = self._map_establishments(df)

        # 8. Convert dates
        df = self._convert_dates(df)

        # 9. Generate IDs
        df = self._generate_ids(df)

        # 10. Generate settlement_id
        df = self._generate_settlement_id(df)

        # 11. Add metadata
        df = self._add_metadata(df)

        self.logger(f"Transformation complete: {len(df)} vouchers", "SUCCESS")

        return df

    def _clean_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean all string columns (trim, uppercase, normalize whitespace)."""
        self.logger("Cleaning strings", "INFO")
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = DataCleaner.clean_string(df[col])
        return df

    def _filter_by_status(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter vouchers to allowed status values only.

        YAML config: filters.column = 'estado_del_vale', filters.allow_values = ['PAGADO'].
        """
        filter_config = self.config.get('filters', {})
        filter_col = filter_config.get('column', 'estado_del_vale')
        allowed_values = filter_config.get('allow_values', ['PAGADO'])

        if filter_col in df.columns:
            initial_count = len(df)
            df = df[df[filter_col].isin(allowed_values)].copy()
            filtered_count = initial_count - len(df)

            if filtered_count > 0:
                self.logger(f"Filtered {filtered_count} vouchers with non-PAGADO status", "INFO")

        return df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename columns according to YAML column_mapping."""
        mapping = self.config.get('column_mapping', {})
        valid_mapping = {k: v for k, v in mapping.items() if k in df.columns}
        df = df.rename(columns=valid_mapping)
        self.logger(f"Columns renamed: {len(valid_mapping)}", "INFO")
        return df

    def _ensure_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure the autorizacion column exists; default to '000000' if absent."""
        if 'autorizacion' not in df.columns:
            df['autorizacion'] = '000000'
            self.logger("Column 'autorizacion' created with default value", "INFO")
        return df

    def _convert_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert amount columns from string to float.

        Strips commas and currency symbols before conversion.
        NaN values are filled with 0.0.
        """
        numeric_cols = [
            'amount_gross', 'amount_commission', 'amount_tax_iva',
            'amount_tax_irf', 'amount_net'
        ]

        for col in numeric_cols:
            if col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = (
                        df[col]
                        .str.replace(',', '', regex=False)
                        .str.replace('$', '', regex=False)
                    )
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        self.logger(
            f"Numeric columns converted: {len([c for c in numeric_cols if c in df.columns])}",
            "INFO"
        )
        return df

    def _normalize_brand(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize brand codes using YAML brand_normalization mapping.

        Example mapping: DC -> DINERS CLUB, MC -> MASTERCARD, ID -> VISA.
        Source column may be 'marca' or already-renamed 'brand'.
        Default: DINERS CLUB.
        """
        col_marca_src = None
        if 'marca' in df.columns:
            col_marca_src = 'marca'
        elif 'brand' in df.columns:
            col_marca_src = 'brand'

        if col_marca_src:
            brand_map = self.config.get('brand_normalization', {})
            df['brand'] = df[col_marca_src].map(brand_map).fillna('DINERS CLUB')
            self.logger(f"Brands normalized: {dict(df['brand'].value_counts())}", "INFO")
        else:
            df['brand'] = 'DINERS CLUB'
            self.logger("Column 'marca' not found, defaulting to DINERS CLUB", "WARN")

        return df

    def _map_establishments(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map establishment codes to business group names.

        YAML establishment_mapping example:
            1065236 -> SALAS VIP, 1070988 -> PARKING, 1551060 -> ASISTENCIAS.

        Generates: establishment_code (original), establishment_name (mapped).
        """
        est_map = self.config.get('establishment_mapping', {})

        if 'codigo_establecimiento' in df.columns:
            df['establishment_code'] = df['codigo_establecimiento'].astype(str)
            df['establishment_name'] = df['establishment_code'].map(est_map).fillna('OTROS')
            self.logger(f"Establishments: {dict(df['establishment_name'].value_counts())}", "INFO")
        else:
            df['establishment_name'] = 'SIN_GRUPO'
            df['establishment_code'] = '0'
            self.logger("Column 'codigo_establecimiento' not found", "WARN")

        return df

    def _convert_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert fecha_venta and fecha_pago to datetime."""
        for col in ['fecha_venta', 'fecha_pago']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df

    def _generate_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate double hash identifiers.

        voucher_hash_key (business matching key):
            Format: DVM_LOTE_REF_MONTO.
            Example: DVM_12345_678_25.51.

        etl_hash (deduplication key):
            Format: MD5(BRAND + LOTE + REF + AUTH + MONTO + YYYYMMDD).
            Includes fecha_venta for strict uniqueness.
        """
        amount_str = DataCleaner.format_decimal_strict(df['amount_gross'])

        # Business matching key
        df['voucher_hash_key'] = (
            "DVM_" +
            df['lote'].astype(str) + "_" +
            df['referencia'].astype(str) + "_" +
            amount_str
        )

        # ETL deduplication key — includes fecha_venta for strict uniqueness
        date_str = df['fecha_venta'].dt.strftime('%Y%m%d').fillna('00000000')
        raw_str_etl = (
            df['brand'].astype(str) +
            df['lote'].astype(str) +
            df['referencia'].astype(str) +
            df['autorizacion'].astype(str) +
            amount_str +
            date_str
        )
        df['etl_hash'] = raw_str_etl.apply(lambda x: hashlib.md5(x.encode()).hexdigest())

        self.logger("IDs generated: voucher_hash_key + etl_hash", "INFO")
        return df

    def _generate_settlement_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate settlement_id.

        Uses comprobante_pago if present; otherwise generates LOTE_MMDD.
        Example fallback: lote=12345, fecha_pago=2026-02-02 -> '12345_0202'.
        """
        if 'comprobante_pago' in df.columns:
            df['settlement_id'] = df['comprobante_pago'].astype(str)
        else:
            mmdd = df['fecha_pago'].dt.strftime('%m%d')
            df['settlement_id'] = df['lote'].astype(str) + "_" + mmdd
            self.logger("Settlement ID generated from lote + date", "INFO")
        return df

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add source_file and reconcile_status metadata columns."""
        if 'source_file' not in df.columns:
            source_name = self.config.get('general', {}).get('source_name', 'DINERS CLUB')
            df['source_file'] = source_name

        df['reconcile_status'] = 'PENDING'
        return df
