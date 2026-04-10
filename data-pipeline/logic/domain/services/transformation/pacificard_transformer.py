"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.transformation.pacificard_transformer
===============================================================================

Description:
    Domain service that transforms raw Pacificard voucher data and enriches it
    with DataBalance reference details. The cross-join uses a ranked match on
    lote + BIN + amount to correctly handle duplicate vouchers.

Responsibilities:
    - Clean and normalize string columns in both Pacificard and DataBalance DataFrames.
    - Rename Pacificard columns from raw to staging names (YAML column_mapping).
    - Convert amount columns to float.
    - Cross-match Pacificard vouchers with DataBalance by lote + BIN + amount + rank.
    - Fill missing referencia, autorizacion, and fecha_venta with defaults.
    - Map station codes to establishment group names (YAML group_mapping).
    - Generate settlement_id with station-based prefixes (YAML payment_id_prefixes).
    - Generate double hash identifiers (voucher_hash_key + etl_hash).

Key Components:
    - PacificardTransformer: Domain service; accepts two DataFrames (Pacificard
      and DataBalance) and returns an enriched, clean DataFrame.

Notes:
    - Cross-join uses cumcount ranking to resolve duplicate lote+BIN+amount groups.
    - settlement_id format: PREFIX_LOTE_MMDD (e.g., SVI_602_0202).
    - voucher_hash_key prefix: PCF_LOTE_REF_MONTO.
    - etl_hash: MD5(PACIFICARD+LOTE+REF+AUTH+MONTO+YYYYMMDD).
    - All rules come from staging_pacificard_rules.yaml.

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


class PacificardTransformer:
    """
    Domain service for transforming Pacificard voucher data with DataBalance cross-join.

    Cross-join strategy:
        1. Extract BIN (first 6 digits of masked card number).
        2. Sort both DataFrames by lote + BIN + amount.
        3. Create cumcount rank to disambiguate duplicate vouchers.
        4. Left join on lote + BIN + amount + rank.

    settlement_id format: PREFIX_LOTE_MMDD
        Example: SVI_602_0202, SVN_603_0203, PARKING_604_0204.

    All configuration from staging_pacificard_rules.yaml:
        column_mapping, group_mapping, payment_id_prefixes.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Args:
            config: Parsed YAML dict from staging_pacificard_rules.yaml.
        """
        self.config = config
        self.logger = get_logger("PACIFICARD_TRANSFORMER")

    def transform(
        self,
        df_pacificard: pd.DataFrame,
        df_databalance: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Transform Pacificard data with DataBalance cross-join.

        Steps:
            1. Clean strings in both DataFrames.
            2. Rename Pacificard columns (YAML mapping).
            3. Convert amount columns to float.
            4. Cross-join with DataBalance (ranked match).
            5. Fill missing reference data with defaults.
            6. Map establishment codes to group names.
            7. Generate settlement_id (PREFIX_LOTE_MMDD).
            8. Generate double hash IDs (voucher_hash_key + etl_hash).
            9. Add metadata columns.

        Args:
            df_pacificard: Raw DataFrame from PacificardExtractor.
            df_databalance: DataFrame from DataBalanceExtractor.

        Returns:
            Enriched, clean DataFrame.
        """

        self.logger(f"Starting Pacificard transformation for {len(df_pacificard)} vouchers", "INFO")

        df_paci = df_pacificard.copy()
        df_db = df_databalance.copy()

        # 1. Clean strings
        df_paci = self._clean_strings(df_paci)
        df_db = self._clean_strings(df_db)

        # 2. Rename columns
        df_paci = self._rename_columns(df_paci)

        # 3. Convert amount columns
        df_paci = self._convert_numeric_columns(df_paci)
        df_db = self._convert_numeric_columns(df_db)

        # 4. Cross-join with DataBalance
        df_merged = self._cross_with_databalance(df_paci, df_db)

        # 5. Fill missing data
        df_merged = self._complete_missing_data(df_merged)

        # 6. Map establishments
        df_merged = self._map_establishments(df_merged)

        # 7. Generate settlement_id
        df_merged = self._generate_settlement_id(df_merged)

        # 8. Generate IDs
        df_merged = self._generate_ids(df_merged)

        # 9. Add metadata
        df_merged = self._add_metadata(df_merged)

        self.logger(f"Transformation complete: {len(df_merged)} vouchers", "SUCCESS")

        return df_merged

    def _clean_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean all string columns."""
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

    def _convert_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert amount columns to float. Strips commas and currency symbols.
        Applies to Pacificard and DataBalance columns.
        """
        numeric_cols_paci = [
            'amount_gross', 'amount_commission', 'amount_tax_iva',
            'amount_tax_irf', 'amount_net'
        ]
        for col in numeric_cols_paci:
            if col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = (
                        df[col]
                        .str.replace(',', '', regex=False)
                        .str.replace('$', '', regex=False)
                    )
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        if 'valor_total' in df.columns:
            df['valor_total'] = pd.to_numeric(df['valor_total'], errors='coerce').fillna(0.0)

        df = df.round(2)
        return df

    def _cross_with_databalance(
        self,
        df_paci: pd.DataFrame,
        df_db: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Cross-match Pacificard vouchers with DataBalance by lote + BIN + amount + rank.

        The cumcount rank resolves ambiguity when multiple vouchers share the
        same lote + BIN + amount combination.
        """
        self.logger("Executing DataBalance cross-join...", "INFO")

        # Extract BIN (first 6 digits of masked card number)
        if 'tarjeta_enmascarada' in df_paci.columns:
            df_paci['bin_temp'] = df_paci['tarjeta_enmascarada'].str[:6]
        else:
            df_paci['bin_temp'] = '000000'

        df_paci = df_paci.sort_values(
            by=['lote', 'bin_temp', 'amount_gross', 'tarjeta_enmascarada']
        ).reset_index(drop=True)

        df_paci['rank_dedupe'] = df_paci.groupby(
            ['lote', 'bin_temp', 'amount_gross']
        ).cumcount()

        df_db = df_db.sort_values(
            by=['lote', 'bin', 'valor_total', 'referencia']
        ).reset_index(drop=True)

        df_db['rank_dedupe'] = df_db.groupby(
            ['lote', 'bin', 'valor_total']
        ).cumcount()

        df_merged = df_paci.merge(
            df_db,
            left_on=['lote', 'bin_temp', 'amount_gross', 'rank_dedupe'],
            right_on=['lote', 'bin', 'valor_total', 'rank_dedupe'],
            how='left',
            suffixes=('', '_db')
        )

        total = len(df_merged)
        matched = df_merged['referencia'].notna().sum()
        self.logger(
            f"Cross-join complete: {matched}/{total} vouchers ({matched/total*100:.1f}%)",
            "SUCCESS"
        )

        return df_merged

    def _complete_missing_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fill missing fields after the cross-join.

        Defaults:
            referencia   -> 'NO_FOUND'
            autorizacion -> 'NO_FOUND'
            fecha_venta  -> fecha_trx if available, else fecha_pago.
            brand        -> 'PACIFICARD' (always fixed).
        """
        df['referencia'] = df['referencia'].fillna('NO_FOUND')
        df['autorizacion'] = df['autorizacion'].fillna('NO_FOUND')

        if 'fecha_trx' in df.columns:
            df['fecha_venta'] = df['fecha_trx'].combine_first(df['fecha_pago'])
        else:
            df['fecha_venta'] = df['fecha_pago']

        df['brand'] = 'PACIFICARD'
        return df

    def _map_establishments(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map station codes to establishment group names.

        YAML group_mapping example:
            SALA VIP INTERNACIONAL -> SALAS VIP
            SALA VIP NACIONAL      -> SALAS VIP
            PARKING                -> PARKING
        """
        group_map = self.config.get('group_mapping', {})
        col_estacion = 'estacion_raw' if 'estacion_raw' in df.columns else 'estacion'

        if col_estacion in df.columns:
            df['establishment_name'] = df[col_estacion].map(group_map).fillna('OTROS')
            df['establishment_code'] = df[col_estacion]
        else:
            df['establishment_name'] = 'OTROS'
            df['establishment_code'] = 'SIN_ESTACION'

        self.logger(f"Establishments: {dict(df['establishment_name'].value_counts())}", "INFO")
        return df

    def _generate_settlement_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate settlement_id with station-based prefix.

        Format: PREFIX_LOTE_MMDD
        Example: SVI_602_0202 (Sala VIP Internacional, lote 602, Feb 2).

        CardAggregator reads 'settlement_id'; comprobante_pago is also populated
        for backward compatibility.
        """
        prefix_map = self.config.get('payment_id_prefixes', {})
        col_estacion = 'estacion_raw' if 'estacion_raw' in df.columns else 'estacion'

        if col_estacion in df.columns:
            df['prefix_temp'] = df[col_estacion].map(prefix_map).fillna('PCF')
        else:
            df['prefix_temp'] = 'PCF'

        df['fecha_pago'] = pd.to_datetime(df['fecha_pago'])
        mmdd = df['fecha_pago'].dt.strftime('%m%d')

        df['comprobante_pago'] = df['prefix_temp'] + "_" + df['lote'].astype(str) + "_" + mmdd
        df['settlement_id'] = df['comprobante_pago']

        return df

    def _generate_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate double hash identifiers.

        voucher_hash_key (business matching key): PCF_LOTE_REF_MONTO.
        etl_hash (deduplication key): MD5(PACIFICARD+LOTE+REF+AUTH+MONTO+YYYYMMDD).
        """
        amount_str = DataCleaner.format_decimal_strict(df['amount_gross'])

        df['voucher_hash_key'] = (
            "PCF_" +
            df['lote'].astype(str) + "_" +
            df['referencia'].astype(str) + "_" +
            amount_str
        )

        date_str = pd.to_datetime(df['fecha_venta']).dt.strftime('%Y%m%d').fillna('00000000')
        raw_str_etl = (
            "PACIFICARD" +
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
        """Add source_file, reconcile_status, and remove temporary columns."""
        if 'source_file' not in df.columns:
            source_name = self.config.get('general', {}).get('source_name', 'PACIFICARD')
            df['source_file'] = source_name

        df['reconcile_status'] = 'PENDING'

        cols_to_drop = [
            'bin_temp', 'rank_dedupe', 'bin', 'valor_total',
            'fecha_trx', 'prefix_temp', 'estacion_raw'
        ]
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')
        return df
