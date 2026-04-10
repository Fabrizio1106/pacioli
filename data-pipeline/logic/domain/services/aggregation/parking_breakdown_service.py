"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.aggregation.parking_breakdown_service
===============================================================================

Description:
    Derived data service that generates a per-batch breakdown of PARKING
    card settlements. Reads from biq_stg.stg_card_details (already-processed
    staging data) rather than raw sources, and outputs records ready for
    stg_parking_pay_breakdown.

Responsibilities:
    - Extract PARKING vouchers from stg_card_details for a given date range,
      joined to stg_card_settlements to obtain settlement_date.
    - Aggregate vouchers by settlement_date + settlement_id + batch_number + brand,
      summing all amount columns and counting vouchers.
    - Generate match_hash_key (BRAND_BATCH_AMOUNT) for invoice matching.
    - Generate etl_hash (MD5) for deduplication, including settlement_date
      to avoid collisions when batch numbers are reused across periods.
    - Add reconcile_status metadata.

Key Components:
    - ParkingBreakdownService: Derived data service; reads from staging,
      writes to stg_parking_pay_breakdown via the calling command.

Notes:
    - Reads biq_stg.stg_card_details (staging), NOT raw_ tables.
      This distinguishes it from CardAggregator (raw -> staging).
    - match_hash_key format: BRAND_BATCH_AMOUNT (e.g., VISA_000602_1234.56).
    - etl_hash includes settlement_date to handle batch number recycling
      across different periods.
    - establishment_name filter defaults to 'PARKING' (configurable in YAML
      under extraction_filters.establishment_name).

Dependencies:
    - pandas, hashlib, datetime, typing, sqlalchemy
    - utils.data_cleaner, utils.logger

===============================================================================
"""

import pandas as pd
import hashlib
from datetime import date
from typing import Dict, Any
from sqlalchemy import text
from sqlalchemy.engine import Engine
from utils.data_cleaner import DataCleaner
from utils.logger import get_logger


class ParkingBreakdownService:
    """
    Derived data service for generating PARKING payment breakdowns by batch.

    Data flow: stg_card_details (staging) -> stg_parking_pay_breakdown (staging).

    Distinction from CardAggregator:
        CardAggregator:          raw vouchers -> stg_card_settlements / stg_card_details.
        ParkingBreakdownService: stg_card_details -> stg_parking_pay_breakdown.
    """

    def __init__(self, engine: Engine, config: Dict[str, Any]):
        """
        Args:
            engine: SQLAlchemy Engine connected to the staging database (biq_stg).
            config: Parsed YAML dict from staging_parking_breakdown_rules.yaml.
        """
        self.engine = engine
        self.config = config
        self.logger = get_logger("PARKING_BREAKDOWN_SVC")

    def generate_breakdown(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Generate the PARKING breakdown for a date range.

        Steps:
            1. Extract PARKING vouchers from stg_card_details.
            2. Aggregate by batch (sum amounts, count vouchers).
            3. Generate match_hash_key and etl_hash.
            4. Add reconcile_status metadata.

        Args:
            start_date: Inclusive start of the settlement_date range.
            end_date:   Inclusive end of the settlement_date range.

        Returns:
            DataFrame with one row per unique batch, ready for
            stg_parking_pay_breakdown. Empty DataFrame if no PARKING
            vouchers are found in the period.
        """
        self.logger(f"Generating PARKING breakdown: {start_date} -> {end_date}", "INFO")

        # 1. Extract vouchers
        df_details = self._extract_parking_vouchers(start_date, end_date)

        if df_details.empty:
            self.logger("No PARKING vouchers found in the period", "WARN")
            return pd.DataFrame()

        # 2. Aggregate by batch
        df_grouped = self._aggregate_by_batch(df_details)

        # 3. Generate hashes
        df_final = self._generate_hashes(df_grouped)

        # 4. Add metadata
        df_final = self._add_metadata(df_final)

        self.logger(f"Breakdown generated: {len(df_final)} batches", "SUCCESS")

        return df_final

    def _extract_parking_vouchers(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Extract PARKING vouchers from stg_card_details joined to stg_card_settlements.

        Filters by establishment_name (configurable in YAML; defaults to 'PARKING')
        and settlement_date range.
        """
        target_est = self.config.get('extraction_filters', {}).get('establishment_name', 'PARKING')

        self.logger(f"Extracting '{target_est}' vouchers", "INFO")

        query = text("""
            SELECT
                S.settlement_date,
                D.settlement_id,
                D.batch_number,
                D.brand,
                D.amount_gross,
                D.amount_commission,
                D.amount_tax_iva,
                D.amount_tax_irf,
                D.amount_net
            FROM biq_stg.stg_card_details D
            JOIN (
                SELECT DISTINCT settlement_id, settlement_date
                FROM biq_stg.stg_card_settlements
            ) S ON D.settlement_id = S.settlement_id
            WHERE S.settlement_date BETWEEN :start AND :end
              AND D.establishment_name = :est
        """)

        df = pd.read_sql(
            query,
            self.engine,
            params={"start": start_date, "end": end_date, "est": target_est}
        )

        self.logger(f"Extracted {len(df)} PARKING vouchers", "INFO")

        return df

    def _aggregate_by_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate PARKING vouchers by batch, summing all amount columns.

        Group keys: settlement_date, settlement_id, batch_number, brand.
        Sums: amount_gross, amount_commission, amount_tax_iva, amount_tax_irf, amount_net.
        Count: count_voucher (number of vouchers per batch).
        """
        df['batch_number'] = df['batch_number'].astype(str).str.strip()
        df['brand'] = df['brand'].astype(str).str.strip().str.upper()
        df['settlement_id'] = df['settlement_id'].astype(str).str.strip()

        group_cols = ['settlement_date', 'settlement_id', 'batch_number', 'brand']

        df_grouped = df.groupby(group_cols).agg({
            'amount_gross': 'sum',
            'amount_commission': 'sum',
            'amount_tax_iva': 'sum',
            'amount_tax_irf': 'sum',
            'amount_net': 'sum',
            'brand': 'count'
        }).rename(columns={'brand': 'count_voucher'}).reset_index()

        self.logger(f"Aggregated into {len(df_grouped)} unique batches", "INFO")

        return df_grouped

    def _generate_hashes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate double hash identifiers for the breakdown records.

        match_hash_key (invoice matching key):
            Format: BRAND_BATCH_AMOUNT
            Example: VISA_000602_1234.56

        etl_hash (deduplication key):
            Format: MD5(BRAND + BATCH + AMOUNT + YYYYMMDD)
            Includes settlement_date to prevent collisions when batch numbers
            are reused across different settlement periods.
        """
        amount_str = DataCleaner.format_decimal_strict(df['amount_gross'])
        brand_str = df['brand'].astype(str).str.strip()
        batch_str = df['batch_number'].astype(str).str.strip()

        df['match_hash_key'] = brand_str + "_" + batch_str + "_" + amount_str

        date_str = pd.to_datetime(df['settlement_date']).dt.strftime('%Y%m%d')
        raw_etl_str = brand_str + batch_str + amount_str + date_str
        df['etl_hash'] = raw_etl_str.apply(lambda x: hashlib.md5(x.encode()).hexdigest())

        self.logger("Hashes generated (match_hash_key + etl_hash)", "INFO")

        return df

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add reconcile_status metadata column."""
        df['reconcile_status'] = 'PENDING'
        return df
