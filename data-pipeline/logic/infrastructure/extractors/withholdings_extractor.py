"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.extractors.withholdings_extractor
===============================================================================

Description:
    Extracts tax withholdings from the SRI (Ecuadorian tax authority) raw data.
    It includes mechanisms for deduplication and filtering against already 
    processed records in the staging area.

Responsibilities:
    - Extract withholdings from biq_raw.raw_retenciones_sri.
    - Implement a lookback period for data retrieval.
    - Perform deduplication based on access key and supporting document number.
    - Filter out records that already exist in the staging table.

Key Components:
    - WithholdingsExtractor: Main class for withholding data extraction.

Notes:
    - Uses a composite key (access_key + supporting_document) for uniqueness.
    - Default lookback period is 90 days.
    - Follows the Data Access Object (DAO) pattern.

Dependencies:
    - pandas
    - sqlalchemy
    - utils.logger

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from datetime import datetime, timedelta
from typing import Optional
from utils.logger import get_logger


class WithholdingsExtractor:
    """
    SRI withholdings extractor from biq_raw.raw_retenciones_sri.
    
    PATTERN: Data Access Object (DAO)
    
    RESPONSIBILITY:
    ---------------
    Extract withholdings from the last N days (default: 90).
    
    DEDUPLICATION:
    --------------
    A withholding can apply to MULTIPLE invoices (consolidated).
    The unique key is: (clave_acceso, num_comp_sustento)
    
    EXISTING FILTER:
    ----------------
    Compares with biq_stg.stg_withholdings using a composite key.
    """
    
    def __init__(
        self,
        engine_raw: Engine,
        engine_stg: Engine
    ):
        """
        Constructor.
        
        Parameters:
        -----------
        engine_raw : SQLAlchemy Engine
            Connection to biq_raw
        
        engine_stg : SQLAlchemy Engine
            Connection to biq_staging (for filtering existing records)
        """
        # 1. Initialization
        self.engine_raw = engine_raw
        self.engine_stg = engine_stg
        self.logger = get_logger("WITHHOLDINGS_EXTRACTOR")
    
    def extract_new_withholdings(
        self,
        lookback_days: int = 90
    ) -> pd.DataFrame:
        """
        Extracts new withholdings (not present in staging).
        
        ALGORITHM:
        ----------
        1. Extract last N days from biq_raw.raw_retenciones_sri
        2. Deduplicate by access key + invoice
        3. Filter those that already exist in biq_stg.stg_withholdings
        
        Parameters:
        -----------
        lookback_days : int
            Days to look back for extraction (default: 90)
        
        Returns:
        --------
        pd.DataFrame
            New withholdings (non-duplicate, non-existing)
        """
        
        # 1. Initialization
        self.logger(
            f"Extracting withholdings (lookback: {lookback_days} days)",
            "INFO"
        )
        
        # 2. Raw Extraction
        df_raw = self._extract_from_raw(lookback_days)
        
        if df_raw.empty:
            self.logger("No data found in biq_raw.raw_retenciones_sri", "WARN")
            return pd.DataFrame()
        
        initial_count = len(df_raw)
        
        # 3. Raw Deduplication
        df_unique = self._deduplicate_raw(df_raw)
        
        dupes_raw = initial_count - len(df_unique)
        if dupes_raw > 0:
            self.logger(
                f"Filtered {dupes_raw} duplicates in RAW",
                "WARN"
            )
        
        # 4. Staging Filter
        df_new = self._filter_existing_in_staging(df_unique)
        
        existing_count = len(df_unique) - len(df_new)
        if existing_count > 0:
            self.logger(
                f"Skipped {existing_count} records already existing in staging",
                "INFO"
            )
        
        self.logger(
            f"Extracted {len(df_new)} new withholdings",
            "SUCCESS"
        )
        
        return df_new
    
    def _extract_from_raw(self, lookback_days: int) -> pd.DataFrame:
        """
        Extracts from biq_raw.raw_retenciones_sri.
        """
        
        # 1. Date Calculation
        cutoff_date = datetime.now() - timedelta(days=lookback_days)
        
        # 2. Query Definition
        query = text("""
            SELECT 
                hash_id,
                razon_social_emisor,
                ruc_emisor,
                rise,
                rimpe,
                agente_retencion,
                obligado_contabilidad,
                contribuyente_especial,
                num_comp_sustento,
                base_ret_renta,
                porcentaje_ret_renta,
                valor_ret_renta,
                base_ret_iva,
                porcentaje_ret_iva,
                valor_ret_iva,
                fecha_emision_ret,
                fecha_autorizacion_ret,
                periodo_fiscal,
                serie_comprobante_ret,
                num_secuencial_ret,
                clave_acceso,
                batch_id
            FROM biq_raw.raw_retenciones_sri
            WHERE fecha_autorizacion_ret >= :cutoff_date
        """)
        
        # 3. Execution
        df = pd.read_sql(
            query,
            self.engine_raw,
            params={"cutoff_date": cutoff_date}
        )
        
        self.logger(
            f"Extracted {len(df)} records from RAW",
            "INFO"
        )
        
        return df
    
    def _deduplicate_raw(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Deduplicate by composite key: (clave_acceso, num_comp_sustento)
        """
        
        # 1. Cleaning
        df['clave_acceso'] = df['clave_acceso'].astype(str).str.strip()
        df['num_comp_sustento'] = df['num_comp_sustento'].astype(str).str.strip()
        
        # 2. Deduplication
        df_unique = df.drop_duplicates(
            subset=['clave_acceso', 'num_comp_sustento'],
            keep='last'
        )
        
        return df_unique
    
    def _filter_existing_in_staging(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters withholdings that already exist in biq_stg.stg_withholdings.
        """
        
        if df.empty:
            return df
        
        # 1. Fetch Existing Keys
        try:
            query = text("""
                SELECT 
                    clave_acceso,
                    invoice_ref_sustento
                FROM biq_stg.stg_withholdings
            """)
            
            df_existing = pd.read_sql(query, self.engine_stg)
            
            if df_existing.empty:
                return df
            
            # Create composite key for comparison
            df_existing['composite_key'] = (
                df_existing['clave_acceso'].astype(str).str.strip() + '|' +
                df_existing['invoice_ref_sustento'].astype(str).str.strip()
            )
            
            existing_keys = set(df_existing['composite_key'])
            
        except Exception as e:
            self.logger(
                f"Error querying staging: {str(e)}",
                "WARN"
            )
            existing_keys = set()
        
        # 2. Filter New Records
        df['composite_key'] = (
            df['clave_acceso'].astype(str).str.strip() + '|' +
            df['num_comp_sustento'].astype(str).str.strip()
        )
        
        df_new = df[~df['composite_key'].isin(existing_keys)].copy()
        
        # 3. Cleanup
        df_new = df_new.drop(columns=['composite_key'])
        
        return df_new
    
    def extract_count(self, lookback_days: int = 90) -> int:
        """Counts withholdings without extracting them."""
        
        # 1. Date Calculation
        cutoff_date = datetime.now() - timedelta(days=lookback_days)
        
        # 2. Query Definition
        query = text("""
            SELECT COUNT(*) as total
            FROM biq_raw.raw_retenciones_sri
            WHERE fecha_autorizacion_ret >= :cutoff_date
        """)
        
        # 3. Execution
        with self.engine_raw.connect() as conn:
            result = conn.execute(
                query,
                {"cutoff_date": cutoff_date}
            ).fetchone()
            
            return result.total if result else 0
