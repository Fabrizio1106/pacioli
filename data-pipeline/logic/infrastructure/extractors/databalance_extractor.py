"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.extractors.databalance_extractor
===============================================================================

Description:
    Extracts transaction data from the DataBalance system, specifically 
    focusing on card vouchers for designated acquirers (e.g., Pacificard). 
    It provides critical complementary data like voucher references and 
    authorization codes not present in primary bank statements.

Responsibilities:
    - Extract card vouchers from raw_databalance with acquirer-based filtering.
    - Provide record counts for process monitoring and data validation.
    - Retrieve available date ranges to coordinate incremental ETL phases.

Key Components:
    - DataBalanceExtractor: Main class for interacting with DataBalance raw data.

Notes:
    - DataBalance contains data for multiple acquirers; filtering by 
      'PACIFICARD' is standard for this pipeline.
    - Requires a lookback window (typically 7-15 days) to ensure matches 
      across systems with different processing times.

Dependencies:
    - pandas
    - sqlalchemy
    - datetime
    - typing
    - utils.logger

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from datetime import date
from typing import Optional
from utils.logger import get_logger


class DataBalanceExtractor:
    """
    Extractor for DataBalance data to match with Pacificard.
    
    Pattern: Data Access Object (DAO)
    """
    
    def __init__(self, engine: Engine):
        """
        Initializes the DataBalanceExtractor.
        
        Parameters:
        -----------
        engine : SQLAlchemy Engine
            Connection to the biq_raw database.
        """
        self.engine = engine
        self.logger = get_logger("DATABALANCE_EXTRACTOR")
    
    def extract(
        self,
        start_date: date,
        end_date: date,
        adquirente: str = 'PACIFICARD'
    ) -> pd.DataFrame:
        """
        Extracts vouchers from DataBalance.
        
        Parameters:
        -----------
        start_date : date
            Range start date (inclusive). Typically includes a lookback window.
        
        end_date : date
            Range end date (inclusive).
        
        adquirente : str
            Acquirer to filter (default: 'PACIFICARD').
        
        Returns:
        --------
        pd.DataFrame containing normalized DataBalance vouchers.
        """
        
        self.logger(
            f"Extracting DataBalance ({adquirente}): {start_date} -> {end_date}",
            "INFO"
        )
        
        # 1. Processing: Build and execute extraction query
        query = text("""
            SELECT 
                lote,
                bin,
                valor_total,
                referencia,
                autorizacion,
                fecha_voucher as fecha_trx
            FROM raw_databalance
            WHERE fecha_voucher BETWEEN :start AND :end
              AND adquirente = :adquirente
        """)
        
        try:
            df = pd.read_sql(
                query,
                self.engine,
                params={
                    "start": start_date,
                    "end": end_date,
                    "adquirente": adquirente
                }
            )
            
            self.logger(
                f"Extracted {len(df)} DataBalance vouchers",
                "SUCCESS"
            )
            
            # 2. Validation: Log unique batch counts
            if not df.empty and 'lote' in df.columns:
                unique_batches = df['lote'].nunique()
                self.logger(
                    f"Unique batches found: {unique_batches}",
                    "INFO"
                )
            
            return df
            
        except Exception as e:
            self.logger(
                f"Error extracting DataBalance: {str(e)}",
                "ERROR"
            )
            raise
    
    def extract_count(
        self,
        start_date: date,
        end_date: date,
        adquirente: str = 'PACIFICARD'
    ) -> int:
        """
        Counts vouchers within a range without extraction.
        """
        
        query = text("""
            SELECT COUNT(*) as total
            FROM raw_databalance
            WHERE fecha_voucher BETWEEN :start AND :end
              AND adquirente = :adquirente
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(
                query,
                {
                    "start": start_date,
                    "end": end_date,
                    "adquirente": adquirente
                }
            ).fetchone()
            
            return result.total if result else 0
    
    def extract_date_range(
        self,
        adquirente: str = 'PACIFICARD'
    ) -> tuple[Optional[date], Optional[date]]:
        """
        Retrieves the available date range for a specific acquirer.
        """
        
        query = text("""
            SELECT
                MIN(fecha_voucher) as fecha_min,
                MAX(fecha_voucher) as fecha_max
            FROM raw_databalance
            WHERE adquirente = :adquirente
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(
                query,
                {"adquirente": adquirente}
            ).fetchone()
            
            if result and result.fecha_min:
                return (result.fecha_min, result.fecha_max)
            else:
                return (None, None)
