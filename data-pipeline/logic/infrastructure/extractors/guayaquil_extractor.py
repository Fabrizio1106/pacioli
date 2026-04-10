"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.extractors.guayaquil_extractor
===============================================================================

Description:
    Extracts raw voucher data from Banco Guayaquil (AMEX) source tables. 
    Acts as a Data Access Object (DAO) to retrieve settlement information 
    for the card reconciliation process.

Responsibilities:
    - Extract all AMEX vouchers within a specified liquidation date range 
      (fecha_liquida).
    - Provide record counts for process monitoring and validation.
    - Identify available date ranges for coordinated ETL scheduling.
    - Ensure raw data extraction without business-level filtering (logic 
      is applied in subsequent Transformer layers).

Key Components:
    - GuayaquilExtractor: Main class for interacting with Guayaquil raw data.

Notes:
    - Uses 'fecha_liquida' (settlement date) as the primary time reference.
    - Specifically targets American Express (AMEX) transactions processed 
      through Banco Guayaquil.

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


class GuayaquilExtractor:
    """
    Extractor for Guayaquil (AMEX) data from biq_raw.raw_guayaquil.
    
    Pattern: Data Access Object (DAO)
    """
    
    def __init__(self, engine: Engine):
        """
        Initializes the GuayaquilExtractor.
        
        Parameters:
        -----------
        engine : SQLAlchemy Engine
            Connection to the biq_raw database.
        """
        self.engine = engine
        self.logger = get_logger("GUAYAQUIL_EXTRACTOR")
    
    def extract(
        self,
        start_date: date,
        end_date: date
    ) -> pd.DataFrame:
        """
        Extracts Guayaquil vouchers within a date range.
        
        Parameters:
        -----------
        start_date : date
            Start date (inclusive) using 'fecha_liquida'.
        
        end_date : date
            End date (inclusive) using 'fecha_liquida'.
        
        Returns:
        --------
        pd.DataFrame containing raw Guayaquil vouchers.
        """
        
        self.logger(
            f"Extracting Guayaquil (AMEX) data: {start_date} -> {end_date}",
            "INFO"
        )
        
        # 1. Processing: Build and execute extraction query
        query = text("""
            SELECT *
            FROM biq_raw.raw_guayaquil
            WHERE fecha_liquida BETWEEN :start AND :end
        """)
        
        try:
            df = pd.read_sql(
                query,
                self.engine,
                params={
                    "start": start_date,
                    "end": end_date
                }
            )
            
            self.logger(
                f"Extracted {len(df)} Guayaquil vouchers",
                "SUCCESS"
            )
            
            # 2. Validation: Log commerce distribution for debugging
            if 'comercio' in df.columns:
                commerces = df['comercio'].value_counts()
                self.logger(
                    f"Commerce distribution: {dict(commerces)}",
                    "INFO"
                )
            
            return df
            
        except Exception as e:
            self.logger(
                f"Error extracting Guayaquil data: {str(e)}",
                "ERROR"
            )
            raise
    
    def extract_count(
        self,
        start_date: date,
        end_date: date
    ) -> int:
        """
        Counts vouchers within a range without extraction.
        """
        
        query = text("""
            SELECT COUNT(*) as total
            FROM biq_raw.raw_guayaquil
            WHERE fecha_liquida BETWEEN :start AND :end
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(
                query,
                {"start": start_date, "end": end_date}
            ).fetchone()
            
            return result.total if result else 0
    
    def extract_date_range(self) -> tuple[Optional[date], Optional[date]]:
        """
        Retrieves the min and max liquidation dates available in the raw table.
        """
        
        query = text("""
            SELECT
                MIN(fecha_liquida) as fecha_min,
                MAX(fecha_liquida) as fecha_max
            FROM biq_raw.raw_guayaquil
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
            
            if result and result.fecha_min:
                return (result.fecha_min, result.fecha_max)
            else:
                return (None, None)
