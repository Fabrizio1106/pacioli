"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.extractors.pacificard_extractor
===============================================================================

Description:
    Extracts raw settlement data for Pacificard transactions from the 
    'raw_pacificard' table. Provides the base voucher information that will 
    subsequently be enriched with DataBalance data to recover missing 
    transaction details (references, authorizations).

Responsibilities:
    - Extract all Pacificard vouchers within a specified payment date 
      range (fecha_pago).
    - Provide record counts for process validation and monitoring.
    - Identify available date ranges for ETL scheduling and orchestration.
    - Analyze the distribution of transactions across stations/establishments.

Key Components:
    - PacificardExtractor: Main class for interacting with Pacificard raw data.

Notes:
    - Uses 'fecha_pago' (settlement date) as the primary time reference.
    - Raw Pacificard data is often incomplete; missing fields like 
      authorization codes are recovered in the Transformer layer.

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


class PacificardExtractor:
    """
    Extractor for Pacificard data from biq_raw.raw_pacificard.
    
    Pattern: Data Access Object (DAO)
    """
    
    def __init__(self, engine: Engine):
        """
        Initializes the PacificardExtractor.
        
        Parameters:
        -----------
        engine : SQLAlchemy Engine
            Connection to the biq_raw database.
        """
        self.engine = engine
        self.logger = get_logger("PACIFICARD_EXTRACTOR")
    
    def extract(
        self,
        start_date: date,
        end_date: date
    ) -> pd.DataFrame:
        """
        Extracts Pacificard vouchers within a date range.
        
        Parameters:
        -----------
        start_date : date
            Start date (inclusive) using 'fecha_pago'.
        
        end_date : date
            End date (inclusive) using 'fecha_pago'.
        
        Returns:
        --------
        pd.DataFrame containing raw Pacificard vouchers.
        """
        
        self.logger(
            f"Extracting Pacificard data: {start_date} -> {end_date}",
            "INFO"
        )
        
        # 1. Processing: Build and execute extraction query
        query = text("""
            SELECT *
            FROM biq_raw.raw_pacificard
            WHERE fecha_pago BETWEEN :start AND :end
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
                f"Extracted {len(df)} Pacificard vouchers",
                "SUCCESS"
            )
            
            # 2. Validation: Log station distribution for debugging
            if 'estacion' in df.columns:
                stations = df['estacion'].value_counts()
                self.logger(
                    f"Station distribution: {dict(stations)}",
                    "INFO"
                )
            
            return df
            
        except Exception as e:
            self.logger(
                f"Error extracting Pacificard data: {str(e)}",
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
            FROM biq_raw.raw_pacificard
            WHERE fecha_pago BETWEEN :start AND :end
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(
                query,
                {"start": start_date, "end": end_date}
            ).fetchone()
            
            return result.total if result else 0
    
    def extract_date_range(self) -> tuple[Optional[date], Optional[date]]:
        """
        Retrieves the min and max payment dates available in the raw table.
        """
        
        query = text("""
            SELECT
                MIN(fecha_pago) as fecha_min,
                MAX(fecha_pago) as fecha_max
            FROM biq_raw.raw_pacificard
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
            
            if result and result.fecha_min:
                return (result.fecha_min, result.fecha_max)
            else:
                return (None, None)
