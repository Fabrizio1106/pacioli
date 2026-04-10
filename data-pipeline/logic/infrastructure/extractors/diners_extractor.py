"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.extractors.diners_extractor
===============================================================================

Description:
    Extracts raw voucher data from Diners Club (biq_raw.raw_diners_club). 
    Follows the Data Access Object (DAO) pattern to provide a clean extraction 
    interface for the settlement processing pipeline.

Responsibilities:
    - Extract all Diners Club vouchers within a specified settlement date 
      range (fecha_del_pago).
    - Provide record counts for process validation and monitoring.
    - Identify the available date range in the raw source for ETL orchestration.
    - Maintain raw data integrity by extracting all records (including voided 
      or reversed) for full auditability.

Key Components:
    - DinersExtractor: Main class for interacting with Diners Club raw data.

Notes:
    - Uses 'fecha_del_pago' (settlement date) as the primary time reference.
    - No data cleaning or filtering is performed at this stage; business 
      logic is applied in the Transformer layer.

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


class DinersExtractor:
    """
    Extractor for Diners Club data from biq_raw.raw_diners_club.
    """
    
    def __init__(self, engine: Engine):
        """
        Initializes the DinersExtractor.
        
        Parameters:
        -----------
        engine : SQLAlchemy Engine
            Connection to the biq_raw database.
        """
        self.engine = engine
        self.logger = get_logger("DINERS_EXTRACTOR")
    
    def extract(
        self,
        start_date: date,
        end_date: date
    ) -> pd.DataFrame:
        """
        Extracts Diners Club vouchers within a date range.
        
        Parameters:
        -----------
        start_date : date
            Start date (inclusive) using 'fecha_del_pago'.
        
        end_date : date
            End date (inclusive) using 'fecha_del_pago'.
        
        Returns:
        --------
        pd.DataFrame containing raw Diners Club vouchers.
        """
        
        self.logger(
            f"Extracting Diners Club data: {start_date} -> {end_date}",
            "INFO"
        )
        
        # 1. Processing: Build and execute extraction query
        query = text("""
            SELECT *
            FROM biq_raw.raw_diners_club
            WHERE fecha_del_pago BETWEEN :start AND :end
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
                f"Extracted {len(df)} Diners vouchers",
                "SUCCESS"
            )
            
            # 2. Validation: Log status counts for debugging
            if 'estado_del_vale' in df.columns:
                statuses = df['estado_del_vale'].value_counts()
                self.logger(
                    f"Status counts: {dict(statuses)}",
                    "INFO"
                )
            
            return df
            
        except Exception as e:
            self.logger(
                f"Error extracting Diners data: {str(e)}",
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
            FROM biq_raw.raw_diners_club
            WHERE fecha_del_pago BETWEEN :start AND :end
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(
                query,
                {"start": start_date, "end": end_date}
            ).fetchone()
            
            return result.total if result else 0
    
    def extract_date_range(self) -> tuple[Optional[date], Optional[date]]:
        """
        Retrieves the min and max settlement dates available in the raw table.
        """
        
        query = text("""
            SELECT
                MIN(fecha_del_pago) as fecha_min,
                MAX(fecha_del_pago) as fecha_max
            FROM biq_raw.raw_diners_club
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
            
            if result and result.fecha_min:
                return (result.fecha_min, result.fecha_max)
            else:
                return (None, None)
