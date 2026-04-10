"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.extractors.bank_extractor
===============================================================================

Description:
    Extracts raw bank transaction data from the 'raw_banco_239' table. Acts 
    as a Data Access Object (DAO) for the initial stage of the ETL pipeline, 
    providing raw movements for further processing and enrichment.

Responsibilities:
    - Retrieve raw bank movements within a specified date range.
    - Provide record counts for process validation.
    - Identify available date ranges for incremental extraction.

Key Components:
    - BankExtractor: Main class for database interaction and raw data extraction.

Notes:
    - Follows the same pattern as SAPExtractor but targets bank-specific 
      schemas and columns.

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


class BankExtractor:
    """
    Extractor for bank data from raw_banco_239.
    
    Pattern: Data Access Object (DAO)
    """
    
    def __init__(self, engine: Engine):
        """
        Initializes the BankExtractor.
        
        Parameters:
        -----------
        engine : SQLAlchemy Engine
            Connection to the biq_raw database.
        """
        self.engine = engine
        self.logger = get_logger("BANK_EXTRACTOR")
    
    def extract(
        self,
        start_date: date,
        end_date: date
    ) -> pd.DataFrame:
        """
        Extracts bank movements within a date range.
        
        Parameters:
        -----------
        start_date : date
        end_date : date
        
        Returns:
        --------
        pd.DataFrame containing bank transaction details.
        """
        
        self.logger(
            f"Extracting bank data: {start_date} -> {end_date}",
            "INFO"
        )
        
        # 1. Processing: Build and execute extraction query
        query = text("""
            SELECT 
                fecha_transaccion,
                referencia,
                referencia2,
                descripcion,
                oficina
            FROM raw_banco_239
            WHERE fecha_transaccion BETWEEN :start AND :end
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
                f"Extracted {len(df)} bank records",
                "SUCCESS"
            )
            
            return df
            
        except Exception as e:
            self.logger(
                f"Error extracting bank data: {str(e)}",
                "ERROR"
            )
            raise
    
    def extract_count(
        self,
        start_date: date,
        end_date: date
    ) -> int:
        """
        Counts bank records within a date range without extracting them.
        """
        
        query = text("""
            SELECT COUNT(*) as total
            FROM raw_banco_239
            WHERE fecha_transaccion BETWEEN :start AND :end
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(
                query,
                {"start": start_date, "end": end_date}
            ).fetchone()
            
            return result.total if result else 0
    
    def extract_date_range(self) -> tuple[Optional[date], Optional[date]]:
        """
        Retrieves the min and max dates available in the raw table.
        """
        
        query = text("""
            SELECT 
                MIN(fecha_transaccion) as fecha_min,
                MAX(fecha_transaccion) as fecha_max
            FROM raw_banco_239
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
            
            if result and result.fecha_min:
                return (result.fecha_min, result.fecha_max)
            else:
                return (None, None)
