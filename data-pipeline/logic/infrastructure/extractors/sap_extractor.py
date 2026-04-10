"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.extractors.sap_extractor
===============================================================================

Description:
    Extracts raw SAP accounting data from the staging/raw database. 
    It focuses exclusively on data retrieval without performing any 
    transformation or cleaning.

Responsibilities:
    - Retrieve raw SAP data from biq_raw.raw_sap_cta_239 for a date range.
    - Provide record counts for validation purposes.
    - Retrieve the available date range in the raw table.

Key Components:
    - SAPExtractor: Main class for SAP data extraction.

Notes:
    - Follows the Data Access Object (DAO) pattern.
    - Implements Dependency Injection for the database engine.
    - Returns raw data to be processed by transformers.

Dependencies:
    - pandas
    - sqlalchemy
    - utils.logger

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from datetime import date, datetime
from typing import Optional
from utils.logger import get_logger


class SAPExtractor:
    """
    SAP data extractor from biq_raw.raw_sap_cta_239.
    
    PATTERN: Data Access Object (DAO)
    
    RESPONSIBILITY:
    ---------------
    Extract SAP accounting movements for a given date range.
    
    IMPORTANT:
    ----------
    This class does NOT:
    - Know what will be done with the data afterwards.
    - Know how it will be cleaned.
    - Know how it will be classified.
    
    It only knows how to EXTRACT from the raw table.
    """
    
    def __init__(self, engine: Engine):
        """
        Extractor constructor.
        
        Parameters:
        -----------
        engine : SQLAlchemy Engine
            Connection to the RAW database (biq_raw)
        
        WHY INJECT THE ENGINE?
        ----------------------
        Dependency Injection Principle:
        - The extractor does NOT create the connection.
        - The extractor RECEIVES the connection.
        
        Advantages:
        ✅ Easy to test (you can inject a fake engine).
        ✅ Reusable (same extractor, different DBs).
        ✅ Decoupled from configuration.
        """
        # 1. Initialization
        self.engine = engine
        self.logger = get_logger("SAP_EXTRACTOR")
    
    def extract(
        self, 
        start_date: date, 
        end_date: date
    ) -> pd.DataFrame:
        """
        Extracts SAP movements for the specified date range.
        
        Parameters:
        -----------
        start_date : date
            Range start date (inclusive)
        
        end_date : date
            Range end date (inclusive)
        
        Returns:
        --------
        pd.DataFrame with raw SAP columns.
        
        IMPORTANT:
        ----------
        Returned data is RAW:
        - May have null values.
        - May have strings with spaces.
        - Amounts may have negative signs.
        - Dates may be in string format.
        
        CLEANING is performed in another component (SAPTransformer).
        """
        
        # 1. Logging
        self.logger(
            f"Extracting SAP data: {start_date} -> {end_date}",
            "INFO"
        )
        
        # 2. Query Definition
        # Parameterized query for SQL injection protection
        query = text("""
            SELECT 
                fecha_documento,
                num_documento,
                clase_documento,
                asignacion,
                texto,
                importe_ml,
                doc_compensacion,
                moneda_local
            FROM biq_raw.raw_sap_cta_239 
            WHERE fecha_documento BETWEEN :start AND :end
        """)
        
        # 3. Execution
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
                f"Extracted {len(df)} records from SAP",
                "SUCCESS"
            )
            
            return df
            
        except Exception as e:
            self.logger(
                f"Error extracting SAP data: {str(e)}",
                "ERROR"
            )
            raise 
    
    def extract_count(
        self,
        start_date: date,
        end_date: date
    ) -> int:
        """
        Counts records without extracting them (useful for validation).
        
        Parameters:
        -----------
        start_date : date
        end_date : date
        
        Returns:
        --------
        int : Number of records
        """
        
        # 1. Query Definition
        query = text("""
            SELECT COUNT(*) as total
            FROM biq_raw.raw_sap_cta_239 
            WHERE fecha_documento BETWEEN :start AND :end
        """)
        
        # 2. Execution
        with self.engine.connect() as conn:
            result = conn.execute(
                query,
                {"start": start_date, "end": end_date}
            ).fetchone()
            
            return result.total if result else 0
    
    def extract_date_range(self) -> tuple[Optional[date], Optional[date]]:
        """
        Retrieves the date range available in the raw table.
        
        Returns:
        --------
        tuple: (min_date, max_date) or (None, None) if no data exists
        """
        
        # 1. Query Definition
        query = text("""
            SELECT 
                MIN(fecha_documento) as fecha_min,
                MAX(fecha_max) as fecha_max
            FROM biq_raw.raw_sap_cta_239
        """)
        
        # 2. Execution
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
            
            if result and result.fecha_min:
                return (result.fecha_min, result.fecha_max)
            else:
                return (None, None)
