"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.extractors.manual_requests_extractor
===============================================================================

Description:
    Extracts manual processing requests from the 'raw_manual_requests' table. 
    Unlike other extractors, it retrieves all available data without date 
    filtering, as these requests represent temporary working sets that are 
    fully reprocessed in each execution cycle.

Responsibilities:
    - Extract all manual requests from the raw source (DAO pattern).
    - Identify requests with multiple bank references (e.g., separated by '/') 
      that require downstream splitting.
    - Provide record counts for process initialization and validation.

Key Components:
    - ManualRequestsExtractor: Main class for interacting with manual request 
      raw data.

Notes:
    - Manual requests are "volatile" data; the staging table is typically 
      truncated and reloaded completely from this source.

Dependencies:
    - pandas
    - sqlalchemy
    - typing
    - utils.logger

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from typing import Optional
from utils.logger import get_logger


class ManualRequestsExtractor:
    """
    Extractor for manual requests from biq_raw.raw_manual_requests.
    """
    
    def __init__(self, engine: Engine):
        """
        Initializes the ManualRequestsExtractor.
        
        Parameters:
        -----------
        engine : SQLAlchemy Engine
            Connection to the biq_raw database.
        """
        self.engine = engine
        self.logger = get_logger("MANUAL_REQUESTS_EXTRACTOR")
    
    def extract_all(self) -> pd.DataFrame:
        """
        Extracts ALL manual requests from the raw table.
        
        Returns:
        --------
        pd.DataFrame containing raw manual requests.
        """
        
        self.logger(
            "Extracting manual requests (ALL)",
            "INFO"
        )
        
        # 1. Processing: Build and execute extraction query
        query = text("""
            SELECT 
                id as raw_id,
                fecha,
                cod_cliente,
                cliente,
                valor,
                ref_banco,
                estado_pago,
                factura,
                detalle
            FROM biq_raw.raw_manual_requests
        """)
        
        try:
            df = pd.read_sql(query, self.engine)
            
            self.logger(
                f"Extracted {len(df)} manual requests",
                "SUCCESS"
            )
            
            # 2. Validation: Detect multiple references requiring splits
            if not df.empty and 'ref_banco' in df.columns:
                multi_ref = df['ref_banco'].fillna('').str.contains('/').sum()
                if multi_ref > 0:
                    self.logger(
                        f"Detected {multi_ref} multiple references (split required)",
                        "INFO"
                    )
            
            return df
            
        except Exception as e:
            self.logger(
                f"Error extracting manual requests: {str(e)}",
                "ERROR"
            )
            raise
    
    def extract_count(self) -> int:
        """
        Counts requests without extraction.
        """
        
        query = text("""
            SELECT COUNT(*) as total
            FROM biq_raw.raw_manual_requests
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
            
            return result.total if result else 0
