"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.customer_matcher_service
===============================================================================

Description:
    Domain service for customer matching using a multi-layer strategy. It 
    identifies SAP customer codes based on RUC (tax ID) and normalized names 
    from withholding documents.

Responsibilities:
    - Execute a multi-layer matching strategy (Exact RUC, Fuzzy Name, Partial RUC).
    - Provide confidence levels and matching methods for each result.
    - Interface with the database using SQLAlchemy sessions for lookups.

Key Components:
    - CustomerMatchResult: Dataclass representing the outcome of a match attempt.
    - CustomerMatcherService: Main service class for customer matching logic.

Notes:
    - Layer 1: Exact RUC (100% confidence).
    - Layer 2: Fuzzy Name using SOUNDEX or LIKE patterns (85% confidence).
    - Layer 3: Partial RUC (70% confidence).
    - Layer 4: Unmatched for manual review.

Dependencies:
    - pandas, sqlalchemy, typing, dataclasses
===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from typing import Optional, Dict
from dataclasses import dataclass


@dataclass
class CustomerMatchResult:
    """Represents the result of a customer matching attempt."""
    customer_code_sap: Optional[str]
    match_confidence: str  # 'EXACT', 'FUZZY', 'UNMATCHED'
    match_method: str      # 'RUC_EXACT', 'NAME_SOUNDEX', etc.
    matched: bool


class CustomerMatcherService:
    """
    Domain service for matching customers using a multi-layer strategy.
    
    Strategy Layers:
    - Layer 1: Exact RUC (Confidence: 100%)
    - Layer 2: Fuzzy Name (Confidence: 85%)
    - Layer 3: Partial RUC (Confidence: 70%)
    - Layer 4: No Match (Manual Review Required)
    """
    
    def __init__(self, session):
        """
        Initializes the service with a database session.
        
        Parameters:
        -----------
        session : SQLAlchemy Session
            Database session for executing queries.
        """
        self.session = session
    
    def match_multi_layer(self, withholding_row: pd.Series) -> CustomerMatchResult:
        """
        Executes the multi-layer strategy to find a customer match.
        
        Parameters:
        -----------
        withholding_row : pd.Series
            Row containing: customer_ruc, customer_name_normalized.
        
        Returns:
        --------
        CustomerMatchResult with the matching outcome.
        """
        
        # 1. Layer 1: Exact RUC
        result = self._layer1_exact_ruc(withholding_row['customer_ruc'])
        if result:
            return result
        
        # 2. Layer 2: Fuzzy Name
        result = self._layer2_fuzzy_name(withholding_row['customer_name_normalized'])
        if result:
            return result
        
        # 3. Layer 3: Partial RUC
        result = self._layer3_partial_ruc(withholding_row['customer_ruc'])
        if result:
            return result
        
        # 4. Layer 4: No Match Found
        return CustomerMatchResult(
            customer_code_sap=None,
            match_confidence='UNMATCHED',
            match_method='NONE',
            matched=False
        )
    
    def _layer1_exact_ruc(self, customer_ruc: str) -> Optional[CustomerMatchResult]:
        """
        Searches for a customer by exact RUC in dim_customers.
        
        Confidence: 100%
        
        Parameters:
        -----------
        customer_ruc : str
            Customer's tax ID (RUC).
        
        Returns:
        --------
        CustomerMatchResult if a match is found, otherwise None.
        """
        
        # 1. Query Construction
        query = text("""
            SELECT customer_id, customer_name
            FROM biq_stg.dim_customers
            WHERE tax_id = :ruc
              AND is_active = TRUE
            LIMIT 1
        """)
        
        # 2. Execution
        result = self.session.execute(query, {"ruc": customer_ruc}).fetchone()
        
        # 3. Result Processing
        if result:
            return CustomerMatchResult(
                customer_code_sap=result[0],  # Positional access for compatibility
                match_confidence='EXACT',
                match_method='RUC_EXACT',
                matched=True
            )
        
        return None
    
    def _layer2_fuzzy_name(self, customer_name: str) -> Optional[CustomerMatchResult]:
        """
        Searches for a customer by name using fuzzy techniques.
        
        Confidence: 85%
        
        Strategies:
        1. SOUNDEX (similar pronunciation).
        2. LIKE pattern (matching specific words).
        
        Parameters:
        -----------
        customer_name : str
            Normalized customer name.
        
        Returns:
        --------
        CustomerMatchResult if a match is found, otherwise None.
        """
        
        # 1. Validation
        if not customer_name or len(customer_name) < 3:
            return None
        
        # 2. Strategy 1: SOUNDEX Lookup
        query_soundex = text("""
            SELECT customer_id, customer_name
            FROM biq_stg.dim_customers
            WHERE SOUNDEX(customer_name) = SOUNDEX(:name)
              AND is_active = TRUE
            LIMIT 1
        """)
        
        result = self.session.execute(query_soundex, {"name": customer_name}).fetchone()
        
        if result:
            return CustomerMatchResult(
                customer_code_sap=result[0],
                match_confidence='FUZZY',
                match_method='NAME_SOUNDEX',
                matched=True
            )
        
        # 3. Strategy 2: LIKE Pattern Lookup (First 2 words)
        words = customer_name.split()[:3]
        
        if len(words) >= 2:
            like_pattern = f"%{words[0]}%{words[1]}%"
            
            query_like = text("""
                SELECT customer_id, customer_name
                FROM biq_stg.dim_customers
                WHERE customer_name LIKE :pattern
                  AND is_active = TRUE
                LIMIT 1
            """)
            
            result = self.session.execute(query_like, {"pattern": like_pattern}).fetchone()
            
            if result:
                return CustomerMatchResult(
                    customer_code_sap=result[0],
                    match_confidence='FUZZY',
                    match_method='NAME_LIKE',
                    matched=True
                )
        
        return None
    
    def _layer3_partial_ruc(self, customer_ruc: str) -> Optional[CustomerMatchResult]:
        """
        Searches by partial RUC (ignoring the verification digit).
        
        Confidence: 70%
        
        Useful for cases where the verification digit is entered incorrectly.
        
        Parameters:
        -----------
        customer_ruc : str
            Customer's tax ID (RUC).
        
        Returns:
        --------
        CustomerMatchResult if a match is found, otherwise None.
        """
        
        # 1. Validation and Preparation
        if not customer_ruc or len(customer_ruc) < 10:
            return None
        
        ruc_partial = customer_ruc[:10]
        
        # 2. Query Construction
        query = text("""
            SELECT customer_id, customer_name, tax_id
            FROM biq_stg.dim_customers
            WHERE LEFT(tax_id, 10) = :ruc_partial
              AND is_active = TRUE
            LIMIT 1
        """)
        
        # 3. Execution and Result Processing
        result = self.session.execute(query, {"ruc_partial": ruc_partial}).fetchone()
        
        if result:
            return CustomerMatchResult(
                customer_code_sap=result[0],
                match_confidence='FUZZY',
                match_method='RUC_PARTIAL',
                matched=True
            )
        
        return None
