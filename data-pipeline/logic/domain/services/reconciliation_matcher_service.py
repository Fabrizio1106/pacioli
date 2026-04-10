"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.reconciliation_matcher_service
===============================================================================

Description:
    Unified wrapper for bank reconciliation matchers. It orchestrates different 
    matching strategies (Deterministic, Probabilistic, and Special Cases) using 
    a composition-based approach.

Responsibilities:
    - Detect transaction types (standard vs. special cases).
    - Execute cascaded matching (Deterministic -> Probabilistic).
    - Provide a unified interface for all reconciliation matchers.
    - Validate match quality and provide result summaries.

Key Components:
    - ReconciliationMatcherService: Main domain service orchestrating matchers.

Notes:
    - Implements the Composition pattern over existing matchers.
    - Decoupled from SQL; delegates data operations to underlying matchers.

Dependencies:
    - pandas
    - typing
    - logic.staging.reconciliation.matchers

===============================================================================
"""

import sys
from pathlib import Path
from typing import Optional, Dict, List
import pandas as pd

# 1. Setup Environment and Imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from logic.staging.reconciliation.matchers.deterministic_matcher import DeterministicMatcher
from logic.staging.reconciliation.matchers.probabilistic_matcher import ProbabilisticMatcher
from logic.staging.reconciliation.matchers.special_cases_matcher import SpecialCasesMatcher


class ReconciliationMatcherService:
    """
    Domain service that unifies all matchers using the Composition strategy.
    
    Responsibility:
    ---------------
    - Detect transaction type (standard vs. special case)
    - Execute cascaded matching (deterministic -> probabilistic)
    - Return unified result
    """
    
    def __init__(self, config: dict):
        """
        Constructor.
        
        Parameters:
        -----------
        config : dict
            Configuration loaded from reconciliation_config.yaml
        """
        # 1. Initialization
        self.config = config
        
        # 2. Matcher Composition
        self.deterministic_matcher = DeterministicMatcher(config)
        self.probabilistic_matcher = ProbabilisticMatcher(config)
        self.special_cases_matcher = SpecialCasesMatcher(config)
    
    def find_best_match(
        self,
        bank_transaction: pd.Series,
        invoices: pd.DataFrame
    ) -> Optional[Dict]:
        """
        Orchestrates the matching process across different strategies.
        """
        # 1. Pre-validation
        if invoices.empty:
            return None
    
        bank_amount = float(bank_transaction['amount_total'])
        payment_date = bank_transaction['bank_date']
        invoices_list = invoices.to_dict('records')
    
        # 2. Configuration Thresholds
        thresholds = self.config.get('confidence_thresholds', {})
        auto_match_min = thresholds.get('auto_match_minimum', 90)
        review_min = thresholds.get('review_minimum', 60)
    
        # 3. Phase 1: Deterministic Matching
        result = self.deterministic_matcher.find_any_deterministic_match(
            bank_amount=bank_amount,
            invoices=invoices_list,
            payment_date=payment_date
        )
    
        if result:
            score = result['score_result'].get('total_score', 0)
            if score >= auto_match_min:
                result['score_result']['status'] = 'MATCHED'
                return result
            elif score >= review_min:
                result['score_result']['status'] = 'REVIEW'
                return result
    
        # 4. Phase 2: Probabilistic Matching
        result = self.probabilistic_matcher.find_any_probabilistic_match(
            bank_amount=bank_amount,
            invoices=invoices_list,
            payment_date=payment_date
        )
    
        if result:
            score = result['score_result'].get('total_score', 0)
            if score >= auto_match_min:
                result['score_result']['status'] = 'MATCHED'
                return result
            elif score >= review_min:
                result['score_result']['status'] = 'REVIEW'
                return result
            else:
                return None
    
        return None
    
    def detect_special_case(
        self,
        bank_transaction: pd.Series
    ) -> Optional[str]:
        """
        Detects if a transaction corresponds to a special business case.
        """
        # 1. Input Preparation
        bank_tx_dict = bank_transaction.to_dict()
        customer_id = bank_transaction['customer_code']
        
        # 2. Detection Logic Delegation
        return self.special_cases_matcher.detect_special_case(
            bank_transaction=bank_tx_dict,
            customer_id=customer_id
        )
    
    def match_urbaparking_batch(
        self,
        bank_transactions: pd.DataFrame,
        invoices: pd.DataFrame,
        payment_date
    ) -> List[Dict]:
        """
        Specialized matching for URBAPARKING cases.
        """
        # 1. Data Conversion
        bank_list = bank_transactions.to_dict('records')
        invoice_list = invoices.to_dict('records')
        
        # 2. Logic Delegation
        return self.special_cases_matcher.match_urbaparking_batch(
            bank_transactions=bank_list,
            invoices=invoice_list,
            payment_date=payment_date
        )
    
    def match_salas_vip_batch(
        self,
        bank_transactions: pd.DataFrame,
        invoices: pd.DataFrame,
        payment_date
    ) -> List[Dict]:
        """
        Specialized matching for SALAS VIP cases.
        """
        # 1. Data Conversion
        bank_list = bank_transactions.to_dict('records')
        invoice_list = invoices.to_dict('records')
        
        # 2. Logic Delegation
        return self.special_cases_matcher.match_salas_vip_batch(
            bank_transactions=bank_list,
            invoices=invoice_list,
            payment_date=payment_date
        )
    
    def match_exterior_transfer(
        self,
        bank_transaction: pd.Series,
        invoices: pd.DataFrame,
        payment_date
    ) -> Optional[Dict]:
        """
        Matching for foreign currency transfers (EXTERIOR).
        """
        # 1. Data Conversion
        bank_dict = bank_transaction.to_dict()
        invoice_list = invoices.to_dict('records')
        
        # 2. Logic Delegation
        return self.special_cases_matcher.match_exterior_transfer(
            bank_transaction=bank_dict,
            invoices=invoice_list,
            payment_date=payment_date
        )
    
    def validate_match_quality(self, match_result: Dict) -> bool:
        """
        Validates the quality and completeness of a match result.
        """
        # 1. Basic Structure Validation
        if not match_result:
            return False
        
        required_keys = ['matched_invoices', 'score_result', 'method']
        if not all(key in match_result for key in required_keys):
            return False
        
        if not match_result['matched_invoices']:
            return False
        
        # 2. Minimum Score Validation
        min_score = self.config.get('confidence_thresholds', {}).get('review_minimum', 60)
        if match_result['score_result'].get('total_score', 0) < min_score:
            return False
        
        return True
    
    def get_match_summary(self, match_result: Dict) -> Dict:
        """
        Extracts a summary of the match results for logging and reporting.
        """
        # 1. Handle No Match Case
        if not match_result:
            return {
                'status': 'PENDING',
                'reason': 'NO_MATCH',
                'confidence': 0.0,
                'method': 'NONE',
                'invoice_count': 0,
                'diff': 0.0
            }
        
        # 2. Extract Summary Metrics
        score_result = match_result['score_result']
        
        return {
            'status': score_result.get('status', 'PENDING'),
            'reason': score_result.get('reason', 'UNKNOWN'),
            'confidence': score_result.get('total_score', 0.0),
            'method': match_result.get('method', 'UNKNOWN'),
            'invoice_count': len(match_result.get('matched_invoices', [])),
            'diff': score_result.get('diff', 0.0)
        }
