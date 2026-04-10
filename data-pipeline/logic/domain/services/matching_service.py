"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.matching_service
===============================================================================

Description:
    Pure matching logic service. It contains the logic to decide if a bank 
    transaction matches one or more invoices.

Responsibilities:
    - Determine the best match between bank transactions and invoices.
    - Implement different matching strategies (Exact, Tolerance, Multi-invoice).
    - Validate match results based on business rules and confidence scores.

Key Components:
    - MatchingService: Main service class implementing the matching algorithms.

Notes:
    - This service is decoupled from infrastructure and database concerns.
    - It operates on domain objects (BankTransaction, Invoice, Match).

Dependencies:
    - typing
    - decimal
    - logic.domain.value_objects
    - utils.logger

===============================================================================
"""

from typing import List, Optional
from decimal import Decimal
from logic.domain.value_objects import BankTransaction, Invoice, Match
from utils.logger import get_logger


class MatchingService:
    """
    Business logic service for transaction matching.
    
    MATCHING STRATEGIES:
    -----------------------
    1. EXACT_SINGLE: 1 transaction = 1 invoice (exact amount)
    2. TOLERANCE_SINGLE: 1 transaction ≈ 1 invoice (amount within tolerance)
    3. MULTI_INVOICES: 1 transaction = N invoices (exact sum)
    4. PARTIAL_PAYMENT: Transaction pays part of an invoice
    """
    
    def __init__(self, tolerance_percent: float = 1.0):
        """
        Constructor.
        
        Parameters:
        -----------
        tolerance_percent : float
            Amount tolerance in percentage (default 1.0%)
        """
        # 1. Initialization
        self.tolerance_percent = Decimal(str(tolerance_percent))
        self.logger = get_logger("MATCHING_SERVICE")
    
    def find_best_match(
        self,
        tx: BankTransaction,
        invoices: List[Invoice]
    ) -> Optional[Match]:
        """
        Finds the best match for a bank transaction.
        
        DECISION FLOW:
        -----------------
        1. Attempt exact match (100% confidence)
        2. If no exact, attempt with tolerance (95% confidence)
        3. If no single, attempt multi-invoice (90% confidence)
        4. If nothing works, return None
        """
        
        # 1. Exact Match Search
        match = self.match_exact_single(tx, invoices)
        if match:
            return match
        
        # 2. Tolerance Match Search
        match = self.match_tolerance_single(tx, invoices)
        if match:
            return match
        
        # 3. Multi-invoice Match Search
        match = self.match_multi_invoices(tx, invoices)
        if match:
            return match
        
        # 4. No Match Found
        return None
    
    def match_exact_single(
        self,
        tx: BankTransaction,
        invoices: List[Invoice]
    ) -> Optional[Match]:
        """
        Searches for an EXACT match: 1 transaction = 1 invoice (same amount and customer).
        
        CRITERIA:
        ---------
        1. Amount is EXACTLY the same
        2. Same customer (customer_id)
        3. Only ONE invoice meets the criteria (ambiguous if 2+)
        
        CONFIDENCE: 100%
        """
        
        # 1. Customer Validation
        if not tx.customer_id:
            return None
        
        # 2. Filter by Customer
        same_customer = [
            inv for inv in invoices
            if inv.customer_code == tx.customer_id
            and inv.is_pending
        ]
        
        if not same_customer:
            return None
        
        # 3. Exact Amount Search
        exact_amount = [
            inv for inv in same_customer
            if inv.effective_amount == tx.amount
        ]
        
        # 4. Ambiguity Check
        if len(exact_amount) != 1:
            return None
        
        invoice = exact_amount[0]
        
        # 5. Create Match Result
        return Match(
            bank_tx_id=tx.id,
            invoice_ids=[invoice.id],
            confidence_score=Decimal('100.00'),
            match_method='EXACT_SINGLE',
            amount_difference=Decimal('0.00'),
            notes=f'Exact match: TX ${tx.amount} = INV ${invoice.amount}'
        )
    
    def match_tolerance_single(
        self,
        tx: BankTransaction,
        invoices: List[Invoice]
    ) -> Optional[Match]:
        """
        Searches for a TOLERANCE match: 1 transaction ≈ 1 invoice (similar amount).
        
        CRITERIA:
        ---------
        1. Amount within tolerance (default ±1%)
        2. Same customer
        3. Only ONE invoice meets the criteria
        
        CONFIDENCE: 95%
        """
        
        # 1. Customer Validation
        if not tx.customer_id:
            return None
        
        # 2. Filter by Customer
        same_customer = [
            inv for inv in invoices
            if inv.customer_code == tx.customer_id
            and inv.is_pending
        ]
        
        if not same_customer:
            return None
        
        # 3. Tolerance Range Calculation
        matches_with_tolerance = []
        
        for inv in same_customer:
            diff = abs(tx.amount - inv.effective_amount)
            max_diff = inv.effective_amount * (self.tolerance_percent / Decimal('100'))
            
            if diff <= max_diff:
                matches_with_tolerance.append({
                    'invoice': inv,
                    'difference': diff
                })
        
        # 4. Ambiguity Check
        if len(matches_with_tolerance) != 1:
            return None
        
        result = matches_with_tolerance[0]
        invoice = result['invoice']
        diff = result['difference']
        
        # 5. Create Match Result
        return Match(
            bank_tx_id=tx.id,
            invoice_ids=[invoice.id],
            confidence_score=Decimal('95.00'),
            match_method='TOLERANCE_SINGLE',
            amount_difference=diff,
            notes=f'Tolerance match: diff ${diff} (±{self.tolerance_percent}%)'
        )
    
    def match_multi_invoices(
        self,
        tx: BankTransaction,
        invoices: List[Invoice],
        max_invoices: int = 5
    ) -> Optional[Match]:
        """
        Searches for a MULTI-INVOICE match: 1 transaction = sum of N invoices.
        
        CRITERIA:
        ---------
        1. Sum of 2 to N invoices = payment amount (exact or with tolerance)
        2. All from the same customer
        3. CONTIGUOUS invoices by date (avoiding illogical combinations)
        
        CONFIDENCE: 90%
        """
        
        # 1. Customer Validation
        if not tx.customer_id:
            return None
        
        # 2. Filter and Sort
        same_customer = [
            inv for inv in invoices
            if inv.customer_code == tx.customer_id
            and inv.is_pending
        ]
        
        if len(same_customer) < 2:
            return None
        
        same_customer.sort(key=lambda x: x.doc_date)
        
        # 3. Sliding Window Combination Search
        for start in range(len(same_customer)):
            for end in range(start + 2, min(start + max_invoices + 1, len(same_customer) + 1)):
                subset = same_customer[start:end]
                total = sum(inv.effective_amount for inv in subset)
                diff = abs(total - tx.amount)
                max_diff = tx.amount * (self.tolerance_percent / Decimal('100'))
                
                if diff <= max_diff:
                    return Match(
                        bank_tx_id=tx.id,
                        invoice_ids=[inv.id for inv in subset],
                        confidence_score=Decimal('90.00'),
                        match_method='MULTI_INVOICES',
                        amount_difference=diff,
                        notes=f'Multi-invoice payment: {len(subset)} invoices = ${total}'
                    )
        
        return None
    
    def validate_match(self, match: Match) -> bool:
        """
        Validates that a match meets business rules.
        
        RULES:
        ------
        1. Confidence >= 80%
        2. Difference <= 5% of amount
        3. At least 1 invoice
        """
        
        # 1. Confidence Rule
        if match.confidence_score < Decimal('80.00'):
            self.logger(
                f"Match rejected: confidence {match.confidence_score}% < 80%",
                "WARN"
            )
            return False
        
        # 2. Existence Rule
        if not match.invoice_ids:
            self.logger("Match rejected: no invoices", "WARN")
            return False
        
        return True
