"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.invoice_matcher_service
===============================================================================

Description:
    Domain service for matching withholding documents with invoices in the 
    customer portfolio. It identifies specific SAP document numbers based on 
    customer codes and invoice references.

Responsibilities:
    - Match individual invoices using customer code and reference numbers.
    - Provide a structured match result containing document numbers and amounts.
    - Delegate data access to the appropriate infrastructure repository.

Key Components:
    - InvoiceMatchResult: Dataclass for standardizing matching outcomes.
    - InvoiceMatcherService: Main service class for invoice matching.

Notes:
    - This service focuses on finding the underlying invoice supporting a 
      withholding tax document.

Dependencies:
    - typing, dataclasses
===============================================================================
"""

from typing import Optional, Dict
from dataclasses import dataclass


@dataclass
class InvoiceMatchResult:
    """Represents the result of an invoice matching attempt."""
    sap_doc_number: Optional[str]
    conciliable_amount: Optional[float]
    matched: bool


class InvoiceMatcherService:
    """
    Domain service for matching withholding entries against portfolio invoices.
    
    Searches for portfolio invoices using:
    - SAP Customer Code
    - Invoice Reference (invoice_ref_sustento)
    """
    
    def __init__(self, repository):
        """
        Initializes the service with a repository for data access.
        
        Parameters:
        -----------
        repository : WithholdingsOperationsRepository
            Repository providing access to portfolio data.
        """
        self.repository = repository
    
    def match_invoice(self, customer_code: str, invoice_ref: str) -> InvoiceMatchResult:
        """
        Searches for a matching invoice in the portfolio.
        
        Parameters:
        -----------
        customer_code : str
            SAP Customer Code.
        invoice_ref : str
            Invoice Reference string.
        
        Returns:
        --------
        InvoiceMatchResult containing the outcome of the match.
        """
        
        # 1. Repository Lookup
        invoice = self.repository.find_invoice_by_ref(customer_code, invoice_ref)
        
        # 2. Result Compilation
        if invoice:
            return InvoiceMatchResult(
                sap_doc_number=invoice['sap_doc_number'],
                conciliable_amount=invoice['conciliable_amount'],
                matched=True
            )
        
        return InvoiceMatchResult(
            sap_doc_number=None,
            conciliable_amount=None,
            matched=False
        )
