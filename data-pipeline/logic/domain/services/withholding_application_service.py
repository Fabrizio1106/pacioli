"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.withholding_application_service
===============================================================================

Description:
    Pure domain service that encapsulates the business logic for applying VAT
    withholding amounts to portfolio invoices. Contains no direct SQL; all data
    access is delegated to an injected repository.

Responsibilities:
    - Retrieve the current portfolio amount before withholding application.
    - Apply the withholding via the repository and compute the resulting balance.
    - Prepare structured audit data for post-application logging.
    - Return a typed ApplicationResult indicating success or failure.

Key Components:
    - ApplicationResult: Dataclass carrying the pre/post amounts and error context.
    - WithholdingApplicationService: Orchestrates the three-step application
      workflow (fetch → apply → compute) without direct database access.

Notes:
    - SQL-free by design; depends exclusively on WithholdingsOperationsRepository.
    - amount_after is computed locally as amount_before − valor_ret_iva.

Dependencies:
    - typing
    - dataclasses

===============================================================================
"""

from typing import Dict
from dataclasses import dataclass


@dataclass
class ApplicationResult:
    """Result of a withholding application attempt."""
    success: bool
    amount_before: float
    amount_after: float
    error_message: str = None


class WithholdingApplicationService:
    """
    Domain service for applying VAT withholding amounts to portfolio invoices.

    Coordinates:
        1. Retrieval of the portfolio amount before application.
        2. Execution of the withholding update via repository.
        3. Computation of the resulting balance.
        4. Preparation of structured audit data.

    Contains no direct SQL — delegates all data access to the injected repository.
    """

    def __init__(self, repository):
        """
        Args:
            repository: WithholdingsOperationsRepository instance for data access.
        """
        self.repository = repository

    def apply_withholding(self, invoice_sap_doc: str,
                          valor_ret_iva: float) -> ApplicationResult:
        """
        Apply a VAT withholding amount to a portfolio invoice.

        Steps:
            1. Fetch current portfolio amount.
            2. Apply withholding via repository (UPDATE on portfolio).
            3. Compute resulting balance as amount_before − valor_ret_iva.

        Args:
            invoice_sap_doc: SAP document number of the target invoice.
            valor_ret_iva: VAT withholding value to apply.

        Returns:
            ApplicationResult with success flag, pre/post amounts, and any error.
        """

        # 1. Fetch amount before application
        amount_before = self.repository.get_portfolio_amount(invoice_sap_doc)

        if amount_before is None:
            return ApplicationResult(
                success=False,
                amount_before=0.0,
                amount_after=0.0,
                error_message=f"Invoice {invoice_sap_doc} not found in portfolio"
            )

        # 2. Apply withholding
        try:
            self.repository.apply_withholding_to_portfolio(
                invoice_sap_doc,
                valor_ret_iva
            )
        except Exception as e:
            return ApplicationResult(
                success=False,
                amount_before=amount_before,
                amount_after=amount_before,
                error_message=str(e)
            )

        # 3. Compute resulting balance
        amount_after = amount_before - valor_ret_iva

        return ApplicationResult(
            success=True,
            amount_before=amount_before,
            amount_after=amount_after
        )

    def prepare_audit_data(self, withholding_id: int, invoice_sap_doc: str,
                           amount_before: float, amount_after: float,
                           applied_val: float) -> Dict:
        """
        Build a structured audit record for a completed withholding application.

        Args:
            withholding_id: Internal ID of the withholding record.
            invoice_sap_doc: SAP document number.
            amount_before: Portfolio amount before application.
            amount_after: Portfolio amount after application.
            applied_val: Withholding value that was applied.

        Returns:
            Dict ready for audit logging.
        """

        return {
            'withholding_id': withholding_id,
            'invoice_sap_doc': invoice_sap_doc,
            'amount_before': amount_before,
            'amount_after': amount_after,
            'withholding_applied': applied_val,
            'applied_by': 'SYSTEM'
        }