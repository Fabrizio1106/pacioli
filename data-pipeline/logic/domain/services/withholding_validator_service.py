"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.withholding_validator_service
===============================================================================

Description:
    Pure domain service that encapsulates the business rules for validating VAT
    withholding eligibility. Contains no direct SQL; validation logic is fully
    independent of the persistence layer.

Responsibilities:
    - Apply four eligibility rules to a withholding row (IVA-only, positive
      amount, valid percentage, correct calculation within $0.01 tolerance).
    - Delegate duplicate-application checks to the injected repository.
    - Classify ineligible records into a canonical exception type for reporting.

Key Components:
    - ValidationResult: Dataclass carrying the eligibility flag and failure reasons.
    - WithholdingValidatorService: Stateless validator; all four rules run on
      each call to validate_eligibility and return a typed result.

Notes:
    - Valid VAT withholding percentages: 20%, 30%, 70%, 100%.
    - Exception priority order: RENTA > PERCENTAGE > CALCULATION > OTHER.
    - SQL-free by design — repository is used only for duplication checks.

Dependencies:
    - typing
    - dataclasses
    - pandas

===============================================================================
"""

from typing import List
from dataclasses import dataclass
import pandas as pd


@dataclass
class ValidationResult:
    """Result of a withholding eligibility check."""
    is_eligible: bool
    reasons: List[str]


class WithholdingValidatorService:
    """
    Domain service for validating VAT withholding eligibility.

    Business rules applied in validate_eligibility:
        1. IVA-only: valor_ret_renta must be 0.
        2. Positive amount: valor_ret_iva must be > 0.
        3. Valid percentage: must be one of 20, 30, 70, or 100.
        4. Correct calculation: base × (pct / 100) must equal valor_ret_iva
           within a $0.01 tolerance.

    Contains no direct SQL — validation logic is fully independent of persistence.
    """

    def __init__(self, repository):
        """
        Args:
            repository: WithholdingsOperationsRepository for duplicate checks.
        """
        self.repository = repository

    def validate_eligibility(self, withholding_row: pd.Series) -> ValidationResult:
        """
        Validate whether a withholding record is eligible for application.

        Args:
            withholding_row: Pandas Series with withholding record fields.

        Returns:
            ValidationResult with is_eligible flag and list of failure reason codes.
        """

        reasons = []

        # Rule 1: IVA-only — income tax withholding must be zero
        if withholding_row['valor_ret_renta'] != 0:
            reasons.append('RENTA_NOT_ZERO')

        # Rule 2: VAT withholding must be positive
        if withholding_row['valor_ret_iva'] <= 0:
            reasons.append('INVALID_AMOUNT')

        # Rule 3: Percentage must be one of the valid values
        pct = float(withholding_row['porcentaje_ret_iva'])
        valid_percentages = [20.0, 30.0, 70.0, 100.0]

        if pct not in valid_percentages:
            reasons.append('INVALID_PERCENTAGE')

        # Rule 4: Calculated value must match declared value within $0.01 tolerance
        if (withholding_row['base_ret_iva'] is not None and
                withholding_row['porcentaje_ret_iva'] is not None):

            base = float(withholding_row['base_ret_iva'])
            porcentaje = float(withholding_row['porcentaje_ret_iva'])
            valor_actual = float(withholding_row['valor_ret_iva'])

            valor_esperado = round(base * (porcentaje / 100), 2)

            if abs(valor_esperado - valor_actual) > 0.01:
                reasons.append('CALCULATION_ERROR')

        return ValidationResult(
            is_eligible=len(reasons) == 0,
            reasons=reasons
        )

    def check_already_applied(self, invoice_sap_doc: str) -> bool:
        """Return True if the invoice already has a withholding applied."""
        return self.repository.is_already_applied(invoice_sap_doc)

    def determine_exception_type(self, reasons: List[str]) -> str:
        """
        Classify an ineligible record into its primary exception type.

        Priority order: RENTA > PERCENTAGE > CALCULATION > INVALID_AMOUNT > OTHER.

        Args:
            reasons: List of failure reason codes from validate_eligibility.

        Returns:
            The highest-priority exception type as a string code.
        """

        if 'RENTA_NOT_ZERO' in reasons:
            return 'RENTA_NOT_ZERO'
        elif 'INVALID_PERCENTAGE' in reasons:
            return 'INVALID_PERCENTAGE'
        elif 'CALCULATION_ERROR' in reasons:
            return 'CALCULATION_ERROR'
        elif 'INVALID_AMOUNT' in reasons:
            return 'INVALID_AMOUNT'
        else:
            return 'OTHER'