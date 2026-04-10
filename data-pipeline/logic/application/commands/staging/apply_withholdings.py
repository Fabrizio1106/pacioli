"""
===============================================================================
Project: PACIOLI
Module: logic.application.commands.staging.apply_withholdings
===============================================================================

Description:
    Orchestration command for applying withholdings to the customer portfolio.
    It manages the validation, application, and auditing phases of the 
    withholding process.

Responsibilities:
    - Validate withholding eligibility based on business rules.
    - Check if withholdings have already been applied to avoid duplicates.
    - Update the customer portfolio by applying withholding amounts.
    - Record audit trails and update reconciliation statuses.

Key Components:
    - ApplyWithholdingsCommand: Main orchestrator for Phase 3 (Application).

Notes:
    - Orchestrates Domain Services and Repositories.
    - Handles error reporting and statistics for the application process.

Dependencies:
    - logic.infrastructure.repositories.withholdings_operations_repository
    - logic.domain.services.withholding_validator_service
    - logic.domain.services.withholding_application_service

===============================================================================
"""

from utils.logger import get_logger
from logic.infrastructure.repositories.withholdings_operations_repository import WithholdingsOperationsRepository
from logic.domain.services.withholding_validator_service import WithholdingValidatorService
from logic.domain.services.withholding_application_service import WithholdingApplicationService


class ApplyWithholdingsCommand:
    """
    Command for withholding application (Phase 3).
    
    Process:
    1. Validate eligibility (Business Rules)
    2. Check if already applied
    3. Apply to portfolio (Update conciliable_amount)
    4. Register audit
    5. Update statuses
    
    Orchestrates Domain Services and Repository.
    """
    
    def __init__(self, uow):
        """
        Parameters:
        -----------
        uow : UnitOfWork
            Unit of Work with DB session
        """
        self.uow = uow
        self.logger = get_logger("APPLY_WITHHOLDINGS")
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'applied': 0,
            'validation_failed': 0,
            'already_applied': 0,
            'errors': 0
        }
    
    def execute(self, force: bool = False, **kwargs) -> bool:
        """
        Command entry point.
        
        Returns:
        --------
        bool: True if successful
        """
        
        self.logger("Starting PHASE 3: Withholdings Application", "INFO")
        self.logger("=" * 70, "INFO")
        
        try:
            # 1. Component initialization
            repo = WithholdingsOperationsRepository(self.uow.session)
            validator = WithholdingValidatorService(repo)
            applicator = WithholdingApplicationService(repo)
            
            # 2. Data loading
            df_retenciones = repo.get_pending_for_application()
            
            if df_retenciones.empty:
                self.logger("No matched withholdings eligible for application", "INFO")
                return True
            
            self.logger(f"{len(df_retenciones)} withholdings candidate for application", "INFO")
            self.stats['total_processed'] = len(df_retenciones)
            
            # 3. Row-by-row processing
            for idx, row in df_retenciones.iterrows():
                self._process_withholding(row, repo, validator, applicator)
                
                # Log progress every 50
                if (idx + 1) % 50 == 0:
                    self.logger(f"   → Processed {idx + 1}/{len(df_retenciones)}...", "INFO")
            
            # 4. Final reporting
            self._report_statistics()
            
            self.logger("\n" + "=" * 70, "SUCCESS")
            self.logger("PHASE 3 COMPLETED", "SUCCESS")
            self.logger("=" * 70, "SUCCESS")
            
            # Metrics tracking for orchestrator
            self.total_applied = self.stats['applied']
            
            return True
            
        except Exception as e:
            self.logger(f"Application error: {e}", "ERROR")
            return False
    
    # ─────────────────────────────────────────────────────────────────────────
    # PROCESSING
    # ─────────────────────────────────────────────────────────────────────────
    
    def _process_withholding(self, withholding_row, repo, validator, applicator):
        """
        Processes an individual withholding.
        
        Parameters:
        -----------
        withholding_row : pd.Series
            Row with withholding data
        repo : WithholdingsRepository
        validator : WithholdingValidatorService
        applicator : WithholdingApplicationService
        """
        
        stg_id = withholding_row['stg_id']
        invoice_doc = withholding_row['invoice_sap_doc']
        
        # 1. Eligibility validation
        validation = validator.validate_eligibility(withholding_row)
        
        if not validation.is_eligible:
            # Validation failed
            self.stats['validation_failed'] += 1
            
            exception_type = validator.determine_exception_type(validation.reasons)
            
            repo.mark_as_ineligible(stg_id, validation.reasons)
            repo.create_validation_exception(
                stg_id=stg_id,
                exception_type=exception_type,
                reasons=validation.reasons,
                withholding_row=withholding_row
            )
            return
        
        # 2. Duplicate check
        if validator.check_already_applied(invoice_doc):
            # Previously applied
            self.stats['already_applied'] += 1
            
            repo.mark_as_ineligible(stg_id, ['DUPLICATE_APPLICATION'])
            repo.create_validation_exception(
                stg_id=stg_id,
                exception_type='DUPLICATE',
                reasons=['DUPLICATE_APPLICATION'],
                withholding_row=withholding_row
            )
            return
        
        # 3. Portfolio application
        try:
            result = applicator.apply_withholding(
                invoice_sap_doc=invoice_doc,
                valor_ret_iva=withholding_row['valor_ret_iva']
            )
            
            if not result.success:
                # Application error
                self.stats['errors'] += 1
                
                repo.create_validation_exception(
                    stg_id=stg_id,
                    exception_type='OTHER',
                    reasons=[result.error_message],
                    withholding_row=withholding_row
                )
                return
            
            # 4. Audit logging
            repo.insert_audit_record(
                withholding_id=stg_id,
                invoice_sap_doc=invoice_doc,
                amount_before=result.amount_before,
                amount_after=result.amount_after,
                applied_val=withholding_row['valor_ret_iva']
            )
            
            # 5. Status update
            repo.mark_as_applied(stg_id)
            
            self.stats['applied'] += 1
            
        except Exception as e:
            # Unexpected error
            self.stats['errors'] += 1
            
            self.logger(f"   Error applying withholding {stg_id}: {e}", "ERROR")
            
            repo.create_validation_exception(
                stg_id=stg_id,
                exception_type='OTHER',
                reasons=[f"Application error: {str(e)}"],
                withholding_row=withholding_row
            )
    
    # ─────────────────────────────────────────────────────────────────────────
    # REPORTING
    # ─────────────────────────────────────────────────────────────────────────
    
    def _report_statistics(self):
        """Final statistics report"""
        
        self.logger("\n" + "=" * 70, "INFO")
        self.logger("WITHHOLDINGS APPLICATION COMPLETED", "SUCCESS")
        self.logger("=" * 70, "INFO")
        
        # 1. Summary calculation
        total = self.stats['total_processed']
        applied = self.stats['applied']
        failed = self.stats['validation_failed']
        already = self.stats['already_applied']
        errors = self.stats['errors']
        
        # 2. Detailed reporting
        if total > 0:
            applied_pct = (applied / total * 100)
            
            self.logger(f"\nTotal processed: {total}", "INFO")
            self.logger(
                f"Successfully applied: {applied} ({applied_pct:.1f}%)",
                "SUCCESS"
            )
            
            if failed > 0:
                failed_pct = (failed / total * 100)
                self.logger(
                    f"Validation failed (Ineligible): {failed} ({failed_pct:.1f}%)",
                    "WARN"
                )
            
            if already > 0:
                already_pct = (already / total * 100)
                self.logger(
                    f"Previously applied (Duplicates): {already} ({already_pct:.1f}%)",
                    "INFO"
                )
            
            if errors > 0:
                errors_pct = (errors / total * 100)
                self.logger(
                    f"System errors: {errors} ({errors_pct:.1f}%)",
                    "ERROR"
                )
