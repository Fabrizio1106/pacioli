"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.unit_of_work
===============================================================================

Description:
    Implements the Unit of Work pattern to manage atomic database transactions. 
    Ensures that multiple repository operations either all succeed together or 
    fail without partial updates, maintaining data consistency across the 
    entire Silver layer.

Responsibilities:
    - Manage database sessions and transaction lifecycles (commit/rollback).
    - Provide centralized, lazy-loaded access to all system repositories.
    - Guarantee ACID compliance for complex business operations involving 
      multiple entities (bank transactions, invoices, payments, etc.).

Key Components:
    - UnitOfWork: Context manager that coordinates transactions and repository 
      access.

Notes:
    - Atomic operations prevent data inconsistencies during process failures.
    - Uses lazy loading for repositories to optimize resource usage.

Dependencies:
    - sqlalchemy.orm
    - typing
    - contextlib
    - utils.logger
    - logic.infrastructure.repositories (various)

===============================================================================
"""

from sqlalchemy.orm import sessionmaker, Session
from typing import Optional
from contextlib import contextmanager
from utils.logger import get_logger


# 1. Main UnitOfWork Class

class UnitOfWork:
    """
    Transaction manager that groups multiple operations.
    
    ACID Principles:
    ---------------
    - Atomicity: All or nothing.
    - Consistency: Data remains valid according to all rules.
    - Isolation: Operations do not interfere with each other.
    - Durability: Changes are permanent once committed.
    
    Usage Example:
    --------------
    with UnitOfWork(engine) as uow:
        # Multiple operations sharing the same transaction
        uow.bank_transactions.update_status(123, 'MATCHED')
        uow.invoices.update_status(456, 'PAID')
        
        # If no exceptions occur: AUTO-COMMIT
        # If an exception is raised: AUTO-ROLLBACK
    """
    
    def __init__(self, engine):
        """
        Initializes the Unit of Work.
        
        Parameters:
        -----------
        engine : SQLAlchemy engine
            Database connection.
        """
        self.engine = engine
        self.SessionLocal = sessionmaker(bind=engine)
        self.logger = get_logger("UNIT_OF_WORK")
        
        # Session is initialized in __enter__
        self.session: Optional[Session] = None
        
        # Internal repository caches
        self._bank_transactions = None
        self._invoices = None
        self._payments = None
    
    # 2. Lifecycle Magic Methods
    
    def __enter__(self):
        """
        Executed when entering the 'with' block.
        Initializes the database session and starts a transaction.
        """
        self.session = self.SessionLocal()
        self.logger("Transaction started", "INFO")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Executed when exiting the 'with' block.
        Commits changes if no error occurred, otherwise rolls back.
        """
        try:
            if exc_type is None:
                # No errors -> COMMIT changes
                self.session.commit()
                self.logger("Transaction confirmed (COMMIT)", "SUCCESS")
            else:
                # Errors found -> ROLLBACK changes
                self.session.rollback()
                self.logger(
                    f"Transaction reverted (ROLLBACK): {exc_val}", 
                    "ERROR"
                )
        finally:
            # Always close the session
            self.session.close()
            self.logger("Session closed", "INFO")
    
    # 3. Repository Properties (Lazy Loading)
    
    @property
    def bank_transactions(self):
        """Lazy loading for BankTransactionRepository."""
        if self._bank_transactions is None:
            from logic.infrastructure.repositories.bank_transaction_repository import BankTransactionRepository
            self._bank_transactions = BankTransactionRepository(self.session)
        return self._bank_transactions
    
    @property
    def invoices(self):
        """Lazy loading for InvoiceRepository."""
        if self._invoices is None:
            from logic.infrastructure.repositories.invoice_repository import InvoiceRepository
            self._invoices = InvoiceRepository(self.session)
        return self._invoices
    
    @property
    def payments(self):
        """Lazy loading for PaymentRepository."""
        if self._payments is None:
            from logic.infrastructure.repositories.payment_repository import PaymentRepository
            self._payments = PaymentRepository(self.session)
        return self._payments

    @property
    def cards(self):
        """Repository for card settlements and details."""
        if not hasattr(self, '_cards') or self._cards is None:
            from logic.infrastructure.repositories.card_repository import CardRepository
            self._cards = CardRepository(self.session)
        return self._cards

    @property
    def parking_breakdown(self):
        """Repository for parking payment breakdown."""
        if not hasattr(self, '_parking_breakdown') or self._parking_breakdown is None:
            from logic.infrastructure.repositories.parking_breakdown_repository import ParkingBreakdownRepository
            self._parking_breakdown = ParkingBreakdownRepository(self.session)
        return self._parking_breakdown

    @property
    def manual_requests(self):
        """Repository for manual requests."""
        if not hasattr(self, '_manual_requests') or self._manual_requests is None:
            from logic.infrastructure.repositories.manual_requests_repository import ManualRequestsRepository
            self._manual_requests = ManualRequestsRepository(self.session)
        return self._manual_requests

    @property
    def withholdings(self):
        """Repository for tax withholdings."""
        if not hasattr(self, '_withholdings') or self._withholdings is None:
            from logic.infrastructure.repositories.withholdings_repository import WithholdingsRepository
            self._withholdings = WithholdingsRepository(self.session)
        return self._withholdings

    @property
    def bank_enrichment(self):
        """Update-only repository for bank transaction enrichment."""
        if not hasattr(self, '_bank_enrichment') or self._bank_enrichment is None:
            from logic.infrastructure.repositories.bank_enrichment_repository import BankEnrichmentRepository
            self._bank_enrichment = BankEnrichmentRepository(self.session)
        return self._bank_enrichment

    @property
    def customer_portfolio(self):
        """Repository for customer portfolio management."""
        if not hasattr(self, '_customer_portfolio') or self._customer_portfolio is None:
            from logic.infrastructure.repositories.customer_portfolio_repository import CustomerPortfolioRepository
            self._customer_portfolio = CustomerPortfolioRepository(self.session)
        return self._customer_portfolio

    @property
    def bank_validation(self):
        """Repository for bank transaction validation metrics (LIQUIDACION TC)."""
        if not hasattr(self, '_bank_validation') or self._bank_validation is None:
            from logic.infrastructure.repositories.bank_repository import BankTransactionRepository
            self._bank_validation = BankTransactionRepository(self.session)
        return self._bank_validation

    @property
    def card_settlements(self):
        """Repository for card settlement reconciliation status updates."""
        if not hasattr(self, '_card_settlements') or self._card_settlements is None:
            from logic.infrastructure.repositories.card_settlement_repository import CardSettlementRepository
            self._card_settlements = CardSettlementRepository(self.session)
        return self._card_settlements

    # 4. Manual Transaction Control
    
    def commit(self):
        """Manually commits the current transaction."""
        self.session.commit()
        self.logger("Manual COMMIT executed", "INFO")
    
    def rollback(self):
        """Manually rolls back the current transaction."""
        self.session.rollback()
        self.logger("Manual ROLLBACK executed", "WARN")
    
    def flush(self):
        """Sends changes to the database without committing."""
        self.session.flush()
        self.logger("FLUSH executed (changes sent to DB)", "INFO")
