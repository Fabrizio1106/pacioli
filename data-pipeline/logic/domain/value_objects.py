"""
===============================================================================
Project: PACIOLI
Module: logic.domain.value_objects
===============================================================================

Description:
    Defines immutable value objects that represent core business concepts
    in the reconciliation domain: bank transactions, open invoices, and
    confirmed match results.

Responsibilities:
    - Provide type-safe, immutable representations of reconciliation entities.
    - Expose business-rule properties (is_pending, is_matched, effective_amount).
    - Support construction from raw repository dictionaries via from_dict().

Key Components:
    - BankTransaction: Immutable snapshot of a bank transaction from staging.
    - Invoice: Immutable snapshot of an open customer portfolio entry (SAP FBL5N).
    - Match: Immutable result of a successful reconciliation operation.

Notes:
    - All dataclasses are frozen (frozen=True) to enforce immutability.
    - Value objects carry no database identity; equality is determined by value.

Dependencies:
    - dataclasses, datetime, decimal, typing

===============================================================================
"""

from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional, List
from decimal import Decimal


# ──────────────────────────────────────────────────────────────────────────────
# VALUE OBJECT 1: BankTransaction
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class BankTransaction:
    """
    Immutable representation of a bank transaction from staging.

    frozen=True enforces immutability: field values cannot be changed after
    creation. To represent a correction, a new instance must be constructed.

    Example:
        tx = BankTransaction(
            id=123,
            doc_number='2000002360',
            amount=Decimal('1255.74'),
            bank_date=datetime(2026, 2, 4),
            customer_id='0992345678001',
            reconcile_status='PENDING'
        )

        print(tx.amount)     # Decimal('1255.74')
        tx.amount = 1000     # Raises FrozenInstanceError
    """
    
    # Identification
    id: int                              # stg_id
    doc_number: str                      # doc_number

    # Dates
    doc_date: Optional[date]             # doc_date
    posting_date: Optional[date]         # posting_date
    bank_date: datetime                  # bank_date (primary)

    # Amounts
    amount: Decimal                      # amount_total
    currency: str = 'USD'                # currency

    # Customer (populated after enrichment)
    customer_id: Optional[str] = None    # enrich_customer_id
    customer_name: Optional[str] = None  # enrich_customer_name

    # References
    bank_ref_1: Optional[str] = None     # bank_ref_1
    bank_ref_2: Optional[str] = None     # bank_ref_2
    doc_reference: Optional[str] = None  # doc_reference

    # Description
    description: Optional[str] = None    # bank_description

    # Brand and category
    brand: Optional[str] = None          # brand (DINERS, PACIFICARD, etc.)
    global_category: Optional[str] = None # global_category

    # Reconciliation status
    reconcile_status: str = 'PENDING'    # reconcile_status

    # Hash for matching
    match_hash: Optional[str] = None     # match_hash_key
    
    def __str__(self) -> str:
        """Return a human-readable summary of the transaction."""
        return (
            f"TX#{self.id} | Doc: {self.doc_number} | "
            f"${self.amount} | {self.bank_date.date()} | "
            f"Status: {self.reconcile_status}"
        )

    @property
    def is_pending(self) -> bool:
        """True if the transaction has not yet been reconciled."""
        return self.reconcile_status == 'PENDING'

    @property
    def is_matched(self) -> bool:
        """True if the transaction has been successfully reconciled."""
        return self.reconcile_status in ['MATCHED', 'MATCHED_MANUAL']

    @classmethod
    def from_dict(cls, data: dict) -> 'BankTransaction':
        """
        Construct a BankTransaction from a repository result dictionary.

        Args:
            data: Dictionary with keys matching the staging table columns.

        Returns:
            BankTransaction instance populated from the dictionary.
        """
        return cls(
            id=data['stg_id'],
            doc_number=data['doc_number'],
            doc_date=data.get('doc_date'),
            posting_date=data.get('posting_date'),
            bank_date=data['bank_date'],
            amount=Decimal(str(data['amount_total'])),
            currency=data.get('currency', 'USD'),
            customer_id=data.get('enrich_customer_id'),
            customer_name=data.get('enrich_customer_name'),
            bank_ref_1=data.get('bank_ref_1'),
            bank_ref_2=data.get('bank_ref_2'),
            doc_reference=data.get('doc_reference'),
            description=data.get('bank_description'),
            brand=data.get('brand'),
            global_category=data.get('global_category'),
            reconcile_status=data.get('reconcile_status', 'PENDING'),
            match_hash=data.get('match_hash_key')
        )


# ──────────────────────────────────────────────────────────────────────────────
# VALUE OBJECT 2: Invoice
# ──────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Invoice:
    """
    Immutable representation of an open customer portfolio entry (SAP FBL5N).

    Source table: biq_stg.stg_customer_portfolio

    Key fields:
        amount_outstanding: Total outstanding balance.
        conciliable_amount: Amount eligible for reconciliation (may differ
            from outstanding due to withholdings, commissions, etc.).
        partial_payment_flag: Whether partial payment is accepted.

    Example:
        invoice = Invoice(
            id=456,
            sap_doc_number='1900012345',
            accounting_doc='5100012345',
            customer_code='0992345678001',
            doc_date=date(2026, 2, 1),
            amount_outstanding=Decimal('1255.74'),
            conciliable_amount=Decimal('1255.74'),
            customer_name='EMPRESA ABC S.A.',
            reconcile_status='PENDING'
        )
    """
    
    # ──────────────────────────────────────────────────────────────────────────
    # Required fields (no default) — must come first in dataclass ordering
    # ──────────────────────────────────────────────────────────────────────────

    # Identification
    id: int                                      # stg_id
    sap_doc_number: str                          # sap_doc_number
    accounting_doc: str                          # accounting_doc (journal entry)
    customer_code: str                           # customer_code (tax ID / RUC)
    doc_date: date                               # doc_date
    amount_outstanding: Decimal                  # amount_outstanding (total balance)
    conciliable_amount: Decimal                  # conciliable_amount (eligible amount)

    # ──────────────────────────────────────────────────────────────────────────
    # Optional fields (with default) — must come after required fields
    # ──────────────────────────────────────────────────────────────────────────

    # Customer
    customer_name: Optional[str] = None          # customer_name

    # Dates
    due_date: Optional[date] = None              # due_date

    # Amounts
    currency: str = 'USD'                        # currency

    # References
    assignment: Optional[str] = None             # assignment
    invoice_ref: Optional[str] = None            # invoice_ref
    internal_ref: Optional[str] = None           # internal_ref

    # Enrichment
    enrich_batch: Optional[str] = None           # enrich_batch
    enrich_ref: Optional[str] = None             # enrich_ref
    enrich_brand: Optional[str] = None           # enrich_brand (DINERS, PACIFICARD, etc.)

    # Reconciliation
    reconcile_status: str = 'PENDING'            # reconcile_status
    reconcile_group: Optional[str] = None        # reconcile_group
    settlement_id: Optional[str] = None          # settlement_id

    # Match metadata
    match_hash: Optional[str] = None             # match_hash_key
    match_method: Optional[str] = None           # match_method
    match_confidence: Optional[str] = None       # match_confidence

    # Financial breakdown
    financial_amount_gross: Optional[Decimal] = None    # financial_amount_gross
    financial_amount_net: Optional[Decimal] = None      # financial_amount_net
    financial_commission: Optional[Decimal] = None      # financial_commission
    financial_tax_iva: Optional[Decimal] = None         # financial_tax_iva
    financial_tax_irf: Optional[Decimal] = None         # financial_tax_irf

    # Other
    gl_account: Optional[str] = None             # gl_account
    sap_text: Optional[str] = None               # sap_text
    partial_payment_flag: bool = False           # partial_payment_flag
    
    def __str__(self) -> str:
        """Return a human-readable summary of the invoice."""
        return (
            f"INV#{self.id} | SAP: {self.sap_doc_number} | "
            f"Customer: {self.customer_code} | "
            f"Outstanding: ${self.amount_outstanding} | "
            f"Conciliable: ${self.conciliable_amount} | "
            f"Status: {self.reconcile_status}"
        )

    @property
    def is_pending(self) -> bool:
        """True if the invoice has not yet been reconciled."""
        return self.reconcile_status == 'PENDING'

    @property
    def is_matched(self) -> bool:
        """True if the invoice has been successfully reconciled."""
        return self.reconcile_status in ['MATCHED', 'MATCHED_MANUAL']

    @property
    def is_partial(self) -> bool:
        """True if the invoice accepts partial payments."""
        return self.partial_payment_flag

    @property
    def effective_amount(self) -> Decimal:
        """
        Amount to use for matching purposes.

        Returns conciliable_amount, which may differ from amount_outstanding
        due to withholdings, commissions, or other adjustments.
        """
        return self.conciliable_amount

    @classmethod
    def from_dict(cls, data: dict) -> 'Invoice':
        """
        Construct an Invoice from a repository result dictionary.

        Source table: biq_stg.stg_customer_portfolio

        Args:
            data: Dictionary with keys matching the portfolio table columns.

        Returns:
            Invoice instance populated from the dictionary.
        """
        return cls(
            # Campos requeridos primero
            id=data['stg_id'],
            sap_doc_number=data['sap_doc_number'],
            accounting_doc=data['accounting_doc'],
            customer_code=data['customer_code'],
            doc_date=data['doc_date'],
            amount_outstanding=Decimal(str(data['amount_outstanding'])),
            conciliable_amount=Decimal(str(data['conciliable_amount'])),
            # Campos opcionales después
            customer_name=data.get('customer_name'),
            due_date=data.get('due_date'),
            currency=data.get('currency', 'USD'),
            assignment=data.get('assignment'),
            invoice_ref=data.get('invoice_ref'),
            internal_ref=data.get('internal_ref'),
            enrich_batch=data.get('enrich_batch'),
            enrich_ref=data.get('enrich_ref'),
            enrich_brand=data.get('enrich_brand'),
            reconcile_status=data.get('reconcile_status', 'PENDING'),
            reconcile_group=data.get('reconcile_group'),
            settlement_id=data.get('settlement_id'),
            match_hash=data.get('match_hash_key'),
            match_method=data.get('match_method'),
            match_confidence=data.get('match_confidence'),
            financial_amount_gross=Decimal(str(data['financial_amount_gross'])) if data.get('financial_amount_gross') else None,
            financial_amount_net=Decimal(str(data['financial_amount_net'])) if data.get('financial_amount_net') else None,
            financial_commission=Decimal(str(data['financial_commission'])) if data.get('financial_commission') else None,
            financial_tax_iva=Decimal(str(data['financial_tax_iva'])) if data.get('financial_tax_iva') else None,
            financial_tax_irf=Decimal(str(data['financial_tax_irf'])) if data.get('financial_tax_irf') else None,
            gl_account=data.get('gl_account'),
            sap_text=data.get('sap_text'),
            partial_payment_flag=bool(data.get('partial_payment_flag', 0))
        )


# ──────────────────────────────────────────────────────────────────────────────
# VALUE OBJECT 3: Match (Reconciliation Result)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Match:
    """
    Immutable result of a successful reconciliation between a bank transaction
    and one or more customer invoices.

    Returned by MatchingService when a valid match is found. Consumers use this
    object to update reconciliation status in the database.

    Example:
        match = Match(
            bank_tx_id=123,
            invoice_ids=[456],
            confidence_score=Decimal('100.00'),
            match_method='EXACT_SINGLE',
            amount_difference=Decimal('0.00'),
            notes='Exact match: amount and customer align'
        )

        uow.bank_transactions.update_reconcile_status(
            tx_id=match.bank_tx_id,
            new_status='MATCHED',
            match_method=match.match_method,
            match_confidence=float(match.confidence_score)
        )
    """
    
    # Related IDs
    bank_tx_id: int                      # ID of the bank transaction
    invoice_ids: List[int]               # Invoice IDs matched (one or more)

    # Match confidence
    confidence_score: Decimal            # Range: 0.00 to 100.00

    # Method used
    match_method: str                    # EXACT_SINGLE, TOLERANCE_SINGLE, etc.

    # Amount discrepancy
    amount_difference: Decimal = Decimal('0.00')  # Difference between bank and invoice amounts

    # Additional notes
    notes: Optional[str] = None

    # Computed at match time
    matched_at: datetime = None
    
    def __post_init__(self):
        """Populate matched_at with the current timestamp if not provided."""
        if self.matched_at is None:
            object.__setattr__(self, 'matched_at', datetime.now())
    
    def __str__(self) -> str:
        """Return a human-readable summary of the match result."""
        inv_str = f"{len(self.invoice_ids)} invoice(s)" if len(self.invoice_ids) > 1 else f"Invoice {self.invoice_ids[0]}"
        return (
            f"Match | TX#{self.bank_tx_id} ↔ {inv_str} | "
            f"Confidence: {self.confidence_score}% | "
            f"Method: {self.match_method}"
        )
    
    @property
    def is_exact_match(self) -> bool:
        """True if the matched amounts are identical (zero difference)."""
        return self.amount_difference == Decimal('0.00')

    @property
    def is_high_confidence(self) -> bool:
        """True if the confidence score is 95% or above."""
        return self.confidence_score >= Decimal('95.00')

    @property
    def is_multi_invoice(self) -> bool:
        """True if the match covers more than one invoice."""
        return len(self.invoice_ids) > 1

    def to_dict(self) -> dict:
        """
        Serialize the match result to a dictionary for persistence or logging.
        """
        return {
            'bank_tx_id': self.bank_tx_id,
            'invoice_ids': self.invoice_ids,
            'confidence_score': float(self.confidence_score),
            'match_method': self.match_method,
            'amount_difference': float(self.amount_difference),
            'notes': self.notes,
            'matched_at': self.matched_at.isoformat()
        }


# ══════════════════════════════════════════════════════════════════════════════
# USAGE EXAMPLE
# ══════════════════════════════════════════════════════════════════════════════

"""
EJEMPLO PRÁCTICO: Convertir datos del repository a Value Objects

from logic.infrastructure.unit_of_work import UnitOfWork
from logic.domain.value_objects import BankTransaction, Invoice, Match
from decimal import Decimal

engine = get_db_engine('stg')

with UnitOfWork(engine) as uow:
    # 1. Obtener datos del repository (dict)
    tx_dict = uow.bank_transactions.get_by_id(123)
    
    # 2. Convertir a Value Object
    tx = BankTransaction.from_dict(tx_dict)
    
    # 3. Ahora puedes usar propiedades y métodos de negocio
    print(tx)  # TX#123 | Doc: 2000002360 | $1255.74 | 2026-02-04 | Status: PENDING
    
    if tx.is_pending:
        print(f"Cliente: {tx.customer_name}")
        print(f"Monto: ${tx.amount}")
    
    # 4. Simular un match
    match = Match(
        bank_tx_id=tx.id,
        invoice_ids=[456],
        confidence_score=Decimal('100.00'),
        match_method='EXACT_SINGLE',
        notes='Match automático: monto y cliente coinciden'
    )
    
    print(match)
    print(f"¿Es exacto? {match.is_exact_match}")
    print(f"¿Alta confianza? {match.is_high_confidence}")
    
    # 5. Guardar el match en BD
    if match.is_high_confidence:
        uow.bank_transactions.update_reconcile_status(
            tx_id=match.bank_tx_id,
            new_status='MATCHED',
            match_method=match.match_method,
            match_confidence=float(match.confidence_score)
        )

────────────────────────────────────────────────────────────────────────────────
BENEFICIOS DE VALUE OBJECTS:
────────────────────────────────────────────────────────────────────────────────
✅ Inmutables (no hay cambios accidentales)
✅ Representan conceptos de negocio (no son solo dicts)
✅ Métodos de negocio (is_pending, is_exact_match, etc.)
✅ Fácil de testear (puedes crear objetos sin BD)
✅ Type hints (el IDE te ayuda con autocompletado)
"""