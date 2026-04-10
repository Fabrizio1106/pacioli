"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.repositories.invoice_repository
===============================================================================

Description:
    Repository for managing SAP Open Items (FBL5N). It handles data access for 
    the customer portfolio, including filtering by customer, amount, and status.

Responsibilities:
    - Retrieve open items by customer code or SAP document number.
    - Search for invoices matching specific amounts within a tolerance range.
    - Update reconciliation status and associated metadata.
    - Provide statistics on open items by status.

Key Components:
    - InvoiceRepository: Data access class for the stg_customer_portfolio table.

Notes:
    - Target Table: biq_stg.stg_customer_portfolio.
    - Primary Key: stg_id.

Dependencies:
    - sqlalchemy
    - logic.infrastructure.repositories.base_repository

===============================================================================
"""

from logic.infrastructure.repositories.base_repository import BaseRepository
from typing import List, Dict, Any, Optional
from sqlalchemy import text
from datetime import datetime, date


class InvoiceRepository(BaseRepository):
    """
    Repository for customer open items (SAP FBL5N).
    
    TABLE: biq_stg.stg_customer_portfolio
    PRIMARY KEY: stg_id
    
    KEY COLUMNS:
    - customer_code: Customer Tax ID (RUC/cédula)
    - amount_outstanding: Total outstanding balance
    - conciliable_amount: Amount available for reconciliation
    - reconcile_status: Status (PENDING, MATCHED, etc.)
    """
    
    def _get_table_name(self) -> str:
        """Table name."""
        return "biq_stg.stg_customer_portfolio"
    
    def _get_primary_key(self) -> str:
        """Primary key field."""
        return "stg_id"
    
    # ──────────────────────────────────────────────────────────────────────────
    # SPECIFIC METHODS FOR INVOICES
    # ──────────────────────────────────────────────────────────────────────────
    
    def get_by_customer(
        self,
        customer_code: str,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieves open items for a specific customer.
        
        Parameters:
        -----------
        customer_code : str
            Customer code (Tax ID)
        
        status : str (optional)
            Filter by reconciliation status
        
        Returns:
        --------
        List of open items
        """
        filters = {"customer_code": customer_code}
        
        if status:
            filters["reconcile_status"] = status
        
        return self.get_all(
            filters=filters,
            order_by="doc_date DESC"
        )
    
    def get_by_customer_and_amount(
        self,
        customer_code: str,
        amount: float,
        tolerance: float = 0.01
    ) -> List[Dict[str, Any]]:
        """
        Searches for items from a customer with a similar amount.
        
        Useful for automatic matching.
        
        Parameters:
        -----------
        customer_code : str
            Customer code
        
        amount : float
            Amount to search for
        
        tolerance : float
            Tolerance (0.01 = ±1%)
        
        Returns:
        --------
        List of matching items
        """
        # 1. Initialization
        min_amount = amount * (1 - tolerance)
        max_amount = amount * (1 + tolerance)
        
        # 2. Query execution
        query = text(f"""
            SELECT * 
            FROM {self._get_table_name()}
            WHERE customer_code = :customer_code
              AND reconcile_status = 'PENDING'
              AND conciliable_amount >= :min_amount
              AND conciliable_amount <= :max_amount
            ORDER BY doc_date DESC
        """)
        
        results = self.session.execute(query, {
            "customer_code": customer_code,
            "min_amount": min_amount,
            "max_amount": max_amount
        }).fetchall()
        
        return [dict(row._mapping) for row in results]
    
    def get_by_sap_doc_number(self, sap_doc_number: str) -> Optional[Dict[str, Any]]:
        """
        Searches for an item by SAP document number.
        
        Parameters:
        -----------
        sap_doc_number : str
            SAP document number
        
        Returns:
        --------
        Dictionary with the item, or None if it doesn't exist
        """
        results = self.get_all(filters={"sap_doc_number": sap_doc_number})
        return results[0] if results else None
    
    def get_pending(
        self,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieves items pending reconciliation.
        
        Parameters:
        -----------
        limit : int (optional)
            Maximum number of records
        
        Returns:
        --------
        List of pending items
        """
        return self.get_all(
            filters={"reconcile_status": "PENDING"},
            order_by="doc_date DESC",
            limit=limit
        )
    
    def get_by_brand(
        self,
        brand: str,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieves items filtered by brand.
        
        Parameters:
        -----------
        brand : str
            Brand (DINERS, PACIFICARD, etc.)
        
        status : str (optional)
            Filter by status
        
        Returns:
        --------
        List of items for that brand
        """
        filters = {"enrich_brand": brand}
        
        if status:
            filters["reconcile_status"] = status
        
        return self.get_all(
            filters=filters,
            order_by="doc_date DESC"
        )
    
    def update_reconcile_status(
        self,
        invoice_id: int,
        new_status: str,
        match_method: Optional[str] = None,
        match_confidence: Optional[float] = None,
        matched_bank_refs: Optional[str] = None
    ) -> bool:
        """
        Updates the reconciliation status of an item.
        
        Parameters:
        -----------
        invoice_id : int
            Item ID
        
        new_status : str
            New status
        
        match_method : str (optional)
            Matching method used
        
        match_confidence : float (optional)
            Match confidence level
        
        matched_bank_refs : str (optional)
            Bank references that matched (JSON or CSV)
        
        Returns:
        --------
        bool : True if updated successfully
        """
        updates = {
            "reconcile_status": new_status,
            "updated_at": datetime.now()
        }
        
        if match_method:
            updates["match_method"] = match_method
        
        if match_confidence is not None:
            updates["match_confidence"] = str(match_confidence)
        
        if matched_bank_refs:
            updates["matched_bank_refs"] = matched_bank_refs
        
        # If marked as MATCHED, record when it was closed
        if new_status in ['MATCHED', 'MATCHED_MANUAL']:
            updates["closed_at"] = datetime.now()
        
        return self.update_by_id(invoice_id, updates)
    
    def get_statistics_by_status(self) -> Dict[str, Any]:
        """
        Retrieves statistics grouped by status.
        
        Returns:
        --------
        Dictionary with statistics
        """
        query = text(f"""
            SELECT 
                reconcile_status,
                COUNT(*) as count,
                SUM(conciliable_amount) as total_amount
            FROM {self._get_table_name()}
            GROUP BY reconcile_status
        """)
        
        results = self.session.execute(query).fetchall()
        
        stats = {
            "count_by_status": {},
            "amount_by_status": {},
            "total_count": 0,
            "total_amount": 0.0
        }
        
        for row in results:
            status = row.reconcile_status or "UNKNOWN"
            count = row.count
            amount = float(row.total_amount or 0)
            
            stats["count_by_status"][status] = count
            stats["amount_by_status"][status] = amount
            stats["total_count"] += count
            stats["total_amount"] += amount
        
        return stats
