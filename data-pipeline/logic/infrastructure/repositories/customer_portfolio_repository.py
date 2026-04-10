"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.repositories.customer_portfolio_repository
===============================================================================

Description:
    Repository for managing customer portfolio documents within the 
    biq_stg.stg_customer_portfolio table. It handles delta synchronization 
    (inserts, updates, closures) and feedback loop synchronization with 
    related staging tables.

Responsibilities:
    - Insert new portfolio documents with pending status.
    - Mark documents as closed in SAP based on external references.
    - Update outstanding balances and conciliable amounts.
    - Persist portfolio changes, including splits and enrichment data.
    - Execute feedback loops to synchronize card details and parking breakdowns.

Key Components:
    - CustomerPortfolioRepository: Main class for portfolio data management.

Notes:
    - Migrated to PostgreSQL with support for BOOLEAN and JSON types.
    - Implements complex update logic with protection against data loss in Phase 3.
    - Uses batch processing for inserts and updates to optimize performance.

Dependencies:
    - pandas
    - numpy
    - sqlalchemy
    - utils.logger

===============================================================================
"""

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session
from utils.logger import get_logger


class CustomerPortfolioRepository:
    """
    Repository for biq_stg.stg_customer_portfolio.
    Operations: delta sync (INSERT/UPDATE/CLOSE) and feedback loop.
    """

    # 1. Configuration
    _TABLE          = "biq_stg.stg_customer_portfolio"
    _TABLE_DETAILS  = "biq_stg.stg_card_details"
    _TABLE_PARKING  = "biq_stg.stg_parking_pay_breakdown"
    _SCHEMA         = "biq_stg"
    _TABLE_NAME     = "stg_customer_portfolio"

    def __init__(self, session: Session):
        # 2. Initialization
        self.session = session
        self.logger  = get_logger("PORTFOLIO_REPO")

    # ─────────────────────────────────────────────────────────────────────────
    # 1. DELTA SYNC - INSERTION
    # ─────────────────────────────────────────────────────────────────────────

    def insert_new_documents(self, df_new: pd.DataFrame) -> int:
        """
        Inserts new portfolio documents with initial status.
        """

        if df_new.empty:
            return 0

        self.logger(f"Inserting {len(df_new)} new documents", "INFO")

        # 1. Column Selection
        cols = [
            'sap_doc_number', 'accounting_doc', 'customer_code',
            'customer_name', 'assignment', 'invoice_ref', 'doc_date',
            'due_date', 'amount_outstanding', 'currency',
            'reconcile_status', 'conciliable_amount',
            'sap_text', 'gl_account', 'internal_ref'
        ]

        valid_cols = [c for c in cols if c in df_new.columns]
        df_clean   = df_new[valid_cols].copy()

        # 2. Defaults Initialization
        df_clean['reconcile_status']    = 'PENDING'
        df_clean['conciliable_amount']  = df_clean['amount_outstanding']
        df_clean['partial_payment_flag'] = False
        df_clean['created_at']           = pd.Timestamp.now()

        df_clean = df_clean.replace({np.nan: None})

        # 3. Persistence
        df_clean.to_sql(
            name=self._TABLE_NAME,
            con=self.session.get_bind(),
            schema=self._SCHEMA,
            if_exists='append',
            index=False,
        )

        return len(df_clean)

    # ─────────────────────────────────────────────────────────────────────────
    # 2. DELTA SYNC - CLOSURE
    # ─────────────────────────────────────────────────────────────────────────

    def mark_as_closed_sap(self, invoice_refs: list) -> int:
        """
        Marks documents as CLOSED_SAP based on provided invoice references.
        """

        if not invoice_refs:
            return 0

        self.logger(f"Marking {len(invoice_refs)} documents as CLOSED_SAP", "INFO")

        # 1. Batch Processing
        chunks        = [invoice_refs[i:i + 1000] for i in range(0, len(invoice_refs), 1000)]
        total_updated = 0

        for chunk in chunks:
            # 2. Query Definition
            # ANY(:refs) is used for efficient PostgreSQL filtering
            query = text("""
                UPDATE biq_stg.stg_customer_portfolio
                SET reconcile_status = 'CLOSED_SAP',
                    closed_at        = NOW()
                WHERE invoice_ref = ANY(:refs)
                  AND reconcile_status != 'CLOSED_SAP'
            """)

            # 3. Execution
            result = self.session.execute(query, {"refs": chunk})
            total_updated += result.rowcount

        return total_updated

    # ─────────────────────────────────────────────────────────────────────────
    # 3. DELTA SYNC - BALANCE UPDATES
    # ─────────────────────────────────────────────────────────────────────────

    def update_balances(self, df_changes: pd.DataFrame) -> int:
        """
        Updates outstanding balances for existing portfolio documents.
        """

        if df_changes.empty:
            return 0

        self.logger(f"Updating {len(df_changes)} balances", "INFO")

        total_updated = 0

        for _, row in df_changes.iterrows():
            # 1. Query Definition
            query = text("""
                UPDATE biq_stg.stg_customer_portfolio
                SET amount_outstanding = :amt_new,
                    conciliable_amount = CASE
                        WHEN reconcile_status = 'PENDING' THEN :amt_new
                        ELSE conciliable_amount
                    END,
                    updated_at = NOW()
                WHERE invoice_ref = :ref
            """)

            # 2. Execution
            result = self.session.execute(query, {
                "amt_new": float(row['amount_outstanding_new']),
                "ref":     row['invoice_ref'],
            })

            total_updated += result.rowcount

        return total_updated

    # ─────────────────────────────────────────────────────────────────────────
    # 4. PORTFOLIO CHANGE PERSISTENCE
    # ─────────────────────────────────────────────────────────────────────────

    def save_portfolio_changes(self, df_portfolio: pd.DataFrame) -> int:
        """
        Persists all modified portfolio records (updates and new splits).
        """

        if df_portfolio.empty:
            return 0

        # 1. Change Detection
        mask_dirty = (
            (df_portfolio['reconcile_status'] != 'PENDING') |
            (df_portfolio['enrich_source'].notna()) |
            (df_portfolio['partial_payment_flag'] == 1)
        )

        df_to_save = df_portfolio[mask_dirty].copy()

        if df_to_save.empty:
            self.logger("No changes detected in portfolio", "INFO")
            return 0

        self.logger(f"Saving {len(df_to_save)} modified records", "INFO")

        df_to_save = df_to_save.replace({np.nan: None})

        # 2. Update vs Insert Split
        mask_update = df_to_save['stg_id'].notna()
        df_updates  = df_to_save[mask_update].copy()
        df_inserts  = df_to_save[~mask_update].copy()

        updates = 0
        inserts = 0

        # 3. Execution - Updates
        if not df_updates.empty:
            chunk_size = 50
            for i in range(0, len(df_updates), chunk_size):
                chunk = df_updates.iloc[i:i + chunk_size]
                for _, row in chunk.iterrows():
                    self._update_single_row(row)
                    updates += 1

            try:
                self.session.commit()
                self.logger(f"Updates committed ({updates})", "INFO")
            except Exception as e:
                self.logger(f"Error committing updates: {e}", "ERROR")
                self.session.rollback()
                raise

        # 4. Execution - Inserts
        if not df_inserts.empty:
            try:
                inserts = self._bulk_insert_splits(df_inserts)
                self.session.commit()
                self.logger(f"Inserts committed ({inserts})", "INFO")
            except Exception as e:
                self.logger(f"Error during inserts: {e}", "ERROR")
                self.session.rollback()
                raise

        self.logger(f"{updates} updated, {inserts} inserted", "SUCCESS")
        return updates + inserts

    def _bulk_insert_splits(self, df_inserts: pd.DataFrame) -> int:
        """
        Performs bulk insertion of new split portfolio records.
        """
 
        # 1. Column Filtering
        possible_cols = [
            'sap_doc_number', 'accounting_doc', 'customer_code',
            'customer_name', 'assignment', 'invoice_ref', 'doc_date',
            'due_date', 'amount_outstanding', 'currency',
            'reconcile_status', 'conciliable_amount', 'enrich_source',
            'enrich_batch', 'enrich_ref', 'enrich_brand', 'enrich_user',
            'reconcile_group', 'match_hash_key', 'etl_hash',
            'settlement_id', 'financial_amount_gross', 'financial_amount_net',
            'financial_commission', 'financial_tax_iva', 'financial_tax_irf',
            'match_method', 'match_confidence', 'is_suggestion',
            'sap_text', 'gl_account', 'internal_ref', 'partial_payment_flag',
            'is_partial_payment', 'sap_residual_amount',
        ]
 
        valid_cols = [c for c in possible_cols if c in df_inserts.columns]
        df_clean   = df_inserts[valid_cols].copy()
        df_clean['created_at'] = pd.Timestamp.now()
        df_clean = df_clean.replace({np.nan: None})
 
        # 2. Type Casting for PostgreSQL Compatibility
        if 'is_partial_payment' in df_clean.columns:
            df_clean['is_partial_payment'] = df_clean['is_partial_payment'].apply(
                lambda x: bool(x) if x is not None and pd.notna(x) else False
            ).astype(bool)
 
        if 'sap_residual_amount' in df_clean.columns:
            df_clean['sap_residual_amount'] = pd.to_numeric(
                df_clean['sap_residual_amount'], errors='coerce'
            )
        
        # 3. Persistence
        df_clean.to_sql(
            name=self._TABLE_NAME,
            con=self.session.get_bind(),
            schema=self._SCHEMA,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=50,
        )
 
        return len(df_clean)

    def _update_single_row(self, row: pd.Series):
        """
        Updates a single portfolio row with complex state protection logic.
        """

        def clean(val):
            if pd.isna(val) or val is np.nan:
                return None
            return val

        # 1. Data Cleaning
        raw_partial = row.get('is_partial_payment')
        is_partial_val = bool(raw_partial) if (
            raw_partial is not None and pd.notna(raw_partial)
        ) else None

        raw_residual = row.get('sap_residual_amount')
        residual_val = float(raw_residual) if (
            raw_residual is not None and pd.notna(raw_residual)
        ) else None

        # 2. Query Definition with Protected Fields
        query = text("""
            UPDATE biq_stg.stg_customer_portfolio
            SET reconcile_status        = :status,
                conciliable_amount      = :amt,
                enrich_source           = :src,
                enrich_batch            = :batch,
                enrich_ref              = :ref,
                enrich_brand            = :brand,
                enrich_user             = :user,
                reconcile_group         = :grp,
                match_hash_key          = :hash,
                settlement_id           = :set_id,
                match_method            = :method,
                match_confidence        = :confidence,
                is_suggestion           = :is_sugg,
                financial_amount_gross  = :f_gross,
                financial_amount_net    = :f_net,
                financial_commission    = :f_comm,
                financial_tax_iva       = :fin_iva,
                financial_tax_irf       = :fin_irf,
                etl_hash                = :ehash,
                partial_payment_flag    = :partial,
                is_partial_payment      = CASE
                    WHEN :is_partial_flag IS NOT DISTINCT FROM NULL
                        THEN is_partial_payment
                    WHEN is_partial_payment = TRUE
                        THEN TRUE
                    ELSE :is_partial_flag
                END,
                sap_residual_amount     = CASE
                    WHEN :residual IS NOT DISTINCT FROM NULL
                        THEN sap_residual_amount
                    WHEN is_partial_payment = TRUE
                        THEN sap_residual_amount
                    ELSE :residual
                END,
                updated_at              = NOW()
            WHERE stg_id = :id
        """)

        # 3. Execution
        self.session.execute(query, {
            "status":          row['reconcile_status'],
            "amt":             clean(row.get('conciliable_amount')),
            "src":             clean(row.get('enrich_source')),
            "batch":           clean(row.get('enrich_batch')),
            "ref":             clean(row.get('enrich_ref')),
            "brand":           clean(row.get('enrich_brand')),
            "user":            clean(row.get('enrich_user')),
            "grp":             clean(row.get('reconcile_group')),
            "hash":            clean(row.get('match_hash_key')),
            "set_id":          clean(row.get('settlement_id')),
            "method":          clean(row.get('match_method')),
            "confidence":      clean(row.get('match_confidence')),
            "is_sugg":         bool(row.get('is_suggestion', False)),
            "f_gross":         clean(row.get('financial_amount_gross')),
            "f_net":           clean(row.get('financial_amount_net')),
            "f_comm":          clean(row.get('financial_commission')),
            "fin_iva":         clean(row.get('financial_tax_iva')),
            "fin_irf":         clean(row.get('financial_tax_irf')),
            "ehash":           clean(row.get('etl_hash')),
            "partial":         bool(row.get('partial_payment_flag', False)),
            "is_partial_flag": is_partial_val,
            "residual":        residual_val,
            "id":              row['stg_id'],
        })

 

    # ─────────────────────────────────────────────────────────────────────────
    # 5. FEEDBACK LOOP OPERATIONS
    # ─────────────────────────────────────────────────────────────────────────

    def update_source_tables(self, df_portfolio: pd.DataFrame) -> int:
        """
        Feedback loop: synchronizes related tables after enrichment.
        """
        self.logger("Executing feedback loop", "INFO")
        total = 0
     
        # 1. VIP Card Sync
        mask_vip = (
            (df_portfolio['reconcile_status'] == 'ENRICHED') &
            (df_portfolio['reconcile_group']  == 'VIP_CARD') &
            (df_portfolio['match_hash_key'].notna())
        )
        vip_hashes = df_portfolio.loc[mask_vip, 'match_hash_key'].unique().tolist()
        if vip_hashes:
            total += self._update_card_details(vip_hashes)
     
        # 2. Parking Breakdown Sync
        mask_parking = (
            (df_portfolio['reconcile_status'] == 'ENRICHED') &
            (df_portfolio['reconcile_group']  == 'PARKING_CARD') &
            (df_portfolio['match_hash_key'].notna())
        )
        parking_hashes = df_portfolio.loc[mask_parking, 'match_hash_key'].unique().tolist()
        if parking_hashes:
            total += self._update_parking_breakdown(parking_hashes)
     
        # 3. Parking Individual Vouchers Sync
        parking_settlements = df_portfolio.loc[mask_parking, 'settlement_id'].dropna().unique().tolist()
        if parking_settlements:
            total += self._update_card_details_parking(parking_settlements)
     
        # 4. Direct SQL Sync Backup
        total += self._sync_card_details_from_portfolio()
     
        return total

    def _update_card_details(self, hashes: list) -> int:
        """Marks VIP vouchers as ASSIGNED based on hash keys."""
        chunks = [hashes[i:i + 1000] for i in range(0, len(hashes), 1000)]
        total  = 0
     
        for chunk in chunks:
            query = text("""
                UPDATE biq_stg.stg_card_details
                SET reconcile_status = 'ASSIGNED',
                    updated_at       = NOW()
                WHERE voucher_hash_key = ANY(:h)
                  AND reconcile_status  = 'PENDING'
            """)
            result = self.session.execute(query, {"h": chunk})
            total += result.rowcount
     
        if total > 0:
            self.logger(f"Marked {total} VIP vouchers as ASSIGNED", "INFO")
        return total

    def _update_parking_breakdown(self, hashes: list) -> int:
        """Marks parking breakdown batches as ASSIGNED."""

        chunks = [hashes[i:i + 1000] for i in range(0, len(hashes), 1000)]
        total  = 0

        for chunk in chunks:
            query = text("""
                UPDATE biq_stg.stg_parking_pay_breakdown
                SET reconcile_status = 'ASSIGNED'
                WHERE match_hash_key = ANY(:h)
                  AND reconcile_status != 'ASSIGNED'
            """)
            result = self.session.execute(query, {"h": chunk})
            total += result.rowcount

        self.logger(f"Marked {total} parking batches as ASSIGNED", "INFO")
        return total

    def _update_card_details_parking(self, settlement_ids: list) -> int:
        """Marks individual parking vouchers as ASSIGNED."""

        if not settlement_ids:
            return 0

        chunks = [settlement_ids[i:i + 1000] for i in range(0, len(settlement_ids), 1000)]
        total  = 0

        for chunk in chunks:
            query = text("""
                UPDATE biq_stg.stg_card_details
                SET reconcile_status = 'ASSIGNED',
                    updated_at       = NOW()
                WHERE settlement_id = ANY(:ids)
                  AND establishment_name = 'PARKING'
                  AND reconcile_status  != 'ASSIGNED'
            """)
            result = self.session.execute(query, {"ids": chunk})
            total += result.rowcount

        self.logger(f"Marked {total} parking vouchers as ASSIGNED", "INFO")
        return total

    def _sync_card_details_from_portfolio(self) -> int:
        """
        Direct SQL synchronization between portfolio and card details.
        Handles both exact hash matches and tolerance-based matches.
        """
        total = 0
     
        # 1. Exact Hash Sync
        query_a = text("""
            UPDATE biq_stg.stg_card_details
            SET    reconcile_status = 'ASSIGNED',
                   updated_at      = NOW()
            FROM   biq_stg.stg_customer_portfolio p
            WHERE  biq_stg.stg_card_details.voucher_hash_key = p.match_hash_key
              AND  p.reconcile_status  = 'ENRICHED'
              AND  p.settlement_id     IS NOT NULL
              AND  biq_stg.stg_card_details.reconcile_status = 'PENDING'
        """)
     
        result_a = self.session.execute(query_a)
        total += result_a.rowcount
     
        if result_a.rowcount > 0:
            self.logger(
                f"Synchronized {result_a.rowcount} vouchers by exact hash",
                "INFO"
            )
     
        # 2. Tolerance-Based Sync (Batch + Ref + Settlement)
        query_b = text("""
            UPDATE biq_stg.stg_card_details
            SET    reconcile_status = 'ASSIGNED',
                   updated_at      = NOW()
            FROM   biq_stg.stg_customer_portfolio p
            WHERE  biq_stg.stg_card_details.settlement_id  = p.settlement_id
              AND  biq_stg.stg_card_details.batch_number::VARCHAR
                   = p.enrich_batch::VARCHAR
              AND  biq_stg.stg_card_details.voucher_ref::VARCHAR
                   = p.enrich_ref::VARCHAR
              AND  p.reconcile_status  = 'ENRICHED'
              AND  p.match_method      = 'VIP_EXACT_BATCH_REF_WRONG_AMT'
              AND  p.match_confidence  = '95'
              AND  p.is_suggestion     = FALSE
              AND  biq_stg.stg_card_details.reconcile_status = 'PENDING'
        """)
     
        result_b = self.session.execute(query_b)
        total += result_b.rowcount
     
        if result_b.rowcount > 0:
            self.logger(
                f"Synchronized {result_b.rowcount} vouchers by tolerance match",
                "INFO"
            )

        return total

    # ─────────────────────────────────────────────────────────────────────────
    # 6. CARD SETTLEMENT AGGREGATIONS
    # ─────────────────────────────────────────────────────────────────────────

    def get_confirmed_aggregated_by_settlement(self) -> pd.DataFrame:
        """
        Returns confirmed (MATCHED/ENRICHED) VIP_CARD/PARKING_CARD portfolio
        records aggregated by settlement_id. Used by the validation metrics
        update to compute total_cartera per settlement.
        """
        query = text("""
            SELECT
                settlement_id,
                COUNT(*)                    AS count_confirmed,
                SUM(conciliable_amount)     AS total_cartera,
                STRING_AGG(
                    CAST(stg_id AS TEXT), ','
                    ORDER BY stg_id
                )                           AS matched_ids
            FROM biq_stg.stg_customer_portfolio
            WHERE settlement_id IS NOT NULL
              AND reconcile_status IN ('MATCHED', 'ENRICHED')
              AND reconcile_group IN ('VIP_CARD', 'PARKING_CARD')
              AND (is_suggestion = FALSE OR is_suggestion IS NULL)
            GROUP BY settlement_id
        """)
        return pd.read_sql(query, self.session.connection())

    def get_suggestions_aggregated_by_settlement(self) -> pd.DataFrame:
        """
        Returns suggested portfolio records aggregated by settlement_id.
        Used alongside confirmed invoices to detect pending suggestions.
        """
        query = text("""
            SELECT
                settlement_id,
                COUNT(*)    AS count_suggestions,
                STRING_AGG(
                    CAST(stg_id AS TEXT), ','
                    ORDER BY stg_id
                )           AS suggested_ids
            FROM biq_stg.stg_customer_portfolio
            WHERE settlement_id IS NOT NULL
              AND reconcile_status = 'ENRICHED'
              AND reconcile_group IN ('VIP_CARD', 'PARKING_CARD')
              AND is_suggestion = TRUE
            GROUP BY settlement_id
        """)
        return pd.read_sql(query, self.session.connection())
