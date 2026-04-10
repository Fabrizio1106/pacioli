"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.extractors.customer_portfolio_extractor
===============================================================================

Description:
    Extracts customer portfolio data from both raw (SAP FBL5N, Webpos) and 
    staging layers. Implements "Active Window" logic to optimize performance 
    by focusing on recently active, pending, or recently closed documents, 
    preventing unnecessary processing of historical data.

Responsibilities:
    - Extract SAP FBL5N snapshots and Webpos data for enrichment.
    - Retrieve active portfolio documents with explicit column selection to 
      ensure data integrity and prevent silent failures.
    - Extract pending bank parking payments and their corresponding breakdowns.
    - Retrieve non-reconciled card vouchers for specialized establishments 
      (VIP, Assistance).

Key Components:
    - CustomerPortfolioExtractor: Orchestrates multi-source portfolio 
      extraction and implements performance optimizations.

Notes:
    - Version 4.0: Uses explicit SELECT columns for portfolio loading to 
      avoid silent schema mismatch bugs.
    - Active Window logic includes pending items and a 30-day lookback for 
      recently closed items.

Dependencies:
    - pandas
    - sqlalchemy
    - sqlalchemy.engine
    - utils.logger

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from utils.logger import get_logger


class CustomerPortfolioExtractor:
    """
    Extractor for CustomerPortfolio with intelligent Active Window.
    """

    def __init__(self, engine_raw: Engine, engine_stg: Engine):
        """
        Initializes the extractor with raw and staging engines.
        """
        self.engine_raw = engine_raw
        self.engine_stg = engine_stg
        self.logger = get_logger("PORTFOLIO_EXTRACTOR")
    
    # 1. Raw Extraction Methods (SAP and Webpos)

    def extract_sap_snapshot(self) -> pd.DataFrame:
        """
        Extracts a full snapshot from SAP FBL5N (raw_customer_portfolio).
        """
        
        self.logger("Extracting SAP snapshot (biq_raw.raw_customer_portfolio)...", "INFO")

        query = text("""
            SELECT 
                hash_id,
                cuenta,
                cliente,
                referencia,
                asignacion,
                fecha_documento,
                importe,
                fecha_de_pago,
                dias,
                texto,
                n_documento,
                clase_de_documento,
                moneda_local,
                cuenta_de_mayor,
                doc_compensacion,
                fecha_compensacion,
                referencia_a_factura
            FROM biq_raw.raw_customer_portfolio
        """)

        df = pd.read_sql(query, self.engine_raw)
        self.logger(f"Extracted {len(df)} documents from SAP", "INFO")
        
        return df

    def extract_webpos(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Extracts Webpos data for enrichment within a specific date range.
        """
        
        self.logger(f"Extracting Webpos data ({start_date} to {end_date})...", "INFO")

        query = text("""
            SELECT 
                factura,
                tipo_pago,
                lote,
                numero_de_referencia,
                usuario,
                estacion,
                total,
                fecha
            FROM biq_raw.raw_webpos
            WHERE fecha BETWEEN CAST(:start AS DATE) - INTERVAL '5 day'
                           AND CAST(:end AS DATE) + INTERVAL '5 day'
        """)

        df = pd.read_sql(
            query,
            self.engine_raw,
            params={"start": start_date, "end": end_date}
        )

        self.logger(f"Extracted {len(df)} Webpos records", "INFO")
        return df

    # 2. Optimized Portfolio Retrieval (Active Window)

    def get_active_portfolio(self, lookback_days: int = 90) -> pd.DataFrame:
        """
        Retrieves the active portfolio using intelligent windowing.
        
        Filtering Rules:
        ----------------
        1. Actively open invoices (PENDING, REVIEW, PARTIAL_PAYMENT).
        2. Recently closed invoices (last 30 days).
        3. Any record touched within the specified lookback window.
        """
    
        self.logger("Loading active portfolio (Active Window v4.0 - Explicit SELECT)...", "INFO")
    
        query = text(f"""
            SELECT
                -- Identifiers
                stg_id,
                sap_doc_number,
                accounting_doc,
                customer_code,
                customer_name,
                invoice_ref,
                assignment,
    
                -- Dates
                doc_date,
                due_date,
                created_at,
                updated_at,
                closed_at,
    
                -- SAP Amounts
                amount_outstanding,
                currency,
                conciliable_amount,
    
                -- Additional SAP Data
                sap_text,
                gl_account,
                internal_ref,
    
                -- Webpos Enrichment
                enrich_source,
                enrich_batch,
                enrich_ref,
                enrich_brand,
                enrich_user,
    
                -- Matching and Reconciliation
                reconcile_status,
                reconcile_group,
                match_hash_key,
                match_method,
                match_confidence,
                is_suggestion,
    
                -- Settlement and Financial
                settlement_id,
                financial_amount_gross,
                financial_amount_net,
                financial_commission,
                financial_tax_iva,
                financial_tax_irf,
    
                -- ETL Metadata
                etl_hash,
    
                -- Partial Payment and Parking
                partial_payment_flag,
                is_partial_payment,
                sap_residual_amount
    
            FROM biq_stg.stg_customer_portfolio
            WHERE
                (
                    -- Rule 1: Live invoices regardless of age
                    reconcile_status IN ('PENDING', 'REVIEW', 'PARTIAL_PAYMENT')
                    AND conciliable_amount > 0
                )
                OR
                (
                    -- Rule 2: Recent closures for late detection
                    reconcile_status = 'CLOSED_SAP'
                    AND updated_at >= NOW() - INTERVAL '30 day'
                )
                OR
                (
                    -- Rule 3: Safety net for recently modified records
                    updated_at >= NOW() - INTERVAL '{lookback_days} days'
                )
        """)
    
        try:
            df = pd.read_sql(query, self.engine_stg)
    
            self.logger(f"Loaded {len(df)} active documents", "INFO")
    
            if not df.empty:
                # 3. Validation: Check status breakdown and critical columns
                status_counts = df['reconcile_status'].value_counts()
                self.logger("Status breakdown:", "INFO")
                for status, count in status_counts.items():
                    self.logger(f"  - {status}: {count}", "INFO")
    
                critical_cols = ['is_partial_payment', 'sap_residual_amount', 'partial_payment_flag']
                for col in critical_cols:
                    if col not in df.columns:
                        self.logger(
                            f"CRITICAL WARNING: Column '{col}' missing from result set. "
                            f"Check biq_stg.stg_customer_portfolio schema.",
                            "ERROR"
                        )
    
            if len(df) > 500_000:
                self.logger(
                    f"WARNING: Loading {len(df)} rows (>500K). "
                    f"Consider reducing lookback_days.",
                    "WARN"
                )
    
            return df
    
        except Exception as e:
            self.logger(f"Error extracting active portfolio: {e}", "ERROR")
            raise

    def get_vip_portfolio(self) -> pd.DataFrame:
        """
        Retrieves only the portfolio documents relevant to Phase 3 (VIP Cascade).

        Rationale:
        ----------
        get_active_portfolio() loads the full active window (~3,000+ documents)
        because Phases 1 and 2 need the complete picture. Phase 3 (VIP Cascade)
        only needs a small, well-defined subset: documents that either are already
        classified as VIP_CARD or belong to the VIP gl_account and could become
        VIP_CARD in this run. Loading the full portfolio in Phase 3 wastes memory,
        transfer time, and makes generate_etl_hash + save_portfolio_changes operate
        on 3,000 rows when only ~200 are relevant.

        Filtering Rules:
        ----------------
        Rule 1 — VIP_CARD candidates (PENDING):
            Documents already classified as VIP_CARD waiting to be matched.
            These are the primary input to enrich_vip_cascade.

        Rule 2 — VIP gl_account (PENDING, any group):
            Documents with gl_account = '1120114035' (Salas VIP contable account)
            that are still PENDING. Some may not yet have reconcile_group = VIP_CARD
            if the Webpos enrichment classified them differently. Including them
            here gives the cascade a chance to pick them up.

        Rule 3 — Recently ENRICHED VIP documents (last 2 days):
            Already processed VIP_CARD docs from recent runs. Required so that
            update_source_tables / _sync_card_details_from_portfolio can see the
            full ENRICHED context and correctly mark vouchers as ASSIGNED without
            re-processing them.

        Safety:
        -------
        - Does NOT load PARKING_CARD, CASH, CLOSED_SAP, or any non-VIP document.
        - save_portfolio_changes uses WHERE stg_id = :id — row-level updates.
          Loading a subset is completely safe: only the rows in the DataFrame
          are touched; all other portfolio records remain unchanged in the DB.
        - update_source_tables extracts hashes from the DataFrame and runs
          UPDATE ... WHERE voucher_hash_key = ANY(:h) — also hash-scoped,
          not affected by what is absent from the DataFrame.
        - _sync_card_details_from_portfolio is a pure SQL JOIN on the DB tables
          and does not depend on the DataFrame at all.

        Returns:
        --------
        pd.DataFrame with the same column schema as get_active_portfolio().
        """
        self.logger(
            "Loading VIP portfolio (Phase 3 scope — gl_account 1120114035)...",
            "INFO"
        )

        query = text("""
            SELECT
                -- Identifiers
                stg_id,
                sap_doc_number,
                accounting_doc,
                customer_code,
                customer_name,
                invoice_ref,
                assignment,

                -- Dates
                doc_date,
                due_date,
                created_at,
                updated_at,
                closed_at,

                -- SAP Amounts
                amount_outstanding,
                currency,
                conciliable_amount,

                -- Additional SAP Data
                sap_text,
                gl_account,
                internal_ref,

                -- Webpos Enrichment
                enrich_source,
                enrich_batch,
                enrich_ref,
                enrich_brand,
                enrich_user,

                -- Matching and Reconciliation
                reconcile_status,
                reconcile_group,
                match_hash_key,
                match_method,
                match_confidence,
                is_suggestion,

                -- Settlement and Financial
                settlement_id,
                financial_amount_gross,
                financial_amount_net,
                financial_commission,
                financial_tax_iva,
                financial_tax_irf,

                -- ETL Metadata
                etl_hash,

                -- Partial Payment and Parking
                partial_payment_flag,
                is_partial_payment,
                sap_residual_amount

            FROM biq_stg.stg_customer_portfolio
            WHERE
                (
                    -- Rule 1: Active VIP_CARD candidates waiting to be matched
                    reconcile_group  = 'VIP_CARD'
                    AND reconcile_status = 'PENDING'
                    AND conciliable_amount > 0
                )
                OR
                (
                    -- Rule 2: VIP gl_account documents still open
                    -- gl_account 1120114035 = Salas VIP contable account
                    gl_account::VARCHAR = '1120114035'
                    AND reconcile_status IN ('PENDING', 'REVIEW', 'PARTIAL_PAYMENT')
                    AND conciliable_amount > 0
                )
                OR
                (
                    -- Rule 3: Recently ENRICHED VIP documents for feedback loop context
                    reconcile_group  = 'VIP_CARD'
                    AND reconcile_status = 'ENRICHED'
                    AND updated_at >= NOW() - INTERVAL '2 days'
                )
        """)

        try:
            df = pd.read_sql(query, self.engine_stg)

            self.logger(f"Loaded {len(df)} VIP-scoped documents", "INFO")

            if not df.empty:
                status_counts = df['reconcile_status'].value_counts()
                self.logger("Status breakdown:", "INFO")
                for status, count in status_counts.items():
                    self.logger(f"  - {status}: {count}", "INFO")

                critical_cols = [
                    'is_partial_payment', 'sap_residual_amount', 'partial_payment_flag'
                ]
                for col in critical_cols:
                    if col not in df.columns:
                        self.logger(
                            f"CRITICAL WARNING: Column '{col}' missing. "
                            f"Check biq_stg.stg_customer_portfolio schema.",
                            "ERROR"
                        )

            return df

        except Exception as e:
            self.logger(f"Error extracting VIP portfolio: {e}", "ERROR")
            raise

    # 4. Specialized Extraction Methods (Parking and VIP)

    def extract_bank_parking_pending(self) -> pd.DataFrame:
        """
        Extracts bank parking payments pending reconciliation.
        """
        
        self.logger("Extracting pending parking payments...", "INFO")

        query = text("""
            SELECT 
                settlement_id,
                batch_number,
                brand,
                amount_total,
                bank_date,
                establishment_name
            FROM biq_stg.stg_bank_transactions
            WHERE trans_type = 'LIQUIDACION TC'
              AND establishment_name = 'PARKING'
              AND reconcile_status IN ('PENDING', 'REVIEW')
              AND is_compensated_sap = FALSE
              AND is_compensated_intraday = FALSE
              AND settlement_id IS NOT NULL
              AND batch_number IS NOT NULL
        """)

        df = pd.read_sql(query, self.engine_stg)
        self.logger(f"Found {len(df)} pending parking payments", "INFO")
        
        return df

    def extract_parking_breakdown(self) -> pd.DataFrame:
        """Extracts batch breakdowns for parking reconciliation."""
        
        self.logger("Extracting parking breakdown data...", "INFO")

        query = text("""
            SELECT 
                stg_id,
                batch_number,
                brand,
                match_hash_key,
                settlement_date,
                settlement_id,
                amount_gross,
                amount_net,
                amount_commission,
                amount_tax_iva,
                amount_tax_irf
            FROM biq_stg.stg_parking_pay_breakdown
            WHERE reconcile_status != 'MATCHED'
        """)

        df = pd.read_sql(query, self.engine_stg)
        self.logger(f"Found {len(df)} available parking batches", "INFO")
        
        return df

    def extract_card_details_vip(self) -> pd.DataFrame:
        """
        Extracts non-reconciled card vouchers for VIP and Assistance services.
        """
        
        self.logger("Extracting card details for VIP/Assistance...", "INFO")

        query = text("""
            SELECT 
                voucher_hash_key,
                settlement_id,
                amount_gross,
                amount_net,
                amount_commission,
                amount_tax_iva,
                amount_tax_irf,
                brand,
                batch_number,
                voucher_ref,
                establishment_name
            FROM biq_stg.stg_card_details
            WHERE reconcile_status != 'CONCILIADO'
              AND establishment_name IN ('SALAS VIP', 'ASISTENCIAS')
        """)

        df = pd.read_sql(query, self.engine_stg)
        
        self.logger(
            f"Found {len(df)} VIP/Assistance vouchers",
            "INFO"
        )
        
        # 5. Logging: Breakdown by establishment
        if not df.empty and 'establishment_name' in df.columns:
            establishment_counts = df['establishment_name'].value_counts()
            for establishment, count in establishment_counts.items():
                self.logger(
                    f"  - {establishment}: {count} vouchers",
                    "INFO"
                )
        
        return df