"""
===============================================================================
Project: PACIOLI
Module: logic.application.commands.staging.process_portfolio_load
===============================================================================

Description:
    Implements a Change Data Capture (CDC) command to synchronize the staging 
    customer portfolio with raw SAP FBL5N data. It ensures the staging area 
    reflects the current SAP state while preserving existing work.

Responsibilities:
    - Perform delta synchronization between raw SAP data and staging portfolio.
    - Implement content-based idempotency checks using fingerprints.
    - Classify records into inserts, updates, closures, or unchanged categories.
    - Handle complex split scenarios (e.g., Parking) with exact amount matching.
    - Maintain the system invariant: SUM(conciliable_amount) == SUM(outstanding).

Key Components:
    - ProcessPortfolioLoadCommand: Main CDC orchestrator for portfolio sync.

Notes:
    - Protected statuses (MATCHED, CLOSED, etc.) are shielded from modification.
    - Uses bulk database operations for performance.
    - Relies on PortfolioHashService for stable record identification.

Dependencies:
    - pandas, sqlalchemy, hashlib, traceback
    - utils.logger, logic.domain.services.portfolio_hash_service
    - logic.infrastructure.batch_tracker

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from utils.logger import get_logger
from logic.domain.services.portfolio_hash_service import PortfolioHashService
from logic.infrastructure.batch_tracker import BatchTracker


class ProcessPortfolioLoadCommand:
    """
    CDC Command to synchronize stg_customer_portfolio with raw FBL5N.
    """

    # Statuses that the CDC NEVER modifies - they are final pipeline states
    PROTECTED_STATUSES = frozenset({
        'MATCHED',
        'INTERNAL_COMPENSATED',
        'CLOSED',
    })

    # Statuses that the CDC can close if SAP no longer sends the invoice
    CLOSEABLE_STATUSES = frozenset({
        'PENDING',
        'ENRICHED',
        'REVIEW',
        'WITHHOLDING_APPLIED',
        'PARTIAL_MATCH',
    })

    def __init__(self, session, engine_config):
        """
        Initialization.
        
        Args:
            session: SQLAlchemy session from UnitOfWork (for staging).
            engine_config: biq_config engine (for BatchTracker).
        """
        self.session       = session
        self.logger        = get_logger("PORTFOLIO_LOAD_CDC")
        self.batch_tracker = BatchTracker(engine_config, "PORTFOLIO_CDC")

    def execute(self, df_raw: pd.DataFrame) -> dict:
        """
        Executes the full CDC process.

        Args:
            df_raw: DataFrame from raw_customer_portfolio.

        Returns:
            dict: Statistics of processed records (skipped, inserted, etc.).
        """
        self.logger("=" * 80, "INFO")
        self.logger("PORTFOLIO CDC v2.0 - Synchronizing FBL5N -> staging", "INFO")
        self.logger("=" * 80, "INFO")

        stats = {
            'skipped': False, 'inserted': 0, 'updated': 0,
            'closed': 0, 'unchanged': 0, 'errors': 0,
        }

        if df_raw.empty:
            self.logger("Empty DataFrame - nothing to process", "INFO")
            return stats

        # 1. Idempotency Check
        fingerprint = self._compute_raw_fingerprint(df_raw)
        self.logger(f"   Raw Fingerprint: {fingerprint[:12]}...", "INFO")

        if self.batch_tracker.should_skip(fingerprint):
            self.logger(
                "SKIP: This portfolio batch has already been processed (same FBL5N content)",
                "INFO"
            )
            stats['skipped'] = True
            return stats

        batch_id = self.batch_tracker.start_batch(
            config_fingerprint=fingerprint,
            metadata={'rows_in_raw': len(df_raw)}
        )
        self.logger.set_batch_id(batch_id)

        try:
            # 2. Normalization
            self.logger("\nStep 1: Normalizing columns...", "INFO")
            df_sap = self._normalize_raw_to_stg(df_raw)
            self.logger(f"   -> {len(df_sap)} valid rows in new load", "INFO")

            # 3. Hash Calculation
            self.logger("\nStep 2: Calculating hashes (bulk)...", "INFO")
            df_sap['etl_hash_new'] = PortfolioHashService.compute_dataframe(df_sap)

            # 4. State Loading
            self.logger("\nStep 3: Reading current staging state...", "INFO")
            df_existing = self._load_all_active_rows()
            self.logger(f"   -> {len(df_existing)} active rows in staging", "INFO")

            # 5. Classification
            self.logger("\nStep 4: Classifying changes in memory...", "INFO")
            actions = self._classify_changes(df_sap, df_existing)

            self.logger(
                f"   -> INSERT:    {len(actions['to_insert'])}\n"
                f"      UPDATE:    {len(actions['to_update'])}\n"
                f"      CLOSE:     {len(actions['to_close'])}\n"
                f"      UNCHANGED: {len(actions['unchanged'])}\n"
                f"      SPLITS:    {len(actions['splits'])}",
                "INFO"
            )

            # 6. Persistence (Bulk operations)
            if not actions['to_insert'].empty:
                stats['inserted'] = self._bulk_insert(actions['to_insert'])

            if actions['to_update']:
                stats['updated'] = self._bulk_update(actions['to_update'])

            if actions['to_update_assignment']:
                assignment_updated = self._update_assignment_only(actions['to_update_assignment'])
                stats['updated'] += assignment_updated
                self.logger(
                    f"   -> {assignment_updated} assignment(s) updated "
                    f"(hash unchanged, SAP assignment drifted)",
                    "INFO"
                )

            if actions['to_close']:
                stats['closed'] = self._bulk_close(actions['to_close'])

            if actions['splits']:
                s_i, s_c = self._handle_splits(actions['splits'])
                stats['inserted'] += s_i
                stats['closed']   += s_c

            stats['unchanged'] = len(actions['unchanged'])

            # 7. Summary
            self.logger("\n" + "=" * 80, "INFO")
            self.logger("CDC v2.0 SUMMARY", "INFO")
            self.logger("=" * 80, "INFO")
            self.logger(f"   Inserted:    {stats['inserted']}", "SUCCESS")
            self.logger(f"   Updated:     {stats['updated']}", "INFO")
            self.logger(f"   Closed:      {stats['closed']}", "WARN" if stats['closed'] > 0 else "INFO")
            self.logger(f"   Unchanged:   {stats['unchanged']}", "INFO")

            self.batch_tracker.complete_batch(
                records_processed=stats['inserted'] + stats['updated'] + stats['closed'],
                result_summary=stats,
            )
            return stats

        except Exception as e:
            self.batch_tracker.fail_batch(str(e))
            self.logger(f"Error in CDC: {e}", "ERROR")
            import traceback
            self.logger(traceback.format_exc(), "ERROR")
            raise

    def _compute_raw_fingerprint(self, df_raw: pd.DataFrame) -> str:
        """
        Computes a raw batch fingerprint for idempotency.
        
        Uses COUNT + content hash to detect if the raw table changed.
        Content-based fingerprinting is more robust than just checking loaded_at.
        """
        import hashlib

        count    = str(len(df_raw))
        
        # Simple but effective fingerprint: COUNT + sum of amounts + sample of references
        total_amount = str(round(df_raw['importe'].fillna(0).astype(float).sum(), 2) if 'importe' in df_raw.columns else 0)
        sample_refs  = '|'.join(sorted(
            df_raw.get('referencia_a_factura', df_raw.get('invoice_ref', pd.Series(['']))).fillna('').astype(str).head(10).tolist()
        ))

        raw = f"{count}|{total_amount}|{sample_refs}"
        return hashlib.md5(raw.encode('utf-8')).hexdigest()

    def _normalize_raw_to_stg(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Maps raw columns (Excel names) to staging columns.
        Filters rows without a valid invoice_ref.
        """
        col_map = {
            'n_documento':          'sap_doc_number',
            'referencia_a_factura': 'invoice_ref',
            'cuenta':               'customer_code',
            'cliente':              'customer_name',
            'asignacion':           'assignment',
            'referencia':           'accounting_doc',
            'fecha_documento':      'doc_date',
            'fecha_de_pago':        'due_date',
            'importe':              'amount_outstanding',
            'moneda_local':         'currency',
            'texto':                'sap_text',
            'cuenta_de_mayor':      'gl_account',
        }

        df = df_raw.rename(columns={k: v for k, v in col_map.items() if k in df_raw.columns}).copy()

        # conciliable_amount equals amount_outstanding when arriving from SAP (no withholdings yet)
        if 'conciliable_amount' not in df.columns:
            df['conciliable_amount'] = df.get('amount_outstanding', 0)

        # invoice_ref is the stable identifier - required for processing
        df = df[df['invoice_ref'].notna() & (df['invoice_ref'].astype(str).str.strip() != '')].copy()
        df['invoice_ref'] = df['invoice_ref'].astype(str).str.strip()

        return df

    def _load_all_active_rows(self) -> pd.DataFrame:
        """
        Loads all active rows (not CLOSED or INTERNAL_COMPENSATED) from staging in one query.
        """
        query = text("""
            SELECT
                stg_id,
                invoice_ref,
                accounting_doc,
                etl_hash,
                reconcile_status,
                conciliable_amount,
                amount_outstanding,
                settlement_id,
                partial_payment_flag
            FROM biq_stg.stg_customer_portfolio
            WHERE reconcile_status NOT IN (
                'CLOSED', 'INTERNAL_COMPENSATED'
            )
            AND invoice_ref IS NOT NULL
        """)

        return pd.read_sql(query, self.session.connection())

    def _classify_changes(
        self,
        df_sap: pd.DataFrame,
        df_existing: pd.DataFrame,
    ) -> dict:
        """
        Classifies changes by comparing new SAP load with current state.
        
        Returns:
            dict: DataFrames for each action category (insert, update, close, etc.).
        """
        sap_refs      = set(df_sap['invoice_ref'].unique())
        existing_refs = set(df_existing['invoice_ref'].unique()) if not df_existing.empty else set()

        # 1. New records
        new_refs = sap_refs - existing_refs
        to_insert = df_sap[df_sap['invoice_ref'].isin(new_refs)].copy()

        # 2. Missing records (candidates for closure)
        missing_refs = existing_refs - sap_refs
        df_missing   = df_existing[df_existing['invoice_ref'].isin(missing_refs)].copy()

        # Only close if in closeable statuses
        to_close_df  = df_missing[df_missing['reconcile_status'].isin(self.CLOSEABLE_STATUSES)]
        to_close_ids = to_close_df['stg_id'].tolist()

        # 3. Existing records - compare hash
        common_refs = sap_refs & existing_refs

        # Handle invoice_refs with single vs multiple active rows
        ref_counts   = df_existing[df_existing['invoice_ref'].isin(common_refs)].groupby('invoice_ref').size()
        single_refs  = set(ref_counts[ref_counts == 1].index)
        multi_refs   = set(ref_counts[ref_counts > 1].index)

        to_update            = []
        to_update_assignment = []   # assignment-only updates (hash igual, assignment diferente)
        unchanged            = []
        splits               = []

        # Compare for single-row invoice_refs
        if single_refs:
            df_common_sap = df_sap[df_sap['invoice_ref'].isin(single_refs)].set_index('invoice_ref')
            df_common_stg = df_existing[df_existing['invoice_ref'].isin(single_refs)].set_index('invoice_ref')

            merged = df_common_sap.join(df_common_stg, lsuffix='_sap', rsuffix='_stg')

            for inv_ref, row in merged.iterrows():
                hash_new = row.get('etl_hash_new', '') or ''
                hash_old = row.get('etl_hash', '') or ''
                status   = row.get('reconcile_status', '') or 'PENDING'

                # CASE: old hash is NULL or empty -> first time CDC sees this row
                if not hash_old:
                    to_update.append({
                        'stg_id':      int(row['stg_id']),
                        'invoice_ref': inv_ref,
                        'etl_hash':    hash_new,
                        'mode':        'hash_only',
                        'sap_row':     row,
                    })
                    continue

                # CASE: hashes are identical → no real change in SAP content fields.
                # But assignment is excluded from the hash deliberately — it can be
                # updated by SAP or the RPA robot after staging (e.g. "PE: 075" →
                # "PE: 075-672-M"). If stale, enrich_parking_transactional cannot
                # extract batch+brand and the invoice never reconciles.
                # Detect assignment drift here and queue a surgical update that
                # touches ONLY assignment + updated_at — nothing else changes.
                if hash_new == hash_old:
                    assignment_sap = self._s(row.get('assignment_sap', row.get('assignment'))) or ''
                    assignment_stg = self._s(row.get('assignment_stg', row.get('assignment'))) or ''

                    if assignment_sap and assignment_sap.strip() != assignment_stg.strip():
                        to_update_assignment.append({
                            'stg_id':      int(row['stg_id']),
                            'invoice_ref': inv_ref,
                            'assignment':  assignment_sap.strip(),
                        })
                    else:
                        unchanged.append({'invoice_ref': inv_ref, 'stg_id': row.get('stg_id')})
                    continue

                # CASE: hash differs -> SAP changed something
                if status in self.PROTECTED_STATUSES:
                    to_update.append({
                        'stg_id':             int(row['stg_id']),
                        'invoice_ref':        inv_ref,
                        'amount_outstanding': row.get('amount_outstanding_sap', row.get('amount_outstanding')),
                        'etl_hash':           hash_new,
                        'mode':               'amount_only',
                        'sap_row':            row,
                    })
                elif status == 'PENDING':
                    to_update.append({
                        'stg_id':      int(row['stg_id']),
                        'invoice_ref': inv_ref,
                        'etl_hash':    hash_new,
                        'mode':        'full',
                        'sap_row':     row,
                    })
                else:
                    to_update.append({
                        'stg_id':      int(row['stg_id']),
                        'invoice_ref': inv_ref,
                        'etl_hash':    hash_new,
                        'mode':        'partial',
                        'sap_row':     row,
                    })

        # Splits - invoice_ref with multiple active rows
        for inv_ref in multi_refs:
            sap_rows      = df_sap[df_sap['invoice_ref'] == inv_ref]
            existing_rows = df_existing[df_existing['invoice_ref'] == inv_ref]
            splits.append({
                'invoice_ref':     inv_ref,
                'sap_rows':        sap_rows,
                'existing_rows':   existing_rows,
            })

        return {
            'to_insert':            to_insert,
            'to_update':            to_update,
            'to_update_assignment': to_update_assignment,
            'to_close':             to_close_ids,
            'unchanged':            unchanged,
            'splits':               splits,
        }

    def _bulk_insert(self, df: pd.DataFrame) -> int:
        """
        Inserts new rows as PENDING in a single bulk operation.
        """
        if df.empty:
            return 0

        query = text("""
            INSERT INTO biq_stg.stg_customer_portfolio (
                sap_doc_number, invoice_ref, customer_code, customer_name,
                assignment, accounting_doc, doc_date, due_date,
                amount_outstanding, conciliable_amount, currency, sap_text,
                gl_account, reconcile_status, etl_hash,
                partial_payment_flag, created_at, updated_at
            ) VALUES (
                :sap_doc_number, :invoice_ref, :customer_code, :customer_name,
                :assignment, :accounting_doc, :doc_date, :due_date,
                :amount_outstanding, :conciliable_amount, :currency, :sap_text,
                :gl_account, 'PENDING', :etl_hash,
                FALSE, NOW(), NOW()
            )
        """)

        params = []
        for _, row in df.iterrows():
            params.append({
                'sap_doc_number':    self._s(row.get('sap_doc_number')),
                'invoice_ref':       self._s(row.get('invoice_ref')),
                'customer_code':     self._s(row.get('customer_code')),
                'customer_name':     self._s(row.get('customer_name')),
                'assignment':        self._s(row.get('assignment')),
                'accounting_doc':    self._s(row.get('accounting_doc')),
                'doc_date':          self._d(row.get('doc_date')),
                'due_date':          self._d(row.get('due_date')),
                'amount_outstanding':self._f(row.get('amount_outstanding')),
                'conciliable_amount':self._f(row.get('conciliable_amount', row.get('amount_outstanding'))),
                'currency':          self._s(row.get('currency'), 'USD'),
                'sap_text':          self._s(row.get('sap_text')),
                'gl_account':        self._s(row.get('gl_account')),
                'etl_hash':          row.get('etl_hash_new', ''),
            })

        self.session.execute(query, params)
        self.logger(f"   Inserted: {len(params)} rows", "SUCCESS")
        return len(params)

    def _bulk_update(self, updates: list) -> int:
        """
        Updates existing rows based on the synchronization mode.
        """
        if not updates:
            return 0

        count = 0
        for upd in updates:
            stg_id   = upd['stg_id']
            mode     = upd['mode']
            row      = upd['sap_row']
            new_hash = upd['etl_hash']

            if mode == 'hash_only':
                # Initialization update for etl_hash only
                q = text("""
                    UPDATE biq_stg.stg_customer_portfolio
                    SET etl_hash   = :etl_hash,
                        updated_at = NOW()
                    WHERE stg_id = :stg_id
                      AND (etl_hash IS NULL OR etl_hash = '')
                """)
                self.session.execute(q, {'stg_id': stg_id, 'etl_hash': new_hash})
                count += 1
                continue

            if mode == 'amount_only':
                q = text("""
                    UPDATE biq_stg.stg_customer_portfolio
                    SET amount_outstanding = :amount,
                        etl_hash          = :etl_hash,
                        updated_at        = NOW()
                    WHERE stg_id = :stg_id
                """)
                self.session.execute(q, {
                    'stg_id':   stg_id,
                    'amount':   self._f(row.get('amount_outstanding_sap', row.get('amount_outstanding'))),
                    'etl_hash': new_hash,
                })

            elif mode == 'full':
                q = text("""
                    UPDATE biq_stg.stg_customer_portfolio
                    SET sap_doc_number    = :sap_doc_number,
                        customer_name     = :customer_name,
                        assignment        = :assignment,
                        accounting_doc    = :accounting_doc,
                        doc_date          = :doc_date,
                        due_date          = :due_date,
                        amount_outstanding = :amount_outstanding,
                        conciliable_amount = :conciliable_amount,
                        currency          = :currency,
                        sap_text          = :sap_text,
                        gl_account        = :gl_account,
                        etl_hash          = :etl_hash,
                        updated_at        = NOW()
                    WHERE stg_id = :stg_id
                """)
                self.session.execute(q, {
                    'stg_id':             stg_id,
                    'sap_doc_number':     self._s(row.get('sap_doc_number_sap', row.get('sap_doc_number'))),
                    'customer_name':      self._s(row.get('customer_name_sap', row.get('customer_name'))),
                    'assignment':         self._s(row.get('assignment_sap', row.get('assignment'))),
                    'accounting_doc':     self._s(row.get('accounting_doc_sap', row.get('accounting_doc'))),
                    'doc_date':           self._d(row.get('doc_date_sap', row.get('doc_date'))),
                    'due_date':           self._d(row.get('due_date_sap', row.get('due_date'))),
                    'amount_outstanding': self._f(row.get('amount_outstanding_sap', row.get('amount_outstanding'))),
                    'conciliable_amount': self._f(row.get('conciliable_amount_sap', row.get('conciliable_amount', row.get('amount_outstanding')))),
                    'currency':           self._s(row.get('currency_sap', row.get('currency')), 'USD'),
                    'sap_text':           self._s(row.get('sap_text_sap', row.get('sap_text'))),
                    'gl_account':         self._s(row.get('gl_account_sap', row.get('gl_account'))),
                    'etl_hash':           new_hash,
                })

            else:  # partial - does not touch conciliable_amount
                q = text("""
                    UPDATE biq_stg.stg_customer_portfolio
                    SET sap_doc_number    = :sap_doc_number,
                        customer_name     = :customer_name,
                        assignment        = :assignment,
                        accounting_doc    = :accounting_doc,
                        doc_date          = :doc_date,
                        due_date          = :due_date,
                        amount_outstanding = :amount_outstanding,
                        currency          = :currency,
                        sap_text          = :sap_text,
                        gl_account        = :gl_account,
                        etl_hash          = :etl_hash,
                        updated_at        = NOW()
                    WHERE stg_id = :stg_id
                """)
                self.session.execute(q, {
                    'stg_id':             stg_id,
                    'sap_doc_number':     self._s(row.get('sap_doc_number_sap', row.get('sap_doc_number'))),
                    'customer_name':      self._s(row.get('customer_name_sap', row.get('customer_name'))),
                    'assignment':         self._s(row.get('assignment_sap', row.get('assignment'))),
                    'accounting_doc':     self._s(row.get('accounting_doc_sap', row.get('accounting_doc'))),
                    'doc_date':           self._d(row.get('doc_date_sap', row.get('doc_date'))),
                    'due_date':           self._d(row.get('due_date_sap', row.get('due_date'))),
                    'amount_outstanding': self._f(row.get('amount_outstanding_sap', row.get('amount_outstanding'))),
                    'currency':           self._s(row.get('currency_sap', row.get('currency')), 'USD'),
                    'sap_text':           self._s(row.get('sap_text_sap', row.get('sap_text'))),
                    'gl_account':         self._s(row.get('gl_account_sap', row.get('gl_account'))),
                    'etl_hash':           new_hash,
                })

            count += 1

        self.logger(f"   Updated: {count} rows", "INFO")
        return count

    def _update_assignment_only(self, updates: list) -> int:
        """
        Surgical UPDATE of assignment column only.

        Called when etl_hash is identical (no SAP content change) but the
        assignment field drifted — e.g. the RPA robot or a manual SAP entry
        added the batch number after the invoice was already in staging:

            "PE: 075"  →  "PE: 075-672-M"

        Only touches: assignment + updated_at.
        Never touches: etl_hash, conciliable_amount, reconcile_status,
                       or any other pipeline-owned field.

        This ensures enrich_parking_transactional can extract batch+brand
        from the updated assignment in the same pipeline run.
        """
        if not updates:
            return 0

        query = text("""
            UPDATE biq_stg.stg_customer_portfolio
            SET assignment = :assignment,
                updated_at = NOW()
            WHERE stg_id = :stg_id
        """)

        self.session.execute(query, updates)
        self.logger(
            f"   Assignment-only updated: {len(updates)} rows", "INFO"
        )
        return len(updates)

    def _bulk_close(self, stg_ids: list) -> int:
        """
        Closes missing rows in a single bulk operation.
        """
        if not stg_ids:
            return 0

        ids_str = ','.join(str(i) for i in stg_ids)

        query = text(f"""
            UPDATE biq_stg.stg_customer_portfolio
            SET reconcile_status  = 'CLOSED',
                conciliable_amount = 0,
                closed_at          = NOW(),
                updated_at         = NOW(),
                sap_text           = CONCAT(
                    COALESCE(sap_text, ''),
                    ' [SAP_REMOVED ', NOW()::date::text, ']'
                )
            WHERE stg_id IN ({ids_str})
              AND reconcile_status NOT IN (
                  'CLOSED', 'MATCHED', 'INTERNAL_COMPENSATED'
              )
        """)

        result = self.session.execute(query)
        closed = result.rowcount
        self.logger(f"   Closed: {closed} rows (SAP_REMOVED)", "WARN" if closed > 0 else "INFO")
        return closed

    def _handle_splits(self, splits: list) -> tuple:
        """
        Handles invoice_ref with multiple active rows (e.g., Parking splits).
        
        Strategy:
            - Looks for exact amount matches between SAP and staging.
            - If found, other non-matching active rows are closed.
            - If no exact match is found, the smallest active row is closed 
              and a new PENDING record is created with the SAP amount.
        """
        total_inserted = 0
        total_closed   = 0

        for split in splits:
            inv_ref       = split['invoice_ref']
            sap_rows      = split['sap_rows']
            existing_rows = split['existing_rows']

            sap_amount = float(sap_rows.iloc[0].get('amount_outstanding', 0))

            # Active rows excluding protected states
            active  = existing_rows[~existing_rows['reconcile_status'].isin(self.PROTECTED_STATUSES)]
            
            # Look for exact amount match
            match_mask = abs(active['conciliable_amount'].astype(float) - sap_amount) < 0.01
            match_rows = active[match_mask]

            if not match_rows.empty:
                # Exact match found - close other active non-matched rows
                matched_stg_id = match_rows.iloc[0]['stg_id']
                to_close_ids   = active[active['stg_id'] != matched_stg_id]['stg_id'].tolist()

                if to_close_ids:
                    closed = self._bulk_close(to_close_ids)
                    total_closed += closed
                    self.logger(
                        f"   Split {inv_ref}: match ${sap_amount:.2f} "
                        f"-> stg_id={matched_stg_id}, closed {len(to_close_ids)} children",
                        "INFO"
                    )
            else:
                # No exact match - close smallest row and create new PENDING
                if not active.empty:
                    smallest = active.nsmallest(1, 'conciliable_amount').iloc[0]
                    closed   = self._bulk_close([int(smallest['stg_id'])])
                    total_closed += closed

                # Create new PENDING row with SAP amount
                sap_row = sap_rows.iloc[0].copy()
                sap_row['etl_hash_new'] = PortfolioHashService.compute(sap_row)

                inserted = self._bulk_insert(pd.DataFrame([sap_row]))
                total_inserted += inserted

                self.logger(
                    f"   Split {inv_ref}: no exact match for ${sap_amount:.2f} "
                    f"-> new PENDING record created",
                    "WARN"
                )

        return total_inserted, total_closed

    @staticmethod
    def _s(value, default=None):
        """Safe string conversion."""
        import math
        if value is None:
            return default
        try:
            if isinstance(value, float) and math.isnan(value):
                return default
        except Exception:
            pass
        s = str(value).strip()
        return s if s and s.lower() not in ('nan', 'none', 'nat') else default

    @staticmethod
    def _f(value, default=0.0):
        """Safe float conversion."""
        import math
        try:
            f = float(value)
            return f if math.isfinite(f) else default
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _d(value):
        """Safe date conversion."""
        import math
        if value is None:
            return None
        try:
            if isinstance(value, float) and math.isnan(value):
                return None
        except Exception:
            pass
        s = str(value).strip()
        if s.lower() in ('nat', 'none', 'nan', ''):
            return None
        try:
            return pd.to_datetime(s).date()
        except Exception:
            return None