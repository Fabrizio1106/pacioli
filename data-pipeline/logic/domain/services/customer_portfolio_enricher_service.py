"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.customer_portfolio_enricher_service
===============================================================================

Description:
    Service for enriching and reconciling customer portfolio data. It implements 
    high-performance logic for matching SAP records with transactional sources 
    like Webpos, VIP, and Parking systems.

Responsibilities:
    - Perform internal netting of debits and credits.
    - Enrich portfolio records with Webpos transaction details.
    - Execute complex multi-layer matching cascades for VIP and Parking.
    - Generate unique matching hashes and ETL synchronization keys.
    - Leverage optional Rust core for optimized combinatorial searches.

Key Components:
    - CustomerPortfolioEnricherService: Main logic engine for portfolio enrichment.

Notes:
    - Optimized version 6.0 features pre-normalization and indexed lookups.
    - Supports proportional financial distributions for multi-invoice matches.
    - Business rules include specific tolerance thresholds for cent differences.

Dependencies:
    - pandas, numpy, re, hashlib, sqlalchemy, typing, collections, difflib
    - pacioli_core (optional Rust extension)
===============================================================================
"""

import re
import hashlib
import pandas as pd
import numpy as np
from typing import Optional, Dict, Set, List, Tuple
from collections import defaultdict
from difflib import SequenceMatcher

from utils.logger import get_logger
from utils.data_cleaner import DataCleaner

# Try to import the Rust core module for performance optimization
try:
    from pacioli_core.pacioli_core import find_invoice_combination as _rust_fic
    from pacioli_core.pacioli_core import fuzzy_batch_match as _rust_fbm
    class _rust:
        find_invoice_combination = staticmethod(_rust_fic)
        fuzzy_batch_match = staticmethod(_rust_fbm)
    _RUST_AVAILABLE = True
except (ImportError, AttributeError):
    _rust = None
    _RUST_AVAILABLE = False


class CustomerPortfolioEnricherService:
    """
    Domain service for customer portfolio enrichment and matching.
    
    This service implements a multi-layer cascade strategy to match SAP records
    with external transactional data sources.
    """

    def __init__(self, config: dict):
        self.config = config
        self.logger = get_logger("PORTFOLIO_ENRICHER")
        if _RUST_AVAILABLE:
            self.logger("Rust core active (pacioli_core)", "INFO")

    # ═════════════════════════════════════════════════════════════════════════
    # LAYERS 0-3: Internal Netting, Webpos, Hashes, Parking
    # ═════════════════════════════════════════════════════════════════════════

    def process_internal_netting(self, df_portfolio: pd.DataFrame) -> pd.DataFrame:
        """Detects and compensates internal debits/credits."""
        # 1. Initialization and Filtering
        self.logger("Executing Internal Netting...", "INFO")
        exclusion_keywords = ['INCENT.', 'DESCUENTO', 'BONIFICACION']
        pattern = '|'.join(exclusion_keywords)
        mask_candidate = (
            (df_portfolio['reconcile_status'] == 'PENDING') &
            (~df_portfolio['sap_text'].astype(str).str.upper().str.contains(
                pattern, regex=True, na=False
            ))
        )
        if not mask_candidate.any():
            return df_portfolio
            
        # 2. Grouping and Merging
        df_proc = df_portfolio[mask_candidate].copy()
        df_proc['abs_amount'] = df_proc['conciliable_amount'].abs()
        df_pos = df_proc[df_proc['conciliable_amount'] > 0].copy()
        df_neg = df_proc[df_proc['conciliable_amount'] < 0].copy()
        merge_keys = ['customer_code', 'sap_doc_number', 'abs_amount']
        df_neg_unique = df_neg.drop_duplicates(subset=merge_keys)
        merged = pd.merge(
            df_pos, df_neg_unique[merge_keys + ['accounting_doc']],
            on=merge_keys, how='inner', suffixes=('', '_nc')
        )
        if merged.empty:
            return df_portfolio
            
        # 3. Status Updates
        all_netted_docs = set(
            merged['accounting_doc'].tolist() +
            merged['accounting_doc_nc'].tolist()
        )
        mask_netted = df_portfolio['accounting_doc'].isin(all_netted_docs)
        df_portfolio.loc[mask_netted, 'reconcile_status'] = 'INTERNAL_COMPENSATED'
        df_portfolio.loc[mask_netted, 'match_method'] = 'INTERNAL_NETTING'
        df_portfolio.loc[mask_netted, 'match_confidence'] = 'HIGH'
        df_portfolio.loc[mask_netted, 'conciliable_amount'] = 0
        self.logger(f"   → {mask_netted.sum()} documents internally compensated", "SUCCESS")
        return df_portfolio

    def enrich_with_webpos(self, df_portfolio: pd.DataFrame, df_webpos: pd.DataFrame) -> pd.DataFrame:
        """Enriches portfolio records with Webpos data."""
        # 1. Column Preparation
        self.logger("Enriching with Webpos...", "INFO")
        mask_pending = (df_portfolio['reconcile_status'] == 'PENDING')
        if not mask_pending.any():
            return df_portfolio
        enrich_cols = ['enrich_batch', 'enrich_ref', 'enrich_source', 'enrich_brand', 'enrich_user']
        for col in enrich_cols:
            if col not in df_portfolio.columns:
                df_portfolio[col] = None
            df_portfolio[col] = df_portfolio[col].astype('object')
            
        # 2. Data Sanitization and Merging
        df_active = df_portfolio[mask_pending].copy()
        df_passive = df_portfolio[~mask_pending].copy()
        df_webpos = self._clean_all_strings(df_webpos)
        df_active['join_key'] = self._sanitize_join_key(df_active['assignment'])
        df_webpos['join_key'] = self._sanitize_join_key(df_webpos['factura'])
        df_webpos = df_webpos.drop_duplicates(subset=['join_key'], keep='last')
        merged = pd.merge(
            df_active,
            df_webpos[['join_key', 'tipo_pago', 'lote', 'numero_de_referencia', 'usuario']],
            on='join_key', how='left'
        )
        
        # 3. Enrichment Application
        mask_matched = merged['tipo_pago'].notna()
        if mask_matched.any():
            merged.loc[mask_matched, 'enrich_batch'] = (
                merged.loc[mask_matched, 'lote'].astype(str).str.replace(r'\.0$', '', regex=True)
            )
            merged.loc[mask_matched, 'enrich_ref'] = (
                merged.loc[mask_matched, 'numero_de_referencia']
                .fillna('0').astype(str).str.replace(r'\.0$', '', regex=True)
            )
            merged.loc[mask_matched, 'enrich_source'] = 'WEBPOS'
            merged.loc[mask_matched, 'enrich_brand'] = merged.loc[mask_matched, 'tipo_pago'].str.upper()
            merged.loc[mask_matched, 'enrich_user'] = merged.loc[mask_matched, 'usuario']
            self.logger(f"   → {mask_matched.sum()} documents enriched with Webpos", "SUCCESS")
        cols_to_drop = ['join_key', 'tipo_pago', 'lote', 'numero_de_referencia', 'usuario']
        merged = merged.drop(columns=cols_to_drop, errors='ignore')
        return pd.concat([df_passive, merged], ignore_index=True)

    def generate_final_hashes(self, df_portfolio: pd.DataFrame) -> pd.DataFrame:
        """Generates the match_hash_key for VIP matching."""
        # 1. Filtering and Grouping
        self.logger("Generating matching hashes...", "INFO")
        mask_vip = (df_portfolio['enrich_source'] == 'WEBPOS')
        if not mask_vip.any():
            return df_portfolio
            
        # 2. Hash Calculation
        batch_series = df_portfolio.loc[mask_vip].apply(
            lambda r: self._normalize_batch(r['enrich_batch'], r['enrich_brand']), axis=1
        )
        ref_series = df_portfolio.loc[mask_vip].apply(
            lambda r: self._normalize_ref(r['enrich_ref'], r['enrich_brand']), axis=1
        )
        df_portfolio.loc[mask_vip, 'temp_prefix'] = df_portfolio.loc[
            mask_vip, 'enrich_brand'
        ].apply(self._get_prefix)
        amount_str = DataCleaner.format_decimal_strict(
            df_portfolio.loc[mask_vip, 'conciliable_amount']
        )
        df_portfolio.loc[mask_vip, 'match_hash_key'] = (
            df_portfolio.loc[mask_vip, 'temp_prefix'] + "_" +
            batch_series + "_" + ref_series + "_" + amount_str
        )
        df_portfolio.loc[mask_vip, 'reconcile_group'] = np.where(
            df_portfolio.loc[mask_vip, 'enrich_brand'].astype(str).str.contains('EFECTIVO', na=False),
            'CASH', 'VIP_CARD'
        )
        
        # 3. Cleanup
        df_portfolio.loc[df_portfolio['reconcile_group'] == 'CASH', 'match_hash_key'] = None
        df_portfolio.drop(columns=['temp_prefix'], inplace=True, errors='ignore')
        hash_count = df_portfolio.loc[mask_vip, 'match_hash_key'].notna().sum()
        self.logger(f"   → {hash_count} hashes generated", "SUCCESS")
        return df_portfolio

    def enrich_parking_transactional(
        self, df_portfolio, df_bank_parking, df_breakdown
    ) -> pd.DataFrame:
        """Post-loop reconciliation for Parking Transactional data."""
        # 1. Validation and Setup
        self.logger("Parking Engine v5.1: Post-Loop Reconciliation...", "INFO")
        if df_breakdown.empty:
            self.logger("No parking batches available", "WARN")
            return df_portfolio
        pk_rules = self.config.get('parking_rules', {})
        target_gl = str(pk_rules.get('target_gl_account', '1120114029'))
        multi_batch_regex = re.compile(r'(\d+)-([A-Za-z]+)')
        letter_map = pk_rules.get('letter_to_brand', {
            'D': 'DINERS CLUB', 'M': 'VISA - MASTERCARD', 'V': 'VISA',
            'PCF': 'PACIFICARD', 'A': 'AMEX', 'DCS': 'DINERS CLUB'
        })
        
        # 2. Data Cleaning for Merging
        df_bank_clean = df_bank_parking[['settlement_id', 'batch_number', 'brand']].copy()
        df_bank_clean['batch_number'] = df_bank_clean['batch_number'].astype(str).str.strip().str.lstrip('0')
        df_bank_clean['brand'] = df_bank_clean['brand'].astype(str).str.strip().str.upper()
        df_breakdown_clean = df_breakdown.copy()
        df_breakdown_clean['batch_number'] = df_breakdown_clean['batch_number'].astype(str).str.strip().str.lstrip('0')
        df_breakdown_clean['brand'] = df_breakdown_clean['brand'].astype(str).str.strip().str.upper()
        df_breakdown_enriched = df_breakdown_clean.merge(
            df_bank_clean, on=['batch_number', 'brand'], how='left', suffixes=('_breakdown', '_bank')
        )
        if 'settlement_id_bank' in df_breakdown_enriched.columns:
            df_breakdown_enriched['settlement_id'] = (
                df_breakdown_enriched['settlement_id_bank']
                .fillna(df_breakdown_enriched.get('settlement_id_breakdown', ''))
            )
        df_breakdown_enriched = df_breakdown_enriched[df_breakdown_enriched['settlement_id'].notna()].copy()
        
        # 3. Matching Logic Initialization
        if df_breakdown_enriched.empty:
            return df_portfolio
        mask_pending = (
            (df_portfolio['reconcile_status'] == 'PENDING') &
            (df_portfolio['gl_account'].astype(str).str.replace('.0', '') == target_gl)
        )
        df_parking_candidates = df_portfolio[mask_pending].copy()
        df_others = df_portfolio[~mask_pending].copy()
        if df_parking_candidates.empty:
            self.logger("No PENDING PARKING invoices found", "WARN")
            return df_portfolio
            
        def extract_batch_brand(assignment):
            if pd.isna(assignment):
                return []
            matches = multi_batch_regex.findall(str(assignment))
            return [(b.strip().lstrip('0'), letter_map.get(bc, 'UNKNOWN')) for b, bc in matches]
            
        df_parking_candidates['batch_brand_list'] = df_parking_candidates['assignment'].apply(extract_batch_brand)
        consumed_amounts = defaultdict(float)
        last_touch = {}
        matches = []
        
        # 4. Main Matching Loop
        for _, lote in df_breakdown_enriched.iterrows():
            settlement_id_raw = lote.get('settlement_id')
            if pd.isna(settlement_id_raw) or str(settlement_id_raw).strip() == '':
                continue
            settlement_id = str(settlement_id_raw).strip()
            batch_number = str(lote['batch_number']).strip().lstrip('0')
            brand = str(lote['brand']).strip().upper()
            amount_gross = self._safe_float(lote.get('amount_gross', 0))
            amount_net = self._safe_float(lote.get('amount_net', 0))
            amount_commission = self._safe_float(lote.get('amount_commission', 0))
            amount_iva = self._safe_float(lote.get('amount_tax_iva', 0))
            amount_irf = self._safe_float(lote.get('amount_tax_irf', 0))
            match_hash = lote.get('match_hash_key')
            hash_generico = f"{brand}_{batch_number}"
            
            # Skip if already processed
            already_processed = df_portfolio[
                (df_portfolio['settlement_id'] == settlement_id) &
                (df_portfolio['reconcile_status'] == 'ENRICHED')
            ]['financial_amount_gross'].sum()
            if already_processed >= amount_gross - 0.01:
                continue
                
            def has_matching_batch_brand(batch_brand_list):
                if not batch_brand_list:
                    return False
                return (batch_number, brand) in batch_brand_list
                
            mask_candidates = (
                (df_parking_candidates['batch_brand_list'].apply(has_matching_batch_brand)) |
                ((df_parking_candidates['match_hash_key'] == hash_generico) &
                 (df_parking_candidates['reconcile_status'] == 'PENDING'))
            )
            candidatas = df_parking_candidates[mask_candidates].copy()
            if candidatas.empty:
                continue
            
            # 5. Process Invoices and Handle Partial Payments
            remaining_gross = amount_gross
            for idx, factura in candidatas.iterrows():
                if remaining_gross <= 0.009:
                    break
                factura_amount_original = self._safe_float(factura['amount_outstanding'])
                saldo_real = factura_amount_original - consumed_amounts.get(idx, 0.0)
                if saldo_real <= 0.009:
                    continue
                monto_a_consumir = min(saldo_real, remaining_gross)
                proporcion = monto_a_consumir / amount_gross if amount_gross > 0 else 0
                
                TOLERANCIA_CENTAVO = 0.009
                es_pago_parcial = (monto_a_consumir < saldo_real - TOLERANCIA_CENTAVO)
                residuo_sap = round(saldo_real - monto_a_consumir, 2) if es_pago_parcial else None
                
                consumed_amounts[idx] += monto_a_consumir
                last_touch[idx] = {'batch': batch_number, 'brand': brand, 'hash': hash_generico}
                
                match = factura.copy()
                match['stg_id'] = None
                match['amount_outstanding'] = monto_a_consumir
                match['conciliable_amount'] = monto_a_consumir
                match['enrich_batch'] = batch_number
                match['enrich_brand'] = brand
                match['enrich_source'] = 'PARKING'
                match['reconcile_group'] = 'PARKING_CARD'
                match['match_hash_key'] = match_hash
                match['reconcile_status'] = 'ENRICHED'
                match['settlement_id'] = settlement_id
                match['financial_amount_gross'] = monto_a_consumir
                match['financial_amount_net'] = amount_net * proporcion
                match['financial_commission'] = amount_commission * proporcion
                match['financial_tax_iva'] = amount_iva * proporcion
                match['financial_tax_irf'] = amount_irf * proporcion
                match['match_method'] = 'BANK_TRANSACTIONAL'
                match['match_confidence'] = '100'
                match['is_suggestion'] = False
                match['partial_payment_flag'] = True
                match['is_partial_payment'] = bool(es_pago_parcial)
                match['sap_residual_amount'] = float(residuo_sap) if residuo_sap is not None else None
                
                matches.append(match)
                remaining_gross -= monto_a_consumir
                
        # 6. Final Consolidation
        parents = []
        for idx, total_consumed in consumed_amounts.items():
            factura = df_parking_candidates.loc[idx]
            factura_amount_original = self._safe_float(factura['amount_outstanding'])
            parent = factura.copy()
            parent['reconcile_status'] = 'CLOSED'
            parent['conciliable_amount'] = False
            parent['partial_payment_flag'] = True
            parents.append(parent)
            
            residuo_amount = factura_amount_original - total_consumed
            if residuo_amount > 0.009:
                touch_ctx = last_touch.get(idx, {})
                residuo = factura.copy()
                residuo['stg_id'] = None
                residuo['amount_outstanding'] = residuo_amount
                residuo['conciliable_amount'] = residuo_amount
                residuo['reconcile_status'] = 'PENDING'
                residuo['partial_payment_flag'] = True
                residuo['enrich_batch'] = touch_ctx.get('batch')
                residuo['enrich_brand'] = touch_ctx.get('brand')
                residuo['enrich_source'] = 'PARKING'
                residuo['reconcile_group'] = 'PARKING_CARD'
                matches.append(residuo)
                
        df_matches = pd.DataFrame(matches) if matches else pd.DataFrame()
        df_parents = pd.DataFrame(parents) if parents else pd.DataFrame()
        self.logger(f"   → Matches: {len(matches)} | Parents: {len(parents)}", "SUCCESS")
        
        # 7. Final DataFrame Processing
        dfs_to_concat = [df for df in [df_others, df_matches, df_parents] if not df.empty]
        if not dfs_to_concat:
            return pd.DataFrame()
        final_df = pd.concat(dfs_to_concat, ignore_index=True)
        
        # Standardize boolean columns
        cols_booleanas = ['is_suggestion', 'partial_payment_flag', 'is_partial_payment']
        for col in cols_booleanas:
            if col in final_df.columns:
                final_df[col] = final_df[col].map(
                    lambda x: True if x in [1, '1', 1.0, True, 'true', 'True'] else False
                ).astype(bool)
        return final_df

    # ═════════════════════════════════════════════════════════════════════════
    # LAYER 4: VIP CASCADE
    # ═════════════════════════════════════════════════════════════════════════

    def enrich_vip_cascade(
        self,
        df_portfolio: pd.DataFrame,
        df_card_details: pd.DataFrame
    ) -> pd.DataFrame:
        """VIP CASCADE: Multi-layer matching logic for card settlements."""
        # 1. Initialization and Filtering
        self.logger("\n" + "═" * 80, "INFO")
        self.logger("VIP CASCADE v6.3: Competitive Pre-scoring + Two-Pass", "INFO")
        self.logger("═" * 80, "INFO")
        
        mask_vip_card = (
            (df_portfolio['reconcile_group'] == 'VIP_CARD') &
            (df_portfolio['reconcile_status'] == 'PENDING')
        )
        if not mask_vip_card.any():
            self.logger("No PENDING VIP_CARD invoices found", "INFO")
            return df_portfolio
            
        df_candidates = df_portfolio[mask_vip_card].copy()
        df_rest = df_portfolio[~mask_vip_card].copy()
        
        # 2. Preparation of Vouchers and Normalization
        vip_establishments = ['SALAS VIP', 'ASISTENCIAS']
        df_card_vip_only = df_card_details[
            df_card_details['establishment_name'].isin(vip_establishments)
        ].copy()
        if df_card_vip_only.empty:
            self.logger("No VIP/ASISTENCIAS vouchers found", "WARN")
            return df_portfolio
            
        df_card_vip_only = df_card_vip_only.rename(columns={
            'amount_gross': 'fin_gross', 'amount_net': 'fin_net',
            'amount_commission': 'fin_comm', 'amount_tax_iva': 'fin_iva',
            'amount_tax_irf': 'fin_irf', 'settlement_id': 'fin_settlement'
        })
        df_card_vip_only['is_used'] = False
        
        # Single-pass normalization for performance
        df_card_vip_only['norm_batch'] = df_card_vip_only['batch_number'].astype(str).str.strip().str.lstrip('0')
        df_card_vip_only['norm_ref'] = df_card_vip_only['voucher_ref'].astype(str).str.strip().str.lstrip('0')
        df_card_vip_only['norm_brand'] = df_card_vip_only['brand'].astype(str).str.strip().str.upper()
        df_card_vip_only['norm_amount'] = df_card_vip_only['fin_gross'].apply(self._safe_float)
        
        df_candidates['norm_batch'] = df_candidates['enrich_batch'].astype(str).str.strip().str.lstrip('0')
        df_candidates['norm_ref'] = df_candidates['enrich_ref'].astype(str).str.strip().str.lstrip('0')
        df_candidates['norm_brand'] = df_candidates['enrich_brand'].astype(str).str.strip().str.upper()
        df_candidates['norm_amount'] = df_candidates['amount_outstanding'].apply(self._safe_float)
        
        # 3. Phase 1: Exact Hash Global
        self.logger("\n" + "─" * 80, "INFO")
        self.logger("PHASE 1: Exact Hash Global", "INFO")
        self.logger("─" * 80, "INFO")
        
        vouchers_index = {str(sid): grp.copy() for sid, grp in df_card_vip_only.groupby('fin_settlement')}
        assigned_invoices = set()
        phase1_matches = 0
        
        hash_to_voucher = {row.get('voucher_hash_key'): (idx, row) for idx, row in df_card_vip_only.iterrows() if row.get('voucher_hash_key')}
        valid_hashes = df_candidates[df_candidates['match_hash_key'].notna()]
        
        for hash_key in valid_hashes['match_hash_key'].unique():
            if hash_key not in hash_to_voucher:
                continue
            v_idx, voucher = hash_to_voucher[hash_key]
            if df_card_vip_only.at[v_idx, 'is_used']:
                continue

            invoice_indices = valid_hashes[valid_hashes['match_hash_key'] == hash_key].index
            for idx in invoice_indices:
                df_candidates.at[idx, 'settlement_id']    = voucher['fin_settlement']
                df_candidates.at[idx, 'reconcile_status'] = 'ENRICHED'
                df_candidates.at[idx, 'match_method']     = 'VIP_EXACT_HASH'
                df_candidates.at[idx, 'match_confidence'] = '100'
                df_candidates.at[idx, 'is_suggestion']    = False
                self._copy_financial_amounts(df_candidates, idx, voucher)
                assigned_invoices.add(idx)
                phase1_matches += 1

            # FIX 3: marcar el voucher como usado TANTO en df_card_vip_only
            # como en vouchers_index para que Pass 1 y Pass 2 no lo reclamen.
            df_card_vip_only.at[v_idx, 'is_used'] = True
            sid_str = str(voucher['fin_settlement'])
            if sid_str in vouchers_index and v_idx in vouchers_index[sid_str].index:
                vouchers_index[sid_str].at[v_idx, 'is_used'] = True

        self.logger(f"PHASE 1 Complete: {phase1_matches} matches found", "SUCCESS")

        # ── FASE 2: PRE-SCORING COMPETITIVO + TWO-PASS ─────────────────────
        self.logger("\n" + "─" * 80, "INFO")
        self.logger("PHASE 2: Competitive Pre-scoring + Two-Pass", "INFO")
        self.logger("─" * 80, "INFO")

        cascade_high = [
            'EXACT_BATCH_REF_WRONG_AMT',
            'EXACT_BATCH_REF_WRONG_BRAND',
            'EXACT_BATCH_AMT_WRONG_REF',
            'EXACT_REF_AMT_WRONG_BATCH',
            'SWAPPED_BATCH_REF',
            'FUZZY_BATCH_REF_MATCH',
            'SAME_BATCH_SAME_AMT',
        ]
        cascade_low = ['SAME_AMT_SAME_BRAND', 'RESCUE_BY_AMOUNT_ONLY']

        layer_counters    = {layer: 0 for layer in cascade_high + cascade_low}
        phase2_confirmed  = 0
        phase2_suggestions = 0
        multi_invoice_blocks = 0

        pending_indices = [idx for idx in df_candidates.index if idx not in assigned_invoices]
        invoice_optimal, voucher_best_score = self._compute_competitive_prescoring(
            df_candidates, pending_indices, vouchers_index
        )

        self.logger(f"   → {len(invoice_optimal)} facturas con voucher óptimo", "INFO")
        self.logger(f"   → {len(voucher_best_score)} vouchers con candidata ganadora", "INFO")

        # ── PASADA 1: Alta calidad — cada voucher = una factura ─────────────
        # Regla de negocio VIP: un voucher representa exactamente una factura.
        # Pass 1 asigna matches de alta confianza respetando la competencia
        # entre facturas por el mismo voucher (pre-scoring competitivo).
        self.logger("   Pasada 1: Capas de alta calidad (confidence >= 75)...", "INFO")

        for idx in df_candidates.index:
            if idx in assigned_invoices:
                continue

            invoice  = df_candidates.loc[idx]
            matched  = False

            opt     = invoice_optimal.get(idx, {})
            opt_sid = opt.get('settlement_id')

            # Priorizar el settlement óptimo de esta factura
            if opt_sid and opt_sid in vouchers_index:
                settlement_order = [opt_sid] + [s for s in vouchers_index if s != opt_sid]
            else:
                settlement_order = list(vouchers_index.keys())

            for layer in cascade_high:
                if matched:
                    break

                for settlement_id in settlement_order:
                    if settlement_id not in vouchers_index:
                        continue

                    settlement_df = vouchers_index[settlement_id]
                    available     = settlement_df[~settlement_df['is_used']]
                    if available.empty:
                        continue

                    if layer == 'EXACT_BATCH_REF_WRONG_AMT':
                        result = self._try_multi_invoice_match_fast(
                            invoice_idx=idx,
                            invoice=invoice,
                            df_candidates=df_candidates,
                            settlement_vouchers=available,
                            assigned_invoices=assigned_invoices,
                            settlement_id=settlement_id,
                        )
                        if result is not None:
                            matched_invoices  = result['matched_invoices']
                            voucher           = result['voucher']
                            voucher_idx_r     = result['voucher_idx']
                            within_tolerance  = result.get('within_tolerance', False)

                            # FIX 1: respetar within_tolerance
                            # diff <= $0.05: match confirmado, comisión absorbe la diff
                            # diff > $0.05:  sugerencia — el analista debe validar
                            if within_tolerance:
                                confidence    = 95
                                is_suggestion = False
                                method        = 'VIP_WITHIN_TOLERANCE'
                            else:
                                confidence    = 89
                                is_suggestion = True
                                method        = 'VIP_EXACT_BATCH_REF_WRONG_AMT'

                            for inv_idx, proportion in matched_invoices:
                                df_candidates.at[inv_idx, 'settlement_id']    = settlement_id
                                df_candidates.at[inv_idx, 'reconcile_status'] = 'ENRICHED'
                                df_candidates.at[inv_idx, 'match_method']     = method
                                df_candidates.at[inv_idx, 'match_confidence'] = str(confidence)
                                df_candidates.at[inv_idx, 'is_suggestion']    = is_suggestion
                                self._copy_financial_amounts_proportional(
                                    df_candidates, inv_idx, voucher, proportion
                                )
                                assigned_invoices.add(inv_idx)
                                layer_counters[layer] += 1
                                if is_suggestion:
                                    phase2_suggestions += 1
                                else:
                                    phase2_confirmed += 1

                            # Marcar voucher como usado en el índice
                            settlement_df.at[voucher_idx_r, 'is_used'] = True

                            if len(matched_invoices) > 1:
                                multi_invoice_blocks += 1
                                self.logger(
                                    f"   Multi-Invoice: {len(matched_invoices)} facturas "
                                    f"para voucher ${voucher['fin_gross']:.2f}", "INFO"
                                )
                            matched = True
                            break

                    else:
                        match = self._find_match_in_layer_fast(invoice, available, layer)
                        if match is None:
                            continue

                        # Verificación competitiva: ceder si otro tiene mayor score
                        v_hash     = match.get('voucher_hash_key', '')
                        best_entry = voucher_best_score.get(v_hash, {})
                        best_score = best_entry.get('score', 0)
                        my_score   = invoice_optimal.get(idx, {}).get('score', 0)

                        if best_score > my_score and best_entry.get('invoice_idx') != idx:
                            continue

                        confidence    = self._get_confidence_for_layer(layer)
                        is_suggestion = confidence < 90

                        df_candidates.at[idx, 'settlement_id']    = settlement_id
                        df_candidates.at[idx, 'reconcile_status'] = 'ENRICHED'
                        df_candidates.at[idx, 'match_method']     = f'VIP_{layer}'
                        df_candidates.at[idx, 'match_confidence'] = str(confidence)
                        df_candidates.at[idx, 'is_suggestion']    = is_suggestion
                        self._copy_financial_amounts(df_candidates, idx, match)

                        assigned_invoices.add(idx)
                        layer_counters[layer] += 1
                        if is_suggestion:
                            phase2_suggestions += 1
                        else:
                            phase2_confirmed += 1

                        # Marcar voucher como usado
                        settlement_df.loc[
                            settlement_df['voucher_hash_key'] == v_hash, 'is_used'
                        ] = True

                        matched = True
                        break

        p1_count = sum(1 for i in df_candidates.index if i in assigned_invoices) - phase1_matches
        self.logger(f"   → Pasada 1: {p1_count} facturas asignadas", "INFO")

        # ── PASADA 2: Baja calidad — SOLO facturas sin match_hash_key ────────
        # Regla de negocio crítica: una factura con match_hash_key ya tiene su
        # voucher identificado por Webpos. Si ese voucher no está disponible
        # (ya fue usado), la factura queda PENDING — NO se le roba un voucher
        # de otro cliente que puede pertenecer a pagos futuros.
        # Pass 2 es un último recurso exclusivamente para facturas huérfanas
        # (sin voucher identificado por Webpos).
        self.logger("   Pasada 2: Capas de baja calidad (solo facturas sin hash)...", "INFO")

        for idx in df_candidates.index:
            if idx in assigned_invoices:
                continue

            invoice = df_candidates.loc[idx]

            # FIX 2: filtro de negocio VIP — no asignar vouchers de baja calidad
            # a facturas que ya tienen un voucher Webpos identificado.
            # Si tiene match_hash_key, su voucher ya fue procesado (Phase 1)
            # o no estaba disponible — en ambos casos queda PENDING, no RESCUE.
            inv_hash_key = invoice.get('match_hash_key', '')
            if pd.notna(inv_hash_key) and str(inv_hash_key).strip() not in ('', 'nan', 'None'):
                # Tiene voucher propio identificado pero no disponible → PENDING
                continue

            matched = False

            for layer in cascade_low:
                if matched:
                    break

                for settlement_id, settlement_df in vouchers_index.items():
                    available = settlement_df[~settlement_df['is_used']]
                    if available.empty:
                        continue

                    match = self._find_best_suggestion_in_layer(invoice, available, layer)
                    if match is None:
                        continue

                    confidence = self._get_confidence_for_layer(layer)

                    df_candidates.at[idx, 'settlement_id']    = settlement_id
                    df_candidates.at[idx, 'reconcile_status'] = 'ENRICHED'
                    df_candidates.at[idx, 'match_method']     = f'VIP_{layer}'
                    df_candidates.at[idx, 'match_confidence'] = str(confidence)
                    df_candidates.at[idx, 'is_suggestion']    = True
                    self._copy_financial_amounts(df_candidates, idx, match)

                    assigned_invoices.add(idx)
                    layer_counters[layer] += 1
                    phase2_suggestions += 1

                    match_hash = match.get('voucher_hash_key', '')
                    if match_hash:
                        settlement_df.loc[
                            settlement_df['voucher_hash_key'] == match_hash, 'is_used'
                        ] = True

                    matched = True
                    break

        for layer, count in layer_counters.items():
            if count > 0:
                self.logger(f"   Capa {layer}: {count} matches", "INFO")

        phase2_matches = phase2_confirmed + phase2_suggestions
        self.logger(
            f"PHASE 2 Complete: {phase2_matches} facturas "
            f"({phase2_confirmed} confirmados, {phase2_suggestions} sugerencias)",
            "SUCCESS"
        )

        norm_cols = ['norm_batch', 'norm_ref', 'norm_brand', 'norm_amount']
        df_candidates.drop(columns=norm_cols, inplace=True, errors='ignore')

        self.logger(f"RESUMEN FINAL VIP CASCADE:", "INFO")
        self.logger(f"   - Fase 1 (Exact Hash): {phase1_matches} confirmados", "INFO")
        self.logger(f"   - Fase 2: {phase2_confirmed} confirmados, {phase2_suggestions} sugerencias", "INFO")

        return pd.concat([df_rest, df_candidates], ignore_index=True)

    def _compute_competitive_prescoring(
        self,
        df_candidates: pd.DataFrame,
        pending_indices: list,
        vouchers_index: Dict[str, pd.DataFrame],
    ) -> tuple:
        """Computes competitive scores for each voucher/invoice pair."""
        invoice_optimal = {}
        voucher_best_score = {}
     
        for invoice_idx in pending_indices:
            invoice = df_candidates.loc[invoice_idx]
            best_score = 0
            best_settlement = None
            best_hash = None
     
            for settlement_id, settlement_df in vouchers_index.items():
                available = settlement_df[~settlement_df['is_used']]
                for _, voucher in available.iterrows():
                    score = 0
                    # 1. Scording logic: Batch (+35), Ref (+35), Amount (+20), Brand (+10)
                    if str(invoice.get('norm_batch')) == str(voucher.get('norm_batch')): score += 35
                    if str(invoice.get('norm_ref')) == str(voucher.get('norm_ref')): score += 35
                    if abs(float(invoice.get('norm_amount', 0)) - float(voucher.get('norm_amount', 0))) <= 0.01: score += 20
                    if str(invoice.get('norm_brand')) == str(voucher.get('norm_brand')): score += 10
                    
                    if score > best_score:
                        best_score = score
                        best_settlement = settlement_id
                        best_hash = voucher.get('voucher_hash_key')
                        
                    if score > voucher_best_score.get(best_hash, {}).get('score', 0):
                        voucher_best_score[best_hash] = {'score': score, 'invoice_idx': invoice_idx}
                        
            if best_score > 0:
                invoice_optimal[invoice_idx] = {'settlement_id': best_settlement, 'voucher_hash': best_hash, 'score': best_score}
                
        return invoice_optimal, voucher_best_score

    def _find_match_in_layer_fast(
        self,
        invoice: pd.Series,
        df_vouchers: pd.DataFrame,
        layer: str
    ) -> Optional[pd.Series]:
        """Match en una capa. O(n) sin copy(). Precondición: norm_* ya calculados."""
        if layer == 'EXACT_BATCH_REF_WRONG_AMT':
            return None

        inv_batch  = invoice.get('norm_batch', '')
        inv_ref    = invoice.get('norm_ref', '')
        inv_brand  = invoice.get('norm_brand', '')
        inv_amount = invoice.get('norm_amount', 0.0)
        tol = 0.01

        if layer == 'EXACT_BATCH_REF_WRONG_BRAND':
            if not inv_batch or not inv_ref:
                return None
            mask = (
                (df_vouchers['norm_batch'] == inv_batch) &
                (df_vouchers['norm_ref']   == inv_ref) &
                (abs(df_vouchers['norm_amount'] - inv_amount) <= tol) &
                (df_vouchers['norm_brand'] != inv_brand)
            )

        elif layer == 'EXACT_BATCH_AMT_WRONG_REF':
            if not inv_batch:
                return None
            mask = (
                (df_vouchers['norm_batch'] == inv_batch) &
                (abs(df_vouchers['norm_amount'] - inv_amount) <= tol) &
                (df_vouchers['norm_ref'] != inv_ref)
            )

        elif layer == 'EXACT_REF_AMT_WRONG_BATCH':
            if not inv_ref:
                return None
            mask = (
                (df_vouchers['norm_ref'] == inv_ref) &
                (abs(df_vouchers['norm_amount'] - inv_amount) <= tol) &
                (df_vouchers['norm_batch'] != inv_batch)
            )

        elif layer == 'SWAPPED_BATCH_REF':
            if not inv_batch or not inv_ref:
                return None
            mask = (
                (df_vouchers['norm_batch'] == inv_ref) &
                (df_vouchers['norm_ref']   == inv_batch) &
                (abs(df_vouchers['norm_amount'] - inv_amount) <= tol)
            )

        elif layer == 'FUZZY_BATCH_REF_MATCH':
            if not inv_batch or not inv_ref:
                return None
            return self._fuzzy_match_fast(inv_batch, inv_ref, inv_amount, df_vouchers)

        elif layer == 'SAME_BATCH_SAME_AMT':
            if not inv_batch:
                return None
            mask = (
                (df_vouchers['norm_batch'] == inv_batch) &
                (abs(df_vouchers['norm_amount'] - inv_amount) <= tol)
            )

        elif layer == 'SAME_AMT_SAME_BRAND':
            mask = (
                (abs(df_vouchers['norm_amount'] - inv_amount) <= tol) &
                (df_vouchers['norm_brand'] == inv_brand)
            )

        elif layer == 'RESCUE_BY_AMOUNT_ONLY':
            mask = (abs(df_vouchers['norm_amount'] - inv_amount) <= tol)

        else:
            return None

        match = df_vouchers[mask].head(1)
        return match.iloc[0] if not match.empty else None

    def _fuzzy_match_fast(
        self,
        inv_batch: str,
        inv_ref: str,
        inv_amount: float,
        df_vouchers: pd.DataFrame,
        threshold: float = 0.70
    ) -> Optional[pd.Series]:
        """Fuzzy match con Jaccard sobre bigramas. Delega a Rust si disponible."""
        candidates = df_vouchers[
            abs(df_vouchers['norm_amount'] - inv_amount) <= 0.01
        ]
        if candidates.empty:
            return None

        if _RUST_AVAILABLE:
            best_idx = _rust.fuzzy_batch_match(
                inv_batch=inv_batch,
                inv_ref=inv_ref,
                inv_amount=inv_amount,
                v_batches=candidates['norm_batch'].tolist(),
                v_refs=candidates['norm_ref'].tolist(),
                v_amounts=candidates['norm_amount'].tolist(),
                v_indices=list(range(len(candidates))),
                threshold=threshold,
                tolerance=0.01
            )
            if best_idx is not None:
                return candidates.iloc[best_idx]
            return None
        else:
            best_score = threshold - 0.001
            best_row   = None
            for _, voucher in candidates.iterrows():
                batch_sim = self._jaccard_similarity(inv_batch, voucher['norm_batch'])
                ref_sim   = self._jaccard_similarity(inv_ref,   voucher['norm_ref'])
                score     = (batch_sim + ref_sim) / 2.0
                if score > best_score:
                    best_score = score
                    best_row   = voucher
            return best_row

    @staticmethod
    def _jaccard_similarity(s1: str, s2: str) -> float:
        """Similitud de Jaccard sobre bigramas."""
        if not s1 or not s2:
            return 0.0
        bg1 = {s1[i:i+2] for i in range(len(s1) - 1)}
        bg2 = {s2[i:i+2] for i in range(len(s2) - 1)}
        if not bg1 or not bg2:
            return float(s1 == s2)
        intersection = len(bg1 & bg2)
        union        = len(bg1 | bg2)
        return intersection / union

    def _try_multi_invoice_match_fast(
        self,
        invoice_idx: int,
        invoice,
        df_candidates,
        settlement_vouchers,
        assigned_invoices: set,
        settlement_id: str
    ) -> Optional[Dict]:
        """
        Multi-Invoice Match con tolerancia de centavos.

        Caso A: diff <= TOLERANCE_CENTS → match directo, confidence=89, is_suggestion=True
        Caso B: voucher > factura       → buscar facturas complementarias
        Caso C: voucher <= factura      → sugerencia simple
        """
        inv_batch  = invoice.get('norm_batch', '')
        inv_ref    = invoice.get('norm_ref', '')
        inv_amount = invoice.get('norm_amount', 0.0)

        if not inv_batch or not inv_ref:
            return None

        potential = settlement_vouchers[
            (settlement_vouchers['norm_batch'] == inv_batch) &
            (settlement_vouchers['norm_ref']   == inv_ref)
        ].copy()

        if potential.empty:
            return None

        inv_cents = round(inv_amount * 100)
        potential = potential[
            potential['norm_amount'].apply(lambda x: round(x * 100) != inv_cents)
        ]

        if potential.empty:
            return None

        voucher     = potential.iloc[0]
        voucher_idx = potential.index[0]

        voucher_amount = self._safe_float(voucher['fin_gross'])
        voucher_cents  = round(voucher_amount * 100)
        diff_cents     = voucher_cents - inv_cents

        TOLERANCE_CENTS = int(round(self.config.get('vip_tolerance_cents', 5)))

        # Caso A: dentro de tolerancia de negocio
        if abs(diff_cents) <= TOLERANCE_CENTS:
            return {
                'matched_invoices': [(invoice_idx, 1.0)],
                'voucher':          voucher,
                'voucher_idx':      voucher_idx,
                'within_tolerance': True,
            }

        # Caso B: voucher mayor → buscar facturas complementarias
        if diff_cents > TOLERANCE_CENTS:
            diff_amount = diff_cents / 100.0

            complementary_candidates = df_candidates[
                (~df_candidates.index.isin(assigned_invoices)) &
                (df_candidates.index != invoice_idx) &
                (df_candidates['reconcile_status'] == 'PENDING')
            ]

            same_batch = complementary_candidates[
                complementary_candidates['norm_batch'] == inv_batch
            ]

            found = self._find_invoice_combination_dp(diff_amount, same_batch)
            if not found:
                found = self._find_invoice_combination_dp(diff_amount, complementary_candidates)

            if found:
                all_invoices = [invoice_idx] + found
                total_amount = sum(
                    self._safe_float(df_candidates.loc[i, 'amount_outstanding'])
                    for i in all_invoices
                )
                matched_invoices = [
                    (i, self._safe_float(df_candidates.loc[i, 'amount_outstanding']) / total_amount
                     if total_amount > 0 else 0)
                    for i in all_invoices
                ]
                return {
                    'matched_invoices': matched_invoices,
                    'voucher':          voucher,
                    'voucher_idx':      voucher_idx,
                    'within_tolerance': False,
                }
            else:
                # Sin complementarias → sugerencia simple con la factura original
                return {
                    'matched_invoices': [(invoice_idx, 1.0)],
                    'voucher':          voucher,
                    'voucher_idx':      voucher_idx,
                    'within_tolerance': False,
                }

        # Caso C: voucher <= factura
        return {
            'matched_invoices': [(invoice_idx, 1.0)],
            'voucher':          voucher,
            'voucher_idx':      voucher_idx,
            'within_tolerance': False,
        }

    def _find_invoice_combination_dp(
        self,
        target_amount: float,
        df_pool: pd.DataFrame,
        tolerance: float = 0.01,
        max_invoices: int = 4
    ) -> Optional[List[int]]:
        """
        Subset sum optimizado. Delega a Rust si disponible.
        Fallback Python: HashMap O(n) para 2 facturas, two-pointer O(n²) para 3.
        """
        if df_pool.empty:
            return None

        amounts    = df_pool['amount_outstanding'].apply(self._safe_float)
        indices    = df_pool.index.tolist()
        valid_mask = amounts <= (target_amount + tolerance)
        amounts    = amounts[valid_mask]
        indices    = [idx for idx, v in zip(indices, valid_mask) if v]

        if not indices:
            return None

        if _RUST_AVAILABLE:
            return _rust.find_invoice_combination(
                amounts=amounts.tolist(),
                indices=indices,
                target=target_amount,
                tolerance=tolerance,
                max_invoices=max_invoices,
            )

        # ── Python fallback ──────────────────────────────────────────────────
        # Caso 1: una factura O(n)
        for idx, amt in zip(indices, amounts):
            if abs(amt - target_amount) <= tolerance:
                return [idx]

        if max_invoices < 2 or len(indices) < 2:
            return None

        # Caso 2: dos facturas con HashMap O(n)
        seen: Dict[int, int] = {}
        tol_c = round(tolerance * 100)
        for idx, amt in zip(indices, amounts):
            c            = round(amt * 100)
            complement_c = round(target_amount * 100) - c
            for delta in range(-tol_c, tol_c + 1):
                if (complement_c + delta) in seen:
                    return [seen[complement_c + delta], idx]
            seen[c] = idx

        if max_invoices < 3 or len(indices) < 3:
            return None

        # Caso 3: tres facturas con two-pointer O(n²)
        sorted_pairs = sorted(zip(indices, amounts.tolist()), key=lambda x: x[1])
        n       = len(sorted_pairs)
        tgt_c   = round(target_amount * 100)

        for i in range(n - 2):
            idx_i, amt_i = sorted_pairs[i]
            rem_c        = tgt_c - round(amt_i * 100)
            if rem_c < 0:
                continue
            lo, hi = i + 1, n - 1
            while lo < hi:
                sum_c = round(sorted_pairs[lo][1] * 100) + round(sorted_pairs[hi][1] * 100)
                if abs(sum_c - rem_c) <= tol_c:
                    return [idx_i, sorted_pairs[lo][0], sorted_pairs[hi][0]]
                elif sum_c < rem_c:
                    lo += 1
                else:
                    hi -= 1

        return None

    def generate_etl_hash(self, df_portfolio: pd.DataFrame) -> pd.DataFrame:
        """Generates ETL synchronization hashes for the entire portfolio."""
        from logic.domain.services.portfolio_hash_service import PortfolioHashService
        self.logger("Generating ETL hashes v3.0 (PortfolioHashService)...", "INFO")
        df_portfolio['etl_hash'] = PortfolioHashService.compute_dataframe(df_portfolio)
        return df_portfolio


    def _find_best_suggestion_in_layer(
        self,
        invoice: pd.Series,
        df_vouchers: pd.DataFrame,
        layer: str,
    ) -> Optional[pd.Series]:
        """
        Versión de _find_match_in_layer_fast para capas de baja calidad.
        En lugar del primer match (head(1)), retorna el candidato de mayor
        score de relevancia. Usado exclusivamente en Pasada 2 (sugerencias).
        """
        if layer == 'EXACT_BATCH_REF_WRONG_AMT':
            return None

        inv_batch  = invoice.get('norm_batch', '')
        inv_ref    = invoice.get('norm_ref', '')
        inv_brand  = invoice.get('norm_brand', '')
        inv_amount = invoice.get('norm_amount', 0.0)
        tol        = 0.01

        if layer == 'SAME_AMT_SAME_BRAND':
            mask = (
                (abs(df_vouchers['norm_amount'] - inv_amount) <= tol) &
                (df_vouchers['norm_brand'] == inv_brand)
            )
        elif layer == 'RESCUE_BY_AMOUNT_ONLY':
            mask = (abs(df_vouchers['norm_amount'] - inv_amount) <= tol)
        else:
            return None

        candidates = df_vouchers[mask]
        if candidates.empty:
            return None

        if len(candidates) == 1:
            return candidates.iloc[0]

        best_score = -1
        best_row   = None

        for _, voucher in candidates.iterrows():
            score = 15  # monto siempre coincide en estas capas
            if inv_batch and voucher.get('norm_batch', '') == inv_batch:
                score += 40
            if inv_ref and voucher.get('norm_ref', '') == inv_ref:
                score += 40
            if inv_brand and voucher.get('norm_brand', '') == inv_brand:
                score += 5
            if score > best_score:
                best_score = score
                best_row   = voucher

        return best_row

    def _get_confidence_for_layer(self, layer: str) -> int:
        """Mapa de confidence por capa del cascade VIP."""
        confidence_map = {
            'EXACT_BATCH_REF_WRONG_AMT':    89,
            'EXACT_BATCH_REF_WRONG_BRAND':  90,
            'EXACT_BATCH_AMT_WRONG_REF':    85,
            'EXACT_REF_AMT_WRONG_BATCH':    85,
            'FUZZY_BATCH_REF_MATCH':        82,
            'SWAPPED_BATCH_REF':            80,
            'SAME_BATCH_SAME_AMT':          75,
            'SAME_AMT_SAME_BRAND':          60,
            'RESCUE_BY_AMOUNT_ONLY':        40,
        }
        return confidence_map.get(layer, 50)

    # ═════════════════════════════════════════════════════════════════════════
    # HELPERS
    # ═════════════════════════════════════════════════════════════════════════

    def _copy_financial_amounts(self, df_target, idx, source_row):
        """Copies financial metrics from source row to target record."""
        df_target.at[idx, 'financial_amount_gross'] = self._safe_float(source_row.get('fin_gross', 0))
        df_target.at[idx, 'financial_amount_net'] = self._safe_float(source_row.get('fin_net', 0))
        df_target.at[idx, 'financial_commission'] = self._safe_float(source_row.get('fin_comm', 0))
        df_target.at[idx, 'financial_tax_iva'] = self._safe_float(source_row.get('fin_iva', 0))
        df_target.at[idx, 'financial_tax_irf'] = self._safe_float(source_row.get('fin_irf', 0))

    def _copy_financial_amounts_proportional(self, df_target, idx, voucher, proportion):
        """Copies proportional financial metrics for split assignments."""
        df_target.at[idx, 'financial_amount_gross'] = self._safe_float(voucher.get('fin_gross', 0)) * proportion
        df_target.at[idx, 'financial_amount_net'] = self._safe_float(voucher.get('fin_net', 0)) * proportion
        df_target.at[idx, 'financial_commission'] = self._safe_float(voucher.get('fin_comm', 0)) * proportion
        df_target.at[idx, 'financial_tax_iva'] = self._safe_float(voucher.get('fin_iva', 0)) * proportion
        df_target.at[idx, 'financial_tax_irf'] = self._safe_float(voucher.get('fin_irf', 0)) * proportion

    @staticmethod
    def _safe_float(value, default=0.0):
        """Safely converts a value to float with handling for invalid inputs."""
        try:
            result = float(value)
            return result if np.isfinite(result) else default
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _normalize_batch(batch, brand):
        """Normalizes batch numbers based on brand-specific rules."""
        b = str(batch).strip()
        br = str(brand).strip().upper()
        return b.zfill(6) if ('PACIFICARD' in br or 'PCF' in br) else b.lstrip('0')

    @staticmethod
    def _normalize_ref(ref, brand):
        """Normalizes reference numbers based on brand-specific rules."""
        r = str(ref).strip()
        br = str(brand).strip().upper()
        return r.zfill(8) if ('PACIFICARD' in br or 'PCF' in br) else r.lstrip('0')

    def _get_prefix(self, brand):
        """Retrieves the prefix for a given brand from configuration."""
        prefixes = self.config.get('brand_prefixes', {})
        for key, val in prefixes.items():
            if key in str(brand).upper():
                return val
        return 'UNK'

    @staticmethod
    def _sanitize_join_key(series):
        """Sanitizes strings for join operations."""
        return (
            series.astype(str)
            .str.replace(r'[^a-zA-Z0-9-]', '', regex=True)
            .str.strip().str.upper()
        )

    @staticmethod
    def _clean_all_strings(df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes all string columns in a DataFrame."""
        df = df.copy()
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = df[col].astype(str).str.strip().str.upper()
                    df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
                except Exception:
                    pass
        return df