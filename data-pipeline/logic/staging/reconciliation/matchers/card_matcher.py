"""
===============================================================================
Project: PACIOLI
Module: logic.staging.reconciliation.matchers.card_matcher
===============================================================================

Description:
    Specialized matcher for credit card settlements (LIQUIDACION TC). It 
    reconciles bank transactions with portfolio data enriched by the VIP 
    cascade process, supporting both confirmed invoices and suggestions.

Responsibilities:
    - Extract pending credit card settlements from the staging bank table.
    - Aggregate portfolio data by settlement_id, distinguishing between 
      confirmed invoices and suggestions.
    - Compare bank amounts against confirmed portfolio totals using defined 
      tolerance levels.
    - Assign reconciliation statuses (MATCHED, REVIEW, PENDING) and reasons.
    - Persist reconciliation results back to the staging layer.

Key Components:
    - CardMatcher: Specialized logic for card settlement reconciliation.

Notes:
    - v2.1: Added support for is_suggestion flag in portfolio aggregation.
    - Only aggregates confirmed invoices (is_suggestion = FALSE) for totals.
    - Suggestions are reported in notes but do not affect the main reconciliation logic.

Dependencies:
    - pandas, sqlalchemy, datetime
    - utils.logger
===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from datetime import datetime, timedelta
from utils.logger import get_logger


class CardMatcher:
    """
    Matcher specialized for credit card settlements.
    
    VERSION: v2.1
    """

    def __init__(self, config: dict):
        # 1. Initialization
        self.config = config
        self.logger = get_logger("CARD_MATCHER")
        
        self.target_trans_type = config.get('card_settlements', {}).get(
            'target_trans_type', 'LIQUIDACION TC'
        )
        
        self.tolerance = config.get('card_settlements', {}).get(
            'tolerance', 0.05
        )
        
        self.ignore_voucher_count = config.get('card_settlements', {}).get(
            'ignore_voucher_count_for', ['URBAPARKING']
        )

    def reconcile_card_settlements(
        self,
        engine_stg,
        payment_date: datetime.date = None
    ) -> dict:
        # 2. Reconcile Card Settlements Lifecycle
        
        self.logger("\n" + "═" * 80, "INFO")
        self.logger("CARD MATCHER v2.1: Credit Card Settlements Reconciliation", "INFO")
        self.logger("═" * 80, "INFO")
        
        stats = {
            'processed': 0,
            'matched': 0,
            'review': 0,
            'pending': 0
        }
        
        # 3. Step 1: Extract Pending Settlements
        
        query_bank = text("""
            SELECT 
                stg_id,
                settlement_id,
                establishment_name,
                brand,
                amount_total,
                bank_date,
                reconcile_status
            FROM biq_stg.stg_bank_transactions
            WHERE trans_type = :trans_type
              AND reconcile_status IN ('PENDING', 'REVIEW')
              AND settlement_id IS NOT NULL
              AND is_compensated_sap = FALSE
              AND is_compensated_intraday = FALSE
            ORDER BY bank_date ASC
        """)
        
        df_bank = pd.read_sql(
            query_bank,
            engine_stg,
            params={'trans_type': self.target_trans_type}
        )
        
        if df_bank.empty:
            self.logger("No pending credit card settlements", "INFO")
            return stats
        
        self.logger(
            f"Credit card settlements extracted: {len(df_bank)}",
            "INFO"
        )
        
        # 4. Step 2: Process Each Settlement
        
        updates = []
        
        for idx, row in df_bank.iterrows():
            settlement_id = row['settlement_id']
            bank_stg_id = row['stg_id']
            establishment = row['establishment_name']
            amount_banco = float(row['amount_total'])
            
            # Aggregate portfolio data (v2.1 - with suggestions)
            portfolio_agg = self._aggregate_portfolio_by_settlement(
                settlement_id,
                engine_stg
            )
            
            # No confirmed data found
            if portfolio_agg['count_invoices'] == 0:
                status = 'PENDING'
                reason = 'CARD_NO_PORTFOLIO_DATA'
                confidence = 0
                diff = amount_banco
                notes = f"No confirmed invoices in portfolio for settlement {settlement_id}"
                
                updates.append({
                    'id': bank_stg_id,
                    'status': status,
                    'reason': reason,
                    'confidence': confidence,
                    'diff': diff,
                    'method': 'CARD_AGGREGATION',
                    'notes': notes,
                    'port_ids': [],
                    'bank_ref_match': settlement_id
                })
                
                stats['pending'] += 1
                continue
            
            # 5. Comparison: Bank vs Portfolio (Confirmed Only)
            
            count_confirmed = portfolio_agg['count_invoices']
            amount_confirmed = portfolio_agg['amount_net']
            count_suggestions = portfolio_agg['count_suggestions']
            suggested_details = portfolio_agg.get('suggestion_details', '')
            
            diff = abs(amount_banco - amount_confirmed)
            
            # 6. Status and Reason Determination
            
            # Perfect match
            if diff <= 0.01:
                status = 'MATCHED'
                reason = 'CARD_PERFECT_MATCH'
                confidence = 100
                notes = (
                    f"Perfect match - Settlement {settlement_id}, "
                    f"Diff: ${diff:.2f}, Confirmed invoices: {count_confirmed}"
                )
                
                if count_suggestions > 0:
                    notes += f" - {count_suggestions} suggestion(s): {suggested_details}"
            
            # Pending suggestions
            elif count_suggestions > 0 and diff > self.tolerance:
                status = 'REVIEW'
                reason = 'CARD_HAS_SUGGESTIONS'
                confidence = 75
                notes = (
                    f"{count_confirmed} confirmed (${amount_confirmed:.2f}), "
                    f"{count_suggestions} suggestion(s) - {suggested_details} - "
                    f"Diff: ${diff:.2f} - Review suggestions"
                )
            
            # Difference within tolerance
            elif diff <= self.tolerance:
                status = 'MATCHED'
                reason = 'CARD_AMOUNT_WITHIN_TOLERANCE'
                confidence = 95
                notes = (
                    f"Difference within tolerance - Settlement {settlement_id}, "
                    f"Diff: ${diff:.2f}"
                )
            
            # Small mismatch
            elif diff <= (self.tolerance * 2):
                status = 'REVIEW'
                reason = 'CARD_AMOUNT_MISMATCH_SMALL'
                confidence = 70
                notes = (
                    f"Small difference - Settlement {settlement_id}, "
                    f"Diff: ${diff:.2f}"
                )
            
            else:
                status = 'REVIEW'
                reason = 'CARD_AMOUNT_MISMATCH_LARGE'
                confidence = 50
                notes = (
                    f"Large difference - Settlement {settlement_id}, "
                    f"Diff: ${diff:.2f}, Investigation required"
                )
            
            # Add update record
            updates.append({
                'id': bank_stg_id,
                'status': status,
                'reason': reason,
                'confidence': confidence,
                'diff': diff,
                'method': 'CARD_AGGREGATION',
                'notes': notes,
                'port_ids': [],  # Invoices already linked by settlement_id
                'bank_ref_match': settlement_id
            })
            
            if status == 'MATCHED':
                stats['matched'] += 1
            elif status == 'REVIEW':
                stats['review'] += 1
            else:
                stats['pending'] += 1
        
        stats['processed'] = len(df_bank)
        
        # 7. Step 3: Persist Updates
        
        if updates:
            self._persist_updates(updates, engine_stg)
        
        # Summary Reporting
        
        self.logger("\n" + "═" * 80, "INFO")
        self.logger("CARD MATCHER v2.1 SUMMARY", "INFO")
        self.logger("═" * 80, "INFO")
        self.logger(f"   Processed: {stats['processed']}", "INFO")
        self.logger(f"   MATCHED: {stats['matched']}", "SUCCESS")
        self.logger(f"   REVIEW: {stats['review']}", "WARN")
        self.logger(f"   PENDING: {stats['pending']}", "INFO")
        self.logger("═" * 80, "INFO")
        
        return stats

    def _aggregate_portfolio_by_settlement(
        self,
        settlement_id: str,
        engine
    ) -> dict:
        # 8. Portfolio Aggregation (with Suggestions Support)
        
        # QUERY 1: Confirmed Invoices (is_suggestion = FALSE)
        
        query_confirmed = text("""
            SELECT 
                COUNT(*) as count_invoices,
                COALESCE(SUM(financial_amount_gross), 0) as amount_gross,
                COALESCE(SUM(financial_amount_net), 0) as amount_net,
                COALESCE(SUM(financial_commission), 0) as amount_commission,
                COALESCE(SUM(financial_tax_iva), 0) as amount_tax_iva,
                COALESCE(SUM(financial_tax_irf), 0) as amount_tax_irf
            FROM biq_stg.stg_customer_portfolio
            WHERE settlement_id = :settlement_id
              AND reconcile_status = 'ENRICHED'
              AND reconcile_group IN ('VIP_CARD', 'PARKING_CARD')
              AND is_suggestion = FALSE
        """)
        
        result_confirmed = pd.read_sql(
            query_confirmed,
            engine,
            params={'settlement_id': settlement_id}
        )
        
        # QUERY 2: Suggested Invoices (is_suggestion = TRUE)
        
        query_suggestions = text("""
            SELECT 
                COUNT(*) as count_suggestions,
                STRING_AGG(CAST(stg_id AS TEXT), ',' ORDER BY stg_id) as suggested_ids,
                STRING_AGG(CONCAT('stg_id=', stg_id::TEXT, ' ($', financial_amount_gross::TEXT, ')'), ',' ORDER BY stg_id) as suggestion_details
            FROM biq_stg.stg_customer_portfolio
            WHERE settlement_id = :settlement_id
              AND reconcile_status = 'ENRICHED'
              AND reconcile_group IN ('VIP_CARD', 'PARKING_CARD')
              AND is_suggestion = TRUE
        """)
        
        result_suggestions = pd.read_sql(
            query_suggestions,
            engine,
            params={'settlement_id': settlement_id}
        )
        
        # Combine Results
        
        if result_confirmed.empty:
            return {
                'count_invoices': 0,
                'count_suggestions': 0,
                'suggested_ids': None,
                'suggestion_details': None,
                'amount_gross': 0,
                'amount_net': 0,
                'amount_commission': 0,
                'amount_tax_iva': 0,
                'amount_tax_irf': 0
            }
        
        confirmed_data = result_confirmed.iloc[0].to_dict()
        
        if not result_suggestions.empty:
            suggestion_data = result_suggestions.iloc[0].to_dict()
        else:
            suggestion_data = {
                'count_suggestions': 0,
                'suggested_ids': None,
                'suggestion_details': None
            }
        
        return {
            'count_invoices': int(confirmed_data['count_invoices']),
            'count_suggestions': int(suggestion_data.get('count_suggestions', 0)),
            'suggested_ids': suggestion_data.get('suggested_ids'),
            'suggestion_details': suggestion_data.get('suggestion_details'),
            'amount_gross': float(confirmed_data['amount_gross']),
            'amount_net': float(confirmed_data['amount_net']),
            'amount_commission': float(confirmed_data['amount_commission']),
            'amount_tax_iva': float(confirmed_data['amount_tax_iva']),
            'amount_tax_irf': float(confirmed_data['amount_tax_irf'])
        }

    def _persist_updates(self, updates: list, engine):
        # 9. Persistence Logic
        
        self.logger(f"Persisting {len(updates)} updates...", "INFO")
        
        for update in updates:
            query = text("""
                UPDATE biq_stg.stg_bank_transactions
                SET 
                    reconcile_status = :status,
                    reconcile_reason = :reason,
                    match_confidence_score = :confidence,
                    enrich_notes = :notes
                WHERE stg_id = :id
            """)
            
            with engine.begin() as conn:
                conn.execute(query, {
                    'id': update['id'],
                    'status': update['status'],
                    'reason': update['reason'],
                    'confidence': update['confidence'],
                    'notes': update['notes']
                })
        
        self.logger(f"   {len(updates)} transactions updated", "SUCCESS")
