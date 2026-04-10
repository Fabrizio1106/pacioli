"""
===============================================================================
Project: PACIOLI
Module: logic.staging.reconciliation.strategies.residuals_reconciliation_strategy
===============================================================================

Description:
    Strategy for reconciling cash deposits that did not match during the 
    primary Urbaparking reconciliation phase. It uses temporal subset sum 
    combinations of invoices within a specific date window to find optimal 
    matches for remaining deposits.

Responsibilities:
    - Filter residual bank transactions and portfolio invoices (e.g., 
      PARKING_AMOUNT_MISMATCH or NO_PORTFOLIO_DATA).
    - Perform 1:1 exact matching for residual items.
    - Execute temporal subset sum matching (1 deposit to N invoices within 
      ±N days).
    - Calculate confidence scores based on temporal evidence and group consistency.
    - Perform post-processing customer re-classification based on matched 
      portfolio evidence.

Key Components:
    - ResidualsReconciliationStrategy: Orchestrator for residual reconciliation.

Notes:
    - v1.0 features include greedy temporal matching (FIFO), date windows (±7 days), 
      and robust confidence scoring.
    - Re-classifies "Unknown" customers (999998) if they match Parking invoices.

Dependencies:
    - typing, collections, datetime, itertools, sys, pathlib
    - logic.staging.reconciliation.strategies.subset_sum_solver
===============================================================================
"""

from typing import List, Dict, Optional, Tuple, Set
from collections import defaultdict
from datetime import datetime, date, timedelta
from itertools import combinations
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from logic.staging.reconciliation.strategies.subset_sum_solver import SubsetSumSolver


class ResidualsReconciliationStrategy:
    """
    Strategy for reconciling URBAPARKING residuals.
    
    Should be executed AFTER UrbaParkingStrategy.
    """
    
    # Relevant Customer IDs
    PARKING_CUSTOMER_ID = '400419'
    UNKNOWN_CUSTOMER_ID = '999998'
    
    # Thresholds
    CONFIDENCE_MATCHED = 85
    CONFIDENCE_REVIEW = 70
    
    def __init__(self, config: Optional[dict] = None):
        # 1. Initialization
        self.config = config or {}
        self.tolerance = 0.00  # Exact match required
        self.subset_solver = SubsetSumSolver(config)
        
        # Configuration
        self.max_date_window_days = self.config.get('residuals_max_date_window_days', 7)
        self.max_invoices_per_match = self.config.get('residuals_max_invoices', 5)
        
        # Metrics
        self.metrics = {
            'total_banco_residuals': 0,
            'total_portfolio_residuals': 0,
            'exact_matches': 0,
            'subset_matches': 0,
            'unmatched': 0,
            'avg_invoices_per_match': 0.0,
            'avg_date_diff_days': 0.0,
            'avg_confidence': 0.0
        }
        
        self.debug_log = []
    
    def reconcile_residuals(
        self,
        bank_transactions: List[dict],
        all_invoices: List[dict]
    ) -> List[Dict]:
        """
        Reconciles URBAPARKING residuals.
        
        Args:
            bank_transactions: ALL bank transactions.
            all_invoices: ALL portfolio invoices.
        """
        # 2. Reconcile Residuals Lifecycle
        
        self.debug_log = []
        self.debug_log.append("\n" + "═" * 80)
        self.debug_log.append("RESIDUALS RECONCILIATION STRATEGY v1.0")
        self.debug_log.append("═" * 80)
        
        # 3. Step 1: Filter Residuals
        
        banco_residuals, portfolio_residuals = self._filter_residuals(
            bank_transactions, all_invoices
        )
        
        if not banco_residuals:
            self.debug_log.append("\nNo bank residuals to process")
            self._print_logs()
            return []
        
        if not portfolio_residuals:
            self.debug_log.append("\nNo invoices available for residuals")
            self._print_logs()
            return []
        
        self.metrics['total_banco_residuals'] = len(banco_residuals)
        self.metrics['total_portfolio_residuals'] = len(portfolio_residuals)
        
        self.debug_log.append(
            f"\nResiduals detected:"
            f"\n  Bank: {len(banco_residuals)} deposits"
            f"\n  Portfolio: {len(portfolio_residuals)} invoices"
        )
        
        # 4. Step 2: Exact Match Residuals
        
        exact_updates, banco_residuals, portfolio_residuals = self._exact_match_residuals(
            banco_residuals, portfolio_residuals
        )
        
        self.metrics['exact_matches'] = len(exact_updates)
        
        if exact_updates:
            self.debug_log.append(
                f"\nExact matches found: {len(exact_updates)}"
            )
        
        all_updates = exact_updates.copy()
        
        # 5. Step 3: Temporal Subset Matching
        
        if banco_residuals and portfolio_residuals:
            subset_updates = self._temporal_subset_matching(
                banco_residuals, portfolio_residuals
            )
            
            self.metrics['subset_matches'] = len(subset_updates)
            all_updates.extend(subset_updates)
        
        # 6. Step 4: Post-Processing - Customer Re-Classification
        # Requires original portfolio data (complete)
        
        _, portfolio_complete = self._filter_residuals(
            bank_transactions, all_invoices
        )
        
        all_updates = self._apply_customer_reclassification(
            all_updates,
            portfolio_complete
        )
        
        # 7. Step 5: Final Metrics & Logging
        
        self._calculate_final_metrics(all_updates)
        self._print_summary()
        self._print_logs()
        
        return all_updates
    
    def _filter_residuals(
        self,
        bank_transactions: List[dict],
        all_invoices: List[dict]
    ) -> Tuple[List[dict], List[dict]]:
        """
        Filters bank and portfolio residuals based on status and customer ID.
        """
        # 8. Helper Methods (Filtering)
        
        # Filter bank
        banco_residuals = []
        
        for tx in bank_transactions:
            # Normalize None/NaN to empty string
            reason = tx.get('reconcile_reason')
            if reason is None or (isinstance(reason, float) and str(reason) == 'nan'):
                reason = ''
            else:
                reason = str(reason).strip()
            
            customer_id = tx.get('enrich_customer_id')
            if customer_id is None or (isinstance(customer_id, float) and str(customer_id) == 'nan'):
                customer_id = ''
            else:
                customer_id = str(customer_id).strip()
            
            status = tx.get('reconcile_status')
            if status is None or (isinstance(status, float) and str(status) == 'nan'):
                status = ''
            else:
                status = str(status).strip()
            
            if (reason in ('PARKING_AMOUNT_MISMATCH', 'NO_PORTFOLIO_DATA') and
                customer_id in (self.PARKING_CUSTOMER_ID, self.UNKNOWN_CUSTOMER_ID) and
                status in ('REVIEW', 'PENDING')):
                banco_residuals.append(tx)
        
        # Sort by AMOUNT DESC (priority to larger deposits)
        banco_residuals.sort(
            key=lambda x: (
                -float(x.get('amount_total', 0)),
                self._parse_date(x.get('bank_date'))
            )
        )
        
        # Filter portfolio
        portfolio_residuals = []
        
        for inv in all_invoices:
            customer_code = str(inv.get('customer_code', ''))
            status = inv.get('reconcile_status', '')
            settlement_id = inv.get('settlement_id')
            
            if (customer_code == self.PARKING_CUSTOMER_ID and
                status == 'PENDING' and
                settlement_id is None):
                portfolio_residuals.append(inv)
        
        # Sort by doc_date ASC, conciliable_amount DESC
        portfolio_residuals.sort(
            key=lambda x: (
                self._parse_date(x.get('doc_date')),
                -float(x.get('conciliable_amount', 0))
            )
        )
        
        return banco_residuals, portfolio_residuals
    
    def _exact_match_residuals(
        self,
        banco_residuals: List[dict],
        portfolio_residuals: List[dict]
    ) -> Tuple[List[Dict], List[dict], List[dict]]:
        """1:1 exact matching between residual transactions."""
        
        updates = []
        used_banco_ids = set()
        used_portfolio_ids = set()
        
        for banco_tx in banco_residuals:
            banco_amount = round(float(banco_tx.get('amount_total', 0)), 2)
            banco_id = banco_tx.get('stg_id')
            
            for invoice in portfolio_residuals:
                if invoice.get('stg_id') in used_portfolio_ids:
                    continue
                
                inv_amount = round(float(invoice.get('conciliable_amount', 0)), 2)
                
                # Exact match
                if abs(banco_amount - inv_amount) <= self.tolerance:
                    update = self._create_residual_update(
                        deposito=banco_tx,
                        facturas=[invoice],
                        match_type='RESIDUAL_EXACT'
                    )
                    
                    updates.append(update)
                    used_banco_ids.add(banco_id)
                    used_portfolio_ids.add(invoice.get('stg_id'))
                    
                    self.debug_log.append(
                        f"  Exact Match: Dep {banco_id} (${banco_amount:.2f}) → "
                        f"Fac {invoice.get('invoice_ref')} (${inv_amount:.2f})"
                    )
                    
                    break
        
        # Filter used records
        banco_restante = [
            tx for tx in banco_residuals
            if tx.get('stg_id') not in used_banco_ids
        ]
        
        portfolio_restante = [
            inv for inv in portfolio_residuals
            if inv.get('stg_id') not in used_portfolio_ids
        ]
        
        return updates, banco_restante, portfolio_restante
    
    def _temporal_subset_matching(
        self,
        banco_residuals: List[dict],
        portfolio_residuals: List[dict]
    ) -> List[Dict]:
        """Matching using subset sum with temporal windows."""
        
        self.debug_log.append("\nStarting temporal subset sum matching...")
        
        # Group portfolio by date
        portfolio_groups = self._group_portfolio_by_date(portfolio_residuals)
        
        updates = []
        used_invoice_ids = set()
        
        for banco_tx in banco_residuals:
            banco_amount = round(float(banco_tx.get('amount_total', 0)), 2)
            banco_id = banco_tx.get('stg_id')
            banco_datetime = self._parse_date(banco_tx.get('bank_date'))
            
            if isinstance(banco_datetime, datetime):
                banco_date = banco_datetime.date()
            else:
                banco_date = banco_datetime
            
            self.debug_log.append(
                f"\n  Checking Dep {banco_id} (${banco_amount:.2f}, {banco_tx.get('bank_date')}):"
            )
            
            # Find match within temporal window
            match_result = self._find_temporal_match(
                banco_amount=banco_amount,
                banco_date=banco_date,
                portfolio_groups=portfolio_groups,
                used_invoice_ids=used_invoice_ids
            )
            
            if match_result:
                facturas = match_result['facturas']
                date_diff = match_result['date_diff']
                
                # Calculate confidence
                confidence = self._calculate_confidence(
                    banco_tx=banco_tx,
                    facturas=facturas,
                    date_diff=date_diff
                )
                
                # Only create update if confidence meets REVIEW threshold
                if confidence >= self.CONFIDENCE_REVIEW:
                    update = self._create_residual_update(
                        deposito=banco_tx,
                        facturas=facturas,
                        match_type='RESIDUAL_SUBSET',
                        confidence_override=confidence
                    )
                    
                    updates.append(update)
                    
                    for fac in facturas:
                        used_invoice_ids.add(fac.get('stg_id'))
                    
                    status_indicator = "MATCHED" if confidence >= self.CONFIDENCE_MATCHED else "REVIEW"
                    self.debug_log.append(
                        f"    {status_indicator} MATCH: {len(facturas)} invoices, "
                        f"diff {date_diff} days, confidence {confidence}"
                    )
                else:
                    self.debug_log.append(
                        f"    Match rejected (confidence {confidence} < {self.CONFIDENCE_REVIEW})"
                    )
            else:
                self.debug_log.append(f"    No match found")
        
        return updates
    
    def _group_portfolio_by_date(
        self,
        portfolio_residuals: List[dict]
    ) -> Dict[date, List[dict]]:
        """Groups invoices by doc_date."""
        groups = defaultdict(list)
        
        for invoice in portfolio_residuals:
            doc_date = self._parse_date(invoice.get('doc_date'))
            groups[doc_date].append(invoice)
        
        return dict(groups)
    
    def _find_temporal_match(
        self,
        banco_amount: float,
        banco_date: date,
        portfolio_groups: Dict[date, List[dict]],
        used_invoice_ids: Set[int]
    ) -> Optional[Dict]:
        """Searches for matches in temporal windows (±N days)."""
        
        for date_diff in range(0, self.max_date_window_days + 1):
            if date_diff == 0:
                target_dates = [banco_date]
            else:
                target_dates = [
                    banco_date - timedelta(days=date_diff),
                    banco_date + timedelta(days=date_diff)
                ]
            
            for target_date in target_dates:
                if target_date not in portfolio_groups:
                    continue
                
                # Filter available invoices
                available_invoices = [
                    inv for inv in portfolio_groups[target_date]
                    if inv.get('stg_id') not in used_invoice_ids
                ]
                
                if not available_invoices:
                    continue
                
                # Subset sum amounts
                amounts = [
                    float(inv.get('conciliable_amount', 0))
                    for inv in available_invoices
                ]
                
                # Use solve_with_strategy from existing project solver
                result = self.subset_solver.solve_with_strategy(
                    target=banco_amount,
                    amounts=amounts,
                    tolerance=self.tolerance,
                    strategy='auto'
                )
                
                if result:
                    indices = result['indices']
                    total_sum = result['sum']
                    
                    # Validate approximation differences
                    if result['strategy'] == 'APPROXIMATION':
                        max_diff = banco_amount * 0.05  # Max 5% diff
                        actual_diff = abs(result['diff'])
                        
                        if actual_diff > max_diff:
                            self.debug_log.append(
                                f"        APPROXIMATION rejected: diff ${actual_diff:.2f} "
                                f"> max ${max_diff:.2f}"
                            )
                            continue
                    
                    # Verify invoice count limit
                    if len(indices) > self.max_invoices_per_match:
                        continue
                    
                    matched_facturas = [available_invoices[i] for i in indices]
                    
                    return {
                        'facturas': matched_facturas,
                        'date_diff': abs((target_date - banco_date).days),
                        'total_sum': total_sum
                    }
        
        return None
    
    def _calculate_confidence(
        self,
        banco_tx: dict,
        facturas: List[dict],
        date_diff: int
    ) -> int:
        """Calculates confidence score based on temporal evidence."""
        base_score = 85
        score = base_score
        
        # Invoice count penalties
        num_facturas = len(facturas)
        if num_facturas > 1:
            score -= (num_facturas - 1) * 2
        
        if num_facturas > 3:
            score -= 5
        
        if num_facturas > 5:
            score -= 10
        
        # Date difference penalties
        score -= date_diff * 3
        
        if date_diff > 2:
            score -= 5
        
        # Bonuses
        if date_diff == 0:
            score += 5
        
        if num_facturas == 2:
            score += 3
        
        if str(banco_tx.get('enrich_customer_id')) == self.PARKING_CUSTOMER_ID:
            score += 5
        
        # Bonus for consecutivity in stg_id
        if self._are_invoices_consecutive(facturas):
            score += 2
        
        return max(0, min(100, score))
    
    def _are_invoices_consecutive(self, facturas: List[dict]) -> bool:
        """Checks if invoices have consecutive or near-consecutive stg_ids."""
        if len(facturas) < 2:
            return False
        
        stg_ids = sorted([f.get('stg_id') for f in facturas])
        
        for i in range(len(stg_ids) - 1):
            if stg_ids[i+1] - stg_ids[i] > 5:
                return False
        
        return True
    
    def _create_residual_update(
        self,
        deposito: dict,
        facturas: List[dict],
        match_type: str,
        confidence_override: Optional[int] = None
    ) -> Dict:
        """Creates an update dictionary for a residual match."""
        # 9. Persistence Update Creation
        banco_amount = float(deposito.get('amount_total', 0))
        total_invoices = sum(float(f.get('conciliable_amount', 0)) for f in facturas)
        diff = abs(banco_amount - total_invoices)
        
        # Determine confidence
        if confidence_override is not None:
            confidence = confidence_override
        else:
            confidence = 90 if match_type == 'RESIDUAL_EXACT' else 85
        
        # Determine status
        if confidence >= self.CONFIDENCE_MATCHED:
            status = 'MATCHED'
        elif confidence >= self.CONFIDENCE_REVIEW:
            status = 'REVIEW'
        else:
            status = 'PENDING'
        
        # Generate notes
        invoice_refs = [f.get('invoice_ref', 'N/A') for f in facturas]
        refs_str = ', '.join(invoice_refs[:3])
        if len(invoice_refs) > 3:
            refs_str += f"... +{len(invoice_refs) - 3}"
        
        notes = (
            f"RESIDUAL {match_type}: {len(facturas)} PARKING invoice(s). "
            f"Invoices: {refs_str}"
        )
        
        if diff > 0:
            notes += f". Diff: ${diff:.2f}"
        
        return {
            'id': deposito.get('stg_id'),
            'status': status,
            'reason': match_type,
            'diff': diff,
            'confidence': confidence,
            'method': 'PARKING_RESIDUAL',
            'notes': notes[:500],
            'port_ids': [f.get('stg_id') for f in facturas],
            'bank_ref_match': deposito.get('bank_ref_1')
        }
    
    def _parse_date(self, date_value) -> date:
        """Parses various date formats into a date object."""
        if date_value is None:
            return date(1900, 1, 1)
        
        if isinstance(date_value, datetime):
            return date_value.date()
        
        if isinstance(date_value, date):
            return date_value
        
        date_str = str(date_value).strip()
        
        if '/' in date_str:
            try:
                date_part = date_str.split(' ')[0]
                month, day, year = date_part.split('/')
                return date(int(year), int(month), int(day))
            except:
                pass
        
        if '-' in date_str:
            try:
                date_part = date_str.split(' ')[0]
                year, month, day = date_part.split('-')
                return date(int(year), int(month), int(day))
            except:
                pass
        
        return date(1900, 1, 1)
    
    def _apply_customer_reclassification(
        self,
        updates: List[Dict],
        all_portfolio: List[dict]
    ) -> List[Dict]:
        """Post-processes updates to re-classify customers based on match evidence."""
        self.debug_log.append("\n" + "═" * 80)
        self.debug_log.append("POST-PROCESSING: Customer Re-Classification")
        self.debug_log.append("═" * 80)
        
        upgrade_count = 0
        
        for update in updates:
            if update.get('status') not in ('MATCHED', 'REVIEW'):
                continue
            
            # Infer customer from matched portfolio
            matched_customer = self._infer_customer_from_matched_portfolio(
                update['port_ids'],
                all_portfolio
            )
            
            if (matched_customer 
                and matched_customer['code'] != '999998'
                and matched_customer.get('consensus', False)
                and update.get('confidence', 0) >= 75):
                
                # Update customer in the update dictionary
                update['enrich_customer_id'] = matched_customer['code']
                update['enrich_customer_name'] = matched_customer['name']
                update['enrich_confidence_score'] = update.get('confidence', 0)
                update['enrich_inference_method'] = 'RESIDUAL_MATCH_UPGRADE'
                
                original_notes = update.get('notes', '')
                update['notes'] = (
                    f"{original_notes} → Reclassified to "
                    f"{matched_customer['code']} via residual match evidence"
                )
                
                upgrade_count += 1
        
        return updates
    
    def _infer_customer_from_matched_portfolio(
        self, 
        portfolio_ids: list, 
        all_portfolio: list
    ) -> Optional[dict]:
        """Infers the dominant customer_code from matched portfolio records."""
        if not portfolio_ids:
            return None
        
        matched_invoices = [
            inv for inv in all_portfolio 
            if inv.get('stg_id') in portfolio_ids
        ]
        
        if not matched_invoices:
            return None
        
        customer_counts = {}
        customer_names = {}
        
        for invoice in matched_invoices:
            customer_code = invoice.get('customer_code')
            customer_name = invoice.get('customer_name')
            
            if customer_code and customer_code != '999998':
                customer_counts[customer_code] = customer_counts.get(customer_code, 0) + 1
                customer_names[customer_code] = customer_name
        
        if not customer_counts:
            return None
        
        if len(customer_counts) == 1:
            dominant_customer = list(customer_counts.keys())[0]
            return {
                'code': dominant_customer,
                'name': customer_names[dominant_customer],
                'consensus': True,
                'invoice_count': customer_counts[dominant_customer]
            }
        
        dominant_customer = max(customer_counts.items(), key=lambda x: x[1])[0]
        return {
            'code': dominant_customer,
            'name': customer_names[dominant_customer],
            'consensus': False,
            'invoice_count': customer_counts[dominant_customer]
        }
    
    def _calculate_final_metrics(self, updates: List[Dict]):
        """Calculates final performance metrics for the reconciliation run."""
        if not updates:
            return
        
        total_invoices = sum(len(u['port_ids']) for u in updates)
        self.metrics['avg_invoices_per_match'] = total_invoices / len(updates)
        
        confidences = [u['confidence'] for u in updates]
        self.metrics['avg_confidence'] = sum(confidences) / len(confidences)
        
        self.metrics['unmatched'] = (
            self.metrics['total_banco_residuals'] - len(updates)
        )
    
    def _print_summary(self):
        """Prints a summary of the reconciliation results to the debug log."""
        self.debug_log.append("\n" + "═" * 80)
        self.debug_log.append("RESIDUALS RECONCILIATION SUMMARY")
        self.debug_log.append("═" * 80)
        
        m = self.metrics
        self.debug_log.append(
            f"\nInput:"
            f"\n  Bank residuals: {m['total_banco_residuals']}"
            f"\n  Portfolio residuals: {m['total_portfolio_residuals']}"
        )
        
        total_matched = m['exact_matches'] + m['subset_matches']
        self.debug_log.append(
            f"\nResults:"
            f"\n  Exact matches: {m['exact_matches']}"
            f"\n  Subset matches: {m['subset_matches']}"
            f"\n  Total matched: {total_matched}"
            f"\n  Unmatched: {m['unmatched']}"
        )
    
    def _print_logs(self):
        """Prints the accumulated debug logs to the standard output."""
        print("\n".join(self.debug_log))
