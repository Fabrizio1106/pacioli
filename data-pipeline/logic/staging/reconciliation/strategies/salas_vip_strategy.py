"""
===============================================================================
Project: PACIOLI
Module: logic.staging.reconciliation.strategies.salas_vip_strategy
===============================================================================

Description:
    Implementation of the Salas VIP reconciliation strategy using a two-phase 
    matching approach with smart reassignment for cash deposits.

Responsibilities:
    - Perform two-phase matching (Greedy 1-to-1 and Split Detection).
    - Handle smart reassignment of Phase 1 matches to complete closures.
    - Maintain business constraints (mono-user splits, FIFO priority).

Key Components:
    - SalasVIPStrategy: Main class for the Salas VIP matching logic.

Notes:
    - Deposits belong to a single closure (user).
    - If a split exists, all parts must belong to the same closure.
    - Older closures are prioritized (FIFO).

Dependencies:
    - typing, collections, itertools, datetime, sys, pathlib
===============================================================================
"""

from typing import List, Dict, Optional, Set, Tuple
from collections import defaultdict
from itertools import combinations
from datetime import datetime, date, timedelta
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))


class SalasVIPStrategy:
    """
    Strategy V11.2: Two-Phase Matching + Smart Reassignment
    
    KEY PRINCIPLE:
    ───────────────
    A deposit belongs to ONLY ONE closure (user).
    If there is a split, all parts belong to the SAME closure.
    """
    
    def __init__(self, config: Optional[dict] = None):
        self.config = config or {}
        
        # 1. Basic Configuration
        self.tolerance = 0.05
        self.max_invoices_per_combination = 10
        
        # 2. Two-Phase Configuration (v11.0+)
        self.enable_split_detection = self.config.get('enable_split_detection', True)
        self.min_match_rate = self.config.get('min_match_rate', 0.70)
        self.max_deposits_per_split = self.config.get('max_deposits_per_split', 3)
        self.split_time_window_minutes = self.config.get('split_time_window_minutes', 10)
        
        # 3. Reassignment Configuration (v11.2)
        self.enable_reassignment = self.config.get('enable_reassignment', True)
        
        # 4. State Initialization
        self.debug_log = []
        self.facturas_usadas_global = set()
        self.queue_index_persistent = 0
        self.portfolio_queue_cached = None
    
    def match_by_user_groups(
        self,
        bank_transactions: List[dict],
        all_invoices: List[dict],
        payment_date=None
    ) -> List[Dict]:
        """Full Strategy V11.2 - Two-Phase Matching with Reassignment."""
        self.debug_log = []
        self.facturas_usadas_global = set()
        
        # 1. Filter CASH invoices
        efectivo_invoices = self._filter_efectivo_invoices(all_invoices)
        if not efectivo_invoices:
            self.debug_log.append("No CASH invoices found")
            return []
        
        self.debug_log.append(f"CASH invoices found: {len(efectivo_invoices)}")
        
        # 2. Create FIFO closure queue (CACHE)
        if self.portfolio_queue_cached is None:
            self.portfolio_queue_cached = self._create_portfolio_queue(efectivo_invoices)
            self.debug_log.append("FIFO queue created and cached (first time)")
        else:
            self.debug_log.append(
                f"Using cached FIFO queue (current queue_index: {self.queue_index_persistent})"
            )
        
        portfolio_queue = self.portfolio_queue_cached
        
        if not portfolio_queue:
            self.debug_log.append("Could not create closure queue")
            return []
        
        # 3. Group bank transactions by deposit date
        bank_batches = self._group_bank_by_deposit_date(bank_transactions)
        
        self.debug_log.append(f"\nBank batches detected: {len(bank_batches)}")
        for batch_date, txs in sorted(bank_batches.items()):
            self.debug_log.append(f"  - {batch_date}: {len(txs)} deposits")
        
        # 4. Process batch by batch
        all_decisions = []
        queue_index = self.queue_index_persistent
        
        sorted_batch_dates = sorted(bank_batches.keys())
        
        for batch_date in sorted_batch_dates:
            bank_batch = bank_batches[batch_date]
            batch_size = len(bank_batch)
            
            self.debug_log.append(
                f"\n{'='*80}\n"
                f"PROCESSING BATCH {batch_date}: {batch_size} deposits\n"
                f"{'='*80}"
            )
            
            # Verify closure availability
            if queue_index >= len(portfolio_queue):
                self.debug_log.append("WARNING: No more closures in the queue")
                for dep in bank_batch:
                    all_decisions.append(self._create_no_closing_decision(dep))
                continue
            
            # Take closures
            end_index = min(queue_index + batch_size, len(portfolio_queue))
            portfolio_slice = portfolio_queue[queue_index:end_index]
            
            self.debug_log.append(
                f"Taking closures [{queue_index}:{end_index}] "
                f"({len(portfolio_slice)} closures out of {len(portfolio_queue)} total)"
            )
            
            for idx, cierre in enumerate(portfolio_slice):
                self.debug_log.append(
                    f"  Closure {queue_index + idx}: {cierre['user']} "
                    f"({cierre['date']}) - ${cierre['total_amount']:.2f} "
                    f"({len(cierre['facturas'])} invoices)"
                )
            
            # 5. Two-Phase Matching with Reassignment (v11.2)
            batch_decisions, batch_success = self._match_batch_two_phase_v2(
                bank_batch=bank_batch,
                portfolio_slice=portfolio_slice,
                batch_date=batch_date
            )
            
            # 6. Validation with threshold
            if batch_success:
                self.debug_log.append(
                    f"\nBATCH {batch_date} SUCCESSFUL: "
                    f"{len(batch_decisions)} assignments completed"
                )
                all_decisions.extend(batch_decisions)
                queue_index = end_index
                self.queue_index_persistent = queue_index
            else:
                self.debug_log.append(
                    f"\nBATCH {batch_date} FAILED: Match rate < {self.min_match_rate*100:.0f}%"
                )
                for dep in bank_batch:
                    all_decisions.append(
                        self._create_batch_failed_decision(dep, batch_date)
                    )
        
        # 7. Final Validation
        self._validate_no_duplicates(all_decisions)
        
        # 8. Final Log Summary
        matched = sum(1 for d in all_decisions if d['status'] == 'MATCHED')
        review = sum(1 for d in all_decisions if d['status'] == 'REVIEW')
        
        self.debug_log.append(
            f"\n{'='*80}\n"
            f"FINAL SUMMARY\n"
            f"{'='*80}\n"
            f"Total decisions: {len(all_decisions)}\n"
            f"  - MATCHED: {matched}\n"
            f"  - REVIEW: {review}\n"
            f"Invoices used: {len(self.facturas_usadas_global)}"
        )
        
        print("\n".join(self.debug_log))
        
        return all_decisions
    
    def _match_batch_two_phase_v2(
        self,
        bank_batch: List[dict],
        portfolio_slice: List[Dict],
        batch_date: str
    ) -> Tuple[List[Dict], bool]:
        """
        Two-Phase Matching V2 with Smart Reassignment.
        
        PHASE 1: Greedy Matching (1-to-1)
          - Calculate uniqueness score
          - Assign direct matches
        
        PHASE 2: Split Detection + Reassignment
          - Try direct matches with available invoices
          - If it fails, search for Phase 1 REASSIGNMENTS
          - Constraint: Splits only from the SAME closure
        
        VALIDATION: Partial Success
          - If match_rate >= 70% → Success
        """
        
        self.debug_log.append(f"\n{'─'*80}")
        self.debug_log.append(f"TWO-PHASE MATCHING v11.2 (Smart Reassignment)")
        self.debug_log.append(f"{'─'*80}")
        
        # 1. PHASE 1: GREEDY MATCHING
        self.debug_log.append(f"\nPHASE 1: Greedy Matching (1-to-1)\n")
        
        phase1_decisions, residual_deposits, phase1_map = self._phase1_greedy_matching_v2(
            bank_batch=bank_batch,
            portfolio_slice=portfolio_slice
        )
        
        matched_phase1 = sum(1 for d in phase1_decisions if d['status'] == 'MATCHED')
        
        self.debug_log.append(
            f"\nPhase 1 Results:"
            f"\n  Matched: {matched_phase1}/{len(bank_batch)}"
            f"\n  Residuals: {len(residual_deposits)} deposits"
        )
        
        # 2. PHASE 2: SPLIT DETECTION + REASSIGNMENT
        phase2_decisions = []
        reassignments = []
        
        if self.enable_split_detection and residual_deposits:
            self.debug_log.append(f"\nPHASE 2: Split Detection + Reassignment\n")
            
            phase2_decisions, reassignments = self._phase2_split_with_reassignment(
                residual_deposits=residual_deposits,
                portfolio_slice=portfolio_slice,
                phase1_map=phase1_map
            )
            
            matched_phase2 = sum(1 for d in phase2_decisions if d['status'] == 'MATCHED')
            
            self.debug_log.append(
                f"\nPhase 2 Results:"
                f"\n  Splits matched: {matched_phase2}"
                f"\n  Reassignments: {len(reassignments)}"
            )
        
        # 3. APPLY REASSIGNMENTS TO PHASE 1
        if reassignments:
            self.debug_log.append(f"\nApplying {len(reassignments)} reassignments...")
            phase1_decisions = self._apply_reassignments(
                phase1_decisions,
                reassignments
            )
        
        # 4. CONSOLIDATE DECISIONS
        all_decisions = phase1_decisions + phase2_decisions
        
        # 5. PARTIAL VALIDATION
        total_deposits = len(bank_batch)
        deposits_matched = sum(1 for d in all_decisions if d['status'] == 'MATCHED')
        match_rate = deposits_matched / total_deposits if total_deposits > 0 else 0
        
        self.debug_log.append(
            f"\n{'─'*80}"
            f"\nFINAL VALIDATION"
            f"\n{'─'*80}"
            f"\n  Match rate: {match_rate*100:.1f}% ({deposits_matched}/{total_deposits})"
            f"\n  Threshold: {self.min_match_rate*100:.0f}%"
        )
        
        success = match_rate >= self.min_match_rate
        
        if not success:
            self.debug_log.append(
                f"\n  VALIDATION FAILED: Match rate {match_rate*100:.1f}% < {self.min_match_rate*100:.0f}%"
            )
        else:
            self.debug_log.append(
                f"\n  VALIDATION SUCCESSFUL: Match rate {match_rate*100:.1f}% >= {self.min_match_rate*100:.0f}%"
            )
        
        return all_decisions, success
    
    def _phase1_greedy_matching_v2(
        self,
        bank_batch: List[dict],
        portfolio_slice: List[Dict]
    ) -> Tuple[List[Dict], List[dict], Dict]:
        """
        Phase 1: Direct 1-to-1 match with uniqueness scoring.
        
        Returns phase1_map with assignment metadata to allow reassignments in Phase 2.
        
        Returns:
          - decisions: List of decisions
          - residual_deposits: Deposits without match
          - phase1_map: {deposit_id: {closure, invoices, ...}}
        """
        
        decisions = []
        depositos_matched = set()
        phase1_map = {}  # Metadata of assignments
        
        # Sort deposits by stg_id
        bank_sorted = sorted(bank_batch, key=lambda x: x.get('stg_id', 0))
        
        # 1. Calculate uniqueness scores
        self.debug_log.append("Calculating uniqueness scores...\n")
        
        depositos_con_score = []
        
        for deposito in bank_sorted:
            deposito_monto = round(float(deposito.get('amount_total', 0)), 2)
            deposito_id = deposito.get('stg_id')
            
            # Count available options
            opciones_count = 0
            
            for cierre_idx, cierre in enumerate(portfolio_slice):
                match_result = self._find_best_combination(
                    deposito_monto=deposito_monto,
                    cierre=cierre,
                    cierre_idx=cierre_idx
                )
                
                if match_result:
                    opciones_count += 1
            
            uniqueness = 1.0 / opciones_count if opciones_count > 0 else 0.0
            
            depositos_con_score.append({
                'deposito': deposito,
                'deposito_id': deposito_id,
                'deposito_monto': deposito_monto,
                'opciones_count': opciones_count,
                'uniqueness': uniqueness
            })
            
            self.debug_log.append(
                f"  Deposit {deposito_id} (${deposito_monto:.2f}): "
                f"{opciones_count} options → uniqueness {uniqueness:.3f}"
            )
        
        # 2. Sort by uniqueness descending
        depositos_con_score.sort(key=lambda x: (-x['uniqueness'], x['deposito_id']))
        
        self.debug_log.append(f"\nAssignment order (by uniqueness):\n")
        for idx, dep_info in enumerate(depositos_con_score):
            self.debug_log.append(
                f"  {idx+1}. Deposit {dep_info['deposito_id']} "
                f"(${dep_info['deposito_monto']:.2f}) - "
                f"uniqueness {dep_info['uniqueness']:.3f}"
            )
        
        # 3. Assign in priority order
        self.debug_log.append(f"\nAssigning matches...\n")
        
        for dep_idx, dep_info in enumerate(depositos_con_score):
            deposito = dep_info['deposito']
            deposito_id = dep_info['deposito_id']
            deposito_monto = dep_info['deposito_monto']
            
            if dep_info['opciones_count'] == 0:
                self.debug_log.append(
                    f"  [{dep_idx+1}] Deposit {deposito_id}: No direct options → RESIDUAL"
                )
                continue
            
            # Search for BEST match (prioritizing oldest)
            mejor_match = None
            mejor_cierre_idx = None
            mejor_cierre = None
            mejor_score = -1
            
            for cierre_idx, cierre in enumerate(portfolio_slice):
                match_result = self._find_best_combination(
                    deposito_monto=deposito_monto,
                    cierre=cierre,
                    cierre_idx=cierre_idx
                )
                
                if match_result:
                    # Score: combo score + bonus for seniority (FIFO)
                    score = match_result['score']
                    score += (len(portfolio_slice) - cierre_idx) * 0.001
                    
                    if score > mejor_score:
                        mejor_score = score
                        mejor_match = match_result
                        mejor_cierre_idx = cierre_idx
                        mejor_cierre = cierre
            
            if mejor_match:
                decision = self._create_decision(
                    deposito=deposito,
                    cierre=mejor_cierre,
                    match=mejor_match
                )
                decisions.append(decision)
                
                depositos_matched.add(deposito_id)
                
                # Mark invoices as used
                for fac in mejor_match['facturas']:
                    self.facturas_usadas_global.add(fac['stg_id'])
                
                # Save metadata for reassignments
                phase1_map[deposito_id] = {
                    'deposito': deposito,
                    'cierre_idx': mejor_cierre_idx,
                    'cierre': mejor_cierre,
                    'match': mejor_match,
                    'decision': decision
                }
                
                self.debug_log.append(
                    f"  [{dep_idx+1}] Deposit {deposito_id}: "
                    f"MATCHED with Closure {mejor_cierre_idx} "
                    f"({mejor_cierre['user']} {mejor_cierre['date']}) - "
                    f"${mejor_match['suma']:.2f}"
                )
        
        # 4. Identify residuals
        residual_deposits = [
            dep_info['deposito']
            for dep_info in depositos_con_score
            if dep_info['deposito_id'] not in depositos_matched
        ]
        
        return decisions, residual_deposits, phase1_map
    
    def _phase2_split_with_reassignment(
        self,
        residual_deposits: List[dict],
        portfolio_slice: List[Dict],
        phase1_map: Dict
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Phase 2: Split Detection with Phase 1 reassignment capability.
        
        Algorithm:
        1. Try direct match with available invoices.
        2. If it fails, search for REASSIGNMENTS that complete closures.
        3. CONSTRAINT: Splits only from the SAME closure (mono-user).
        
        Returns:
          - decisions: New decisions for residuals.
          - reassignments: Reassignments to apply in Phase 1.
        """
        
        decisions = []
        reassignments = []
        
        # Identify closures with available invoices
        available_closures = self._get_closures_with_available_invoices(portfolio_slice)
        
        self.debug_log.append(
            f"Closures with available invoices: {len(available_closures)}\n"
        )
        
        for cierre in available_closures:
            available_amount = sum(
                f.get('conciliable_amount', 0)
                for f in cierre['facturas']
                if f['stg_id'] not in self.facturas_usadas_global
            )
            
            self.debug_log.append(
                f"  {cierre['user']} ({cierre['date']}): "
                f"${available_amount:.2f} available"
            )
        
        # Process each residual deposit
        for dep in residual_deposits:
            dep_id = dep.get('stg_id')
            dep_amount = round(float(dep.get('amount_total', 0)), 2)
            
            self.debug_log.append(f"\nProcessing residual {dep_id} (${dep_amount:.2f}):")
            
            # 1. Try direct match with available invoices
            direct_match = self._try_direct_match_with_available(
                dep, available_closures
            )
            
            if direct_match:
                self.debug_log.append(f"  Direct match found")
                decisions.append(direct_match)
                continue
            
            # 2. Search for reassignment (v11.2)
            if self.enable_reassignment:
                reassignment = self._find_optimal_reassignment(
                    dep, portfolio_slice, phase1_map
                )
                
                if reassignment:
                    self.debug_log.append(
                        f"  REASSIGNMENT found: "
                        f"{len(reassignment['move_deposits'])} deposits to reassign"
                    )
                    reassignments.append(reassignment)
                    
                    # Create decision for residual
                    residual_decision = self._create_decision(
                        deposito=dep,
                        cierre=reassignment['target_closure'],
                        match=reassignment['residual_match']
                    )
                    decisions.append(residual_decision)
                    continue
            
            self.debug_log.append(f"  No match or reassignment found")
        
        return decisions, reassignments
    
    def _find_optimal_reassignment(
        self,
        dep_residual: dict,
        portfolio_slice: List[Dict],
        phase1_map: Dict
    ) -> Optional[Dict]:
        """
        Search for optimal reassignment to complete a closure.
        
        STRATEGY:
        ──────────
        1. Identify partially used closures.
        2. Are there Phase 1 deposits that together complete that closure?
        3. Does the residual deposit match the freed closure?
        4. Constraint: Reassigned deposits must be from the SAME closure.
        """
        
        dep_amount = round(float(dep_residual.get('amount_total', 0)), 2)
        
        # Identify partially used closures
        partial_closures = self._identify_partial_closures(
            portfolio_slice, phase1_map
        )
        
        best_reassignment = None
        min_movements = float('inf')
        
        for cierre in partial_closures:
            cierre_total = cierre['total_amount']
            used_amount = cierre['used_amount']
            available_amount = cierre['available_amount']
            
            # Does the residual complete this closure?
            if abs(dep_amount - available_amount) <= self.tolerance:
                # Search for Phase 1 deposits using this closure
                deposits_using_closure = self._find_deposits_using_closure(
                    cierre, phase1_map
                )
                
                if not deposits_using_closure:
                    continue
                
                # Can those deposits form a complete closure?
                target_closure_match = self._can_form_complete_closure(
                    deposits_using_closure, portfolio_slice
                )
                
                if target_closure_match:
                    # VIABLE REASSIGNMENT
                    movements = len(deposits_using_closure)
                    
                    if movements < min_movements:
                        min_movements = movements
                        
                        best_reassignment = {
                            'move_deposits': deposits_using_closure,
                            'from_closure': cierre,
                            'target_closure': target_closure_match['cierre'],
                            'target_match': target_closure_match['match'],
                            'residual_match': {
                                'facturas': [
                                    f for f in cierre['facturas']
                                    if f['stg_id'] not in self.facturas_usadas_global
                                ],
                                'suma': available_amount,
                                'diferencia': abs(dep_amount - available_amount),
                                'score': 75  # Reduced confidence for reassignment
                            }
                        }
        
        return best_reassignment
    
    def _identify_partial_closures(
        self,
        portfolio_slice: List[Dict],
        phase1_map: Dict
    ) -> List[Dict]:
        """Identifies partially used closures from Phase 1."""
        
        partial_closures = []
        
        for cierre in portfolio_slice:
            total_amount = cierre['total_amount']
            
            # Calculate used amount
            used_amount = sum(
                f.get('conciliable_amount', 0)
                for f in cierre['facturas']
                if f['stg_id'] in self.facturas_usadas_global
            )
            
            available_amount = sum(
                f.get('conciliable_amount', 0)
                for f in cierre['facturas']
                if f['stg_id'] not in self.facturas_usadas_global
            )
            
            # Only partially used closures
            if 0 < used_amount < total_amount:
                partial_closures.append({
                    'cierre': cierre,
                    'user': cierre['user'],
                    'date': cierre['date'],
                    'total_amount': total_amount,
                    'used_amount': round(used_amount, 2),
                    'available_amount': round(available_amount, 2),
                    'facturas': cierre['facturas']
                })
        
        return partial_closures
    
    def _find_deposits_using_closure(
        self,
        cierre_info: Dict,
        phase1_map: Dict
    ) -> List[dict]:
        """Finds which Phase 1 deposits use this closure."""
        
        cierre = cierre_info['cierre']
        cierre_facturas_ids = {f['stg_id'] for f in cierre['facturas']}
        
        using_deposits = []
        
        for dep_id, mapping in phase1_map.items():
            # Does this deposit use invoices from this closure?
            used_facturas_ids = {
                f['stg_id'] for f in mapping['match']['facturas']
            }
            
            if used_facturas_ids & cierre_facturas_ids:  # Intersection
                using_deposits.append(mapping['deposito'])
        
        return using_deposits
    
    def _can_form_complete_closure(
        self,
        deposits: List[dict],
        portfolio_slice: List[Dict]
    ) -> Optional[Dict]:
        """
        Verifies if a group of deposits can form a complete closure.
        
        CONSTRAINT v11.2:
        ─────────────────
        Deposits must sum EXACTLY to a closure from the SAME user.
        """
        
        total_amount = sum(float(d.get('amount_total', 0)) for d in deposits)
        total_amount = round(total_amount, 2)
        
        # Search for closure matching the total
        for cierre in portfolio_slice:
            cierre_amount = cierre['total_amount']
            
            if abs(total_amount - cierre_amount) <= self.tolerance:
                # Verify invoice availability
                match_result = self._find_best_combination_for_amount(
                    total_amount, cierre
                )
                
                if match_result:
                    return {
                        'cierre': cierre,
                        'match': match_result
                    }
        
        return None
    
    def _find_best_combination_for_amount(
        self,
        target_amount: float,
        cierre: Dict
    ) -> Optional[Dict]:
        """Searches for invoice combination summing to target amount."""
        
        all_combos = cierre['all_combinations']
        
        for monto_combo, lista_combos in all_combos.items():
            diferencia = abs(target_amount - monto_combo)
            
            if diferencia > self.tolerance:
                continue
            
            for combo_obj in lista_combos:
                facturas = combo_obj['facturas']
                
                # Verify NOT globally used
                if any(f['stg_id'] in self.facturas_usadas_global for f in facturas):
                    continue
                
                return {
                    'facturas': facturas,
                    'suma': monto_combo,
                    'diferencia': diferencia,
                    'score': 75,  # Reduced confidence for reassignment
                    'num_fac': len(facturas)
                }
        
        return None
    
    def _apply_reassignments(
        self,
        phase1_decisions: List[Dict],
        reassignments: List[Dict]
    ) -> List[Dict]:
        """
        Applies reassignments to Phase 1 decisions.
        
        Process:
        1. Identify deposits to reassign.
        2. Update decisions with new closure.
        3. Mark as "REASSIGNED".
        """
        
        # Decision map by deposit_id
        decisions_map = {
            d['id']: d for d in phase1_decisions
        }
        
        for reassignment in reassignments:
            move_deposits = reassignment['move_deposits']
            target_closure = reassignment['target_closure']
            target_match = reassignment['target_match']
            
            # Update each involved deposit
            for dep in move_deposits:
                dep_id = dep.get('stg_id')
                
                if dep_id in decisions_map:
                    # Create new decision
                    new_decision = self._create_decision(
                        deposito=dep,
                        cierre=target_closure,
                        match=target_match,
                        is_reassigned=True
                    )
                    
                    # Replace
                    decisions_map[dep_id] = new_decision
                    
                    self.debug_log.append(
                        f"  Deposit {dep_id} reassigned to "
                        f"{target_closure['user']} ({target_closure['date']})"
                    )
        
        return list(decisions_map.values())
    
    def _get_closures_with_available_invoices(
        self,
        portfolio_slice: List[Dict]
    ) -> List[Dict]:
        """Returns closures that have available invoices."""
        
        available = []
        
        for cierre in portfolio_slice:
            has_available = any(
                f['stg_id'] not in self.facturas_usadas_global
                for f in cierre['facturas']
            )
            
            if has_available:
                available.append(cierre)
        
        return available
    
    def _try_direct_match_with_available(
        self,
        deposito: dict,
        available_closures: List[Dict]
    ) -> Optional[Dict]:
        """Tries direct match with available invoices."""
        
        deposito_monto = round(float(deposito.get('amount_total', 0)), 2)
        
        for cierre in available_closures:
            # Search match only with available invoices
            match_result = self._find_best_combination(
                deposito_monto=deposito_monto,
                cierre=cierre,
                cierre_idx=0
            )
            
            if match_result:
                decision = self._create_decision(
                    deposito=deposito,
                    cierre=cierre,
                    match=match_result
                )
                
                # Mark invoices as used
                for fac in match_result['facturas']:
                    self.facturas_usadas_global.add(fac['stg_id'])
                
                return decision
        
        return None
    
    # ═══════════════════════════════════════════════════════════════════
    # AUXILIARY METHODS
    # ═══════════════════════════════════════════════════════════════════
    
    def _create_portfolio_queue(self, invoices: List[dict]) -> List[Dict]:
        """Creates chronological FIFO closure queue."""
        groups = defaultdict(list)
        for inv in invoices:
            user = inv.get('enrich_user', 'UNKNOWN')
            doc_date = inv.get('doc_date')
            date_normalized = self._normalize_date(doc_date)
            groups[(user, date_normalized)].append(inv)
        
        cierres = []
        for (user, date), facturas in groups.items():
            facturas_sorted = sorted(facturas, key=lambda x: x.get('stg_id', 0))
            all_combos = self._calculate_all_combinations(facturas_sorted)
            total_amount = sum(
                float(f.get('conciliable_amount', 0)) 
                for f in facturas_sorted
            )
            
            cierres.append({
                'user': user,
                'date': date,
                'facturas': facturas_sorted,
                'all_combinations': all_combos,
                'total_amount': round(total_amount, 2)
            })
        
        cierres_sorted = sorted(cierres, key=lambda x: (x['date'], x['user']))
        
        self.debug_log.append(f"\nFIFO queue created: {len(cierres_sorted)} closures")
        
        for idx, cierre in enumerate(cierres_sorted):
            num_combos = sum(len(v) for v in cierre['all_combinations'].values())
            self.debug_log.append(
                f"  {idx}. {cierre['user']} ({cierre['date']}): "
                f"${cierre['total_amount']:.2f} - "
                f"{len(cierre['facturas'])} invoices, "
                f"{num_combos} combinations"
            )
        
        return cierres_sorted
    
    def _normalize_date(self, date_value) -> str:
        """Normalizes date to YYYY-MM-DD format."""
        if date_value is None:
            return "1900-01-01"
        
        if isinstance(date_value, (date, datetime)):
            return date_value.strftime('%Y-%m-%d')
        
        date_str = str(date_value).strip()
        
        if '/' in date_str:
            try:
                parts = date_str.split('/')
                if len(parts) == 3:
                    month, day, year = parts
                    return f"{year.zfill(4)}-{month.zfill(2)}-{day.zfill(2)}"
            except:
                pass
        
        if '-' in date_str and len(date_str) >= 8:
            return date_str
        
        return "1900-01-01"
    
    def _calculate_all_combinations(self, facturas: List[dict]) -> Dict[float, List[Dict]]:
        """Calculates ALL possible invoice combinations."""
        combinations_by_amount = defaultdict(list)
        
        max_r = min(self.max_invoices_per_combination, len(facturas))
        
        for r in range(1, max_r + 1):
            for combo in combinations(facturas, r):
                suma = sum(float(f.get('conciliable_amount', 0)) for f in combo)
                suma_rounded = round(suma, 2)
                
                combinations_by_amount[suma_rounded].append({
                    'facturas': list(combo),
                    'suma': suma_rounded,
                    'num_fac': len(combo)
                })
        
        for monto in combinations_by_amount:
            combinations_by_amount[monto] = sorted(
                combinations_by_amount[monto],
                key=lambda x: x['num_fac']
            )
        
        return dict(combinations_by_amount)
    
    def _find_best_combination(
        self,
        deposito_monto: float,
        cierre: Dict,
        cierre_idx: int
    ) -> Optional[Dict]:
        """Finds BEST invoice combination for a deposit."""
        all_combos = cierre['all_combinations']
        
        candidatos = []
        
        for monto_combo, lista_combos in all_combos.items():
            diferencia = abs(deposito_monto - monto_combo)
            
            if diferencia > self.tolerance + 0.0001:
                continue
            
            for combo_obj in lista_combos:
                facturas = combo_obj['facturas']
                
                if any(f['stg_id'] in self.facturas_usadas_global for f in facturas):
                    continue
                
                score = self._calculate_combo_score(facturas, diferencia)
                
                candidatos.append({
                    'facturas': facturas,
                    'suma': monto_combo,
                    'diferencia': diferencia,
                    'score': score,
                    'num_fac': len(facturas)
                })
        
        if not candidatos:
            return None
        
        candidatos.sort(key=lambda x: x['score'], reverse=True)
        
        return candidatos[0]
    
    def _calculate_combo_score(self, facturas: List[dict], diferencia: float) -> float:
        """Calculates combination score."""
        score = 1000.0
        score -= diferencia * 1000
        score -= len(facturas) * 1.0
        
        try:
            fecha_promedio = sum(
                self._date_to_days(f.get('doc_date'))
                for f in facturas
            ) / len(facturas)
            
            if 0 < fecha_promedio < 900000:
                score += (900000 - fecha_promedio) * 0.00001
        except:
            pass
        
        return max(0.0, score)
    
    def _date_to_days(self, date_value) -> int:
        """Converts date to total days."""
        if date_value is None:
            return 0
        
        try:
            if isinstance(date_value, (date, datetime)):
                return date_value.year * 365 + date_value.month * 30 + date_value.day
            
            date_str = str(date_value).strip()
            
            if '/' in date_str:
                parts = date_str.split('/')
                if len(parts) == 3:
                    month, day, year = parts
                    return int(year) * 365 + int(month) * 30 + int(day)
            
            if '-' in date_str:
                parts = date_str.split('-')
                if len(parts) == 3:
                    year, month, day = parts
                    return int(year) * 365 + int(month) * 30 + int(day)
        except:
            pass
        
        return 0
    
    def _group_bank_by_deposit_date(self, bank_transactions: List[dict]) -> Dict[str, List[dict]]:
        """Groups bank transactions by deposit date."""
        batches = defaultdict(list)
        for tx in bank_transactions:
            deposit_date = self._normalize_date(tx.get('doc_date'))
            batches[deposit_date].append(tx)
        return dict(batches)
    
    def _create_decision(
        self,
        deposito,
        cierre,
        match,
        is_reassigned: bool = False
    ) -> Dict:
        """Creates MATCHED decision metadata."""
        facturas = match['facturas']
        diferencia = match['diferencia']
        
        if abs(diferencia) == 0:
            conf = 100
        elif abs(diferencia) <= 0.01:
            conf = 95
        elif abs(diferencia) <= 0.05:
            conf = 90
        else:
            conf = 85
        
        if len(facturas) > 1:
            conf -= 5
        
        if is_reassigned:
            conf = min(conf, 75)
        
        confidence = max(0, min(100, conf))
        
        user = cierre['user']
        date = cierre['date']
        
        notes = f"Closure: {user} ({date}). {len(facturas)} invoice(s)."
        
        if is_reassigned:
            notes = f"REASSIGNED. {notes}"
        
        if abs(diferencia) == 0:
            notes = f"Exact match. {notes}"
        elif abs(diferencia) > 0:
            notes += f" Rounding difference: ${abs(diferencia):.2f}"
        
        invoice_refs = []
        for fac in facturas:
            ref = fac.get('invoice_ref') or fac.get('assignment', 'N/A')
            invoice_refs.append(str(ref))
        
        notes += f" Invoices: {', '.join(invoice_refs[:5])}"
        if len(invoice_refs) > 5:
            notes += f"... +{len(invoice_refs) - 5}"
        
        return {
            'id': deposito.get('stg_id'),
            'status': 'MATCHED',
            'reason': 'VIP_SPLIT_SAME_CLOSURE' if is_reassigned else 'VIP_CLOSING_MATCH',
            'method': 'VIP_TWO_PHASE',
            'confidence': confidence,
            'diff': diferencia,
            'diff_adjustment': diferencia,
            'port_ids': [f['stg_id'] for f in facturas],
            'bank_ref_match': deposito.get('bank_ref_1', str(deposito.get('stg_id'))),
            'notes': notes[:500]
        }
    
    def _create_no_match_in_batch_decision(self, deposito) -> Dict:
        """Creates decision when no match is found in batch."""
        return {
            'id': deposito.get('stg_id'),
            'status': 'REVIEW',
            'reason': 'VIP_NO_MATCH_IN_BATCH',
            'method': 'VIP_TWO_PHASE',
            'confidence': 0,
            'diff': 0,
            'diff_adjustment': 0,
            'port_ids': [],
            'bank_ref_match': deposito.get('bank_ref_1', str(deposito.get('stg_id'))),
            'notes': f'No closure available. Amount: ${deposito.get("amount_total", 0):.2f}'
        }
    
    def _create_no_closing_decision(self, deposito) -> Dict:
        """Creates decision when closure queue is exhausted."""
        return {
            'id': deposito.get('stg_id'),
            'status': 'REVIEW',
            'reason': 'VIP_NO_CLOSING',
            'method': 'VIP_TWO_PHASE',
            'confidence': 0,
            'diff': 0,
            'diff_adjustment': 0,
            'port_ids': [],
            'bank_ref_match': deposito.get('bank_ref_1', str(deposito.get('stg_id'))),
            'notes': 'Closure queue exhausted.'
        }
    
    def _create_batch_failed_decision(self, deposito, batch_date) -> Dict:
        """Creates decision when batch validation fails."""
        return {
            'id': deposito.get('stg_id'),
            'status': 'REVIEW',
            'reason': 'VIP_BATCH_VALIDATION_FAILED',
            'method': 'VIP_TWO_PHASE',
            'confidence': 0,
            'diff': 0,
            'diff_adjustment': 0,
            'port_ids': [],
            'bank_ref_match': deposito.get('bank_ref_1', str(deposito.get('stg_id'))),
            'notes': f'Batch {batch_date} failed validation (match rate < {self.min_match_rate*100:.0f}%).'
        }
    
    def _filter_efectivo_invoices(self, all_invoices) -> List[dict]:
        """Filters CASH invoices."""
        efectivo = []
        for inv in all_invoices:
            brand_value = inv.get('enrich_brand')
            if brand_value is None:
                brand = ''
            else:
                brand = str(brand_value).upper()
            if 'EFECTIVO' in brand:
                efectivo.append(inv)
        return efectivo
    
    def _validate_no_duplicates(self, decisiones) -> None:
        """Validates that no invoice or deposit is assigned twice."""
        facturas_vistas = set()
        depositos_vistos = set()
        
        for decision in decisiones:
            for fac_id in decision.get('port_ids', []):
                if fac_id in facturas_vistas:
                    raise ValueError(f"BUG: Invoice {fac_id} assigned twice")
                facturas_vistas.add(fac_id)
            
            dep_id = decision.get('id')
            if dep_id and dep_id in depositos_vistos:
                raise ValueError(f"BUG: Deposit {dep_id} assigned twice")
            if dep_id:
                depositos_vistos.add(dep_id)
