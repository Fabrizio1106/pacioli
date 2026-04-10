"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.enrichment.cash_deposit_enricher
===============================================================================

Description:
    Phase 4 enricher. Classifies cash deposit transactions (DEPOSITO EFECTIVO)
    into three patterns using sequence analysis and YAML-configured rules.
    All pattern parameters (prices, tolerances, thresholds) come from YAML.

Responsibilities:
    - Detect Salas VIP patterns: groups where >= 65% of amounts match known
      VIP prices within tolerance → customer_id = '999999'.
    - Detect Parking patterns: groups of >= 3 deposits in a tight time/sequence
      window → customer_id = '400419' (URBAPARKING).
    - Flag orphan deposits with no clear pattern → customer_id = '999998'
      (EFECTIVO - PENDIENTE CLASIFICAR), confidence = 50.
    - Write enrichment results to stg_bank_transactions via BankEnrichmentRepository.

Key Components:
    - CashDepositEnricher: Phase 4 enricher. Uses UnitOfWork + repository.
    - _process_sequences: Groups deposits by time and sequence number gaps.
    - _prepare_updates: Classifies each group into VIP / PARKING / UNKNOWN.
    - _clean_ref: Extracts integer reference number for sequence ordering.

Notes:
    - Version 8.0: UNKNOWN deposits use customer_id='999998' (distinct from
      VIP at '999999') to prevent downstream filtering ambiguity.
    - All thresholds, prices, and customer IDs are read from
      config['cash_deposit_rules']; no hardcoded business values.

Dependencies:
    - pandas
    - sqlalchemy
    - utils.logger
    - logic.infrastructure.unit_of_work

===============================================================================
"""

import pandas as pd
from utils.logger import get_logger
from sqlalchemy.engine import Engine

from logic.infrastructure.unit_of_work import UnitOfWork


class CashDepositEnricher:
    """
    Phase 4 enricher: classifies cash deposits by sequence pattern.

    Classification rules:
        1. VIP pattern: group purity >= 65% (purity = VIP-priced deposits / total).
           → customer_id = '999999' (SALAS VIP).
        2. Parking pattern: group size >= 3 deposits.
           → customer_id = '400419' (URBAPARKING).
        3. Unknown orphan: does not match any pattern.
           → customer_id = '999998' (EFECTIVO - PENDIENTE CLASIFICAR), confidence = 50.

    All thresholds and price lists are loaded from YAML via config['cash_deposit_rules'].
    """

    def __init__(self, engine_stg: Engine, config: dict):
        self.engine_stg = engine_stg
        self.logger = get_logger("CASH_ENRICHER")

        # Load configuration from YAML
        self.rules = config.get('cash_deposit_rules', {})
        self.vip_cfg = self.rules.get('salas_vip', {})
        self.parking_cfg = self.rules.get('parking', {})
        self.unknown_cfg = self.rules.get('unknown_orphan', {})
        self.v7_params = self.rules.get('strategy_v7_params', {})

        # Dynamic rule parameters
        self.VIP_PRICES = set(self.vip_cfg.get('known_prices', []))
        self.PRICE_TOL = float(self.vip_cfg.get('price_tolerance', 0.05))
        self.TIME_GAP_MINUTES = float(self.v7_params.get('time_gap_minutes', 5.0))
        self.SEQ_GAP_THRESHOLD = int(self.v7_params.get('seq_gap_threshold', 100))
        self.VIP_PURITY_THRESH = float(self.v7_params.get('vip_purity_thresh', 0.65))

        # Customer IDs and names per category
        self.VIP_ID = str(self.vip_cfg.get('customer_id', '999999'))
        self.VIP_NAME = str(self.vip_cfg.get('customer_name', 'SALAS VIP - EFECTIVO'))

        self.PARKING_ID = str(self.parking_cfg.get('customer_id', '400419'))
        self.PARKING_NAME = str(self.parking_cfg.get('customer_name', 'URBAPARKING - EFECTIVO'))

        # UNKNOWN uses '999998' (distinct from VIP '999999') to prevent downstream ambiguity
        self.UNKNOWN_ID = str(self.unknown_cfg.get('customer_id', '999998'))
        self.UNKNOWN_NAME = str(self.unknown_cfg.get('customer_name', 'EFECTIVO - PENDIENTE CLASIFICAR'))
        self.UNKNOWN_SCORE = int(self.unknown_cfg.get('confidence_score', 50))

        if not self.VIP_PRICES:
            self.logger("WARNING: No 'known_prices' found in YAML config.", "WARN")

        self.logger(
            f"Config loaded: VIP={self.VIP_ID}, PARKING={self.PARKING_ID}, "
            f"UNKNOWN={self.UNKNOWN_ID}",
            "INFO"
        )

    def enrich(self, engine_stg: Engine = None) -> int:
        """
        Run the Phase 4 cash deposit enrichment pipeline.

        Returns:
            Number of transactions updated.
        """
        if not self.rules.get('enabled', True):
            return 0

        with UnitOfWork(self.engine_stg) as uow:
            repo = uow.bank_enrichment

            # 1. Load pending deposits
            df = repo.get_pending_transactions(
                trans_types=['DEPOSITO EFECTIVO'],
                only_unenriched=True
            )

            if not df.empty:
                df = df[
                    df['enrich_confidence_score'].isna() |
                    (df['enrich_confidence_score'] < 80)
                ].copy()

            if df.empty:
                return 0

            # 2. Apply sequence analysis and pattern classification
            df_processed = self._process_sequences(df)
            updates, stats = self._prepare_updates(df_processed)

            # 3. Write results
            if updates:
                repo.update_customer_match_batch(updates)

                self.logger(f"   VIP batches detected: {stats['vip']}", "INFO")
                self.logger(f"   Parking batches detected: {stats['parking']}", "INFO")
                self.logger(f"   Unknown orphans: {stats['unknown']}", "INFO")

            return len(updates)

    def _process_sequences(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Group deposits into sequences by time gap and reference number gap.

        A new group starts when:
            - Time since last deposit exceeds TIME_GAP_MINUTES, OR
            - Sequential reference number gap exceeds SEQ_GAP_THRESHOLD.
        """
        df = df.copy()
        df['bank_date'] = pd.to_datetime(df['bank_date'])
        df['ref_num'] = df['bank_ref_1'].apply(self._clean_ref)
        df = df.sort_values(by=['bank_date', 'ref_num']).reset_index(drop=True)

        # Flag deposits whose amount matches a known VIP price
        df['is_vip_price'] = df['amount_total'].apply(
            lambda x: any(abs(x - p) <= self.PRICE_TOL for p in self.VIP_PRICES)
        )

        # Assign group IDs based on time and sequence number gaps
        group_ids = []
        current_group = 0
        prev_time = None
        prev_ref = None

        for _, row in df.iterrows():
            curr_time = row['bank_date']
            curr_ref = row['ref_num']
            start_new_group = False

            if prev_time is None:
                start_new_group = True
            else:
                diff_mins = (curr_time - prev_time).total_seconds() / 60.0
                diff_ref = abs(curr_ref - prev_ref)

                if diff_mins > self.TIME_GAP_MINUTES:
                    start_new_group = True
                elif curr_ref != -1 and prev_ref != -1 and diff_ref > self.SEQ_GAP_THRESHOLD:
                    start_new_group = True

            if start_new_group:
                current_group += 1

            group_ids.append(current_group)
            prev_time = curr_time
            prev_ref = curr_ref

        df['group_id'] = group_ids
        return df

    def _prepare_updates(self, df: pd.DataFrame) -> tuple:
        """
        Classify each deposit group into VIP, PARKING, or UNKNOWN.

        Classification logic:
            1. purity >= VIP_PURITY_THRESH → SALAS VIP (confidence 95).
            2. total_tx >= 3 → PARKING (confidence 90).
            3. Otherwise → UNKNOWN (confidence = UNKNOWN_SCORE).

        Returns:
            Tuple of (updates_list, stats_dict).
        """
        updates = []
        stats = {'vip': 0, 'parking': 0, 'unknown': 0}

        for gid, block in df.groupby('group_id'):
            total_tx = len(block)
            vip_matches = block['is_vip_price'].sum()
            purity = vip_matches / total_tx if total_tx > 0 else 0

            if purity >= self.VIP_PURITY_THRESH:
                cid = self.VIP_ID
                cname = self.VIP_NAME
                score = 95
                method = f"VIP_SEQ_PURE_{int(purity*100)}"
                notes = f"VIP batch detected (purity {purity:.0%})"
                stats['vip'] += 1

            elif total_tx >= 3:
                cid = self.PARKING_ID
                cname = self.PARKING_NAME
                score = 90
                method = f"PARKING_SEQ_SIZE_{total_tx}"
                notes = f"Parking batch detected ({total_tx} deposits)"
                stats['parking'] += 1

            else:
                cid = self.UNKNOWN_ID
                cname = self.UNKNOWN_NAME
                score = self.UNKNOWN_SCORE
                method = "UNKNOWN_ORPHAN"
                notes = f"Orphan with no clear pattern ({total_tx} deposit(s))"
                stats['unknown'] += 1

            for _, row in block.iterrows():
                updates.append({
                    "stg_id": row['stg_id'],
                    "customer_id": cid,
                    "customer_name": cname,
                    "confidence": score,
                    "method": method,
                    "notes": notes
                })

        return updates, stats

    def _clean_ref(self, val) -> int:
        """Extract integer reference number from bank_ref_1 for sequence sorting."""
        try:
            s = str(val).split('.')[0]
            digits = "".join(filter(str.isdigit, s))
            return int(digits) if digits else -1
        except:
            return -1
