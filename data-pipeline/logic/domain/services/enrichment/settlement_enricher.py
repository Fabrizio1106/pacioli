"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.enrichment.settlement_enricher
===============================================================================

Description:
    Phase 5 enricher (waterfall). Copies voucher data from stg_card_settlements
    to matched bank transactions in stg_bank_transactions. Does not perform
    accounting calculations or set reconcile_reason — those responsibilities
    belong to the accounting validation phase.

Responsibilities:
    - Strategy 1 (Universal): Enrich transactions that already have a manually
      assigned settlement_id but lack voucher amount fields.
    - Strategy 2 (Hash Match): Match bank transactions to settlements by
      match_hash_key (Visa, Diners, Amex — 1:1 matches).
    - Strategy 3 (Split Batch): Match grouped bank transactions to settlements
      by batch_number (Pacificard split records — 1:N matches) with
      proportional amount allocation.
    - Copy settlement_id, establishment_name, and voucher amounts to the
      bank transaction row.

Key Components:
    - SettlementEnricher: Phase 5 waterfall enricher.
    - _execute_hash_matching: Main strategy for Diners/Visa/Amex.
    - _execute_split_batch_matching: Pacificard split case.
    - _execute_universal_enrichment: Manual override support.
    - _apply_voucher_data_copy: Shared batch UPDATE writer.

Notes:
    - Version 3.0: Responsibility narrowed to data copy only.
      diff_adjustment and reconcile_reason are computed by a separate
      accounting validation step (update_bank_validation_metrics).
    - Split batch: last row in a group absorbs rounding residuals.

Dependencies:
    - pandas
    - sqlalchemy
    - utils.logger

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from utils.logger import get_logger


class SettlementEnricher:
    """
    Phase 5 waterfall enricher: copies voucher data to bank transactions.

    Single responsibility: copy settlement_id, establishment_name, and voucher
    amounts from stg_card_settlements to stg_bank_transactions.

    Does NOT compute: diff_adjustment, reconcile_reason, reconcile_status,
    or tolerance decisions — those belong to the accounting validation phase.

    Strategies:
        1. Hash Match — match_hash_key for 1:1 (Visa, Diners, Amex).
        2. Split Batch — batch_number for 1:N (Pacificard split records).
        3. Universal — manually-assigned settlement_id without voucher amounts.
    """

    def __init__(self, engine_stg: Engine, config: dict):
        self.engine_stg = engine_stg
        self.config = config
        self.logger = get_logger("SETTLEMENT_ENRICHER")

    def enrich(self, engine_stg: Engine) -> int:
        """
        Run settlement enrichment across all three strategies in order.

        Returns:
            Total number of bank transactions enriched.
        """
        self.logger("Starting settlement enrichment (v3.0)...", "INFO")
        tolerance = float(self.config.get('tolerance_threshold', 0.05))
        total = 0

        # Strategy 1: Universal (manual overrides)
        count_univ = self._execute_universal_enrichment(tolerance)
        total += count_univ

        # Strategy 2: Hash Match (primary engine for Diners/Visa/Amex)
        count_hash = self._execute_hash_matching(tolerance)
        total += count_hash

        # Strategy 3: Split Batch (Pacificard split records)
        count_split = self._execute_split_batch_matching(tolerance)
        total += count_split

        if total > 0:
            self.logger(
                f"{total} settlements enriched "
                f"(Hash: {count_hash}, Split: {count_split}, Manual: {count_univ}).",
                "SUCCESS"
            )

        return total

    # ═════════════════════════════════════════════════════════════════════════
    # STRATEGY 1: HASH MATCHING (Diners, Visa, Amex)
    # ═════════════════════════════════════════════════════════════════════════

    def _execute_hash_matching(self, tolerance: float) -> int:
        """
        Match open bank transactions to settlements by match_hash_key (1:1).

        Copies voucher data only; does not compute diff_adjustment or set
        reconcile_reason.
        """
        df_bank = self._load_bank_for_hash()
        if df_bank.empty:
            return 0

        df_sett = self._load_settlements_for_hash()
        if df_sett.empty:
            return 0

        merged = pd.merge(df_bank, df_sett, on='match_hash_key', how='inner')
        if merged.empty:
            return 0

        updates = []

        for _, row in merged.iterrows():
            bank_net = float(row['amount_total'])
            sett_net = float(row['sett_net'])

            # Basic tolerance check to prevent erroneous hash collisions
            if abs(bank_net - sett_net) > tolerance:
                continue

            updates.append({
                "stg_id": row['stg_id'],
                "settlement_id": str(row['settlement_id']),
                "establishment_name": str(row.get('establishment_name', '')),
                "final_gross": float(row['sett_gross']),
                "final_net": float(row['sett_net']),
                "final_commission": float(row['sett_comm']),
                "final_tax_iva": float(row['sett_iva']),
                "final_tax_irf": float(row['sett_irf'])
            })

        if updates:
            self._apply_voucher_data_copy(updates)
            self.logger(f"   Hash Match: {len(updates)} transactions (Visa/Diners/Amex).", "INFO")

        return len(updates)

    # ═════════════════════════════════════════════════════════════════════════
    # STRATEGY 2: SPLIT BATCH MATCHING (Pacificard)
    # ═════════════════════════════════════════════════════════════════════════

    def _execute_split_batch_matching(self, tolerance: float) -> int:
        """
        Group unmatched bank transactions by batch_number and match to settlements.

        Proportionally allocates voucher amounts across the group. Last row
        absorbs rounding residuals. Copies voucher data only.
        """
        df_bank_splits = self._load_bank_card_splits()
        if df_bank_splits.empty:
            return 0

        df_settlements = self._load_settlements_for_batch()
        if df_settlements.empty:
            return 0

        df_bank_splits['clean_batch_num'] = pd.to_numeric(
            df_bank_splits['clean_batch'],
            errors='coerce'
        )
        df_settlements['clean_batch_num'] = pd.to_numeric(
            df_settlements['clean_batch'],
            errors='coerce'
        )

        occupied_settlements = set()
        matches_found = []

        for _, bank_row in df_bank_splits.iterrows():
            if pd.isna(bank_row.get('distinct_date')):
                continue

            b_batch_num = bank_row['clean_batch_num']
            b_brand = str(bank_row['clean_brand']).upper()
            b_sum = float(bank_row['bank_sum'])
            b_date = pd.to_datetime(bank_row['distinct_date'])

            if pd.isna(b_batch_num):
                continue

            # Filter candidates by batch number and brand
            candidates = df_settlements[
                (df_settlements['clean_batch_num'] == b_batch_num) &
                (df_settlements['clean_brand'].astype(str).str.upper() == b_brand)
            ].copy()

            if candidates.empty:
                continue

            # Exclude already-occupied settlements
            candidates = candidates[
                ~candidates['settlement_id'].astype(str).isin(occupied_settlements)
            ]
            if candidates.empty:
                continue

            # Amount tolerance check
            candidates['diff'] = abs(candidates['sett_net'].astype(float) - b_sum)
            candidates = candidates[candidates['diff'] <= tolerance]
            if candidates.empty:
                continue

            # Date check: settlement must not be more than 1 day after bank date
            candidates = candidates[
                pd.to_datetime(candidates['settlement_date']) <= (b_date + pd.Timedelta(days=1))
            ]
            if candidates.empty:
                continue

            # Take the oldest qualifying candidate
            best = candidates.sort_values('settlement_date').iloc[0]
            matches_found.append({
                'bank_ids': bank_row['bank_ids'],
                'bank_sum': b_sum,
                'sett_data': best
            })
            occupied_settlements.add(str(best['settlement_id']))

        if not matches_found:
            return 0

        total_updated = self._apply_split_updates(matches_found)
        self.logger(f"   Split Batch: {len(matches_found)} groups processed.", "INFO")
        return total_updated

    def _apply_split_updates(self, matches: list) -> int:
        """
        Apply proportional voucher amount updates for split-batch groups.

        Last row in each group absorbs rounding residuals.
        """
        total = 0

        with self.engine_stg.begin() as conn:
            for item in matches:
                ids_list = str(item['bank_ids']).split(',')
                sett = item['sett_data']
                total_bank_net = item['bank_sum']

                if not ids_list:
                    continue

                q = text(
                    "SELECT stg_id, amount_total "
                    "FROM biq_stg.stg_bank_transactions "
                    "WHERE stg_id IN :ids"
                )
                bank_lines = pd.read_sql(q, conn, params={"ids": tuple(ids_list)})

                acum = {'gross': 0.0, 'comm': 0.0, 'iva': 0.0, 'irf': 0.0}

                for i, line in enumerate(bank_lines.itertuples()):
                    is_last = (i == len(bank_lines) - 1)
                    factor = (
                        float(line.amount_total) / total_bank_net
                        if total_bank_net > 0
                        else 0
                    )

                    if is_last:
                        # Last row absorbs rounding residuals
                        new_gross = float(sett['sett_gross']) - acum['gross']
                        new_comm = float(sett['sett_comm']) - acum['comm']
                        new_iva = float(sett['sett_iva']) - acum['iva']
                        new_irf = float(sett['sett_irf']) - acum['irf']
                    else:
                        new_gross = round(float(sett['sett_gross']) * factor, 2)
                        new_comm = round(float(sett['sett_comm']) * factor, 2)
                        new_iva = round(float(sett['sett_iva']) * factor, 2)
                        new_irf = round(float(sett['sett_irf']) * factor, 2)

                    acum['gross'] += new_gross
                    acum['comm'] += new_comm
                    acum['iva'] += new_iva
                    acum['irf'] += new_irf

                    conn.execute(text("""
                        UPDATE biq_stg.stg_bank_transactions
                        SET settlement_id = :sid,
                            establishment_name = :est,
                            final_amount_gross = :gross,
                            final_amount_net = :net,
                            final_amount_commission = :comm,
                            final_amount_tax_iva = :iva,
                            final_amount_tax_irf = :irf
                        WHERE stg_id = :id
                    """), {
                        "sid": sett['settlement_id'],
                        "est": sett['establishment_name'],
                        "gross": new_gross,
                        "net": float(line.amount_total),
                        "comm": new_comm,
                        "iva": new_iva,
                        "irf": new_irf,
                        "id": line.stg_id
                    })
                    total += 1

        return total

    # ═════════════════════════════════════════════════════════════════════════
    # STRATEGY 3: UNIVERSAL ENRICHMENT (Manual Overrides)
    # ═════════════════════════════════════════════════════════════════════════

    def _execute_universal_enrichment(self, tolerance: float) -> int:
        """
        Enrich transactions that have a manually assigned settlement_id but no
        voucher amount fields. Copies voucher data only.
        """
        query = text("""
            SELECT stg_id, settlement_id, amount_total
            FROM biq_stg.stg_bank_transactions
            WHERE settlement_id IS NOT NULL
              AND final_amount_gross IS NULL
              AND trans_type = 'LIQUIDACION TC'
              AND is_compensated_sap = FALSE
              AND is_compensated_intraday = FALSE
        """)

        df_bank = pd.read_sql(query, self.engine_stg)
        if df_bank.empty:
            return 0

        df_sett = self._load_settlements_for_batch()
        if df_sett.empty:
            return 0

        updates = []

        for _, bank_row in df_bank.iterrows():
            sid = str(bank_row.get('settlement_id', '')).strip()

            cands = df_sett[df_sett['settlement_id'].astype(str).str.strip() == sid]
            if cands.empty:
                continue

            sett = cands.iloc[0]
            b_net = float(bank_row.get('amount_total', 0))
            s_net = float(sett.get('sett_net', 0))

            if abs(b_net - s_net) > tolerance:
                continue

            updates.append({
                "stg_id": bank_row['stg_id'],
                "settlement_id": sid,
                "establishment_name": str(sett.get('establishment_name', '')),
                "final_gross": float(sett.get('sett_gross', 0)),
                "final_net": float(sett.get('sett_net', 0)),
                "final_commission": float(sett.get('sett_comm', 0)),
                "final_tax_iva": float(sett.get('sett_iva', 0)),
                "final_tax_irf": float(sett.get('sett_irf', 0))
            })

        if updates:
            self._apply_voucher_data_copy(updates)

        return len(updates)

    # ═════════════════════════════════════════════════════════════════════════
    # DATA LOADERS
    # ═════════════════════════════════════════════════════════════════════════

    def _load_bank_for_hash(self) -> pd.DataFrame:
        """Load open card transactions that have a match_hash_key."""
        query = text("""
            SELECT stg_id, match_hash_key, amount_total
            FROM biq_stg.stg_bank_transactions
            WHERE trans_type = 'LIQUIDACION TC'
              AND settlement_id IS NULL
              AND match_hash_key IS NOT NULL
              AND match_hash_key != ''
              AND is_compensated_sap = FALSE
              AND is_compensated_intraday = FALSE
        """)
        return pd.read_sql(query, self.engine_stg)

    def _load_settlements_for_hash(self) -> pd.DataFrame:
        """Load settlements that have a match_hash_key for hash matching."""
        query = text("""
            SELECT settlement_id, match_hash_key, establishment_name,
                   amount_gross as sett_gross,
                   amount_net as sett_net,
                   amount_commission as sett_comm,
                   amount_tax_iva as sett_iva,
                   amount_tax_irf as sett_irf
            FROM biq_stg.stg_card_settlements
            WHERE match_hash_key IS NOT NULL
              AND match_hash_key != ''
        """)
        return pd.read_sql(query, self.engine_stg)

    def _load_bank_card_splits(self) -> pd.DataFrame:
        """Load grouped bank transactions for split-batch matching (groups with count > 1)."""
        query = text("""
            SELECT STRING_AGG(CAST(stg_id AS TEXT), ',') as bank_ids,
                   TRIM(batch_number) as clean_batch,
                   TRIM(brand) as clean_brand,
                   SUM(amount_total) as bank_sum,
                   MIN(bank_date) as distinct_date
            FROM biq_stg.stg_bank_transactions
            WHERE trans_type = 'LIQUIDACION TC'
              AND settlement_id IS NULL
              AND batch_number IS NOT NULL
              AND TRIM(batch_number) != ''
              AND is_compensated_sap = FALSE
              AND is_compensated_intraday = FALSE
            GROUP BY TRIM(brand), TRIM(batch_number)
            HAVING COUNT(*) > 1
        """)
        return pd.read_sql(query, self.engine_stg)

    def _load_settlements_for_batch(self) -> pd.DataFrame:
        """Load all settlements for batch and universal matching."""
        query = text("""
            SELECT settlement_id, establishment_name,
                   TRIM(batch_number) as clean_batch,
                   TRIM(brand) as clean_brand,
                   settlement_date,
                   amount_gross as sett_gross,
                   amount_net as sett_net,
                   amount_commission as sett_comm,
                   amount_tax_iva as sett_iva,
                   amount_tax_irf as sett_irf
            FROM biq_stg.stg_card_settlements
            ORDER BY settlement_date DESC
        """)
        return pd.read_sql(query, self.engine_stg)

    # ═════════════════════════════════════════════════════════════════════════
    # SHARED UPDATE WRITER
    # ═════════════════════════════════════════════════════════════════════════

    def _apply_voucher_data_copy(self, updates: list):
        """
        Write voucher data to stg_bank_transactions in batch.

        Updates: settlement_id, establishment_name, and all five voucher amount
        fields. Does not touch diff_adjustment or reconcile_reason.

        Args:
            updates: List of dicts with keys: stg_id, settlement_id,
                     establishment_name, final_gross, final_net,
                     final_commission, final_tax_iva, final_tax_irf.
        """
        query = text("""
            UPDATE biq_stg.stg_bank_transactions
            SET settlement_id = :settlement_id,
                establishment_name = :establishment_name,
                final_amount_gross = :final_gross,
                final_amount_net = :final_net,
                final_amount_commission = :final_commission,
                final_amount_tax_iva = :final_tax_iva,
                final_amount_tax_irf = :final_tax_irf
            WHERE stg_id = :stg_id
        """)

        with self.engine_stg.begin() as conn:
            conn.execute(query, updates)