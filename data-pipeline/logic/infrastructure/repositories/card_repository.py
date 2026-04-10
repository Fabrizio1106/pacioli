"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.repositories.card_repository
===============================================================================

Description:
    Repository for managing card settlements, details, and hash counters 
    within the staging environment. It handles complex insertion logic 
    including hash generation and counter management.

Responsibilities:
    - Save card settlements while ensuring unique hash keys and managing counters.
    - Retrieve and update hash counters for consistent hash generation.
    - Save card details with duplicate prevention based on ETL hashes.
    - Handle bulk insertions into PostgreSQL with schema-specific configurations.

Key Components:
    - CardRepository: Main class for card-related data persistence.

Notes:
    - Uses PostgreSQL-specific syntax like ON CONFLICT DO UPDATE and GREATEST.
    - Employs ANY(:hash_bases) for efficient filtering of multiple hash bases.
    - Inherits from BaseRepository for standard CRUD patterns.

Dependencies:
    - pandas
    - sqlalchemy
    - typing
    - logic.infrastructure.repositories.base_repository
    - utils.logger

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import Set
from logic.infrastructure.repositories.base_repository import BaseRepository
from utils.logger import get_logger


class CardRepository(BaseRepository):
    """
    Repository for card settlements and details.
    TABLES:
        biq_stg.stg_card_settlements
        biq_stg.stg_card_details
        biq_stg.card_hash_counters
    """

    def __init__(self, session: Session):
        # 1. Initialization
        self.table_settlements = "stg_card_settlements"
        self.table_details     = "stg_card_details"
        self.table_counters    = "card_hash_counters"
        self.schema            = "biq_stg"

        super().__init__(session)
        self.logger = get_logger("CARD_REPOSITORY")

    def _get_table_name(self) -> str:
        return f"{self.schema}.{self.table_settlements}"

    def _get_primary_key(self) -> str:
        return "stg_id"

    # ─────────────────────────────────────────────────────────────────────────
    # 1. SETTLEMENT OPERATIONS
    # ─────────────────────────────────────────────────────────────────────────

    def save_settlements(self, df: pd.DataFrame) -> int:
        """
        Saves settlements ensuring unique hash keys and updating global counters.
        """

        if df.empty:
            self.logger("Settlements DataFrame is empty", "WARN")
            return 0

        if 'match_hash_base' not in df.columns:
            raise ValueError("DataFrame must contain 'match_hash_base' column")

        # 1. Preparation
        self.logger(f"Saving settlements: {len(df)} candidates", "INFO")

        # 2. Counter Management
        counters  = self._get_counters_from_table(df)
        df        = self._apply_counters_to_settlements(df, counters)

        # 3. Duplicate Prevention
        existing_hashes = self._get_existing_hashes(
            f"{self.schema}.{self.table_settlements}", 'etl_hash'
        )

        df_new = df[~df['etl_hash'].isin(existing_hashes)].copy()

        if df_new.empty:
            self.logger("All settlements already exist (duplicates)", "WARN")
            return 0

        # 4. Persistence
        df_for_counter_update = df_new[['match_hash_base', 'match_hash_key']].copy()
        df_new = df_new.drop(columns=['match_hash_base'], errors='ignore')

        rows_inserted = self._bulk_insert(df_new, self.table_settlements)
        self._update_hash_counters(df_for_counter_update)

        self.logger(f"Inserted {rows_inserted} settlements", "SUCCESS")
        return rows_inserted

    # ─────────────────────────────────────────────────────────────────────────
    # 2. HASH COUNTER MANAGEMENT
    # ─────────────────────────────────────────────────────────────────────────

    def _get_counters_from_table(self, df: pd.DataFrame) -> dict:
        """
        Retrieves the latest counters for the given hash bases.
        """

        unique_bases = df['match_hash_base'].unique().tolist()

        if not unique_bases:
            return {}

        self.logger(
            f"Querying card_hash_counters: {len(unique_bases)} hash_bases",
            "INFO"
        )

        try:
            # 1. Query Definition
            # ANY(:hash_bases) with a list is idiomatic in PostgreSQL
            query = text("""
                SELECT hash_base, last_counter
                FROM biq_stg.card_hash_counters
                WHERE hash_base = ANY(:hash_bases)
            """)

            # 2. Execution
            conn   = self.session.connection()
            result = conn.execute(query, {'hash_bases': unique_bases}).fetchall()

            counters = {row[0]: row[1] for row in result}

            self.logger(
                f"Retrieved {len(counters)} counters ({len(unique_bases) - len(counters)} new)",
                "SUCCESS"
            )

            return counters

        except Exception as e:
            self.logger(
                f"Error querying card_hash_counters: {e}. Using counter=0",
                "WARN"
            )
            return {}

    def _apply_counters_to_settlements(self, df: pd.DataFrame, counters: dict) -> pd.DataFrame:
        """
        Applies counters to generate unique match_hash_keys.
        """

        # 1. Ranking and Counter Mapping
        df = df.copy()
        df['rank_local']    = df.groupby('match_hash_base').cumcount() + 1
        df['last_counter']  = df['match_hash_base'].map(counters).fillna(0).astype(int)
        df['counter_global'] = df['last_counter'] + df['rank_local']
        
        # 2. Hash Key Generation
        df['match_hash_key'] = (
            df['match_hash_base'] + "_" + df['counter_global'].astype(str)
        )

        self.logger(
            f"Generated {df['match_hash_key'].nunique()} unique hash_keys",
            "INFO"
        )

        # 3. Cleanup
        df = df.drop(columns=['rank_local', 'last_counter', 'counter_global'], errors='ignore')
        return df

    def _update_hash_counters(self, df: pd.DataFrame):
        """
        Updates global hash counters using PostgreSQL's ON CONFLICT logic.
        """

        # 1. Data Aggregation
        df['counter_extracted'] = (
            df['match_hash_key'].str.split('_').str[-1].astype(int)
        )

        updates = (
            df.groupby('match_hash_base')['counter_extracted']
            .max()
            .reset_index()
        )

        # 2. Execution
        conn = self.session.connection()

        for _, row in updates.iterrows():
            # ON CONFLICT DO UPDATE (PostgreSQL) replaces MySQL's ON DUPLICATE KEY UPDATE
            upsert_query = text("""
                INSERT INTO biq_stg.card_hash_counters (hash_base, last_counter, last_updated_at)
                VALUES (:hash_base, :counter, NOW())
                ON CONFLICT (hash_base) DO UPDATE
                    SET last_counter    = GREATEST(
                                            biq_stg.card_hash_counters.last_counter,
                                            EXCLUDED.last_counter
                                          ),
                        last_updated_at = NOW()
            """)

            conn.execute(upsert_query, {
                'hash_base': row['match_hash_base'],
                'counter':   int(row['counter_extracted']),
            })

        self.logger(
            f"Updated card_hash_counters for {len(updates)} hash_bases",
            "SUCCESS"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # 3. CARD DETAIL OPERATIONS
    # ─────────────────────────────────────────────────────────────────────────

    def save_details(self, df: pd.DataFrame) -> int:
        """
        Saves card details preventing duplicates based on ETL hashes.
        """

        if df.empty:
            self.logger("Details DataFrame is empty", "WARN")
            return 0

        # 1. Preparation
        self.logger(f"Saving details: {len(df)} candidates", "INFO")

        # 2. Duplicate Prevention
        existing_hashes = self._get_existing_hashes(
            f"{self.schema}.{self.table_details}", 'etl_hash'
        )

        df_new = df[~df['etl_hash'].isin(existing_hashes)].copy()

        if df_new.empty:
            self.logger("All details already exist (duplicates)", "WARN")
            return 0

        # 3. Persistence
        rows_inserted = self._bulk_insert(df_new, self.table_details)
        self.logger(f"Inserted {rows_inserted} details", "SUCCESS")
        return rows_inserted

    # ─────────────────────────────────────────────────────────────────────────
    # 4. HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    def _get_existing_hashes(self, full_table_name: str, hash_column: str) -> Set[str]:
        """
        Retrieves a set of existing hashes for a given table and column.
        """

        try:
            conn  = self.session.connection()
            query = text(f"SELECT {hash_column} FROM {full_table_name}")
            df    = pd.read_sql(query, conn)
            hashes = set(df[hash_column].dropna())
            if hashes:
                self.logger(
                    f"Found {len(hashes)} existing hashes in {full_table_name}", "INFO"
                )
            return hashes
        except Exception as e:
            self.logger(f"Could not read {full_table_name}: {e}", "WARN")
            return set()

    def _bulk_insert(self, df: pd.DataFrame, table_name: str) -> int:
        """
        Performs a bulk insertion using the schema kwarg for PostgreSQL compatibility.
        """
        conn = self.session.connection()

        df.to_sql(
            name=table_name,
            con=conn,
            schema=self.schema,
            if_exists='append',
            index=False,
            chunksize=1000,
        )

        return len(df)
