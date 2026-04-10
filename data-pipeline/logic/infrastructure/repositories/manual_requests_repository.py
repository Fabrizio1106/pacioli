"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.repositories.manual_requests_repository
===============================================================================

Description:
    Repository for managing manual requests in the staging layer. It implements
    a TRUNCATE + INSERT strategy to refresh the data completely.

Responsibilities:
    - Replace all existing manual requests with new data from a DataFrame.
    - Truncate the staging table and restart its identity sequence.
    - Perform bulk insertion of records.

Key Components:
    - ManualRequestsRepository: Data access class for the stg_manual_requests table.

Notes:
    - Target Table: biq_stg.stg_manual_requests.
    - Uses PostgreSQL-specific TRUNCATE with RESTART IDENTITY.

Dependencies:
    - pandas
    - sqlalchemy
    - logic.infrastructure.repositories.base_repository

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from logic.infrastructure.repositories.base_repository import BaseRepository
from utils.logger import get_logger


class ManualRequestsRepository(BaseRepository):
    """
    Repository for biq_stg.stg_manual_requests.
    Strategy: TRUNCATE + INSERT (replace-all).
    """

    _SCHEMA     = "biq_stg"
    _TABLE_NAME = "stg_manual_requests"         # name only, for to_sql

    def __init__(self, session: Session):
        # table defined before super().__init__() because the parent might use it
        self.table_name = f"{self._SCHEMA}.{self._TABLE_NAME}"
        super().__init__(session)
        self.logger = get_logger("MANUAL_REQUESTS_REPO")

    def _get_table_name(self) -> str:
        return self.table_name

    def _get_primary_key(self) -> str:
        return "request_id"

    # ─────────────────────────────────────────────────────────────────────────

    def replace_all(self, df: pd.DataFrame) -> int:
        # 1. Validation
        if df.empty:
            self.logger("Empty DataFrame, nothing to save", "WARN")
            return 0

        # 2. Data replacement
        self.logger(f"Replacing ALL data: {len(df)} records", "INFO")

        self._truncate_table()
        rows_inserted = self._bulk_insert(df)

        self.logger(f"{rows_inserted} requests inserted", "SUCCESS")
        return rows_inserted

    def _truncate_table(self):
        # 1. SQL Execution
        try:
            self.logger(f"TRUNCATE {self.table_name}...", "INFO")

            # CHANGE: RESTART IDENTITY restarts the PK SERIAL sequence.
            # In MySQL TRUNCATE does this implicitly;
            # in PostgreSQL it must be explicitly requested.
            # CASCADE covers FKs referencing this table (safe if none).
            self.session.execute(
                text(f"TRUNCATE TABLE {self.table_name} RESTART IDENTITY CASCADE")
            )

            self.logger("Table truncated successfully", "INFO")

        except Exception as e:
            self.logger(f"Error truncating table: {e}", "ERROR")
            raise

    def _bulk_insert(self, df: pd.DataFrame) -> int:
        # 1. Connection and insertion
        try:
            conn = self.session.connection()

            # CHANGE: separate schema kwarg — "schema.table" cannot be passed
            # as table name in to_sql with PostgreSQL.
            df.to_sql(
                name=self._TABLE_NAME,
                con=conn,
                schema=self._SCHEMA,
                if_exists='append',
                index=False,
                chunksize=1000,
            )

            return len(df)

        except Exception as e:
            self.logger(f"Error inserting data: {e}", "ERROR")
            raise
