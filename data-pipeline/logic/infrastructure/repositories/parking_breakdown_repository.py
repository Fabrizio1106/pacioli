"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.repositories.parking_breakdown_repository
===============================================================================

Description:
    Repository for managing parking payment breakdowns in the staging layer. 
    It supports saving breakdowns for specific periods and retrieving summaries.

Responsibilities:
    - Save breakdown data by clearing existing records for a period and inserting new ones.
    - Retrieve a summary of breakdowns grouped by settlement date, brand, and batch.
    - Perform bulk insertion of breakdown records.

Key Components:
    - ParkingBreakdownRepository: Data access class for the stg_parking_pay_breakdown table.

Notes:
    - Target Table: biq_stg.stg_parking_pay_breakdown.
    - Strategy: DELETE by period + INSERT.

Dependencies:
    - pandas
    - sqlalchemy
    - logic.infrastructure.repositories.base_repository

===============================================================================
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import date
from logic.infrastructure.repositories.base_repository import BaseRepository
from utils.logger import get_logger


class ParkingBreakdownRepository(BaseRepository):
    """
    Repository for biq_stg.stg_parking_pay_breakdown.
    Strategy: DELETE by period + INSERT.
    """

    _SCHEMA     = "biq_stg"
    _TABLE_NAME = "stg_parking_pay_breakdown"   # name only, for to_sql

    def __init__(self, session: Session):
        self.table_name = f"{self._SCHEMA}.{self._TABLE_NAME}"
        super().__init__(session)
        self.logger = get_logger("PARKING_BREAKDOWN_REPO")

    def _get_table_name(self) -> str:
        return self.table_name

    def _get_primary_key(self) -> str:
        return "stg_id"

    # ─────────────────────────────────────────────────────────────────────────

    def save_breakdown(
        self,
        df: pd.DataFrame,
        start_date: date,
        end_date: date,
    ) -> int:
        # 1. Validation
        if df.empty:
            self.logger("Breakdown DataFrame is empty, nothing to save", "WARN")
            return 0

        # 2. Data persistence
        self.logger(
            f"Saving breakdown: {len(df)} batches for {start_date} → {end_date}",
            "INFO"
        )

        self._clear_period_breakdown(start_date, end_date)
        rows_inserted = self._bulk_insert(df)

        self.logger(
            f"{rows_inserted} batches inserted for {start_date} → {end_date}",
            "SUCCESS"
        )
        return rows_inserted

    def _clear_period_breakdown(self, start_date: date, end_date: date):
        # 1. Query execution
        delete_query = text("""
            DELETE FROM biq_stg.stg_parking_pay_breakdown
            WHERE settlement_date >= :start_date
              AND settlement_date <= :end_date
        """)

        result = self.session.execute(delete_query, {
            'start_date': start_date,
            'end_date':   end_date,
        })

        self.session.flush()

        self.logger(
            f"Deleted {result.rowcount} batches for period {start_date} → {end_date}",
            "INFO"
        )

    def _bulk_insert(self, df: pd.DataFrame) -> int:
        # 1. Connection and insertion
        conn = self.session.connection()

        # CHANGE: separate schema kwarg
        df.to_sql(
            name=self._TABLE_NAME,
            con=conn,
            schema=self._SCHEMA,
            if_exists='append',
            index=False,
            chunksize=1000,
        )

        return len(df)

    def get_breakdown_summary(
        self,
        start_date: date = None,
        end_date: date = None,
    ) -> pd.DataFrame:
        # 1. Summary retrieval
        conn = self.session.connection()

        # CHANGE: always use text() — previously there was a path where the query
        # was passed as raw string to pd.read_sql, which generates a
        # warning/error in SQLAlchemy 2.x with PostgreSQL.
        if start_date and end_date:
            query = text("""
                SELECT
                    settlement_date,
                    brand,
                    batch_number,
                    COUNT(*)         AS lotes,
                    SUM(amount_net)  AS total_net
                FROM biq_stg.stg_parking_pay_breakdown
                WHERE settlement_date >= :start_date
                  AND settlement_date <= :end_date
                GROUP BY settlement_date, brand, batch_number
                ORDER BY settlement_date, brand, batch_number
            """)
            return pd.read_sql(
                query, conn,
                params={'start_date': start_date, 'end_date': end_date}
            )

        query = text("""
            SELECT
                settlement_date,
                brand,
                batch_number,
                COUNT(*)         AS lotes,
                SUM(amount_net)  AS total_net
            FROM biq_stg.stg_parking_pay_breakdown
            GROUP BY settlement_date, brand, batch_number
            ORDER BY settlement_date, brand, batch_number
        """)
        return pd.read_sql(query, conn)
