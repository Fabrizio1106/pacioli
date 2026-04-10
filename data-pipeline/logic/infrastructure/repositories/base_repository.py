"""
===============================================================================
Project: PACIOLI
Module: logic.infrastructure.repositories.base_repository
===============================================================================

Description:
    Base abstract repository providing common data access patterns for the 
    application. It implements the Repository Pattern to decouple business 
    logic from data persistence details.

Responsibilities:
    - Provide standard CRUD operations (get_by_id, get_all, insert, update, delete).
    - Handle batch insertions for performance optimization.
    - Provide utility methods for counting and checking existence of records.
    - Support retrieving data as Pandas DataFrames for analysis.

Key Components:
    - BaseRepository: Abstract base class for all repositories.

Notes:
    - Migrated from MySQL to PostgreSQL.
    - Uses SQLAlchemy Session for database interactions.
    - Requires subclasses to implement _get_table_name and _get_primary_key.
    - Employs RETURNING clauses for retrieving generated IDs in PostgreSQL.

Dependencies:
    - sqlalchemy
    - pandas
    - abc
    - utils.logger

===============================================================================
"""

import re
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text
import pandas as pd
from utils.logger import get_logger


# Allowlist for ORDER BY — permits identifiers, dots, commas, spaces, and
# an optional trailing ASC/DESC. Rejects semicolons, quotes, and subqueries.
_SAFE_ORDER_BY = re.compile(r'^[\w\s,.()\-]+(?:\s+(?:ASC|DESC))?$', re.IGNORECASE)


class BaseRepository(ABC):
    """
    Base class for all repositories.

    PATTERN: Repository Pattern
    --------------------------
    Separates business logic from data access logic.
    """

    def __init__(self, session: Session):
        # 1. Initialization
        self.session = session
        self.logger = get_logger(f"REPO_{self._get_table_name()}")

    # ──────────────────────────────────────────────────────────────────────────
    # ABSTRACT METHODS
    # ──────────────────────────────────────────────────────────────────────────

    @abstractmethod
    def _get_table_name(self) -> str:
        """
        Returns the full name of the table including schema.

        IMPORTANT for PostgreSQL:
        -------------------------
        Always include the schema to avoid search_path ambiguities.
        """
        pass

    @abstractmethod
    def _get_primary_key(self) -> str:
        """
        Returns the name of the primary key field.
        """
        pass

    # ──────────────────────────────────────────────────────────────────────────
    # 1. RETRIEVAL METHODS
    # ──────────────────────────────────────────────────────────────────────────

    def get_by_id(self, id_value: Any) -> Optional[Dict[str, Any]]:
        """
        Retrieves a record by its ID.
        Returns a dict with the record or None if it does not exist.
        """
        table = self._get_table_name()
        pk    = self._get_primary_key()

        query  = text(f"SELECT * FROM {table} WHERE {pk} = :id")
        result = self.session.execute(query, {"id": id_value}).fetchone()

        return dict(result._mapping) if result else None

    def get_all(
        self,
        filters:  Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit:    Optional[int] = None,
        offset:   Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieves records with optional filtering, ordering, and pagination.
        """
        # 1. Query Building
        table      = self._get_table_name()
        query_str  = f"SELECT * FROM {table}"
        params     = {}

        if filters:
            where_clauses = []
            for i, (key, value) in enumerate(filters.items()):
                param_name = f"filter_{i}"
                if any(op in key for op in ['>=', '<=', '!=', '>', '<']):
                    where_clauses.append(f"{key.strip()} :{param_name}")
                else:
                    where_clauses.append(f"{key} = :{param_name}")
                params[param_name] = value
            query_str += " WHERE " + " AND ".join(where_clauses)

        if order_by:
            if not _SAFE_ORDER_BY.match(order_by.strip()):
                raise ValueError(f"Invalid order_by value: {order_by!r}")
            query_str += f" ORDER BY {order_by}"

        if limit is not None:
            query_str      += " LIMIT :_limit"
            params['_limit'] = int(limit)

        if offset is not None:
            query_str       += " OFFSET :_offset"
            params['_offset'] = int(offset)

        # 2. Execution
        results = self.session.execute(text(query_str), params).fetchall()
        return [dict(row._mapping) for row in results]

    # ──────────────────────────────────────────────────────────────────────────
    # 2. INSERTION METHODS
    # ──────────────────────────────────────────────────────────────────────────

    def insert(self, data: Dict[str, Any]) -> int:
        """
        Inserts a new record and returns the generated ID using RETURNING clause.
        """
        table  = self._get_table_name()
        pk     = self._get_primary_key()

        columns      = ", ".join(data.keys())
        placeholders = ", ".join([f":{key}" for key in data.keys()])

        # RETURNING is the correct way in PostgreSQL to retrieve the inserted ID
        query  = text(f"""
            INSERT INTO {table} ({columns})
            VALUES ({placeholders})
            RETURNING {pk}
        """)

        result = self.session.execute(query, data)
        row    = result.fetchone()
        return row[0] if row else None

    def insert_many(self, data_list: List[Dict[str, Any]]) -> int:
        """
        Inserts multiple records in a single batch for performance.
        """
        if not data_list:
            return 0

        # 1. Query Definition
        table        = self._get_table_name()
        columns      = ", ".join(data_list[0].keys())
        placeholders = ", ".join([f":{key}" for key in data_list[0].keys()])

        query = text(f"""
            INSERT INTO {table} ({columns})
            VALUES ({placeholders})
        """)

        # 2. Execution
        self.session.execute(query, data_list)
        return len(data_list)

    # ──────────────────────────────────────────────────────────────────────────
    # 3. UPDATE & DELETE METHODS
    # ──────────────────────────────────────────────────────────────────────────

    def update_by_id(self, id_value: Any, updates: Dict[str, Any]) -> bool:
        """
        Updates specific fields of a record by its ID.
        """
        table = self._get_table_name()
        pk    = self._get_primary_key()

        # 1. Clause Building
        set_clauses = [f"{key} = :{key}" for key in updates.keys()]
        set_str     = ", ".join(set_clauses)

        query  = text(f"UPDATE {table} SET {set_str} WHERE {pk} = :id_value")
        params = {**updates, "id_value": id_value}

        # 2. Execution
        result = self.session.execute(query, params)
        return result.rowcount > 0

    def delete_by_id(self, id_value: Any) -> bool:
        """
        Permanently deletes a record by its ID. Use with caution.
        """
        table  = self._get_table_name()
        pk     = self._get_primary_key()

        query  = text(f"DELETE FROM {table} WHERE {pk} = :id_value")
        result = self.session.execute(query, {"id_value": id_value})
        return result.rowcount > 0

    # ──────────────────────────────────────────────────────────────────────────
    # 4. UTILITY METHODS
    # ──────────────────────────────────────────────────────────────────────────

    def count(self, filters: Optional[Dict[str, Any]] = None) -> int:
        """
        Counts records with optional filtering using positional access for robustness.
        """
        table     = self._get_table_name()
        query_str = f"SELECT COUNT(*) FROM {table}"
        params    = {}

        if filters:
            where_clauses = []
            for i, (key, value) in enumerate(filters.items()):
                param_name = f"filter_{i}"
                where_clauses.append(f"{key} = :{param_name}")
                params[param_name] = value
            query_str += " WHERE " + " AND ".join(where_clauses)

        result = self.session.execute(text(query_str), params).fetchone()

        # Positional access [0] is compatible with PostgreSQL and SQLAlchemy 2.x
        return result[0] if result else 0

    def exists(self, filters: Dict[str, Any]) -> bool:
        """
        Checks if at least one record matches the filters.
        """
        return self.count(filters) > 0

    # ──────────────────────────────────────────────────────────────────────────
    # 5. DATA ANALYSIS METHODS
    # ──────────────────────────────────────────────────────────────────────────

    def get_dataframe(
        self,
        filters: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Retrieves data as a Pandas DataFrame for vectorized processing.
        """
        # 1. Query Building
        table     = self._get_table_name()
        cols      = "*" if not columns else ", ".join(columns)
        query_str = f"SELECT {cols} FROM {table}"
        params    = {}

        if filters:
            where_clauses = []
            for i, (key, value) in enumerate(filters.items()):
                param_name = f"filter_{i}"
                where_clauses.append(f"{key} = :{param_name}")
                params[param_name] = value
            query_str += " WHERE " + " AND ".join(where_clauses)

        # 2. Execution
        return pd.read_sql(
            text(query_str),
            self.session.connection(),
            params=params,
        )
