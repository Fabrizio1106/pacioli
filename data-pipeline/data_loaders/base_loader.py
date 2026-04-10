"""
===============================================================================
Project: PACIOLI
Module: data_loaders.base_loader
===============================================================================

Description:
    Defines the abstract BaseLoader class that standardizes the bronze-layer
    ingestion pipeline. It orchestrates file discovery, batch identification,
    business-rule application, hash generation, temporal continuity checks,
    and atomic load into PostgreSQL staging/target tables.

Responsibilities:
    - Load YAML configuration and initialize logging and database engine.
    - Discover input files and dispatch them through the template-method pipeline.
    - Generate sequential batch identifiers compatible with PostgreSQL.
    - Persist DataFrames into target tables using a staging + upsert pattern.
    - Detect temporal gaps against the database state.
    - Archive processed files into success/failed folders with timestamped names.

Key Components:
    - BaseLoader: Abstract template-method class that concrete loaders extend.
    - load: Entry point that discovers and processes files in the input folder.
    - run_pipeline: Template method orchestrating a single file's lifecycle.
    - load_to_sql: Atomic load via unique staging table + INSERT ON CONFLICT.
    - _get_next_batch_id: PostgreSQL-compatible sequential batch id generator.
    - check_temporal_continuity: Detects day gaps between file and DB state.
    - move_file: Archives the current file into success/failed subfolders.

Notes:
    - VERSION 1.1:
      FIX: load_to_sql now infers explicit SQLAlchemy column types for the
           staging table before calling pandas.to_sql(). Previously, columns
           containing Python `date` or `datetime` objects were written as TEXT
           because the loaders read source files with dtype=str and then apply
           parse_to_sql_date(), leaving the Python objects untyped from
           pandas' perspective. PostgreSQL then rejected the INSERT from the
           staging table with "column is of type date but expression is of
           type text". The fix inspects each column's actual Python values and
           maps them to the appropriate SQLAlchemy type before to_sql().

    - Concrete subclasses must implement read_file, specific_business_rules
      and generate_hash_id.
    - Staging tables are created with a unique timestamp suffix to avoid
      collisions between concurrent executions.
    - The target table must have a PRIMARY KEY or UNIQUE constraint for
      ON CONFLICT DO NOTHING to behave as a deduplication mechanism.
    - Input files with extensions .xlsx, .xls, .msg, .csv and .txt are
      considered candidates. Excel lock files (~$*) are ignored.

Dependencies:
    - abc, os, shutil, datetime, pathlib
    - pandas, yaml, sqlalchemy
    - config.settings (PATHS)
    - utils.logger (get_logger)
    - utils.db_config (get_db_engine)

===============================================================================
"""

from abc import ABC, abstractmethod
import pandas as pd
import yaml
import shutil
import os
from datetime import datetime, date
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.types import (
    Date, DateTime, Text, Numeric, Integer, BigInteger, Boolean
)

from config.settings import PATHS
from utils.logger import get_logger
from utils.db_config import get_db_engine


class BaseLoader(ABC):
    """
    Abstract base class for all bronze-layer data loaders.

    Purpose:
        Provide a reusable template-method skeleton that standardizes the
        ingestion pipeline across heterogeneous sources (bank statements,
        payment processors, SAP exports, webpos files, etc.).

    Responsibilities:
        - Bootstrap configuration, logger and database engine.
        - Resolve input/output directories from a YAML configuration.
        - Iterate over pending files and delegate to the template method.
        - Provide shared infrastructure for batch IDs, SQL load, temporal
          continuity checks and file archival.

    Important Behaviors:
        - Subclasses implement read_file, specific_business_rules and
          generate_hash_id to customize source-specific logic.
        - SQL load is atomic per file: a unique staging table is created,
          data is upserted into the target and the staging table is always
          dropped in a finally block, even on failure.
    """

    def __init__(self, config_yaml_path: str):

        # 1. Config
        try:
            with open(config_yaml_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            raise FileNotFoundError(
                f"No se pudo leer config: {config_yaml_path}. Error: {e}"
            )

        # 2. Logger y DB
        self.logger = get_logger(self.config['loader_name'])
        try:
            self.engine = get_db_engine('raw')
        except Exception as e:
            self.logger(f"Error crítico DB: {e}", "CRITICAL")
            raise

        # 3. Routes
        self.input_dir = PATHS['raw'] / self.config['input_subfolder']

        success_sub      = self.config.get('success_subfolder', 'default_processed')
        self.output_dir  = PATHS['processed'] / "success" / success_sub

        failed_sub       = self.config.get('failed_subfolder', 'default_failed')
        self.failed_dir  = PATHS['processed'] / "failed" / failed_sub

        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.failed_dir, exist_ok=True)

        self.report            = {"total": 0, "loaded": 0, "rejected": 0}
        self.current_file_path = None

    # ─────────────────────────────────────────────────────────────────────────
    # Master method
    # ─────────────────────────────────────────────────────────────────────────

    def load(self) -> int:
        """
        Discover pending files in the input folder and process them.

        Returns:
            int: Total number of rows successfully inserted across all files.

        Notes:
            - Files with extensions .xlsx, .xls, .msg, .csv and .txt are
              considered. Excel lock files (starting with '~$') are skipped.
            - Errors in one file are logged and do not abort the batch.
        """

        total_inserted     = 0
        allowed_extensions = ('.xlsx', '.xls', '.msg', '.csv', '.txt')

        if not self.input_dir.exists():
            self.logger(f"Carpeta no existe: {self.input_dir}", "WARN")
            return 0

        files = [
            f for f in os.listdir(self.input_dir)
            if f.lower().endswith(allowed_extensions)
            and not f.startswith('~$')
        ]
        files.sort()

        if not files:
            self.logger(
                f"Sin archivos pendientes en {self.config['input_subfolder']}",
                "INFO"
            )
            return 0

        self.logger(f"Procesando {len(files)} archivos...", "INFO")

        for file_name in files:
            full_path = self.input_dir / file_name
            try:
                self.run_pipeline(str(full_path))
                total_inserted += self.report.get('loaded', 0)
            except Exception as e:
                self.logger(f"Error procesando {file_name}: {e}", "ERROR")
                continue

        return total_inserted

    # ─────────────────────────────────────────────────────────────────────────
    # BATCH ID
    # ─────────────────────────────────────────────────────────────────────────

    def _get_next_batch_id(self):
        """
        Generate the next sequential batch identifier for the target table.

        Returns:
            str: Batch id in the format '<prefix><n>'. Falls back to a
                 timestamp-based id if the database query fails.
        """

        prefix = self.config.get('batch_prefix', 'BATCH-')
        table  = self.config['target_table']

        sql = text("""
            SELECT MAX(
                CAST(
                    SPLIT_PART(
                        batch_id,
                        :sep,
                        array_length(string_to_array(batch_id, :sep), 1)
                    ) AS INTEGER
                )
            )
            FROM :table_name
            WHERE batch_id LIKE :prefix_like
        """.replace(':table_name', table))

        try:
            with self.engine.connect() as conn:
                max_id = conn.execute(sql, {
                    'sep':         '-',
                    'prefix_like': f"{prefix}%",
                }).scalar()

            next_val = 1 if max_id is None else int(max_id) + 1
            return f"{prefix}{next_val}"

        except Exception as e:
            self.logger(f"Fallback Batch ID por error: {e}", "WARN")
            return f"{prefix}{datetime.now().strftime('%Y%m%d%H%M%S')}"

    # ─────────────────────────────────────────────────────────────────────────
    # Abstract methods
    # ─────────────────────────────────────────────────────────────────────────

    @abstractmethod
    def read_file(self, file_path: str) -> pd.DataFrame: pass

    @abstractmethod
    def specific_business_rules(self, df: pd.DataFrame) -> pd.DataFrame: pass

    @abstractmethod
    def generate_hash_id(self, df: pd.DataFrame) -> pd.DataFrame: pass

    # ─────────────────────────────────────────────────────────────────────────
    # Load to BD
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _infer_sqlalchemy_dtypes(df: pd.DataFrame) -> dict:
        """
        Inspect the actual Python values in each column and return a dict
        of SQLAlchemy types suitable for pandas.to_sql(dtype=...).

        Problem this solves:
            Loaders read source files with dtype=str and then apply
            parse_to_sql_date() which returns Python `date` objects.
            pandas.to_sql() does not automatically detect these as DATE —
            it writes them as TEXT because the column's pandas dtype is
            still `object`. PostgreSQL then rejects the INSERT from the
            staging table with:
                "column is of type date but expression is of type text"

        Strategy:
            For each object-dtype column, sample the first non-null value
            and map it to the appropriate SQLAlchemy type:
                datetime  → DateTime
                date      → Date
                int/float → left to pandas (already numeric dtype)
                other     → Text (safe default)

        Returns:
            dict: {column_name: SQLAlchemy type instance}
        """
        dtype_map = {}
        for col in df.columns:
            if df[col].dtype == object:
                # Sample the first non-null value to detect the Python type
                sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(sample, datetime):
                    dtype_map[col] = DateTime()
                elif isinstance(sample, date):
                    dtype_map[col] = Date()
                else:
                    dtype_map[col] = Text()
        return dtype_map

    def load_to_sql(self, df: pd.DataFrame):
        """
        Atomically load the DataFrame via a unique staging table and
        INSERT ... ON CONFLICT DO NOTHING.

        Args:
            df (pd.DataFrame): DataFrame already transformed by the pipeline.

        Side Effects:
            - Creates a temporary staging table named '_staging_<table>_<ts>'.
            - Inserts non-conflicting rows into the target table.
            - Drops the staging table in a finally block, even on failure.
            - Updates self.report['loaded'] with the number of inserted rows.

        Notes:
            VERSION 1.1 FIX — explicit dtype for staging table:
                pandas.to_sql() infers column types from the pandas dtype,
                not from the Python object type. Object columns that contain
                Python `date` instances are written as TEXT. The fix calls
                _infer_sqlalchemy_dtypes() to build an explicit dtype map
                and passes it to to_sql(dtype=...) so PostgreSQL receives
                the correct DATE/DATETIME type in the staging table.

            PostgreSQL migration notes:
                1. DROP TEMPORARY TABLE → DROP TABLE IF EXISTS. to_sql()
                   creates ordinary tables, not TEMP. A unique timestamp
                   suffix prevents collisions between concurrent runs.
                2. INSERT IGNORE → INSERT ... ON CONFLICT DO NOTHING.
                   Requires the target table to declare a PRIMARY KEY or
                   UNIQUE constraint for conflict detection.
                3. The staging table is always dropped in finally to avoid
                   leftover garbage on error.
        """

        table      = self.config['target_table']
        ts         = datetime.now().strftime('%Y%m%d%H%M%S%f')
        temp_table = f"_staging_{table.replace('.', '_')}_{ts}"

        # Build explicit dtype map before opening the connection
        # so the inspection cost is outside the transaction.
        explicit_dtypes = self._infer_sqlalchemy_dtypes(df)

        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                # 1. Create staging table with correct column types
                df.to_sql(
                    name=temp_table,
                    con=conn,
                    if_exists='replace',
                    index=False,
                    dtype=explicit_dtypes,   # ← FIX: prevents TEXT for date columns
                )

                columns_list = list(df.columns)
                cols_sql     = ", ".join(f'"{c}"' for c in columns_list)
                query = text(f"""
                    INSERT INTO {table} ({cols_sql})
                    SELECT {cols_sql} FROM "{temp_table}"
                    ON CONFLICT DO NOTHING
                """)

                res = conn.execute(query)
                trans.commit()

                self.report['loaded'] = res.rowcount

                if res.rowcount == 0:
                    self.logger("Info: 0 registros nuevos (duplicados).", "INFO")
                else:
                    self.logger(f"Success: {res.rowcount} registros nuevos.", "INFO")

            except Exception as e:
                trans.rollback()
                self.logger(f"Critical SQL Error: {e}", "CRITICAL")
                raise

            finally:
                # Always drop the staging table, even on error
                try:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{temp_table}"'))
                    conn.commit()
                except Exception:
                    pass

    # ─────────────────────────────────────────────────────────────────────────
    # CONTINUIDAD TEMPORAL
    # ─────────────────────────────────────────────────────────────────────────

    def check_temporal_continuity(self, df: pd.DataFrame):
        """
        Detect and warn about temporal gaps between the DataFrame and the
        database state.

        Args:
            df (pd.DataFrame): DataFrame about to be loaded.

        Side Effects:
            Emits a WARN log if there is a gap of more than one day between
            the maximum date stored in the target table and the minimum
            date in the incoming file.

        Notes:
            The date column is resolved from the YAML 'date_columns' list and
            the 'column_mapping' dictionary. Silent return if no date column
            is configured or if the configured column is missing.
        """

        date_cols = self.config.get('date_columns', [])
        if not date_cols:
            return

        raw_col_name = date_cols[0]
        sql_date_col = self.config['column_mapping'].get(raw_col_name, raw_col_name)

        if sql_date_col not in df.columns:
            return

        min_file_date = df[sql_date_col].min()
        if pd.isna(min_file_date):
            return

        try:
            table = self.config['target_table']
            with self.engine.connect() as conn:
                max_db_date = conn.execute(
                    text(f'SELECT MAX("{sql_date_col}") FROM {table}')
                ).scalar()
        except Exception:
            return

        if max_db_date:
            d_db   = max_db_date.date() if hasattr(max_db_date, 'date') else max_db_date
            d_file = min_file_date.date() if hasattr(min_file_date, 'date') else min_file_date

            if d_db and d_file:
                delta = (d_file - d_db).days
                if delta > 1:
                    self.logger(
                        f"GAP DETECTADO: {delta - 1} días desde {d_db}", "WARN"
                    )

    # ─────────────────────────────────────────────────────────────────────────
    # Move files
    # ─────────────────────────────────────────────────────────────────────────

    def move_file(self, is_success: bool):
        """
        Move the currently processed file into the success or failed folder,
        appending a timestamp to the filename and partitioning by year/month.

        Args:
            is_success (bool): True to route to success_subfolder, False to
                               route to failed_subfolder.
        """

        file_path = self.current_file_path
        if not file_path or not os.path.exists(file_path):
            return

        target_root = self.output_dir if is_success else self.failed_dir
        filename    = os.path.basename(file_path)
        name, ext   = os.path.splitext(filename)
        new_name    = f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}"

        dest_folder = (
            target_root
            / datetime.now().strftime("%Y")
            / datetime.now().strftime("%m")
        )
        os.makedirs(dest_folder, exist_ok=True)

        try:
            shutil.move(file_path, dest_folder / new_name)
        except Exception as e:
            self.logger(f"Error moviendo archivo: {e}", "ERROR")

    # ─────────────────────────────────────────────────────────────────────────
    # Template method
    # ─────────────────────────────────────────────────────────────────────────

    def run_pipeline(self, file_path: str):
        """
        Orchestrate the full ingestion lifecycle for a single file.

        Steps:
            1. Read file into a raw DataFrame (subclass).
            2. Enrich with source_file, batch_id and loaded_at columns.
            3. Apply YAML column mapping and subclass business rules.
            4. Generate hash_id for deduplication (subclass).
            5. Check temporal continuity and load into SQL.
            6. Archive the file into success or failed on result.

        Raises:
            Exception: Re-raises any pipeline exception after moving the
                       file to the failed folder and logging the failure.
        """

        file_name = os.path.basename(file_path)
        self.logger(f"--- Iniciando: {file_name} ---", "INFO")
        self.current_file_path = file_path

        try:
            df = self.read_file(file_path)
            df.columns = df.columns.str.strip()

            df['source_file'] = file_name
            df['batch_id']    = self._get_next_batch_id()
            df['loaded_at']   = datetime.now()

            self.report['total'] = len(df)

            df = df.rename(columns=self.config['column_mapping'])
            df = self.specific_business_rules(df)
            df = self.generate_hash_id(df)

            self.check_temporal_continuity(df)
            self.load_to_sql(df)

            self.move_file(is_success=True)

        except Exception as e:
            self.logger(f"Fallo en pipeline: {e}", "CRITICAL")
            self.move_file(is_success=False)
            raise