"""
===============================================================================
Project: PACIOLI
Module: utils.db_config
===============================================================================

Description:
    Centralized PostgreSQL connection factory for the PACIOLI pipeline.
    Loads credentials from the .env file and returns SQLAlchemy engines
    with the search_path pre-configured to the requested schema layer.

Responsibilities:
    - Load environment variables from .env at project root.
    - Validate the presence of mandatory credentials.
    - Build engine URLs with a schema-qualified search_path.
    - Expose layer-specific engines (config, raw, stg, gold).
    - Provide a self-test entry point for connection diagnostics.

Key Components:
    - get_env_var: Helper that enforces presence of required env vars.
    - get_connection_string: Build a PostgreSQL URL for a schema.
      (Public — used by utils.logger for its singleton engine.)
    - get_db_engine: Factory returning an Engine bound to a layer.
    - get_single_engine: Unbound Engine for cross-schema queries.
    - test_connection: Lightweight connectivity check.

Notes:
    - VERSION 1.1 — changes from v1:
        * get_connection_string is now explicitly part of the public API.
          utils.logger imports it directly to build its singleton DB engine
          with a smaller pool (pool_size=2) instead of inheriting the
          pipeline pool (pool_size=200).
        * pool_size reduced from 200 to 10 per layer engine. 200 connections
          per layer × 4 layers = 800 potential connections for a single run,
          which is excessive for a single-process pipeline. 10 is enough with
          max_overflow=5 for bursts.

    - MySQL → PostgreSQL migration: a single database (pacioli_db) hosts
      every schema. Layer selection is performed via the search_path option
      instead of separate databases.
    - Connection pooling is pre-ping enabled with a one-hour recycle.

Dependencies:
    - os, sys, pathlib
    - python-dotenv
    - sqlalchemy (create_engine, text, Engine)

===============================================================================
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# --- 1. Environment Variables ---
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / '.env'

if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)
else:
    print(f"WARNING: .env not found at {ENV_PATH}")

# --- 2. Helper ---
def get_env_var(var_name: str, default=None, required=False):
    value = os.getenv(var_name, default)
    if required and not value:
        print(f"CRITICAL ERROR: Required variable '{var_name}' is not defined.")
        sys.exit(1)
    return value

# --- 3. Schema names ---
DB_CONFIG_SCHEMA = 'biq_config'
DB_RAW_SCHEMA    = 'biq_raw'
DB_STG_SCHEMA    = 'biq_stg'
DB_GOLD_SCHEMA   = 'biq_gold'

# --- 4. Credentials ---
DB_HOST     = get_env_var("DB_HOST",     required=True)
DB_PORT     = get_env_var("DB_PORT",     default="5432")
DB_USER     = get_env_var("DB_USER",     required=True)
DB_PASSWORD = get_env_var("DB_PASSWORD", required=True)
DB_NAME     = get_env_var("DB_NAME",     default="pacioli_db")

DB_DRIVER = 'postgresql+psycopg2'

# --- 5. Schema → URL map ---
_SCHEMA_MAP = {
    'config': DB_CONFIG_SCHEMA,
    'raw':    DB_RAW_SCHEMA,
    'stg':    DB_STG_SCHEMA,
    'gold':   DB_GOLD_SCHEMA,
}


# =============================================================================
# PUBLIC API
# =============================================================================

def get_connection_string(layer_or_schema: str) -> str:
    """
    Build a PostgreSQL connection string pinned to a given schema.

    Args:
        layer_or_schema (str): Either a layer alias ('config', 'raw', 'stg',
            'gold') or a raw schema name (e.g. 'biq_config'). Layer aliases
            are resolved first; unrecognized values are used as-is so that
            callers can pass a schema name directly.

    Returns:
        str: SQLAlchemy URL with search_path set to the resolved schema.

    Notes:
        This function is part of the public API and is imported by
        utils.logger to build its singleton logging engine with a smaller
        connection pool than the default pipeline engines.
    """
    schema = _SCHEMA_MAP.get(layer_or_schema, layer_or_schema)
    return (
        f"{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        f"?options=-csearch_path%3D{schema},public"
    )


def get_db_engine(layer: str) -> Engine:
    """
    Return a SQLAlchemy Engine pinned to the requested layer's schema.

    Args:
        layer (str): One of 'config', 'raw', 'stg', 'gold'.

    Returns:
        Engine: SQLAlchemy Engine configured with pre-ping pooling and
        the matching search_path.

    Raises:
        ValueError: If the layer is unknown.

    Notes:
        pool_size is 10 (down from 200 in v1). For a single-process
        pipeline, 10 connections per layer is sufficient. The logger
        uses its own singleton engine with pool_size=2.
    """
    if layer not in _SCHEMA_MAP:
        raise ValueError(
            f"Unknown DB layer: '{layer}'. "
            f"Available: {list(_SCHEMA_MAP.keys())}"
        )

    return create_engine(
        get_connection_string(layer),
        echo=False,
        pool_recycle=3600,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=5,
    )


def get_single_engine() -> Engine:
    """
    Return an Engine without a pinned search_path.

    Suitable for cross-schema queries where tables must be fully qualified
    as schema.table within the same session.
    """
    url = (
        f"{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(
        url,
        echo=False,
        pool_recycle=3600,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
    )


def test_connection(layer: str = 'config') -> bool:
    """
    Verify that a PostgreSQL connection for the given layer works.

    Args:
        layer (str): Target layer; defaults to 'config'.

    Returns:
        bool: True when the connection succeeds, False otherwise.
    """
    try:
        engine = get_db_engine(layer)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT current_database(), current_schema()"))
            db, schema = result.fetchone()
            print(f"OK — Database: {db} | Active schema: {schema}")
        return True
    except Exception as e:
        print(f"Connection error on layer '{layer}': {e}")
        return False


# Self-test
if __name__ == "__main__":
    print("Verifying connections to pacioli_db...")
    for layer in ['config', 'raw', 'stg']:
        test_connection(layer)