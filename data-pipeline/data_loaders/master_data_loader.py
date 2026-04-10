"""
===============================================================================
Project: PACIOLI
Module: data_loaders.master_data_loader
===============================================================================

Description:
    Loads the customer master file into the staging schema dimension
    biq_stg.dim_customers. Standalone loader — does not inherit from
    BaseLoader because it targets the staging layer directly.

Responsibilities:
    - Read a customer master file (CSV or Excel).
    - Normalize column names and map source columns into the canonical
      customer_id / customer_name / tax_id schema.
    - Enforce presence of the key columns and uppercase customer names.
    - Refresh biq_stg.dim_customers atomically via TRUNCATE + append.

Key Components:
    - MasterDataLoader: Standalone customer master loader.
    - run: Orchestrates the load of a given master file.

Notes:
    - Writes to the staging schema (biq_stg), not the raw schema.
    - Uses schema-aware to_sql with the 'schema' kwarg to handle
      PostgreSQL qualified names correctly.
    - TRUNCATE uses RESTART IDENTITY CASCADE.

Dependencies:
    - pandas, sqlalchemy.text
    - utils.logger (get_logger)
    - utils.db_config (get_db_engine)
    - utils.text_normalizer (normalize_text)

===============================================================================
"""

# ══════════════════════════════════════════════════════════════════════════════
# MANUAL LOADER — NOT PART OF THE AUTOMATED PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
#
# This loader is NOT called by main_silver_orchestrator.py or any automated
# pipeline step. It must be run MANUALLY by an operator when customer master
# data needs to be refreshed.
#
# HOW TO RUN:
#   From the data-pipeline directory:
#       python data_loaders/master_data_loader.py
#
# WHEN TO RUN:
#   - When new customers are added in SAP.
#   - When existing customer data changes in SAP (name, tax_id, etc.).
#   - Before running the pipeline if dim_customers may be stale.
#
# WHAT IT DOES:
#   Loads the customer dimension data directly into biq_stg.dim_customers,
#   bypassing the raw layer. Performs a full TRUNCATE + reload (not incremental).
# ══════════════════════════════════════════════════════════════════════════════

import pandas as pd
from sqlalchemy import text
from utils.logger import get_logger
from utils.db_config import get_db_engine
from utils.text_normalizer import normalize_text


class MasterDataLoader:
    """
    Standalone loader for the customer master dimension.

    Purpose:
        Refresh biq_stg.dim_customers from an authoritative customer
        master file with a full replace strategy.

    Responsibilities:
        - Read the master file (CSV or Excel).
        - Canonicalize column names (customer_id, customer_name, tax_id).
        - Truncate and refresh biq_stg.dim_customers.
    """

    def __init__(self):
        self.logger = get_logger("MASTER_DATA_LOADER")
        self.engine_stg = get_db_engine('stg')

    def run(self, file_path):
        self.logger(f"Cargando maestro de clientes desde: {file_path}", "INFO")

        try:
            if str(file_path).endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)

            df.columns = [str(c).strip().lower() for c in df.columns]

            rename_map = {
                'cuenta':           'customer_id',
                'cod_cliente':      'customer_id',
                'customer_id':      'customer_id',
                'nombre 1':         'customer_name',
                'nombre':           'customer_name',
                'cliente':          'customer_name',
                'nombre_cliente':   'customer_name',
                'nif':              'tax_id',
                'ruc':              'tax_id',
                'ruc_id':           'tax_id',
            }
            df = df.rename(columns=rename_map)

            if 'customer_id' not in df.columns or 'customer_name' not in df.columns:
                self.logger(
                    f"Columnas faltantes. Encontré: {list(df.columns)}", "ERROR"
                )
                return

            df = df.dropna(subset=['customer_id', 'customer_name'])
            df['customer_id']   = df['customer_id'].astype(str).str.split('.').str[0]
            df['customer_name'] = df['customer_name'].astype(str).str.strip().str.upper()

            if 'tax_id' not in df.columns:
                df['tax_id'] = None

            with self.engine_stg.connect() as conn:
                self.logger("Limpiando tabla maestra anterior...", "INFO")
                conn.execute(
                    text("TRUNCATE TABLE biq_stg.dim_customers RESTART IDENTITY CASCADE")
                )
                conn.commit()

            cols_sql      = ['customer_id', 'customer_name', 'tax_id']
            cols_to_load  = [c for c in cols_sql if c in df.columns]

            df[cols_to_load].to_sql(
                name='dim_customers',
                con=self.engine_stg,
                schema='biq_stg',
                if_exists='append',
                index=False,
            )

            self.logger(f"Maestro cargado: {len(df)} registros.", "SUCCESS")

        except Exception as e:
            self.logger(f"Error cargando clientes: {e}", "ERROR")