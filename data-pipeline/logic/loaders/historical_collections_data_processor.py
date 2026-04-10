"""
===============================================================================
Project: PACIOLI
Module: logic.loaders.historical_collections_data_processor
===============================================================================

Description:
    Processes historical bank collection files to create a clean training 
    dataset for the automated reconciliation engine. It extracts transaction 
    details, customer mappings, and invoice payment associations from 
    legacy Excel files.

Responsibilities:
    - Parse and normalize historical Excel files (XLSX).
    - Map heterogeneous bank columns to a standardized schema.
    - Implement "anti-bleeding" logic to propagate transaction data within 
      logical blocks.
    - Filter out redundant accounting totals and noisy data to ensure 
      high-quality training signals.
    - Load the processed dataset into the staging database.

Key Components:
    - HistoricalDataProcessor: Orchestrates the extraction, cleaning, and 
      loading of historical collection data.

Notes:
    - Relies on specific Excel sheet structures (usually 'COBRO').
    - Uses configuration fingerprints to ensure idempotency where applicable.

Dependencies:
    - pandas
    - numpy
    - sqlalchemy
    - utils.logger
    - utils.db_config
    - utils.parsers

===============================================================================
"""

import pandas as pd
import numpy as np
import os
from sqlalchemy import text
from utils.logger import get_logger
from utils.db_config import get_db_engine
from utils.parsers import parse_bank_datetime


class HistoricalDataProcessor:
    """
    Processor for historical collection data used for training.
    """
    
    def __init__(self):
        """Initializes the processor with required utilities and mappings."""
        self.logger = get_logger("HISTORICAL_TRAINING_LOADER")
        self.engine_stg = get_db_engine('stg')
        
        # Mapping from source Excel columns to standardized database names
        self.column_mapping = {
            # Bank Signals
            'FECHA': 'fecha_transaccion',       
            'FECHA VALIDA': 'fecha_transaccion', 
            'REFERENCIA': 'referencia_bancaria',
            'REFERENCIA2': 'referencia_2',
            'DESCRIPCION': 'descripcion_bancaria',
            'DESCRIPCION BANCARIA': 'descripcion_bancaria',
            'VALOR': 'monto_banco',
            
            # Labels
            'CLIENTE': 'cliente_nombre_manual',
            'COD CLIENTE': 'cliente_cod_manual',
            
            # Behavior
            'FACTURA': 'factura_pagada',
            'VALOR COBRADO': 'valor_cobrado_factura'
        }
        
        # Columns to propagate within the same transaction group (anti-bleeding)
        self.cols_to_propagate = [
            'fecha_transaccion', 
            'referencia_bancaria', 
            'referencia_2',
            'descripcion_bancaria', 
            'monto_banco', 
            'cliente_nombre_manual', 
            'cliente_cod_manual'
        ]

    def run(self, folder_path):
        """
        Main execution loop for processing all files in a folder.
        
        # 1. Initialization
        """
        if not os.path.exists(folder_path):
            self.logger(f"Folder not found: {folder_path}", "ERROR")
            return

        files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx') and not f.startswith('~$')]
        files.sort()
        
        self.logger(f"Processing {len(files)} historical files...", "INFO")
        
        # 2. Processing
        all_data = []
        for file_name in files:
            full_path = os.path.join(folder_path, file_name)
            try:
                df_clean = self._process_single_file(full_path)
                if not df_clean.empty:
                    all_data.append(df_clean)
            except Exception as e:
                self.logger(f"Error in {file_name}: {e}", "ERROR")

        # 3. Validation and Loading
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            self._load_to_sql(final_df)
        else:
            self.logger("No valid data generated.", "WARN")

    def _process_single_file(self, file_path):
        """Processes an individual historical Excel file."""
        try:
            xls = pd.ExcelFile(file_path)
            sheet_name = 'COBRO' if 'COBRO' in xls.sheet_names else xls.sheet_names[0]
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=3)
        except Exception as e:
            self.logger(f"Skipping {os.path.basename(file_path)}: {e}", "WARN")
            return pd.DataFrame()

        # 1. Cleaning: Standardize column names
        df.columns = [str(c).strip().upper() for c in df.columns]

        # 2. Anchor Detection: Identify the start of the data block
        start_col_index = 0
        for i, col in enumerate(df.columns):
            if "FECHA" in col and "FC" not in col: 
                start_col_index = i
                break
        if start_col_index > 0:
            df = df.iloc[:, start_col_index:]

        # 3. Mapping: Apply column mappings safely
        new_data = {}
        processed_targets = set()
        for source_name, target_name in self.column_mapping.items():
            if source_name in df.columns and target_name not in processed_targets:
                new_data[target_name] = df[source_name]
                processed_targets.add(target_name)
        
        df_mapped = pd.DataFrame(new_data)
        if df_mapped.empty: return pd.DataFrame()

        # 4. Parsing: Convert date strings to datetime objects
        if 'fecha_transaccion' in df_mapped.columns:
            df_mapped['fecha_transaccion'] = df_mapped['fecha_transaccion'].apply(parse_bank_datetime)

        # 5. Anti-Bleeding Logic: Propagate transaction details within logical blocks
        if 'fecha_transaccion' in df_mapped.columns:
            # Create a unique ID per transaction group (increment on valid dates)
            df_mapped['trx_group'] = df_mapped['fecha_transaccion'].notna().cumsum()
            
            # Forward-fill within the group only
            for col in self.cols_to_propagate:
                if col in df_mapped.columns:
                    df_mapped[col] = df_mapped.groupby('trx_group')[col].ffill()

        # 6. Numeric Cleaning
        for col in ['monto_banco', 'valor_cobrado_factura']:
            if col in df_mapped.columns:
                df_mapped[col] = pd.to_numeric(df_mapped[col], errors='coerce').fillna(0.0)

        # 7. Filtering: Remove noise and non-training data
        
        # A. Filter by required labels (customer names)
        if 'cliente_nombre_manual' in df_mapped.columns:
            df_mapped = df_mapped.dropna(subset=['cliente_nombre_manual'])
            
            # Remove obvious accounting junk
            keywords_basura = ['TOTAL', 'SALDO', 'COMISIONES', 'INTERES', 'RETENCION', 'RESUMEN']
            mask_basura = df_mapped['cliente_nombre_manual'].str.contains('|'.join(keywords_basura), case=False, na=False)
            df_mapped = df_mapped[~mask_basura]

        # B. Remove "Manual Totals" rows that lack invoice details
        if 'factura_pagada' in df_mapped.columns and 'valor_cobrado_factura' in df_mapped.columns:
            factura_str = df_mapped['factura_pagada'].astype(str).str.strip().replace({'nan': '', 'None': ''})
            
            # Condition: Has value but no invoice ID
            mask_total_manual = (df_mapped['valor_cobrado_factura'].abs() > 0.01) & (factura_str == '')
            df_mapped = df_mapped[~mask_total_manual]

        df_mapped['archivo_origen'] = os.path.basename(file_path)
        
        if 'trx_group' in df_mapped.columns:
            df_mapped = df_mapped.drop(columns=['trx_group'])

        return df_mapped

    def _load_to_sql(self, df):
        """Loads the final cleaned dataset to SQL."""
        table = "stg_historical_collection_training_dataset"
        schema_name = "biq_stg"
        
        expected_cols = [
            'fecha_transaccion', 'referencia_bancaria', 'referencia_2', 'descripcion_bancaria', 
            'monto_banco', 'cliente_nombre_manual', 'cliente_cod_manual', 
            'factura_pagada', 'valor_cobrado_factura', 'archivo_origen'
        ]
        
        # Ensure all expected columns exist
        for col in expected_cols:
            if col not in df.columns: df[col] = None
                
        df = df[expected_cols]

        # 1. Initialization: Clear previous data
        with self.engine_stg.connect() as conn:
            self.logger("Clearing previous historical data...", "WARN")
            conn.execute(text(f"TRUNCATE TABLE {schema_name}.{table}"))
            conn.commit()
            
        # 2. Loading: Bulk insert to SQL
        df.to_sql(table, self.engine_stg, schema=schema_name, if_exists='append', index=False, chunksize=2000)
        self.logger(f"TRAINING DATASET READY: {len(df)} records loaded.", "SUCCESS")
