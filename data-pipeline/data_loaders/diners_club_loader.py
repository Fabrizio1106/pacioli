"""
===============================================================================
Project: PACIOLI
Module: data_loaders.diners_club_loader
===============================================================================

Description:
    Implements DinersClubLoader for ingesting Diners Club settlement files.
    Enforces strict temporal continuity against the bronze table so that no
    gaps or overlapping records are introduced into the ledger history.

Responsibilities:
    - Read Diners Club Excel settlement reports.
    - Validate temporal continuity: the last DB date must match the file
      and counts at the overlap date must be consistent.
    - Filter the file down to strictly new records beyond the DB cutoff.
    - Normalize voucher/billing/payment dates and monetary columns.
    - Generate a SHA-256 hash id with intra-file duplicate ranking.

Key Components:
    - DinersClubLoader: Concrete BaseLoader implementation.
    - _validate_continuity_and_filter: Rejects incomplete or out-of-sequence
      files and trims records prior to the last loaded date.
    - specific_business_rules: Date/currency normalization and continuity gate.
    - generate_hash_id: Deterministic SHA-256 with duplicate disambiguation.

Notes:
    - Continuity validation is a hard gate: broken sequences raise an error
      and the file is routed to the failed folder.
    - The target_table in the YAML must be schema-qualified (e.g.
      biq_raw.raw_diners_club).

Dependencies:
    - pandas, hashlib, sqlalchemy.text
    - data_loaders.base_loader (BaseLoader)
    - utils.parsers (parse_to_sql_date, parse_currency)

===============================================================================
"""

import pandas as pd
import hashlib
from sqlalchemy import text
from .base_loader import BaseLoader
from utils.parsers import parse_to_sql_date, parse_currency


class DinersClubLoader(BaseLoader):
    """
    Loader for Diners Club settlement Excel files.

    Purpose:
        Ingest Diners settlement reports enforcing strict temporal
        continuity to guarantee an unbroken ledger history in the bronze
        table.

    Responsibilities:
        - Validate temporal continuity against the target table.
        - Filter records to those strictly newer than the last DB date.
        - Normalize dates and an exhaustive set of monetary columns.
        - Produce a deterministic hash id per voucher row.
    """

    def read_file(self, file_path):
        header_row = self.config.get('header_row', 0)
        df = pd.read_excel(file_path, header=header_row, dtype=str)
        return df

    def _validate_continuity_and_filter(self, df):
        """
        Validate temporal continuity against the database and filter out
        records that are already loaded.

        Args:
            df (pd.DataFrame): DataFrame with parsed 'fecha_del_pago'.

        Returns:
            pd.DataFrame: Subset containing only records strictly newer
                          than the last date present in the target table.

        Raises:
            ValueError: If the last DB date is missing from the file
                        (continuity break) or if the count of records at
                        the overlap date is lower in the file than in the
                        database (incomplete overlap).

        Notes:
            The target_table in the YAML must be schema-qualified
            (e.g. biq_raw.raw_diners_club).
        """
        table        = self.config['target_table']
        sql_date_col = "fecha_del_pago"

        query = text(f"""
            SELECT {sql_date_col}, COUNT(*) AS total
            FROM {table}
            WHERE {sql_date_col} = (SELECT MAX({sql_date_col}) FROM {table})
            GROUP BY {sql_date_col}
        """)

        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()

        if not result:
            self.logger("Tabla vacía. Primera carga autorizada.", "INFO")
            return df

        # Positional access — compatible with PostgreSQL
        last_db_date  = result[0]
        last_db_count = result[1]

        if hasattr(last_db_date, 'date'):
            last_db_date = last_db_date.date()

        self.logger(
            f"Punto de Control SQL: {last_db_date} ({last_db_count} registros)", "INFO"
        )

        if sql_date_col not in df.columns:
            raise ValueError(f"Falta columna {sql_date_col} para validar continuidad.")

        file_counts = df[sql_date_col].value_counts().sort_index()

        if last_db_date not in file_counts.index:
            min_file_date = df[sql_date_col].min()
            raise ValueError(
                f" RECHAZADO: Ruptura de Continuidad.\n"
                f"   SQL termina el: {last_db_date}\n"
                f"   Archivo inicia el: {min_file_date}\n"
                f"   Debes cargar un archivo que incluya el {last_db_date}."
            )

        file_count_at_date = file_counts[last_db_date]

        if file_count_at_date < last_db_count:
            raise ValueError(
                f" RECHAZADO: Data Incompleta en el empalme.\n"
                f"   Fecha: {last_db_date}\n"
                f"   Registros en SQL: {last_db_count}\n"
                f"   Registros en Archivo: {file_count_at_date}"
            )

        self.logger(" Validación de Continuidad Exitosa.", "SUCCESS")

        rows_before = len(df)
        df_new      = df[df[sql_date_col] > last_db_date].copy()
        rows_after  = len(df_new)

        if rows_after == 0:
            self.logger(
                " Archivo válido pero sin datos nuevos posteriores al corte.", "WARN"
            )
        else:
            self.logger(
                f"Cortando historia: {rows_before - rows_after} registros ignorados.", "INFO"
            )

        return df_new

    def specific_business_rules(self, df):

        col_id = "ruc_comercio"
        df[col_id] = df[col_id].replace(r'^\s*$', float('nan'), regex=True)
        df = df.dropna(subset=[col_id])

        df['fecha_del_vale']    = df['fecha_del_vale'].apply(parse_to_sql_date)
        df['fecha_facturacion'] = df['fecha_facturacion'].apply(parse_to_sql_date)
        df['fecha_del_pago']    = df['fecha_del_pago'].apply(parse_to_sql_date)
        df = df.dropna(subset=['fecha_del_pago'])

        df = self._validate_continuity_and_filter(df)

        money_cols = [
            'valor_cuota_consumo', 'valor_iva_de_cuota', 'valor_otros_impuestos_cuota',
            'valor_propina_cuota', 'valor_ice_cuota', 'valor_intereses_socio_cuota',
            'valor_bruto_cuota', 'valor_comision_cuota', 'porcentaje_comision',
            'valor_retencion_iva_cuota', 'valor_retencion_irf_cuota', 'valor_pago_cuota',
            'valor_total_consumo', 'valor_total_iva', 'valor_total_otros_impuestos',
            'valor_total_de_la_propina', 'valor_total_del_ice', 'valor_total_bruto',
            'valor_total_comision', 'valor_total_retencion_iva',
            'valor_total_retencion_irf', 'valor_total_pago', 'valor_pendiente_del_vale',
        ]
        for col in money_cols:
            if col in df.columns:
                df[col] = df[col].apply(parse_currency)

        return df

    def generate_hash_id(self, df):

        hash_cols = [
            'ruc_comercio', 'numero_recap_o_lote', 'numero_vale',
            'numero_tarjeta', 'valor_pago_cuota', 'comprobante_de_pago',
            'codigo_unico', 'marca', 'fecha_del_vale',
        ]

        df['base_fingerprint'] = df[hash_cols].astype(str).fillna('').sum(axis=1)
        df['duplicate_rank']   = df.groupby('base_fingerprint').cumcount()
        df['hash_source']      = df['base_fingerprint'] + "_" + df['duplicate_rank'].astype(str)

        df['hash_id'] = df['hash_source'].apply(
            lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest()
        )

        return df.drop(columns=['base_fingerprint', 'duplicate_rank', 'hash_source'])


