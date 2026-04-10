"""
===============================================================================
Project: PACIOLI
Module: data_loaders.pacificard_loader
===============================================================================

Description:
    Implements PacificardLoader for ingesting Pacificard settlement files.
    Handles both native Excel files and Outlook .msg attachments: when
    given an email file, the loader extracts the Excel attachment and
    derives the station label from the email subject.

Responsibilities:
    - Accept Excel or .msg inputs transparently.
    - Extract the Excel attachment from .msg emails and tag rows with a
      station label resolved from the configured station_mapping.
    - Drop rows without a 'numero_recap' and normalize payment dates.
    - Normalize all Pacificard-specific monetary fields.
    - Generate a SHA-256 hash id with intra-file duplicate ranking.

Key Components:
    - PacificardLoader: Concrete BaseLoader implementation.
    - read_file: Dual-mode reader supporting .xlsx/.xls and .msg.
    - specific_business_rules: Date/money normalization and filtering.
    - generate_hash_id: SHA-256 fingerprint with duplicate disambiguation.

Notes:
    - The station label is matched against the uppercased email subject;
      if no station code matches, 'SIN ETIQUETA' is assigned.
    - Requires extract_msg for Outlook attachment handling.

Dependencies:
    - pandas, hashlib, io, extract_msg, sqlalchemy.text
    - data_loaders.base_loader (BaseLoader)
    - utils.parsers (parse_to_sql_date, parse_currency)

===============================================================================
"""
import pandas as pd
import hashlib
import extract_msg
import io
from sqlalchemy import text
from .base_loader import BaseLoader
from utils.parsers import parse_to_sql_date, parse_currency


class PacificardLoader(BaseLoader):
    """
    Loader for Pacificard settlement files received as Excel or .msg.

    Purpose:
        Ingest Pacificard settlement reports that may arrive as plain
        Excel files or embedded in Outlook email messages. When the input
        is an email, the loader extracts the Excel attachment and tags
        every row with a station label derived from the email subject.

    Responsibilities:
        - Route by file extension between Excel and .msg handlers.
        - Derive a 'estacion' column from the station_mapping and the
          email subject.
        - Normalize dates and an exhaustive set of monetary columns.
        - Generate a SHA-256 hash id with duplicate disambiguation.
    """

    def read_file(self, file_path):

        if file_path.lower().endswith(('.xlsx', '.xls')):
            return pd.read_excel(file_path, dtype=str)

        try:
            msg     = extract_msg.Message(file_path)
            subject = str(msg.subject).upper()

            mapping  = self.config.get('station_mapping', {})
            estacion = 'SIN ETIQUETA'
            for code, label in mapping.items():
                if code in subject:
                    estacion = label
                    break

            self.logger(f"📧 Asunto: '{subject}' -> Estación: {estacion}", "INFO")

            excel_attachment = None
            for attachment in msg.attachments:
                if attachment.longFilename.lower().endswith(('.xlsx', '.xls')):
                    excel_attachment = attachment
                    break

            if not excel_attachment:
                raise ValueError("El correo no contiene adjuntos Excel (.xlsx/.xls)")

            file_data  = io.BytesIO(excel_attachment.data)
            header_row = self.config.get('header_row', 0)
            df         = pd.read_excel(file_data, header=header_row, dtype=str)
            df['estacion'] = estacion

            msg.close()
            return df

        except Exception as e:
            raise ValueError(f"Error leyendo archivo .msg: {e}")

    def specific_business_rules(self, df):

        col_id = 'numero_recap'
        if col_id in df.columns:
            df[col_id] = df[col_id].replace(r'^\s*$', float('nan'), regex=True)
            df = df.dropna(subset=[col_id])

        if 'fecha_pago' in df.columns:
            df['fecha_pago'] = df['fecha_pago'].apply(parse_to_sql_date)
            df = df.dropna(subset=['fecha_pago'])

        if 'fecha_vigencia_autorizacion' in df.columns:
            df['fecha_vigencia_autorizacion'] = df['fecha_vigencia_autorizacion'].apply(
                parse_to_sql_date
            )

        money_cols = [
            'consumo_tarifa_12_pct', 'consumo_tarifa_0_pct', 'valor_iva', 'valor_ice',
            'valor_no_comisionable', 'valor_transaccion', 'valor_comision', 'retencion_iva',
            'retencion_fuente', 'valor_de_pago', 'cuota_pagada', 'valor_pendiente',
        ]
        for col in money_cols:
            if col in df.columns:
                df[col] = df[col].apply(parse_currency)

        return df

    def generate_hash_id(self, df):

        hash_cols = [
            'estacion', 'fecha_pago', 'numero_recap', 'numero_tarjeta',
            'valor_transaccion', 'numero_sri_autorizacion', 'comprob_de_retencion',
        ]

        df['base_fingerprint'] = df[hash_cols].astype(str).fillna('').sum(axis=1)
        df['duplicate_rank']   = df.groupby('base_fingerprint').cumcount()
        df['hash_source']      = df['base_fingerprint'] + "_" + df['duplicate_rank'].astype(str)

        df['hash_id'] = df['hash_source'].apply(
            lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest()
        )

        return df.drop(columns=['base_fingerprint', 'duplicate_rank', 'hash_source'])


