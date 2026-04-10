"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.transformation.withholdings_transformer
===============================================================================

Description:
    Domain service that transforms raw SRI withholding records into clean,
    classified records ready for staging. Applies withholding-specific business
    rules including invoice series classification and reference generation.

Responsibilities:
    - Clean and normalize all string columns.
    - Normalize customer names: remove legal suffixes, replace accented characters,
      strip punctuation.
    - Clean invoice references: strip common prefixes (FACTURA, FACT, etc.).
    - Normalize boolean fields (rise, rimpe, agente_retencion, etc.).
    - Round numeric withholding amount columns to 2 decimals.
    - Generate withholding_ref in SERIE-SECUENCIAL format.
    - Extract invoice series (e.g., "003-029") from invoice reference strings.
    - Classify records as registrable (series 003-029) or non-registrable.
    - Add ETL metadata columns.

Key Components:
    - WithholdingsTransformer: Domain service; receives a raw DataFrame from
      WithholdingsExtractor and returns a clean, classified DataFrame.

Notes:
    - Only invoice series 003-029 is registrable; all others receive
      reconcile_status = 'NON_REGISTRABLE_SERIES'.
    - withholding_ref format: SERIE-SECUENCIAL (e.g., 001-001-0001234).
    - invoice_series is extracted via regex pattern XXX-XXX from invoice_ref_clean.
    - customer_name_normalized removes CIA., LTDA., S.A., C.A. and accented chars
      to support fuzzy matching in downstream enrichment.

Dependencies:
    - pandas, re, datetime, typing
    - utils.data_cleaner, utils.logger

===============================================================================
"""

import pandas as pd
import re
from datetime import datetime
from typing import Dict, Any
from utils.data_cleaner import DataCleaner
from utils.logger import get_logger


class WithholdingsTransformer:
    """
    Domain service for transforming SRI withholding records.

    Business rules:
        - Only invoice series 003-029 is considered registrable for SAP posting.
          All other series are marked NON_REGISTRABLE_SERIES.
        - Customer names are normalized (accents, legal suffixes removed) to
          support downstream fuzzy matching.
        - withholding_ref uniquely identifies a withholding document:
          format SERIE-SECUENCIAL (e.g., 001-001-0001234).

    All configuration sourced from staging_withholdings_rules.yaml.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Args:
            config: Parsed YAML dict from staging_withholdings_rules.yaml.
        """
        self.config = config
        self.logger = get_logger("WITHHOLDINGS_TRANSFORMER")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw withholding records into a clean staging DataFrame.

        Steps:
            1. Clean all string columns.
            2. Normalize customer names (legal suffixes, accents, punctuation).
            3. Clean invoice references (strip common prefixes).
            4. Normalize boolean fields.
            5. Round numeric withholding columns to 2 decimals.
            6. Generate withholding_ref (SERIE-SECUENCIAL format).
            7. Extract invoice series from cleaned reference.
            8. Classify records by invoice series (registrable vs. non-registrable).
            9. Add ETL metadata columns.

        Args:
            df: Raw DataFrame from WithholdingsExtractor.

        Returns:
            Transformed DataFrame with additional columns including
            customer_name_normalized, invoice_ref_clean, withholding_ref,
            invoice_series, is_registrable, reconcile_status, and metadata fields.
        """
        self.logger(f"Starting transformation of {len(df)} withholdings", "INFO")

        df = df.copy()

        # 1. Clean strings
        df = self._clean_strings(df)

        # 2. Normalize customer names
        df = self._normalize_customer_names(df)

        # 3. Clean invoice references
        df = self._clean_invoice_references(df)

        # 4. Normalize boolean fields
        df = self._normalize_booleans(df)

        # 5. Round numeric columns
        df = self._round_numeric_columns(df)

        # 6. Generate withholding_ref
        df = self._generate_withholding_ref(df)

        # 7. Extract invoice series
        df = self._extract_invoice_series(df)

        # 8. Classify by series
        df = self._classify_by_series(df)

        # 9. Add metadata
        df = self._add_metadata(df)

        self.logger(f"Transformation complete: {len(df)} withholdings", "SUCCESS")

        self._report_statistics(df)

        return df

    def _clean_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean all string columns (trim, uppercase, normalize whitespace)."""
        self.logger("Cleaning strings", "INFO")
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = DataCleaner.clean_string(df[col])
        return df

    def _normalize_customer_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize customer names for downstream fuzzy matching.

        Normalization steps:
            - Uppercase and trim.
            - Remove legal entity suffixes: CIA., LTDA., S.A., C.A.
            - Replace accented characters: N~->N, A'->A, etc.
            - Strip punctuation: . , - ' "
            - Collapse multiple spaces.
        """
        self.logger("Normalizing customer names", "INFO")
        df['customer_name_normalized'] = df['razon_social_emisor'].apply(
            self._normalize_customer_name
        )
        return df

    def _normalize_customer_name(self, name: str) -> str:
        """Normalize a single customer name string."""
        if pd.isna(name):
            return ''

        name = str(name).upper().strip()

        for word in ['CIA.', 'LTDA.', 'S.A.', 'C.A.']:
            name = name.replace(word, '')

        for old, new in {'N\u00d1': 'N', '\u00d1': 'N', '\u00c1': 'A', '\u00c9': 'E',
                         '\u00cd': 'I', '\u00d3': 'O', '\u00da': 'U'}.items():
            name = name.replace(old, new)

        for char in ['.', ',', '-', "'", '"']:
            name = name.replace(char, '')

        name = re.sub(r'\s+', ' ', name).strip()

        return name

    def _clean_invoice_references(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean invoice reference strings by stripping common prefixes.

        Prefixes removed: FACTURA, *FACTURA, FACT.
        Result stored in invoice_ref_clean.
        """
        self.logger("Cleaning invoice references", "INFO")
        df['invoice_ref_clean'] = df['num_comp_sustento'].apply(self._clean_invoice_ref)
        return df

    def _clean_invoice_ref(self, ref: str) -> str:
        """Strip common invoice prefixes from a single reference string."""
        if pd.isna(ref):
            return ''

        ref = str(ref).strip()

        for prefix in ['FACTURA', '*FACTURA', 'FACT']:
            if ref.upper().startswith(prefix):
                ref = ref[len(prefix):].strip()

        return ref

    def _normalize_booleans(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert boolean fields from string/mixed types to Python bool.

        Fields normalized: rise, rimpe, agente_retencion,
        obligado_contabilidad, contribuyente_especial.
        Truthy values: SI, SI, S, TRUE, 1, YES, Y.
        """
        self.logger("Normalizing boolean fields", "INFO")

        bool_fields = [
            'rise', 'rimpe', 'agente_retencion',
            'obligado_contabilidad', 'contribuyente_especial'
        ]

        for field in bool_fields:
            if field in df.columns:
                df[field] = df[field].apply(self._normalize_boolean)

        return df

    def _normalize_boolean(self, value) -> bool:
        """Return True if value represents an affirmative boolean."""
        if pd.isna(value):
            return False
        return str(value).upper().strip() in ['SI', 'S\u00cd', 'S', 'TRUE', '1', 'YES', 'Y']

    def _round_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Round withholding amount columns to 2 decimal places."""
        numeric_cols = [
            'base_ret_renta', 'porcentaje_ret_renta', 'valor_ret_renta',
            'base_ret_iva', 'porcentaje_ret_iva', 'valor_ret_iva'
        ]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).round(2)

        return df

    def _generate_withholding_ref(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate withholding_ref as a unique document identifier.

        Format: SERIE-SECUENCIAL (e.g., 001-001-0001234).
        Sequential number is zero-padded to 7 digits.
        """
        self.logger("Generating withholding_ref", "INFO")

        df['withholding_ref'] = df.apply(
            lambda row: self._build_withholding_ref(
                row.get('serie_comprobante_ret'),
                row.get('num_secuencial_ret')
            ),
            axis=1
        )

        return df

    def _build_withholding_ref(self, serie, secuencial) -> str:
        """Build a withholding reference string from series and sequential number."""
        if pd.isna(serie) or pd.isna(secuencial):
            return ''

        serie_clean = str(serie).strip()
        sec_clean = str(secuencial).strip().zfill(7)

        return f"{serie_clean}-{sec_clean}"

    def _extract_invoice_series(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract the invoice series prefix from the cleaned invoice reference.

        Pattern: XXX-XXX (e.g., "003-029-000123456" -> "003-029").
        """
        self.logger("Extracting invoice series", "INFO")
        df['invoice_series'] = df['invoice_ref_clean'].apply(self._extract_series)
        return df

    def _extract_series(self, ref) -> str:
        """Extract series prefix matching pattern XXX-XXX from a reference string."""
        if pd.isna(ref):
            return ''

        match = re.search(r'(\d{3})-(\d{3})', str(ref))
        if match:
            return f"{match.group(1)}-{match.group(2)}"

        return ''

    def _classify_by_series(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Classify withholdings as registrable or non-registrable based on invoice series.

        Only series 003-029 is registrable (eligible for SAP posting).
        All other series receive reconcile_status = 'NON_REGISTRABLE_SERIES'.
        """
        self.logger("Classifying by invoice series", "INFO")

        df['is_registrable'] = (df['invoice_series'] == '003-029')

        df['reconcile_status'] = df['is_registrable'].apply(
            lambda x: 'NEW' if x else 'NON_REGISTRABLE_SERIES'
        )

        df['validation_status'] = 'PASS'
        df['validation_errors'] = None

        return df

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add ETL metadata columns: etl_version, created_at, match_confidence, eligibility_status."""
        df['etl_version'] = '2.0'
        df['created_at'] = datetime.now()
        df['match_confidence'] = 'UNMATCHED'
        df['eligibility_status'] = 'PENDING'
        df['ineligibility_reasons'] = None
        return df

    def _report_statistics(self, df: pd.DataFrame):
        """Log transformation statistics: total, registrable, non-registrable, top series."""
        total = len(df)
        registrable = df['is_registrable'].sum()
        no_registrable = total - registrable

        self.logger("Transformation statistics:", "INFO")
        self.logger(f"   Total: {total}", "INFO")
        self.logger(f"   Registrable (003-029): {registrable}", "SUCCESS")
        self.logger(f"   Non-registrable: {no_registrable}", "WARN")

        series_counts = df['invoice_series'].value_counts().head(5)
        if not series_counts.empty:
            self.logger("   Top invoice series:", "INFO")
            for serie, count in series_counts.items():
                self.logger(f"     {serie}: {count}", "INFO")
