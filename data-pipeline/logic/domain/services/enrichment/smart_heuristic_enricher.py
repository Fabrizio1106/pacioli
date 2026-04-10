"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.enrichment.smart_heuristic_enricher
===============================================================================

Description:
    Phase 3 enricher — last resort. Only receives transactions that were NOT
    matched by Phase 0 (Manual Requests), Phase 1 (Cards), Phase 2 (Text
    Rules), or Phase 4/5 (Cash/Settlements). Assigns customer identity to
    transfer-type transactions using fuzzy matching against a validated
    historical dataset and the SAP customer master.

Responsibilities:
    - Classify bank_ref_2 (or bank_ref_1 as fallback) as ACCOUNT_NUMBER,
      CLIENT_NAME, or AMBIGUOUS.
    - Search the historical collection dataset by exact reference number.
    - Search the historical dataset by fuzzy name match against customer_name.
    - Search the historical dataset by fuzzy match against referencia_2
      (the bank's own reference field — often a truncated name).
    - Fall back to SAP customer master fuzzy name match.
    - Use adaptive threshold for short/truncated names (< 25 chars → 75).
    - Fall back to difflib.SequenceMatcher when rapidfuzz is not installed.
    - Pre-compute normalized search indexes before the main loop.
    - Write enrichment results back to stg_bank_transactions in batch.

Key Components:
    - SmartHeuristicEnricher: Phase 3 enricher.

Notes:
    - VERSION 2.0 changes from v1:

      [FIX 1] difflib fallback:
          rapidfuzz is optional but was the ONLY matching engine. When not
          installed, ALL CLIENT_NAME lookups returned None silently — the
          enricher was a complete no-op for name-based matching. Now
          difflib.SequenceMatcher (stdlib) is used as fallback. Slower
          (~3x) but functional. Install rapidfuzz for production speed.

      [FIX 2] bank_ref_1 as secondary source:
          When bank_ref_2 is NULL or empty, the enricher now tries bank_ref_1
          as the search term. This captures cases like UNITED AIRLINES where
          the customer name appears only in bank_ref_1.

      [FIX 3] referencia_2 index in historical dataset:
          The historical dataset's referencia_2 column (which often contains
          truncated bank-assigned names like "MERAMEXAIR SA LOCAL") is now
          indexed and searched separately. A transaction with bank_ref_2 =
          "MERAMEXAIR SA" can match historical referencia_2 = "MERAMEXAIR SA
          LOCAL" with ~85 score — below the name threshold but above the
          ref2 threshold of 70.

      [FIX 4] Adaptive threshold for truncated names:
          Bank systems often truncate names to 20-30 characters. A truncated
          query can never reach 90 similarity against the full name. When the
          search term is < 25 characters, the threshold is automatically
          lowered to 75, and a prefix-match bonus is applied.

      [FIX 5] cliente_cod_manual float cleanup:
          When pandas reads Excel columns with NULLs, integer IDs become
          floats ("400031.0"). customer_id values are now cleaned to strip
          the ".0" suffix before returning matches.

    - Source priority (unchanged):
          historical exact ref (98) > historical fuzzy name (90/75) >
          historical fuzzy ref2 (70) > SAP master fuzzy (88/75).

Dependencies:
    - re, unicodedata, difflib (stdlib)
    - pandas
    - sqlalchemy
    - rapidfuzz (optional — falls back to difflib)
    - utils.logger

===============================================================================
"""

import re
import unicodedata
import difflib
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from utils.logger import get_logger

try:
    from rapidfuzz import fuzz, process as rfprocess
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False


# ---------------------------------------------------------------------------
# Fuzzy matching helpers — unified API over rapidfuzz / difflib
# ---------------------------------------------------------------------------

def _fuzzy_score(a: str, b: str) -> float:
    """
    Compute a 0-100 similarity score between two strings.

    Uses rapidfuzz.fuzz.WRatio when available (faster, better for partial
    matches), otherwise falls back to difflib.SequenceMatcher * 100.
    Both return values in the same 0-100 range.
    """
    if not a or not b:
        return 0.0
    if RAPIDFUZZ_AVAILABLE:
        return fuzz.WRatio(a, b)
    return difflib.SequenceMatcher(None, a, b).ratio() * 100


def _fuzzy_extract_one(
    query: str,
    candidates: list,
    threshold: float,
) -> tuple:
    """
    Find the best match for `query` in `candidates` above `threshold`.

    Returns:
        (matched_string, score, list_index) or None if no match found.

    Uses rapidfuzz.process.extractOne when available, otherwise iterates
    with difflib — same semantics, different speed.
    """
    if not candidates:
        return None

    if RAPIDFUZZ_AVAILABLE:
        result = rfprocess.extractOne(
            query, candidates,
            scorer=fuzz.WRatio,
            score_cutoff=threshold,
        )
        return result   # (match, score, index) or None

    # difflib fallback — O(n) but acceptable for ≤ 20k candidates
    best_score = threshold - 1
    best_idx   = -1
    for i, cand in enumerate(candidates):
        score = difflib.SequenceMatcher(None, query, cand).ratio() * 100
        if score > best_score:
            best_score = score
            best_idx   = i

    if best_idx == -1:
        return None
    return (candidates[best_idx], best_score, best_idx)


def _clean_customer_id(raw_id) -> str:
    """
    Normalize a customer_id that may have been stored as a float.

    When pandas reads an Excel column with NULLs the integer IDs become
    floats ("400031.0"). Strip the ".0" suffix so downstream systems
    receive "400031".
    """
    s = str(raw_id).strip()
    if s.endswith('.0'):
        s = s[:-2]
    return s


class SmartHeuristicEnricher:
    """
    Phase 3 enricher: smart heuristic matching with cross-validation.

    Called only for transactions that survived all previous enrichment
    phases without a customer assignment.

    Internal pipeline per transaction:
        1. Determine search term: bank_ref_2, else bank_ref_1.
        2. Classify term (ACCOUNT_NUMBER / CLIENT_NAME / AMBIGUOUS).
        3. Search:
           - ACCOUNT_NUMBER → exact lookup in historical dataset.
           - CLIENT_NAME    → fuzzy name lookup (historical → SAP master),
                              then fuzzy ref2 lookup in historical.
        4. Write winning result.
    """

    # Threshold for reference_2 index search (lower — ref2 is often truncated)
    _REF2_THRESHOLD        = 70
    # Threshold for fuzzy name search (standard)
    _NAME_THRESHOLD_FULL   = 90
    # Threshold when search term is short (likely truncated by bank)
    _NAME_THRESHOLD_SHORT  = 75
    # Names shorter than this character count are treated as potentially truncated
    _TRUNCATION_CHAR_LIMIT = 25

    def __init__(self, engine_stg: Engine, engine_raw: Engine, config: dict):
        self.engine_stg = engine_stg
        self.engine_raw = engine_raw
        self.config     = config
        self.logger     = get_logger("HEURISTIC_ENRICHER")

        self.legal_entities = {
            'S.A.S.': 'SAS', 'S.A.': 'SA', 'CIA. LTDA.': 'CIALTDA',
            'CIA LTDA': 'CIALTDA', 'CIA.LTDA': 'CIALTDA',
            'COMPAÑIA LIMITADA': 'CIALTDA', 'SOCIEDAD ANONIMA': 'SA',
            'DEL': '', 'DE LA': '', 'DE LOS': '', 'DE LAS': '',
        }

        self.exclusion_patterns = [
            'FIDEICOMISO MERC', 'PAGO PROVEEDOR DE PRODUBANCO',
            'NOTA DE CREDITO', 'RETENCION', 'AJUSTE CONTABLE',
        ]

        self.garbage_patterns = [
            'CREDENCIALES', 'SEMINARIOS', 'COV', 'PENDIENTE',
            'COMISION', 'UTILIDADES', 'RETENCION', 'AJUSTE',
            'NOTA DE', 'DEPOSITO', 'RETIRO', 'CARGO', 'ABONO',
        ]

        # Known bank / intermediary names — when bank_ref_2 matches one of
        # these, the customer name is in bank_ref_1 instead.
        self.known_intermediaries = [
            'BANCO GENERAL RUMINAHUI', 'BANCO DINERS CLUB',
            'COOPERATIVA DE AHORRO', 'BANCO PICHINCHA',
            'BANCO PACIFICO', 'BANCO GUAYAQUIL', 'BANCO BOLIVARIANO',
            'BANCO INTERNACIONAL', 'BANCO PRODUBANCO',
            'BANCO DEL AUSTRO', 'BANCO SOLIDARIO',
        ]

        if not RAPIDFUZZ_AVAILABLE:
            self.logger(
                "rapidfuzz not available — using difflib fallback. "
                "Install rapidfuzz for better performance: pip install rapidfuzz",
                "WARN"
            )

    # =========================================================================
    # ENTRY POINT
    # =========================================================================

    def enrich(self, engine_stg: Engine) -> int:
        """
        Run Phase 3 heuristic enrichment.

        Returns:
            Number of transactions updated.
        """
        self.logger("Starting Smart Heuristic Enrichment...", "INFO")

        heuristic_config = self.config.get('heuristic_rules', {})
        target_types     = heuristic_config.get('target_trans_types', [
            'TRANSFERENCIA DIRECTA', 'TRANSFERENCIA SPI',
            'OTROS', 'TRANSFERENCIA EXTERIOR',
        ])
        thresholds = heuristic_config.get('thresholds', {})

        df_pending = self._load_pending_transfers(target_types)

        if df_pending.empty:
            self.logger("No pending transfer transactions found.", "INFO")
            return 0

        self.logger(f"{len(df_pending)} transfers to process.", "INFO")

        df_historical = self._load_historical_data()
        df_master     = self._load_sap_master()

        # ── Pre-compute search indexes ────────────────────────────────────────
        # customer_name index (primary fuzzy target)
        hist_names, hist_name_idx = [], []
        if not df_historical.empty:
            hist_names, hist_name_idx = self._build_search_index(
                df_historical, 'customer_name'
            )
            self.logger(f"Historical index: {len(hist_names)} entries.", "INFO")

        # referencia_2 index (secondary — often truncated bank names)
        hist_ref2, hist_ref2_idx = [], []
        if not df_historical.empty and 'bank_ref_2' in df_historical.columns:
            hist_ref2, hist_ref2_idx = self._build_search_index(
                df_historical, 'bank_ref_2'
            )
            self.logger(f"Historical ref2 index: {len(hist_ref2)} entries.", "INFO")

        master_names, master_idx = [], []
        if not df_master.empty:
            master_names, master_idx = self._build_search_index(
                df_master, 'customer_name'
            )
            self.logger(f"SAP master index: {len(master_names)} entries.", "INFO")

        # ── Main enrichment loop ──────────────────────────────────────────────
        updates       = []
        progress_step = max(1, len(df_pending) // 5)

        for i, (_, row) in enumerate(df_pending.iterrows()):

            # FIX 2: Use bank_ref_1 as fallback when bank_ref_2 is empty or
            # contains a known intermediary bank name (not the real customer).
            bank_ref_2 = str(row.get('bank_ref_2', '') or '').strip()
            bank_ref_1 = str(row.get('bank_ref_1', '') or '').strip()

            search_term = self._resolve_search_term(bank_ref_2, bank_ref_1)

            if not search_term:
                continue

            if self._should_exclude(search_term):
                continue

            ref_type, ref_clean = self._classify_bank_ref_2(search_term)

            if ref_type == 'AMBIGUOUS':
                continue

            result = None

            if ref_type == 'ACCOUNT_NUMBER':
                result = self._search_historical_by_number(
                    ref_clean, df_historical, thresholds
                )

            elif ref_type == 'CLIENT_NAME':
                # FIX 4: Adaptive threshold for short/truncated names
                is_truncated    = len(ref_clean) < self._TRUNCATION_CHAR_LIMIT
                name_threshold  = (
                    self._NAME_THRESHOLD_SHORT if is_truncated
                    else self._NAME_THRESHOLD_FULL
                )

                # Primary: fuzzy match against historical customer_name
                result = self._search_historical_by_name(
                    ref_clean, df_historical, thresholds,
                    hist_names, hist_name_idx,
                    override_threshold=name_threshold,
                )

                # Secondary: fuzzy match against historical referencia_2
                # FIX 3: catches truncated ref2 entries ("MERAMEXAIR SA LOCAL")
                if not result:
                    result = self._search_historical_by_ref2(
                        ref_clean, df_historical,
                        hist_ref2, hist_ref2_idx,
                    )

                # Tertiary: SAP customer master
                if not result:
                    result = self._search_master_by_name(
                        ref_clean, df_master, thresholds,
                        master_names, master_idx,
                        override_threshold=name_threshold,
                    )

            if result:
                updates.append({
                    "stg_id":        row['stg_id'],
                    "customer_id":   result['customer_id'],
                    "customer_name": result['customer_name'],
                    "confidence":    result['confidence'],
                    "method":        result['method'],
                    "notes":         result.get('notes'),
                })

            if (i + 1) % progress_step == 0:
                pct = (i + 1) / len(df_pending) * 100
                self.logger(
                    f"   {i+1}/{len(df_pending)} processed ({pct:.0f}%)...",
                    "INFO"
                )

        if not updates:
            self.logger("No heuristic matches found.", "INFO")
            return 0

        self._apply_updates(updates)
        self.logger(
            f"{len(updates)} transactions enriched via heuristics.", "SUCCESS"
        )
        return len(updates)

    # =========================================================================
    # SEARCH TERM RESOLUTION
    # =========================================================================

    def _resolve_search_term(self, bank_ref_2: str, bank_ref_1: str) -> str:
        """
        Determine the best search term for a transaction.

        Priority:
            1. bank_ref_2 if non-empty and not a known intermediary bank.
            2. bank_ref_1 as fallback (covers cases like UNITED AIRLINES
               where the customer name appears only in bank_ref_1).

        Returns the chosen term, or empty string if neither is usable.
        """
        if bank_ref_2:
            upper = bank_ref_2.upper()
            is_intermediary = any(b in upper for b in self.known_intermediaries)
            if not is_intermediary:
                return bank_ref_2

        # Fall back to bank_ref_1
        if bank_ref_1:
            # Skip pure numeric bank_ref_1 values that are transaction IDs,
            # not customer names (e.g. "22089139-417" — these are handled
            # by ACCOUNT_NUMBER path only if they appear in bank_ref_2)
            has_letters = any(c.isalpha() for c in bank_ref_1)
            if has_letters:
                return bank_ref_1

        return bank_ref_2   # return original even if intermediary — caller decides

    # =========================================================================
    # DATA LOADERS
    # =========================================================================

    def _load_pending_transfers(self, target_types: list) -> pd.DataFrame:
        """Load transfer transactions pending enrichment from Phase 3."""
        placeholders = ", ".join([f":type_{i}" for i in range(len(target_types))])
        params       = {f"type_{i}": t for i, t in enumerate(target_types)}

        # FIX 2: also load bank_ref_1 so it can be used as fallback source
        query = text(f"""
            SELECT stg_id, bank_ref_1, bank_ref_2, trans_type
            FROM biq_stg.stg_bank_transactions
            WHERE trans_type IN ({placeholders})
              AND (enrich_confidence_score IS NULL
                   OR enrich_confidence_score < 99)
        """)
        return pd.read_sql(query, self.engine_stg, params=params)

    def _load_historical_data(self) -> pd.DataFrame:
        """
        Load the validated historical reference-to-customer mapping dataset.

        Column aliases:
            referencia_bancaria   → bank_ref
            cliente_cod_manual    → customer_id
            cliente_nombre_manual → customer_name
            referencia_2          → bank_ref_2  (used for ref2 index)
        """
        try:
            # Removed ORDER BY created_at — order is irrelevant for matching
            # and avoids potential issues with timestamp format variations.
            query = text("""
                SELECT
                    referencia_bancaria   AS bank_ref,
                    referencia_2          AS bank_ref_2,
                    cliente_cod_manual    AS customer_id,
                    cliente_nombre_manual AS customer_name
                FROM biq_stg.stg_historical_collection_training_dataset
                WHERE cliente_cod_manual IS NOT NULL
                  AND cliente_cod_manual != ''
                  AND cliente_nombre_manual IS NOT NULL
                  AND cliente_nombre_manual NOT IN ('', 'N/A', 'UNKNOWN', 'RETENCIÓN')
            """)
            df = pd.read_sql(query, self.engine_stg)
            self.logger(f"Historical dataset loaded: {len(df)} records.", "INFO")
            return df
        except Exception as e:
            self.logger(f"Error loading historical dataset: {e}", "WARN")
            return pd.DataFrame()

    def _load_sap_master(self) -> pd.DataFrame:
        """Load the active SAP customer master."""
        try:
            query = text("""
                SELECT customer_id, customer_name, tax_id
                FROM biq_stg.dim_customers
                WHERE is_active = TRUE
                ORDER BY customer_name
            """)
            df = pd.read_sql(query, self.engine_stg)
            self.logger(f"SAP master loaded: {len(df)} customers.", "INFO")
            return df
        except Exception as e:
            self.logger(f"Error loading SAP master: {e}", "WARN")
            return pd.DataFrame()

    # =========================================================================
    # INDEX BUILDER
    # =========================================================================

    def _build_search_index(self, df: pd.DataFrame, name_col: str) -> tuple:
        """
        Pre-compute a normalized name list for fuzzy matching.

        Called once before the main loop to avoid re-normalizing the same
        candidate names for each transaction.

        Returns:
            (normalized_names, df_row_indices)
        """
        names, indices = [], []
        for idx, row in df.iterrows():
            raw  = row.get(name_col, '')
            if not raw or str(raw).strip() in ('', 'nan', 'None', 'N/A'):
                continue
            norm = self._normalize_text(str(raw), remove_legal=True)
            if norm and len(norm) >= 4:
                names.append(norm)
                indices.append(idx)
        return names, indices

    # =========================================================================
    # SEARCH STRATEGIES
    # =========================================================================

    def _search_historical_by_number(
        self,
        ref: str,
        df_historical: pd.DataFrame,
        thresholds: dict,
    ) -> dict:
        """
        Exact-reference lookup in the historical dataset.

        Searches bank_ref first; falls back to bank_ref_2.
        Also tries stripping formatting (dashes, dots) from the reference
        to handle format mismatches between bank and historical data.
        """
        if df_historical.empty:
            return None

        threshold = thresholds.get('historical_exact_match', 98)

        # Normalize the reference for comparison (strip separators)
        ref_norm = re.sub(r'[\-\.]', '', ref).strip()

        def _match_ref_col(col_name: str, confidence_penalty: int = 0):
            if col_name not in df_historical.columns:
                return None
            col = df_historical[col_name].astype(str).str.strip()
            # Exact match
            match = df_historical[col == ref]
            if match.empty:
                # Try normalized (no separators)
                col_norm = col.str.replace(r'[\-\.]', '', regex=True)
                match    = df_historical[col_norm == ref_norm]
            if not match.empty:
                row = match.iloc[0]
                return {
                    'customer_id':   _clean_customer_id(row['customer_id']),
                    'customer_name': str(row['customer_name']),
                    'confidence':    threshold - confidence_penalty,
                    'method':        f'HISTORICAL_{col_name.upper()}_MATCH',
                    'notes':         f"Exact reference: {ref}",
                }
            return None

        return (
            _match_ref_col('bank_ref', 0) or
            _match_ref_col('bank_ref_2', 2)
        )

    def _search_historical_by_name(
        self,
        name: str,
        df_historical: pd.DataFrame,
        thresholds: dict,
        hist_names: list,
        hist_indices: list,
        override_threshold: float = None,
    ) -> dict:
        """
        Fuzzy name lookup in the historical dataset using the customer_name index.

        FIX 1: uses _fuzzy_extract_one which works with both rapidfuzz and difflib.
        FIX 4: accepts override_threshold for truncated name handling.
        """
        if df_historical.empty or not hist_names:
            return None

        threshold      = override_threshold or thresholds.get('historical_fuzzy_match', 90)
        name_norm      = self._normalize_text(name, remove_legal=True)

        if self._is_garbage_name(name_norm) or len(name_norm) < 4:
            return None

        result = _fuzzy_extract_one(name_norm, hist_names, threshold)
        if result is None:
            return None

        _, score, list_idx = result
        df_idx = hist_indices[list_idx]
        row    = df_historical.loc[df_idx]

        return {
            'customer_id':   _clean_customer_id(row['customer_id']),
            'customer_name': str(row['customer_name']),
            'confidence':    int(score),
            'method':        'HISTORICAL_FUZZY_NAME',
            'notes':         f"Score: {score:.1f}",
        }

    def _search_historical_by_ref2(
        self,
        name: str,
        df_historical: pd.DataFrame,
        hist_ref2: list,
        hist_ref2_idx: list,
    ) -> dict:
        """
        FIX 3: Fuzzy lookup against the historical referencia_2 column.

        The historical dataset stores the bank's own reference name in
        referencia_2 (e.g. "MERAMEXAIR SA LOCAL", "SECURE WRAP ECU").
        These are often truncated versions of the customer name and can
        match a bank's bank_ref_2 even when the full customer_name does not.

        Uses a lower threshold (_REF2_THRESHOLD = 70) because ref2 values
        are expected to be partial/truncated.
        """
        if not hist_ref2:
            return None

        name_norm = self._normalize_text(name, remove_legal=True)
        if self._is_garbage_name(name_norm) or len(name_norm) < 4:
            return None

        result = _fuzzy_extract_one(name_norm, hist_ref2, self._REF2_THRESHOLD)
        if result is None:
            return None

        _, score, list_idx = result
        df_idx = hist_ref2_idx[list_idx]

        # hist_ref2_idx references df_historical directly
        from pandas import DataFrame
        if not isinstance(df_historical, DataFrame) or df_idx not in df_historical.index:
            return None

        row = df_historical.loc[df_idx]

        # Require a valid customer_id — some ref2 entries map to generic names
        cid = _clean_customer_id(row['customer_id'])
        if not cid or cid in ('', 'nan', 'None'):
            return None

        return {
            'customer_id':   cid,
            'customer_name': str(row['customer_name']),
            'confidence':    int(score),
            'method':        'HISTORICAL_FUZZY_REF2',
            'notes':         f"Ref2 match score: {score:.1f}",
        }

    def _search_master_by_name(
        self,
        name: str,
        df_master: pd.DataFrame,
        thresholds: dict,
        master_names: list,
        master_indices: list,
        override_threshold: float = None,
    ) -> dict:
        """
        Fuzzy name lookup in the SAP customer master.

        FIX 1: uses _fuzzy_extract_one (rapidfuzz or difflib).
        FIX 4: accepts override_threshold for truncated name handling.
        """
        if df_master.empty or not master_names:
            return None

        threshold = override_threshold or thresholds.get('master_fuzzy_match', 88)
        name_norm = self._normalize_text(name, remove_legal=True)

        if self._is_garbage_name(name_norm) or len(name_norm) < 4:
            return None

        result = _fuzzy_extract_one(name_norm, master_names, threshold)
        if result is None:
            return None

        _, score, list_idx = result
        df_idx = master_indices[list_idx]
        row    = df_master.loc[df_idx]

        return {
            'customer_id':   _clean_customer_id(row['customer_id']),
            'customer_name': str(row['customer_name']),
            'confidence':    int(score),
            'method':        'MASTER_FUZZY_MATCH',
            'notes':         f"Score: {score:.1f}",
        }

    # =========================================================================
    # CLASSIFICATION AND NORMALIZATION
    # =========================================================================

    def _classify_bank_ref_2(self, text_val: str) -> tuple:
        """
        Classify a reference value as ACCOUNT_NUMBER, CLIENT_NAME, or AMBIGUOUS.

        Returns:
            (type_code, cleaned_value)
        """
        if not text_val:
            return 'AMBIGUOUS', ''

        text_clean = str(text_val).strip()

        # Pure numeric (with optional dashes/dots) and >= 6 chars → account number
        if (text_clean.replace('.', '').replace('-', '').isdigit()
                and len(text_clean) >= 6):
            return 'ACCOUNT_NUMBER', text_clean

        # Contains letters → client name
        if any(c.isalpha() for c in text_clean):
            return 'CLIENT_NAME', text_clean

        if len(text_clean) < 5:
            return 'AMBIGUOUS', text_clean

        return 'UNKNOWN', text_clean

    def _normalize_text(self, text_val: str, remove_legal: bool = False) -> str:
        """Normalize text for fuzzy matching: uppercase, strip accents, alphanumeric."""
        if not isinstance(text_val, str) or not text_val.strip():
            return ""

        text_val = text_val.upper().strip()

        # Strip accent marks
        text_val = ''.join(
            c for c in unicodedata.normalize('NFD', text_val)
            if unicodedata.category(c) != 'Mn'
        )

        # Keep alphanumeric and spaces only
        text_val = re.sub(r'[^A-Z0-9\s]', ' ', text_val)

        if remove_legal:
            for variant, normalized in self.legal_entities.items():
                text_val = text_val.replace(variant.upper(), normalized)

        return " ".join(text_val.split())

    def _extract_core_name(self, text_val: str) -> str:
        """Strip legal entity suffixes to get the core business name."""
        if not text_val:
            return ""
        normalized = self._normalize_text(text_val, remove_legal=True)
        for entity in ['SAS', 'SA', 'CIALTDA', 'CIA']:
            normalized = normalized.replace(entity, ' ')
        return " ".join(normalized.split()).strip()

    def _calculate_smart_score(self, query: str, candidate: str) -> float:
        """Composite score: 70% core name similarity + 30% full name similarity."""
        query_core     = self._extract_core_name(query)
        candidate_core = self._extract_core_name(candidate)

        if len(query_core) < 4 or len(candidate_core) < 4:
            return 0.0

        core_sim = _fuzzy_score(query_core, candidate_core)
        if core_sim < 70:
            return 0.0

        full_sim = _fuzzy_score(
            self._normalize_text(query,     remove_legal=True),
            self._normalize_text(candidate, remove_legal=True),
        )
        return (core_sim * 0.7) + (full_sim * 0.3)

    def _is_garbage_name(self, name: str) -> bool:
        """Return True if the name matches a garbage pattern or lacks real words."""
        if not name or len(name) < 5:
            return True
        name_upper = name.upper()
        for pattern in self.garbage_patterns:
            if pattern in name_upper:
                return True
        words = [w for w in name_upper.split() if len(w) >= 4 and w.isalpha()]
        return len(words) == 0

    def _should_exclude(self, text_val: str) -> bool:
        """Return True if the value matches an accounting-adjustment exclusion."""
        if not text_val:
            return False
        text_upper = text_val.upper()
        return any(p in text_upper for p in self.exclusion_patterns)

    # =========================================================================
    # PERSISTENCE
    # =========================================================================

    def _apply_updates(self, updates: list):
        """Apply enrichment updates to stg_bank_transactions in batch."""
        query = text("""
            UPDATE biq_stg.stg_bank_transactions
            SET enrich_customer_id        = :customer_id,
                enrich_customer_name      = :customer_name,
                enrich_confidence_score   = :confidence,
                enrich_inference_method   = :method,
                enrich_notes              = :notes
            WHERE stg_id = :stg_id
        """)
        with self.engine_stg.begin() as conn:
            conn.execute(query, updates)