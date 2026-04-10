"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.new_data_detector
===============================================================================

Description:
    Domain service for detecting new data in the RAW layer. It determines if
    specific processes require execution based on the availability of new
    data batches.

Responsibilities:
    - Map processes to their corresponding RAW tables.
    - Compare processed batch IDs against available RAW batch IDs.
    - Handle complex detection for processes with multiple data sources.
    - Manage dependency-based detection for derived processes.

Key Components:
    - NewDataDetector: Intelligent detector class for RAW data changes.
    - RAW_TABLE_MAPPING: Configuration mapping processes to RAW storage.

Notes:
    - VERSION 1.1:
      FIX: SAP source table was 'biq_raw.raw_sap_cta_239' — the engine_raw
           already points to the biq_raw schema via search_path, so the prefix
           caused a cross-database reference error identical to Bug #1.
           Corrected to 'raw_sap_cta_239' (no schema prefix needed).
           The same fix is applied to all other table references that had
           the 'biq_raw.' prefix — the engine_raw handles schema resolution.

Dependencies:
    - sqlalchemy
    - typing
    - utils.logger

===============================================================================
"""

from sqlalchemy import text
from sqlalchemy.engine import Engine
from typing import Dict, Optional

from utils.logger import get_logger


# =============================================================================
# PROCESS → RAW TABLE MAPPING
# =============================================================================
# Rules for table names:
#   - engine_raw has search_path = biq_raw → NO 'biq_raw.' prefix needed.
#   - Use bare table names: 'raw_sap_cta_239', NOT 'biq_raw.raw_sap_cta_239'.
# =============================================================================

RAW_TABLE_MAPPING = {

    # ── SAP_TRANSACTIONS: two RAW sources ────────────────────────────────────
    'SAP_TRANSACTIONS': {
        'type': 'TRANSACTIONAL',
        'has_period': True,
        'multiple_sources': True,
        'sources': [
            {
                'table': 'raw_sap_cta_239',      # FIX: was 'biq_raw.raw_sap_cta_239'
                'date_column': 'fecha_documento',
                'priority': 1,
            },
            {
                'table': 'raw_banco_239',
                'date_column': 'fecha_transaccion',
                'priority': 2,
            },
        ],
    },

    # ── Card processes ────────────────────────────────────────────────────────
    'DINERS_CARDS': {
        'table': 'raw_diners_club',              # FIX: was 'biq_raw.raw_diners_club'
        'date_column': 'fecha_del_vale',
        'has_period': True,
        'type': 'TRANSACTIONAL',
        'triggers': ['PARKING_BREAKDOWN'],
    },

    'GUAYAQUIL_CARDS': {
        'table': 'raw_guayaquil',                # FIX: was 'biq_raw.raw_guayaquil'
        'date_column': 'fecha_transaccion',
        'has_period': True,
        'type': 'TRANSACTIONAL',
        'triggers': ['PARKING_BREAKDOWN'],
    },

    'PACIFICARD_CARDS': {
        'table': 'raw_pacificard',               # FIX: was 'biq_raw.raw_pacificard'
        'date_column': 'fecha_pago',
        'has_period': True,
        'type': 'TRANSACTIONAL',
        'triggers': ['PARKING_BREAKDOWN'],
    },

    # ── Stateful processes ────────────────────────────────────────────────────
    'CUSTOMER_PORTFOLIO_PHASE1': {
        'table': 'raw_customer_portfolio',       # FIX: was 'biq_raw.raw_customer_portfolio'
        'date_column': 'fecha_documento',
        'has_period': False,
        'type': 'STATEFUL',
        'always_process': True,
    },

    'CUSTOMER_PORTFOLIO_PHASE2': {
        'table': None,
        'has_period': False,
        'type': 'STATEFUL',
        'always_process': True,
    },

    'CUSTOMER_PORTFOLIO_PHASE3': {
        'table': None,
        'has_period': False,
        'type': 'STATEFUL',
        'always_process': True,
    },

    'WITHHOLDINGS_PROCESS': {
        'table': 'raw_retenciones_sri',          # FIX: was 'biq_raw.raw_retenciones_sri'
        'date_column': 'fecha_emision_ret',
        'has_period': False,
        'type': 'STATEFUL',
    },

    'WITHHOLDINGS_MATCH': {
        'table': None,
        'has_period': False,
        'type': 'STATEFUL',
        'always_process': True,
    },

    'WITHHOLDINGS_APPLY': {
        'table': None,
        'has_period': False,
        'type': 'STATEFUL',
        'always_process': True,
    },

    'MANUAL_REQUESTS': {
        'table': 'raw_manual_requests',          # FIX: was 'biq_raw.raw_manual_requests'
        'date_column': 'fecha',
        'has_period': False,
        'type': 'STATEFUL',
    },

    'BANK_ENRICHMENT': {
        'table': None,
        'has_period': False,
        'type': 'STATEFUL',
        'always_process': True,
    },

    'BANK_RECONCILIATION': {
        'table': None,
        'has_period': False,
        'type': 'STATEFUL',
        'always_process': True,
    },

    # ── Derived processes ─────────────────────────────────────────────────────
    'PARKING_BREAKDOWN': {
        'table': None,
        'has_period': True,
        'type': 'DERIVED',
        'depends_on': ['DINERS_CARDS', 'GUAYAQUIL_CARDS', 'PACIFICARD_CARDS'],
    },
}


# =============================================================================
# DETECTOR
# =============================================================================

class NewDataDetector:
    """
    Intelligent detector for new data in the RAW layer.

    Compares batch_ids recorded in etl_process_windows.notes (what was last
    processed) against the latest batch_id available in each RAW table (what
    is now available). When they differ, the process must re-run.
    """

    def __init__(self, engine_raw: Engine, engine_config: Engine):
        self.engine_raw    = engine_raw
        self.engine_config = engine_config
        self.logger        = get_logger("NEW_DATA_DETECTOR")
        self._detection_cache: dict = {}

    # =========================================================================
    # PUBLIC
    # =========================================================================

    def has_new_data(
        self,
        process_name: str,
        period_start: Optional[str] = None,
        period_end:   Optional[str] = None,
    ) -> Dict:
        """
        Detect whether a process has new RAW data since its last execution.

        Args:
            process_name: Key from RAW_TABLE_MAPPING.
            period_start: 'YYYY-MM-DD' start of the processing window.
            period_end:   'YYYY-MM-DD' end of the processing window.

        Returns:
            dict with keys:
                has_new_data (bool), reason (str),
                last_processed_batch, last_raw_batch, cascade_processes.
        """
        cache_key = f"{process_name}_{period_start}_{period_end}"
        if cache_key in self._detection_cache:
            return self._detection_cache[cache_key]

        config = RAW_TABLE_MAPPING.get(process_name)

        # Unknown process → execute by default (safe fallback)
        if not config:
            return {
                'has_new_data': True,
                'reason': f'{process_name} not in mapping → executing by default',
                'last_processed_batch': None,
                'last_raw_batch': None,
                'cascade_processes': [],
            }

        # always_process flag → stateful snapshot, always runs
        if config.get('always_process'):
            result = {
                'has_new_data': True,
                'reason': f'{process_name} is STATEFUL (snapshot) — always executes',
                'last_processed_batch': None,
                'last_raw_batch': None,
                'cascade_processes': [],
            }
            self._detection_cache[cache_key] = result
            return result

        if config['type'] == 'DERIVED':
            return self._check_derived_process(
                process_name, config, period_start, period_end
            )

        result = self._check_batch_id_difference(
            process_name, config, period_start, period_end
        )
        self._detection_cache[cache_key] = result
        return result

    def clear_cache(self) -> None:
        """Clear the detection cache (call at end of each pipeline run)."""
        self._detection_cache = {}

    # =========================================================================
    # PRIVATE — detection strategies
    # =========================================================================

    def _check_batch_id_difference(
        self,
        process_name: str,
        config: Dict,
        period_start: Optional[str],
        period_end:   Optional[str],
    ) -> Dict:
        """Compare last-processed batch_id vs latest available in RAW."""

        if config.get('multiple_sources'):
            return self._check_multiple_sources(
                process_name, config, period_start, period_end
            )

        table_name = config.get('table')
        if not table_name:
            return {
                'has_new_data': True,
                'reason': f'{process_name} has no RAW table → executing',
                'last_processed_batch': None,
                'last_raw_batch': None,
                'cascade_processes': [],
            }

        date_column = config['date_column']
        has_period  = config['has_period']

        last_processed_batch = self._get_last_processed_batch(
            process_name, period_start
        )
        last_raw_batch = self._get_last_raw_batch(
            table_name,
            date_column,
            period_start if has_period else None,
            period_end   if has_period else None,
        )

        # No RAW data for this period → nothing to do
        if not last_raw_batch:
            return {
                'has_new_data': False,
                'reason': f'No data in {table_name} for the specified period',
                'last_processed_batch': last_processed_batch,
                'last_raw_batch': None,
                'cascade_processes': [],
            }

        # First execution or batch changed → run
        if not last_processed_batch or last_raw_batch != last_processed_batch:
            cascade = config.get('triggers', [])
            reason  = (
                f'First execution — batch {last_raw_batch} available'
                if not last_processed_batch
                else f'New data: batch {last_raw_batch} (processed: {last_processed_batch})'
            )
            return {
                'has_new_data': True,
                'reason': reason,
                'last_processed_batch': last_processed_batch,
                'last_raw_batch': last_raw_batch,
                'cascade_processes': cascade,
            }

        return {
            'has_new_data': False,
            'reason': f'Already processed — batch {last_raw_batch}',
            'last_processed_batch': last_processed_batch,
            'last_raw_batch': last_raw_batch,
            'cascade_processes': [],
        }

    def _check_multiple_sources(
        self,
        process_name: str,
        config: Dict,
        period_start: Optional[str],
        period_end:   Optional[str],
    ) -> Dict:
        """Check multiple RAW tables (SAP case: sap_cta_239 + banco_239)."""

        sources = config['sources']
        last_processed_batches = self._get_last_processed_batches_multi(
            process_name, period_start
        )

        has_new = False
        details: list = []
        all_raw_batches: dict = {}

        for source in sources:
            table_name  = source['table']
            date_column = source['date_column']

            # Derive a short key from the table name for storage in notes
            source_key = table_name.replace('raw_', '').replace('_', '')

            last_processed = last_processed_batches.get(source_key)
            last_raw       = self._get_last_raw_batch(
                table_name, date_column, period_start, period_end
            )

            all_raw_batches[source_key] = last_raw

            if last_raw and last_raw != last_processed:
                has_new = True
                details.append(
                    f"{table_name}: batch {last_raw} "
                    f"(processed: {last_processed or 'none'})"
                )
            elif last_raw:
                details.append(f"{table_name}: no changes (batch {last_raw})")
            else:
                details.append(f"{table_name}: no data for period")

        if has_new:
            return {
                'has_new_data': True,
                'reason': f'New data detected: {"; ".join(details)}',
                'last_processed_batch': self._encode_multi_batch(last_processed_batches),
                'last_raw_batch':       self._encode_multi_batch(all_raw_batches),
                'cascade_processes':    config.get('triggers', []),
            }

        return {
            'has_new_data': False,
            'reason': f'All sources current: {"; ".join(details)}',
            'last_processed_batch': self._encode_multi_batch(last_processed_batches),
            'last_raw_batch':       self._encode_multi_batch(all_raw_batches),
            'cascade_processes':    [],
        }

    def _check_derived_process(
        self,
        process_name: str,
        config: Dict,
        period_start: Optional[str],
        period_end:   Optional[str],
    ) -> Dict:
        """Run a DERIVED process only when at least one dependency has new data."""

        for dep in config.get('depends_on', []):
            dep_result = self.has_new_data(dep, period_start, period_end)
            if dep_result['has_new_data']:
                return {
                    'has_new_data': True,
                    'reason': f'{process_name} must run because {dep} has new data',
                    'last_processed_batch': None,
                    'last_raw_batch': None,
                    'cascade_processes': [],
                }

        return {
            'has_new_data': False,
            'reason': f'{process_name}: no changes in dependencies',
            'last_processed_batch': None,
            'last_raw_batch': None,
            'cascade_processes': [],
        }

    # =========================================================================
    # PRIVATE — DB queries
    # =========================================================================

    def _get_last_processed_batch(
        self,
        process_name: str,
        period_start: Optional[str],
    ) -> Optional[str]:
        """
        Read the batch_id that was last successfully processed for this process.
        Stored in etl_process_windows.notes by ProcessMetricsRepository.save_batch_id().
        """
        if period_start:
            periodo = period_start[:7]   # 'YYYY-MM-DD' → 'YYYY-MM'
            query = text("""
                SELECT notes
                FROM biq_config.etl_process_windows
                WHERE process_name = :process_name
                  AND periodo_mes  = :periodo
                  AND status       = 'COMPLETED'
                ORDER BY completed_at DESC
                LIMIT 1
            """)
            params: dict = {'process_name': process_name, 'periodo': periodo}
        else:
            query = text("""
                SELECT notes
                FROM biq_config.etl_process_windows
                WHERE process_name = :process_name
                  AND status       = 'COMPLETED'
                ORDER BY completed_at DESC
                LIMIT 1
            """)
            params = {'process_name': process_name}

        try:
            with self.engine_config.connect() as conn:
                result = conn.execute(query, params).fetchone()
                return result[0] if result else None
        except Exception as e:
            self.logger(f"Error reading last processed batch: {e}", "WARN")
            return None

    def _get_last_raw_batch(
        self,
        table_name:  str,
        date_column: str,
        period_start: Optional[str],
        period_end:   Optional[str],
    ) -> Optional[str]:
        """
        Read the latest batch_id available in a RAW table.

        NOTE: table_name must be a bare name (no schema prefix) because
        engine_raw already has search_path = biq_raw. Using 'biq_raw.table'
        would cause a cross-database reference error.
        """
        if period_start and period_end:
            query = text(f"""
                SELECT batch_id
                FROM {table_name}
                WHERE {date_column} BETWEEN :start_date AND :end_date
                  AND batch_id IS NOT NULL
                ORDER BY loaded_at DESC
                LIMIT 1
            """)
            params: dict = {'start_date': period_start, 'end_date': period_end}
        else:
            query = text(f"""
                SELECT batch_id
                FROM {table_name}
                WHERE batch_id IS NOT NULL
                ORDER BY loaded_at DESC
                LIMIT 1
            """)
            params = {}

        try:
            with self.engine_raw.connect() as conn:
                result = conn.execute(query, params).fetchone()
                return result[0] if result else None
        except Exception as e:
            self.logger(f"Error reading last RAW batch from {table_name}: {e}", "WARN")
            return None

    def _get_last_processed_batches_multi(
        self,
        process_name: str,
        period_start: Optional[str],
    ) -> Dict[str, str]:
        """Decode the pipe-separated multi-source batch_id string from notes."""
        notes = self._get_last_processed_batch(process_name, period_start)
        if not notes:
            return {}
        batches: dict = {}
        for part in notes.split('|'):
            if ':' in part:
                key, value = part.split(':', 1)
                batches[key.strip()] = value.strip()
        return batches

    @staticmethod
    def _encode_multi_batch(batches: Dict[str, str]) -> str:
        """Encode multiple batch_ids as 'key1:val1|key2:val2'."""
        if not batches:
            return ''
        return '|'.join(f"{k}:{v}" for k, v in batches.items() if v)