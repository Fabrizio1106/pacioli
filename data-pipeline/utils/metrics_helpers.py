"""
===============================================================================
Project: PACIOLI
Module: utils.metrics_helpers
===============================================================================

Description:
    Utility helpers for extracting record counts from completed pipeline
    commands. Centralizes the per-command attribute inspection logic that
    was previously embedded in the main orchestrator.

Responsibilities:
    - Provide safe_get_attr for deep attribute/dict traversal without
      raising exceptions.
    - Provide COMMAND_METRICS_EXTRACTORS, a registry that maps each command
      class name to a lambda that knows how to read its record count.
    - Provide get_records_count(command) as the single public entry point
      used by the orchestrator after executing any command.

Notes:
    - Adding a new command: add one entry to COMMAND_METRICS_EXTRACTORS
      keyed by the command's class name. The orchestrator requires no changes.
    - Extractors must never raise; they return 0 on any failure.

Dependencies:
    - None (stdlib only)

===============================================================================
"""

from typing import Any


# =============================================================================
# SAFE ATTRIBUTE ACCESSOR
# =============================================================================

def safe_get_attr(obj: Any, *attr_path: str, default: Any = 0) -> Any:
    """
    Traverse a chain of attribute or dict-key lookups without raising.

    Args:
        obj:        Root object to start the traversal from.
        *attr_path: Chain of attribute names or dict keys to follow.
        default:    Value returned when any step in the path is missing
                    or the final value is None.

    Returns:
        The value at the end of the path, or `default`.

    Examples:
        safe_get_attr(cmd, 'stats', 'total')         # cmd.stats['total']
        safe_get_attr(cmd, 'records_processed')       # cmd.records_processed
        safe_get_attr(cmd, 'stats', 'matched', default=0)
    """
    current = obj
    for attr in attr_path:
        if hasattr(current, attr):
            current = getattr(current, attr)
        elif isinstance(current, dict) and attr in current:
            current = current[attr]
        else:
            return default
    return current if current is not None else default


# =============================================================================
# PER-COMMAND RECORD COUNT EXTRACTORS
# =============================================================================
# Each entry maps a command class name (str) to a lambda that accepts the
# command instance and returns an integer record count.
#
# Rules:
#   - Lambdas must not raise — use safe_get_attr or guard with `or 0`.
#   - When a command has no meaningful count (e.g. RestoreApproved), return 0.
#   - Adding a new command: add one entry here. No other file needs changing.
# =============================================================================

COMMAND_METRICS_EXTRACTORS: dict = {

    'ProcessSAPStagingCommand': lambda cmd: (
        safe_get_attr(cmd, 'stats', 'total') or
        safe_get_attr(cmd, 'total_records') or
        safe_get_attr(cmd, 'records_processed') or
        0
    ),

    'ProcessCustomerPortfolioCommand': lambda cmd: (
        safe_get_attr(cmd, 'documents_processed') or
        safe_get_attr(cmd, 'stats', 'processed') or
        safe_get_attr(cmd, 'total_processed') or
        0
    ),

    'ProcessDinersCommand': lambda cmd: (
        safe_get_attr(cmd, 'total_settlements', default=0) +
        safe_get_attr(cmd, 'total_details',     default=0)
    ),

    'ProcessGuayaquilCommand': lambda cmd: (
        safe_get_attr(cmd, 'total_settlements', default=0) +
        safe_get_attr(cmd, 'total_details',     default=0)
    ),

    'ProcessPacificardCommand': lambda cmd: (
        safe_get_attr(cmd, 'total_settlements', default=0) +
        safe_get_attr(cmd, 'total_details',     default=0)
    ),

    'ProcessParkingBreakdownCommand': lambda cmd: (
        safe_get_attr(cmd, 'total_lotes') or
        safe_get_attr(cmd, 'lotes_inserted') or
        safe_get_attr(cmd, 'stats', 'total') or
        0
    ),

    'ProcessWithholdingsCommand': lambda cmd: (
        safe_get_attr(cmd, 'total_inserted') or
        safe_get_attr(cmd, 'stats', 'inserted') or
        safe_get_attr(cmd, 'records_processed') or
        0
    ),

    'MatchWithholdingsCommand': lambda cmd: (
        safe_get_attr(cmd, 'total_matched') or
        safe_get_attr(cmd, 'stats', 'matched') or
        0
    ),

    'ApplyWithholdingsCommand': lambda cmd: (
        safe_get_attr(cmd, 'total_applied') or
        safe_get_attr(cmd, 'stats', 'applied') or
        0
    ),

    'ProcessManualRequestsCommand': lambda cmd: (
        safe_get_attr(cmd, 'total_requests') or
        safe_get_attr(cmd, 'stats', 'total') or
        0
    ),

    'ProcessBankEnrichmentCommand': lambda cmd: (
        safe_get_attr(cmd, 'total_enriched') or
        safe_get_attr(cmd, 'stats', 'enriched') or
        0
    ),

    'ReconcileBankTransactionsCommand': lambda cmd: (
        safe_get_attr(cmd, 'total_transactions') or
        safe_get_attr(cmd, 'stats', 'total') or
        (
            safe_get_attr(cmd, 'stats', 'matched', default=0) +
            safe_get_attr(cmd, 'stats', 'review',  default=0) +
            safe_get_attr(cmd, 'stats', 'pending', default=0)
        ) or
        0
    ),

    'UpdateBankValidationMetricsCommand': lambda cmd: (
        safe_get_attr(cmd, 'settlements_updated') or
        safe_get_attr(cmd, 'total_updated') or
        0
    ),

    # No meaningful record count — correction step only
    'RestoreApprovedTransactionsCommand': lambda cmd: 0,
    'ValidatePortfolioMatchesCommand':    lambda cmd: 0,
}


# =============================================================================
# PUBLIC ENTRY POINT
# =============================================================================

def get_records_count(command: Any) -> int:
    """
    Extract the record count from a completed command instance.

    Tries the registered extractor for the command's class first,
    then falls back to common generic attribute names.

    Args:
        command: Any executed command instance.

    Returns:
        int: Number of records processed, or 0 if not determinable.
    """
    class_name = command.__class__.__name__
    extractor  = COMMAND_METRICS_EXTRACTORS.get(class_name)

    if extractor:
        try:
            count = extractor(command)
            if isinstance(count, (int, float)) and count > 0:
                return int(count)
        except Exception:
            pass

    # Generic fallback strategies (tried in order)
    _fallbacks = [
        lambda: safe_get_attr(command, 'records_processed'),
        lambda: safe_get_attr(command, 'total_processed'),
        lambda: safe_get_attr(command, 'stats', 'total'),
    ]
    for strategy in _fallbacks:
        try:
            count = strategy()
            if count and count > 0:
                return int(count)
        except Exception:
            continue

    return 0