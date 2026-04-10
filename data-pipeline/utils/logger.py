"""
===============================================================================
Project: PACIOLI
Module: utils.logger
===============================================================================

Description:
    Centralized dual-sink logging facility for the PACIOLI pipeline. Every
    log record is written simultaneously to a local .txt file (fail-safe
    backup) and to the biq_config.pacioli_logs table in PostgreSQL (the
    primary queryable source of truth).

Responsibilities:
    - Maintain a single shared .log file per pipeline execution (run-level).
    - Maintain a single shared DB engine singleton for all loggers in the
      same process (avoids creating hundreds of pooled connections).
    - Assign a single run_id UUID per pipeline execution, shared across
      all loggers, enabling per-run queries in the log table.
    - Build a closure-based logger function scoped to a process name.
    - Write colorized records to stdout, persistent records to .txt, and
      structured records to the PostgreSQL log table.
    - Filter decorative messages (separator lines, empty strings) from the
      DB sink — they are useful on console but noise in SQL queries.
    - Auto-populate source_file and source_line from the call stack when
      not explicitly provided by the caller.
    - Lazily initialize the database engine and disable DB logging
      gracefully if the connection fails.
    - Allow the batch_id to be updated after construction so that commands
      can bind their batch context once it becomes available.
    - Provide a log_exception shortcut that captures a full traceback.

Key Components:
    - get_logger: Factory that returns a log callable bound to a process
      name and optional batch id.
    - log (inner closure): The returned callable used across the pipeline.
    - log.set_batch_id: Allows updating the batch_id after logger creation.
    - log_exception: Attached helper to record exceptions with traceback.

Notes:
    - VERSION 2.1 — enterprise adjustments over v2.0:

      [1] run_id per pipeline execution:
          A UUID4 is generated once per Python process (_PIPELINE_RUN_ID).
          It is used as the default batch_id for every DB row when no
          explicit batch_id has been set yet. This means:
            - SELECT * FROM pacioli_logs WHERE batch_id = '<run_id>'
              returns ALL logs from one complete pipeline execution.
            - When a command calls set_batch_id(batch_tracker_id), that
              specific command's logs switch to the precise batch_id, while
              framework/orchestrator logs keep the run_id. Both are useful.

      [2] Decorative message filter:
          Lines composed entirely of ═, ─, spaces, or empty strings are
          skipped in the DB insert. They still appear in console and .log
          for human readability. This eliminates ~15% of rows that have no
          query value, keeps the table lean, and mirrors how enterprise
          logging platforms (Datadog, Splunk, CloudWatch) handle it.

      [3] source_file / source_line auto-detection:
          When the caller does not provide these fields (which is 100% of
          existing callers), inspect.stack() walks up the call stack to
          find the first frame outside this module and populates both
          fields automatically. Zero changes required in existing code.

      [4] strip() on message before DB insert:
          Some orchestrator messages embed leading \n for console spacing.
          Stripping prevents broken LIKE/ilike queries on the message column.

      [5] CAST(:details AS jsonb) — retained from v2.0 fix:
          Prevents the SQLAlchemy `:param::type` double-colon conflict with
          psycopg2's %(param)s bind-parameter style.

Dependencies:
    - os, sys, traceback, json, datetime, pathlib, typing, uuid, inspect
    - sqlalchemy (lazy import inside DB writer)
    - utils.db_config (lazy import inside DB writer)

===============================================================================
"""

import os
import sys
import traceback
import uuid
import inspect
from datetime import datetime
from typing import Callable, Literal, Optional
from pathlib import Path

# --- Types ---
LogLevel = Literal["DEBUG", "INFO", "WARNING", "WARN", "ERROR", "CRITICAL", "SUCCESS"]

# --- Routes ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOGS_DIR     = PROJECT_ROOT / "logs"

# --- Console colors ---
_COLORS = {
    "DEBUG":    "\033[90m",
    "INFO":     "\033[0m",
    "WARN":     "\033[93m",
    "WARNING":  "\033[93m",
    "ERROR":    "\033[91m",
    "CRITICAL": "\033[95m",
    "SUCCESS":  "\033[92m",
    "RESET":    "\033[0m",
}

# Characters that compose purely decorative separator lines
_DECORATOR_CHARS = frozenset("═─ \t\n\r")


def _is_decorative(message: str) -> bool:
    """
    Return True if the message carries no operational information.

    Pure separator lines (════, ────), empty strings, and whitespace-only
    strings are decorative. They add console readability but are noise in
    SQL queries and should not occupy rows in pacioli_logs.
    """
    stripped = message.strip()
    if not stripped:
        return True
    return all(c in _DECORATOR_CHARS for c in stripped)


def _caller_info(depth: int = 4) -> tuple:
    """
    Walk the call stack to find the first caller frame outside this module.

    Args:
        depth: Number of innermost frames to skip before searching.

    Returns:
        (source_file, source_line) as strings, or (None, None) on failure.
    """
    try:
        this_file = Path(__file__).resolve()
        for frame_info in inspect.stack()[depth:]:
            frame_path = Path(frame_info.filename).resolve()
            if frame_path != this_file:
                return str(frame_path), frame_info.lineno
    except Exception:
        pass
    return None, None


# =============================================================================
# MODULE-LEVEL SINGLETONS
# Shared across ALL loggers created in the same Python process.
# =============================================================================

_SHARED_LOG_FILE: Optional[Path] = None
_DB_ENGINE_SINGLETON = None
_DB_AVAILABLE        = True

# Single run_id per process — identifies a complete pipeline run in the
# log table. Set once on first call to _get_run_id().
_PIPELINE_RUN_ID: Optional[str] = None


def _get_run_id() -> str:
    """Return the pipeline run UUID, generating it on the first call."""
    global _PIPELINE_RUN_ID
    if _PIPELINE_RUN_ID is None:
        _PIPELINE_RUN_ID = str(uuid.uuid4())
    return _PIPELINE_RUN_ID


def _ensure_shared_log_file() -> Path:
    """
    Return the shared .log file path, creating it on the first call.

    The filename embeds the timestamp and a short prefix of the run_id so
    log files are both sorted chronologically and tied to a specific run.
    """
    global _SHARED_LOG_FILE

    if _SHARED_LOG_FILE is not None:
        return _SHARED_LOG_FILE

    try:
        os.makedirs(LOGS_DIR, exist_ok=True)
    except OSError as e:
        print(f"CRITICAL: Cannot create logs directory at {LOGS_DIR}. {e}")
        sys.exit(1)

    run_prefix        = _get_run_id()[:8]
    timestamp         = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    _SHARED_LOG_FILE  = LOGS_DIR / f"pipeline_{timestamp}_{run_prefix}.log"
    return _SHARED_LOG_FILE


def _get_db_engine():
    """
    Return the shared DB engine, initializing it lazily on first call.

    Permanently disabled on first connection failure so subsequent log
    calls do not retry failed connections on every message.
    """
    global _DB_ENGINE_SINGLETON, _DB_AVAILABLE

    if not _DB_AVAILABLE:
        return None
    if _DB_ENGINE_SINGLETON is not None:
        return _DB_ENGINE_SINGLETON

    try:
        from sqlalchemy import create_engine
        from utils.db_config import get_connection_string
        _DB_ENGINE_SINGLETON = create_engine(
            get_connection_string('biq_config'),
            echo=False,
            pool_recycle=3600,
            pool_pre_ping=True,
            pool_size=2,
            max_overflow=3,
        )
        return _DB_ENGINE_SINGLETON
    except Exception as exc:
        _DB_AVAILABLE = False
        print(f"[LOGGER] DB engine init failed — DB logging disabled: {exc}")
        return None


# =============================================================================
# PUBLIC API
# =============================================================================

def get_logger(
    process_name: str,
    batch_id: Optional[str] = None,
    write_to_db: bool = True,
) -> Callable:
    """
    Create a closure-based logger scoped to a pipeline process.

    All loggers in the same Python process share:
      - A single .log file
      - A single DB connection pool
      - A single pipeline run_id (UUID)

    Args:
        process_name (str): Logical process name (e.g. 'SAP_CMD_V4').
        batch_id (str, optional): Explicit batch identifier. When None,
            the pipeline run_id is used as the default — ensuring every
            DB row is always associated with a run even before BatchTracker
            assigns a specific id.
        write_to_db (bool): Persist records in biq_config.pacioli_logs.
            Defaults to True.

    Returns:
        Callable with signature:
            log(message, level, details, source_file, source_line)
        Attributes:
            log.set_batch_id(id)   — bind a BatchTracker batch_id
            log.process_name       — the bound process name
            log.run_id             — pipeline run UUID (read-only)
            log.log_filepath       — path to the shared .log file
            log.exception(...)     — exception helper with traceback
            log._state             — mutable state dict (batch_id)
    """
    log_filepath = _ensure_shared_log_file()
    run_id       = _get_run_id()

    # Default batch_id to run_id so every row is always run-traceable
    _state = {"batch_id": batch_id if batch_id is not None else run_id}

    # -------------------------------------------------------------------------
    def _write_to_db(
        level: str,
        message: str,
        details: Optional[dict],
        source_file: Optional[str],
        source_line: Optional[int],
    ):
        """
        Persist one log record in biq_config.pacioli_logs.

        Applies three filters before inserting:
          1. Decorative messages are silently skipped.
          2. Message is stripped of surrounding whitespace/newlines.
          3. source_file/line auto-detected from call stack if not given.

        Never raises — any failure is swallowed to protect the pipeline.
        """
        if not write_to_db:
            return
        if _is_decorative(message):
            return

        engine = _get_db_engine()
        if engine is None:
            return

        try:
            import json
            from sqlalchemy import text

            db_level = level if level in (
                "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
            ) else "INFO"
            if db_level == "WARN":
                db_level = "WARNING"

            clean_msg    = message.strip()
            details_json = json.dumps(details, ensure_ascii=False) if details else None

            # Auto-detect caller location when not explicitly provided
            eff_file, eff_line = source_file, source_line
            if eff_file is None and eff_line is None:
                eff_file, eff_line = _caller_info(depth=4)

            sql = text("""
                INSERT INTO biq_config.pacioli_logs
                    (log_level, process_name, batch_id, message,
                     details, source_file, source_line)
                VALUES
                    (:level, :process, :batch, :message,
                     CAST(:details AS jsonb), :src_file, :src_line)
            """)
            with engine.begin() as conn:
                conn.execute(sql, {
                    "level":    db_level,
                    "process":  process_name,
                    "batch":    _state["batch_id"],
                    "message":  clean_msg,
                    "details":  details_json,
                    "src_file": eff_file,
                    "src_line": eff_line,
                })
        except Exception:
            pass

    # -------------------------------------------------------------------------
    def log(
        message: str,
        level: LogLevel = "INFO",
        details: Optional[dict] = None,
        source_file: Optional[str] = None,
        source_line: Optional[int] = None,
    ):
        """
        Write a log record to stdout, the shared .log file, and the DB.

        The console and .log file always receive the full message including
        decorative separators. The DB sink applies filters (decorative skip,
        strip, source auto-detection) before persisting.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        color     = _COLORS.get(level, "")
        reset     = _COLORS["RESET"]
        formatted = f"[{timestamp}] [{level:<8}] {message}"

        # 1. Console
        print(f"{color}{formatted}{reset}")

        # 2. .log file
        try:
            with open(log_filepath, "a", encoding="utf-8") as f:
                f.write(formatted + "\n")
                if details:
                    import json
                    f.write(
                        f"             DETAILS: "
                        f"{json.dumps(details, ensure_ascii=False)}\n"
                    )
        except Exception as e:
            print(f"!! LOGGING ERROR (file): {e}")

        # 3. PostgreSQL (filtered)
        _write_to_db(level, message, details, source_file, source_line)

    # -------------------------------------------------------------------------
    def log_exception(
        message: str,
        exc: Exception,
        source_file: Optional[str] = None,
    ):
        """
        Log an exception at ERROR level with its full traceback in details.

        Example:
            try:
                risky_operation()
            except Exception as e:
                logger.exception("Load failed", e, source_file=__file__)
        """
        tb = traceback.format_exc()
        log(
            message=f"{message}: {type(exc).__name__}: {exc}",
            level="ERROR",
            details={"traceback": tb},
            source_file=source_file,
        )

    def set_batch_id(new_batch_id: str):
        """
        Bind a BatchTracker batch_id to all subsequent DB log rows.

        Call this immediately after BatchTracker.start_batch() so that
        command-level logs carry the precise batch_id rather than the
        generic pipeline run_id.

        Example:
            batch_id = self.batch_tracker.start_batch(fingerprint)
            self.logger.set_batch_id(batch_id)
        """
        _state["batch_id"] = new_batch_id

    log.exception    = log_exception        # type: ignore
    log.set_batch_id = set_batch_id         # type: ignore
    log.process_name = process_name         # type: ignore
    log.run_id       = run_id               # type: ignore
    log.log_filepath = str(log_filepath)    # type: ignore
    log._state       = _state               # type: ignore

    return log


# =============================================================================
# Self-test
# =============================================================================
if __name__ == "__main__":
    print(f"Log directory: {LOGS_DIR}\n")

    log1 = get_logger("test_process_A", write_to_db=False)
    log2 = get_logger("test_process_B", write_to_db=False)

    log1("Logger A initialized", "INFO")
    log2("Logger B initialized", "INFO")

    assert log1.log_filepath == log2.log_filepath, "Shared file FAILED"
    assert log1.run_id == log2.run_id,             "Shared run_id FAILED"
    print(f"Shared log : {log1.log_filepath}")
    print(f"Pipeline run_id: {log1.run_id}")

    log1("═══ separator ═══", "INFO")   # decorative → skipped in DB
    log1("", "INFO")                     # empty → skipped in DB
    log1("Real operational message", "INFO")  # goes to DB

    log1.set_batch_id("BATCH-2026-001")
    log1("After set_batch_id — uses BATCH-2026-001", "INFO")
    print(f"set_batch_id: {log1._state['batch_id']}")

    print("\nTesting DB write...")
    db_log = get_logger("test_v21", write_to_db=True)
    db_log("v2.1 test — run_id as default batch_id", "INFO")
    db_log("═══════════════", "INFO")   # should NOT appear in DB
    db_log("v2.1 test — with details", "INFO", details={"version": "2.1", "ok": True})
    print("Done — check pacioli_logs for 2 rows from test_v21 (not 3)")