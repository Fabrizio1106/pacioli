# PACIOLI — Silver Layer: Architecture

> **Layer:** Silver (Staging & Reconciliation)
> **Version:** 3.1 · Last reviewed: 2026-04-06

---

## 1. Purpose and Position in the Pipeline

The **Silver Layer** is the second and most complex tier of the PACIOLI data pipeline. It sits between the RAW layer (ingested, unmodified source files) and the Gold layer (reporting-ready dimensions and aggregates).

Its responsibilities, inspired by the BlackLine reconciliation model, are:

- **Normalize** data from multiple heterogeneous sources (SAP, card processors, bank statements, SRI) into a canonical staging schema.
- **Enrich** transactions with business identity (customer, brand, reference).
- **Reconcile** bank movements against portfolio invoices using a deterministic-then-probabilistic cascade.
- **Validate** results and maintain an auditable state machine for every transaction.

The silver layer never reads from gold or writes directly to reporting tables. It only reads from `biq_raw` and writes to `biq_stg`.

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        main_silver_orchestrator.py                          │
│                        SilverLayerOrchestrator                              │
│                                                                             │
│  PRE-CHECK          PHASE 0            PHASES 1-6          POST-CHECK       │
│  ─────────          ───────            ──────────          ─────────        │
│  ensure_periods  →  RAW Loaders    →   Staging Groups  →   auto_close      │
│  _exist()           (10 sources)       (Groups 1-6)        _periods_if     │
│                                                            _ready()         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Architectural Layers

The silver layer follows a strict **Layered / Domain-Driven Design** structure. Dependencies flow in one direction only: Application → Domain → Infrastructure.

```
┌──────────────────────────────────────────────┐
│         APPLICATION LAYER                    │
│   Commands (staging/*)                       │
│   Use-case orchestrators; no business logic  │
├──────────────────────────────────────────────┤
│         DOMAIN LAYER                         │
│   Services (transformation, enrichment,      │
│   compensation, hashing, aggregation,        │
│   reconciliation, withholdings, portfolio)   │
│   Pure business logic; no SQL                │
├──────────────────────────────────────────────┤
│         INFRASTRUCTURE LAYER                 │
│   Extractors (read-only SQL adapters)        │
│   Repositories (write SQL adapters)          │
│   UnitOfWork (transaction management)        │
│   BatchTracker (idempotency + audit)         │
└──────────────────────────────────────────────┘
```

---

## 4. Orchestration Design

### 4.1 SilverLayerOrchestrator

The orchestrator (`main_silver_orchestrator.py`) is the **sole entry point**. Its responsibilities are limited to:

- Declaring execution order.
- Reading process windows from `biq_config.etl_process_windows`.
- Deciding whether to run or skip each command (idempotency via `NewDataDetector` + `ProcessMetricsRepository`).
- Tracking aggregate statistics (total / successful / skipped / failed).
- Pre/post period window management.

The orchestrator contains **no business logic**. It delegates all computation to commands.

### 4.2 Execution Groups

| Group | Name | Commands |
|---|---|---|
| 0 | RAW Loading | All 10 RAW loaders (always runs) |
| 1 | Foundations | SAP Staging, Portfolio CDC, Portfolio Phase 1 |
| 2 | Cards | Diners, Guayaquil, Pacificard |
| 3 | Derived | Parking Breakdown |
| 4 | Withholdings | Process → Match → Apply |
| 5 | Advanced Portfolio | Manual Requests, Bank Enrichment, Portfolio Ph2 / Ph3 |
| 6 | Final | Bank Reconciliation, Validation Metrics, Portfolio Matches, Restore Approved |

Groups execute sequentially. Within each group, commands execute sequentially. A critical failure in any command aborts the group and the pipeline.

### 4.3 Idempotency Gate

Before executing any staging command, the orchestrator checks:

1. `ProcessMetricsRepository.get_process_status(name)` — is the process already `COMPLETED`?
2. `NewDataDetector.has_new_data(name, period_start, period_end)` — is there a new RAW batch since the last successful run?

If the process is COMPLETED **and** there is no new data, the command is **skipped**. This makes the pipeline safe to re-run without duplicating work. The `--force` flag bypasses this check entirely.

---

## 5. Period Window System

The pipeline operates on **processing windows** stored in `biq_config.etl_process_windows`. Each transactional process has an independent PENDING window defining the accounting period it should process (`window_start`, `window_end`, `periodo_mes`).

### Dual-Lock Closure (SmartPeriodClosure v2.7)

Period closure requires two independent conditions to be satisfied simultaneously:

| Lock | Name | Condition |
|---|---|---|
| Lock 1 | Calendar Gate | Current date is past the defined cutoff (e.g., day 15 of the following month). |
| Lock 2 | SAP Business Gate | All SAP records for the period are posted and compensated (`SAPPeriodClosureValidator`). |

**Pre-Check** (`ensure_periods_exist`): Runs **before** Phase 0. Only creates missing windows. Never evaluates closure — RAW tables are still empty at this point.

**Post-Check** (`auto_close_periods_if_ready`): Runs **after** all staging groups complete. Evaluates both locks using real loaded data. Closed periods immediately trigger `open_next_periods()`.

This two-phase design fixes a race condition present in earlier versions where closure was evaluated against an empty RAW table.

---

## 6. Command Pattern

Each staging command implements a single `execute(**kwargs)` method. Commands:

- Accept optional `force_start_date` / `force_end_date` for transactional processes.
- Return `True` (success) or `False` (failure).
- Expose an optional `batch_tracker` attribute so the orchestrator can extract the processed `batch_id` and persist it for the `NewDataDetector`.
- Are instantiated fresh on every pipeline run (no shared state between runs).

Commands that require atomic database operations receive a `UnitOfWork` instance at construction time.

---

## 7. Data Enrichment Architecture

Bank transaction enrichment follows a **priority cascade**. Each phase only receives transactions not resolved by earlier phases:

```
Phase 0 — Manual Requests      (fixed reference map, confidence 100)
Phase 1 — Card Brand Rules      (LIQUIDACION TC, YAML-driven, confidence 100)
Phase 2 — Specific Text Rules   (exact text match, YAML-driven, confidence 100)
Phase 4 — Cash Deposit Logic    (VIP prices + sequence analysis, Strategy V7)
Phase 5 — Settlement Enricher   (settlement_id linkage)
Phase 3 — Smart Heuristic       (fuzzy: historical → SAP master, threshold 70–98)
```

The SmartHeuristicEnricher uses a three-source priority hierarchy:

```
Historical Dataset (4 years of manual validation)
    ├── Exact reference number match  → confidence 98
    ├── Fuzzy name match              → confidence 90 (75 for truncated names)
    └── Fuzzy referencia_2 match      → confidence 70

SAP Customer Master
    └── Fuzzy name match              → confidence 88 (75 for truncated names)
```

---

## 8. Reconciliation Architecture

Reconciliation is the most complex component. It is organized as a **multi-phase cascade matcher** applied to each bank transaction:

```
┌─────────────────────────────────────────────────────────┐
│              ReconcileBankTransactionsCommand            │
│                                                         │
│  Phase 0 ── CardMatcher                                 │
│              LIQUIDACION TC only                        │
│              settlement_id aggregation                  │
│                                                         │
│  Phase 1 ── DeterministicMatcher (per customer)         │
│              1. Exact Single                            │
│              2. Exact Contiguous Multi                  │
│              3. Tolerance Single (±$0.05)               │
│                                                         │
│  Phase 2 ── ProbabilisticMatcher (per customer)         │
│              4. Greedy Sequential                       │
│              5. Subset Sum (contiguous)                 │
│              6. Subset Sum (with gaps, penalized)       │
│              7. Best Effort (always REVIEW)             │
│                                                         │
│  Phase 3 ── Special Cases                               │
│              MultiPaymentStrategy (same-day groups)     │
│                                                         │
│  Phase 4.5 ─ SalasVIPStrategy (cash VIP)               │
│              UrbaparkingStrategy (parking)              │
│                                                         │
│  Phase 5 ── ResidualsReconciliationStrategy             │
│              (relaxed window ±3 days, ≤5 invoices)      │
└─────────────────────────────────────────────────────────┘
```

### Status Machine

Every bank transaction and portfolio invoice moves through a defined set of states:

```
PENDING → MATCHED         (automatic, confidence ≥ 90)
PENDING → REVIEW          (automatic, confidence 60–89, or best-effort)
PENDING → PENDING         (no match found)
REVIEW  → MATCHED_MANUAL  (human approval — protected by RestoreApprovedTransactionsCommand)
```

### Scoring Engine

The ScoringEngine computes a 0–100 confidence score using five weighted criteria:

| Criterion | Weight |
|---|---|
| Exact amount match | 40% |
| Tolerance match | 30% |
| Date proximity | 15% |
| Invoice continuity (no gaps) | 10% |
| Reference field match | 5% |

Scores ≥ 90 → `MATCHED`. Scores 60–89 → `REVIEW`. Below 60 → `PENDING`.

---

## 9. Rust Extension (`pacioli_core`)

`pacioli_core` is a **PyO3-compiled Rust extension** used as an optional performance accelerator for combinatorially expensive operations within `CustomerPortfolioEnricherService`.

Exported functions:

| Function | Algorithm | Complexity |
|---|---|---|
| `find_invoice_combination` | Subset sum: 1-invoice scan → 2-invoice HashMap → 3-invoice two-pointer | O(n) / O(n) / O(n²) |
| `fuzzy_batch_match` | Jaccard similarity on character bigrams, pre-filtered by amount | O(n·k) with ~90% pre-filter rejection |

The Python code gracefully falls back to pure-Python implementations when the Rust module is not available (`_RUST_AVAILABLE = False`).

**Build command:** `cd pacioli_core && maturin develop --release`

**Release profile:** `-O3`, LTO enabled, single codegen unit, panic=abort.

---

## 10. Infrastructure Patterns

### Unit of Work

`UnitOfWork` wraps a SQLAlchemy session as a context manager. All repositories accessed within a `with UnitOfWork(engine) as uow:` block share the same transaction. An unhandled exception triggers automatic rollback.

```python
with UnitOfWork(engine_stg) as uow:
    uow.bank_transactions.update_status(stg_id, 'MATCHED')
    uow.invoices.update_status(port_id, 'PAID')
    # auto-commit on exit
```

### BatchTracker

`BatchTracker` provides ETL idempotency at the batch level. It:
- Creates a batch record with `status = RUNNING` at the start.
- Transitions to `COMPLETED` (with record counts and duration) or `FAILED` on exit.
- Uses a configuration fingerprint hash to detect and reject duplicate executions.

### Repository Pattern

Repositories encapsulate all SQL. Domain services never build queries directly. This separation allows SQL to be changed without touching business logic and makes domain services fully testable without a database.

---

## 11. Separation of Concerns Summary

| Concern | Handled By |
|---|---|
| Execution order & lifecycle | `SilverLayerOrchestrator` |
| Period window management | `SmartPeriodClosure` |
| Idempotency (process level) | `NewDataDetector` + `ProcessMetricsTracker` |
| Idempotency (batch level) | `BatchTracker` |
| Business logic | Domain Services |
| Data access (read) | Extractors |
| Data access (write) | Repositories |
| Transaction atomicity | `UnitOfWork` |
| Reconciliation rules | `config/rules/*.yaml` |
| Performance-critical paths | `pacioli_core` (Rust) |

---

## 12. Database Schema Boundaries

| Schema | Role | Who Reads | Who Writes |
|---|---|---|---|
| `biq_raw` | Raw ingested files | Extractors, NewDataDetector | RAW Loaders (Phase 0) |
| `biq_stg` | Staged, enriched, reconciled data | Extractors, Repositories | Repositories |
| `biq_config` | ETL control tables (windows, metrics) | SmartPeriodClosure, NewDataDetector, MetricsRepo | SmartPeriodClosure, MetricsRepo |

---

## 13. Design Decisions

**Why a phased cascade matcher instead of a single unified algorithm?**
Different transaction types have fundamentally different matching properties. Credit card settlements are identified by `settlement_id` and require portfolio aggregation — an entirely different join strategy than FIFO invoice matching. Isolating them in Phase 0 avoids polluting the general matcher with card-specific conditionals.

**Why separate PRE-CHECK and POST-CHECK for period closure?**
Period closure requires reading from RAW tables to validate that all data has been posted. Running closure evaluation before Phase 0 (when RAW tables are still empty) caused false closures in v2.6. The v2.7 split fixes this by guaranteeing RAW data is loaded before any closure decision is made.

**Why a Rust extension for subset sum?**
The portfolio matching problem requires finding combinations of up to 15 invoices from a pool of potentially 90+ candidates. In Python, brute-force enumeration of this space is too slow for a daily batch. The Rust two-pointer implementation reduces the 3-invoice case from O(n³) to O(n²) and eliminates floating-point arithmetic errors by converting to integer centavos.

**Why YAML-driven rule configuration?**
Business rules change frequently (new card brands, new special customers, tolerance adjustments). Externalizing them to YAML allows operations teams to modify rules without code changes, deployments, or regression risk.
