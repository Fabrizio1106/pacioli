# ADR-001: Hexagonal Architecture for the Data Pipeline

## Status
Accepted

## Context

The data pipeline began as a collection of Jupyter notebooks and ad-hoc Python scripts. Each script mixed raw SQL queries, data transformation logic, and file I/O in the same function. A single process like SAP staging would extract records from the database, normalize amounts, detect compensations, generate hashes, and write results back — all in one flat sequence of imperative code.

This made the pipeline hard to maintain in practice:

- **Testing was impossible without a live database.** Every function touched a DB session directly, so there was no way to test business logic in isolation.
- **Changes cascaded unpredictably.** A change to how the bank enrichment join worked required understanding the surrounding SQL and transformation code to avoid breaking adjacent steps.
- **Domain knowledge was invisible.** Matching rules, tolerance thresholds, and classification logic were buried inside loops alongside `pd.read_sql` and `session.execute` calls, with no structural boundary between "what the business requires" and "how data is fetched."
- **Duplication accumulated.** Several pipeline processes independently re-implemented similar SQL patterns (e.g., fetching pending transactions, marking records as processed) because there was no shared infrastructure layer to extend.

## Decision

The pipeline logic was restructured into three layers following the hexagonal (ports and adapters) pattern. All new pipeline processes and all refactored ones conform to this structure. The three layers, as they exist in `data-pipeline/logic/`, are:

### Layer 1 — Application (`logic/application/commands/staging/`)

One command class per pipeline process (18 total). Command classes are the only entry points called by `main_silver_orchestrator.py`.

**Responsibilities:**
- Instantiate the Unit of Work and open the DB session.
- Call infrastructure extractors to load raw data.
- Call domain services to transform and process data.
- Call infrastructure repositories to persist results.
- Manage the transaction lifecycle (commit / rollback).

**Must not:** contain SQL, business logic, or file I/O. A command class that grows business rules is a signal that logic belongs in a domain service.

Example: `ProcessSAPStagingCommand` calls `SAPExtractor` → `SAPTransformer` → `BankEnricher` → `HashGenerator` → `SAPRepository`, then commits.

### Layer 2 — Domain (`logic/domain/services/`)

Pure business logic services, organized by concern:

| Subdirectory | Examples |
|---|---|
| `transformation/` | `SAPTransformer`, `DinersTransformer`, `PacificardTransformer` |
| `enrichment/` | `BankEnricher`, `CardEnricher`, `SettlementEnricher` |
| `classification/` | `TransactionClassifier` |
| `compensation/` | `IntradayCompensationDetector`, `SAPCompensationHandler` |
| `aggregation/` | `CardAggregator`, `ParkingBreakdownService` |
| `hashing/` | `HashGenerator`, `HistoricalContextService` |
| `card_settlement/` | `GoldenRuleService`, `SplitPaymentService` |

Domain services receive DataFrames (or plain Python structures) and return DataFrames. They have no database sessions, no file handles, and no HTTP calls. This makes them testable with in-memory data and replaceable without touching infrastructure.

The reconciliation matchers (`logic/staging/reconciliation/`) also belong to the domain: `DeterministicMatcher`, `ProbabilisticMatcher`, `SalasVIPStrategy`, `SubsetSumSolver`, and the `ScoringEngine` all operate purely on Python data structures passed in by the command layer.

### Layer 3 — Infrastructure (`logic/infrastructure/`)

All SQL and all file reading lives here.

- **Repositories** (`infrastructure/repositories/`) — One repository per aggregate: `BankReconciliationRepository`, `CustomerPortfolioRepository`, `CardSettlementRepository`, etc. Each repository receives a SQLAlchemy session and issues `session.execute(text(...))` or `pd.read_sql(...)` calls. No business logic.
- **Extractors** (`infrastructure/extractors/`) — Read raw files (SAP exports, card CSVs, bank statements) and return DataFrames. No transformation logic.
- **Unit of Work** (`infrastructure/unit_of_work.py`) — Wraps the SQLAlchemy session lifecycle. Command classes acquire a UoW via context manager; all repositories within a single process share the same session and participate in the same transaction.

### Dependency Rule

Dependencies flow inward only:

```
application/commands  →  domain/services  →  (no outward dependencies)
application/commands  →  infrastructure/  →  database / files
```

Domain services never import from `application` or `infrastructure`. Infrastructure never imports from `application`. This boundary is enforced by convention and visible in every module's import block.

## Consequences

### Positive

- **Domain logic is testable in isolation.** Any domain service can be instantiated and called with a DataFrame constructed in a test — no database fixture required.
- **SQL is findable.** All queries are in repository classes under `infrastructure/repositories/`. When a query needs tuning, there is exactly one place to look.
- **New pipeline processes follow a template.** Adding a process means creating a command class, wiring existing or new domain services, and adding a repository method if new SQL is needed. The pattern is consistent across all 18 processes.
- **The Rust hot-path integrates cleanly.** `pacioli_core` (PyO3) is imported inside domain services (`CustomerPortfolioEnricherService`) as an optional dependency. Infrastructure and application layers have no knowledge of Rust; the boundary is fully contained within the domain.
- **Failures are easier to diagnose.** Stack traces cross layer boundaries in a predictable order: orchestrator → command → domain service → repository. The layer that raised the exception identifies the category of problem (SQL error vs. transformation bug vs. orchestration issue).

### Negative / Trade-offs

- **More files for simple operations.** A straightforward pipeline step that loads rows, filters them, and writes them back requires at least three classes across three directories. This verbosity can feel disproportionate for small tasks.
- **The `logic/staging/reconciliation/` sub-layer adds a fourth level.** The reconciliation matchers and strategies sit under `logic/staging/` rather than `logic/domain/`, creating a parallel subtree that does not map cleanly to the three-layer model. This was a pragmatic decision to isolate the most complex matching logic into its own namespace.
- **Unit of Work must be threaded manually.** Every command class that needs DB access must receive and pass the UoW explicitly. There is no dependency injection container — wiring is done by the orchestrator calling `__init__` with the session. This is simple but verbose.
- **No formal interface (ABC) enforcement.** The dependency rule is enforced by convention and code review, not by Python abstract base classes or a linter rule. A developer can import a repository from a domain service without a tooling error.
