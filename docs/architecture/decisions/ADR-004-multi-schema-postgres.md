# ADR-004: Five-Schema PostgreSQL Layout

## Status
Accepted

## Context

PACIOLI has two distinct runtime components that touch the database independently and for different purposes:

- The **data pipeline** (Python) reads raw source files, transforms them, runs matching algorithms, and writes results to staging tables. It runs once per day on a schedule or on analyst demand.
- The **Node.js API** serves the frontend, manages user sessions, records analyst decisions, and exports approved reconciliations to the Gold Layer. It runs continuously.

Early in development, all tables lived in a single `public` schema. As the system grew, this created three practical problems:

**Ownership ambiguity.** It was not clear from the schema which tables belonged to the pipeline, which to the API, and which to the downstream RPA robot. A developer modifying a pipeline process had no structural signal that `transaction_workitems` was an API-owned table they should not be writing to directly.

**Access control was coarse.** A single schema means a single privilege boundary. Granting the pipeline service account write access to raw and staging tables also granted it implicit access to auth and session tables — a privilege it should never need.

**Conceptual separation was missing.** Raw file extracts, staging/matching results, approved postings, authentication state, and ETL configuration are five distinct concerns with different lifecycles, different writers, and different consumers. Treating them as one flat namespace obscured these boundaries.

## Decision

The database is divided into five schemas within a single PostgreSQL cluster and a single database (`pacioli`):

| Schema | Owner | Purpose |
|--------|-------|---------|
| `biq_raw` | Pipeline | Raw file extracts written by the 10 RAW loaders. Never modified after initial load. Read by pipeline staging processes. |
| `biq_stg` | Pipeline + API | Staging tables: bank transactions, customer portfolio, card details, card settlements, parking breakdown, withholdings, and audit records. Pipeline writes; API reads and updates reconcile/work status. |
| `biq_gold` | API | Approved posting records consumed by the RPA robot: `gold_headers`, `gold_details`, `gold_diffs`. API writes on Gold export; RPA reads and updates `rpa_status`. |
| `biq_auth` | API | Users, JWT sessions, transaction workitems, reversal requests, assignment rules. Pipeline never touches this schema. |
| `biq_config` | Pipeline + API | ETL processing windows, assignment configuration, process metrics. Pipeline writes metrics; API reads windows and rules. |

Each schema is created with `CREATE SCHEMA` and service accounts are granted only the privileges their component requires:
- The pipeline account has read/write on `biq_raw`, `biq_stg`, `biq_config`; no access to `biq_auth` or `biq_gold`.
- The API account has read on `biq_stg`, `biq_config`; read/write on `biq_auth`, `biq_gold`; and targeted update rights on `biq_stg` (reconcile_status, work_status transitions).

All five schemas live in the same PostgreSQL instance. There is no microservice database isolation — this is one cluster, one database, five schemas. Cross-schema queries (e.g., the API joining `biq_stg.stg_bank_transactions` against `biq_auth.transaction_workitems`) use fully qualified `schema.table` references.

## Consequences

### Positive

- **Ownership is structurally visible.** A developer reading a query knows immediately which component owns a table from its schema prefix. `biq_auth.transaction_workitems` is an API table; `biq_raw.raw_bank_transactions` is pipeline-only.
- **Privilege separation is enforceable.** PostgreSQL schema-level `GRANT` and `REVOKE` enforce the ownership boundaries at the database level, not just by convention. The pipeline account cannot accidentally corrupt auth or gold data.
- **The RPA robot has a clean read target.** The RPA robot needs only `SELECT` on `biq_gold` and `UPDATE` on `gold_headers.rpa_status`. A single schema with a clear ownership boundary minimizes the surface area it touches.
- **Schema dumps are modular.** Each schema can be dumped and restored independently (`pg_dump --schema=biq_stg`). This is useful for moving staging data between environments without touching auth or configuration.
- **Single cluster keeps cross-schema joins fast.** Because all schemas are in the same PostgreSQL instance, joins across schema boundaries (e.g., workitems joined to bank transactions) execute locally with no network hop or distributed query planner overhead.

### Negative / Trade-offs

- **No foreign key constraints across schemas.** PostgreSQL supports cross-schema foreign keys syntactically, but enforcing them would couple schema lifecycles together. The decision was made not to define them: `biq_auth.transaction_workitems` references bank transaction IDs in `biq_stg` by value only. Referential integrity across schemas is maintained by application logic, not the database engine.
- **Both pipeline and API must agree on `biq_stg` column contracts.** `biq_stg` is a shared schema — the pipeline writes it, the API reads and updates it. A column rename or type change in a pipeline migration must be coordinated with the API. There is no schema registry or contract test to catch mismatches automatically.
- **Schema proliferation risk.** Five schemas is a reasonable number for this system's size. Adding more schemas for minor concerns (e.g., a separate `biq_reports` schema for materialized report views) would reduce clarity rather than increase it. New tables should be placed in an existing schema unless there is a strong ownership argument for a new one.
- **Single cluster is a single point of failure.** All five schemas share the same PostgreSQL process. There is no schema-level failover. If the database is unavailable, the pipeline, the API, and the RPA robot all stop simultaneously.
