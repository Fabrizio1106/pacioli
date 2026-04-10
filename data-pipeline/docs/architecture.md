# PACIOLI — Bronze Layer Architecture

This document describes the architecture of the bronze layer of the PACIOLI
data pipeline. It covers the three directories `data_loaders/`,
`config/schemas/` and `utils/`, their role inside the pipeline, the way they
interact with other layers and the design decisions that shape the layer.

---

## 1. Role in the pipeline

The bronze layer is the **ingestion boundary** between heterogeneous source
systems and the PACIOLI data warehouse. It is responsible for bringing raw
business data into PostgreSQL in a format that is:

- **Traceable** — every record carries `source_file`, `batch_id`,
  `loaded_at` and a deterministic `hash_id`.
- **Idempotent** — re-running a file never produces duplicates thanks to the
  natural-key based hash and the database-level uniqueness constraints.
- **Auditable** — successful and failed files are archived into timestamped
  success/failed folders partitioned by year and month.
- **Fail-soft** — defensive parsers keep the ETL running through dirty rows
  and the logger records every anomaly both in a local `.txt` and in the
  `biq_config.pacioli_logs` table.

Downstream layers (silver/gold) never read the source files directly; they
only consume the normalized tables stored under the `biq_raw` and `biq_stg`
schemas produced here.

## 2. Layer responsibilities

The bronze layer owns three concerns, each mapped to one directory:

### `data_loaders/` — ingestion orchestration
Implements the **template method** pattern. `BaseLoader` defines the
canonical lifecycle (`load` → `run_pipeline` → `read_file` →
`specific_business_rules` → `generate_hash_id` → `load_to_sql` →
`move_file`), and each source-specific subclass customizes the hooks that
differ between inputs. This is the only place that knows:

- Which files to pick up and where to archive them.
- How to turn a raw file into a clean DataFrame matching the target table.
- How to hash records to guarantee idempotent loads.
- Which SQL loading strategy applies (INSERT ON CONFLICT DO NOTHING,
  INSERT ON CONFLICT DO UPDATE, or TRUNCATE + INSERT snapshot).

### `config/schemas/` — declarative source contracts
A YAML file per loader that acts as the single source of truth for
per-source wiring:

- `loader_name`, `target_table`, `batch_prefix`.
- `input_subfolder`, `success_subfolder`, `failed_subfolder`.
- `header_row`, `column_mapping`, `required_columns`, `date_columns`.
- Optional `hash_columns`, `station_mapping`, `upsert_conflict_column`.

Keeping this contract out of the code base means new sources can be
onboarded by adding a subclass of `BaseLoader` plus a YAML file, without
touching existing loaders.

### `utils/` — cross-cutting building blocks
Reusable primitives shared by every loader:

- **Parsers** (`parsers.py`) — dirty-data tolerant date and money parsers.
- **DataCleaner** (`data_cleaner.py`) — vectorized helpers for pandas
  Series, including the strict two-decimal formatter required for stable
  hashing.
- **Text normalizer** (`text_normalizer.py`) — canonicalization used by
  master-data loaders for fuzzy matching.
- **Excel normalizer** (`excel_normalizer.py`) — COM-based repair pass for
  legacy `.xls` files produced by upstream systems.
- **DB config** (`db_config.py`) — schema-aware SQLAlchemy engine factory
  with a pinned `search_path` per layer.
- **Logger** (`logger.py`) — dual-sink structured logger (file + DB).

## 3. Interaction with other layers

```
┌─────────────┐   discovery   ┌──────────────┐   transform   ┌────────────┐
│  data_raw/  │──────────────►│ data_loaders │──────────────►│  biq_raw   │
│ (drop zone) │               │  (BaseLoader │               │ (bronze)   │
└─────────────┘               │   template)  │               └────────────┘
                               └─────┬────────┘
                                     │ config
                                     ▼
                              ┌──────────────┐
                              │ config/      │
                              │ schemas/     │
                              │ (YAML)       │
                              └──────────────┘
                                     │ utilities
                                     ▼
                              ┌──────────────┐
                              │   utils/     │
                              │  parsers,    │
                              │ db_config,   │
                              │  logger, …   │
                              └──────────────┘
```

- **Upstream**: the `data_raw/` drop zone receives files from bank portals,
  payment processors, SAP exports and manual uploads. The bronze layer
  discovers them automatically.
- **Configuration**: `config/settings.py` resolves absolute paths; each
  loader's YAML under `config/schemas/` maps the file shape to the bronze
  table shape.
- **Persistence**: every loader writes to `pacioli_db` in PostgreSQL,
  targeting the `biq_raw` schema (or `biq_stg` for the master-data loader).
  Tables are refreshed through staging tables + INSERT ON CONFLICT (no-op,
  update, or snapshot) for atomicity.
- **Downstream**: silver and gold layers (not covered in this document) read
  exclusively from `biq_raw` / `biq_stg` tables. The bronze layer is their
  stable contract.
- **Observability**: the logger writes structured rows to
  `biq_config.pacioli_logs`, providing a single queryable source of truth
  for pipeline runs across every loader.

## 4. Design decisions

- **Template method over inheritance spaghetti** — `BaseLoader` encodes the
  invariant pipeline steps; subclasses only implement three abstract hooks.
  Adding a new source is a local change.
- **YAML-first configuration** — paths, column mappings and key fields live
  in YAML, keeping Python code focused on behavior and making per-source
  tuning friction-free.
- **Hash-based idempotency** — every row carries a SHA-256 (or MD5 where
  appropriate) hash computed from a canonical fingerprint. Combined with
  database UNIQUE constraints and `INSERT ... ON CONFLICT DO NOTHING`, this
  makes file reprocessing safe by construction.
- **Atomic staging loads** — instead of writing directly to the target
  table, every loader materializes a uniquely named staging table, upserts
  from it in a single transaction and drops the staging table in a
  `finally` block. This isolates concurrent runs and guarantees cleanup.
- **Three SQL strategies depending on semantics**
  - *Append-only dedup*: `BaseLoader.load_to_sql` using `ON CONFLICT DO
    NOTHING` (e.g. bank, databalance, guayaquil, pacificard, webpos,
    diners, retenciones).
  - *True upsert*: `sap_239_loader.SapLoader` uses `ON CONFLICT ... DO
    UPDATE SET ... = EXCLUDED.*` because SAP records have mutable fields.
  - *Snapshot*: `fbl5n_loader.FBL5NLoader` and
    `manual_requests_loader.ManualRequestsLoader` use `TRUNCATE ... RESTART
    IDENTITY CASCADE` followed by full INSERT, reflecting their
    point-in-time semantics.
- **Schema-aware engine factory** — a single physical database
  (`pacioli_db`) hosts every schema. `get_db_engine(layer)` returns an
  engine with the `search_path` pinned to the correct schema, replacing the
  legacy MySQL pattern of one database per layer.
- **Dual logging** — the local `.txt` file guarantees observability even
  when the DB is unreachable; the `biq_config.pacioli_logs` table makes
  logs queryable alongside the loaded data.
- **Fail-soft parsers** — `parse_to_sql_date` and `parse_currency` return
  safe defaults (`None`, `0.0`) on unrecognized input, so a single bad row
  never takes down a batch.
- **Temporal continuity checks** — loaders that model continuous time
  series (Diners, bank, SAP) check that the incoming file connects
  seamlessly to the existing DB state and log or reject gaps.

## 5. Separation of responsibilities

- `data_loaders/` owns **orchestration and per-source semantics**.
- `config/schemas/` owns **wiring and contracts**.
- `utils/` owns **primitives that no loader should reimplement**.

Any pull request that requires touching more than one of these three
directories for a single concern is a signal that the boundary is being
violated and should be reviewed carefully.
