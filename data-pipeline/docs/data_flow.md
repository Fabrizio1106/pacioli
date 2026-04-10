# PACIOLI — Bronze Layer Data Flow

This document walks through the data flow inside the bronze layer
(`data_loaders/`, `config/schemas/`, `utils/`). It follows a single file
from the moment it lands in the drop zone to the moment it becomes a row in
the `biq_raw` (or `biq_stg`) schema of `pacioli_db`.

---

## 1. Input sources

Files are dropped into subfolders of `data_raw/` by upstream actors:

- **Banks** (Guayaquil, Cta 239 principal): Excel extracts (`.xlsx`, `.xls`).
- **Card processors** (Diners Club, Pacificard): Excel exports or Outlook
  `.msg` emails with embedded Excel attachments.
- **Payment aggregators** (Databalance, Webpos): Excel exports.
- **SAP** (account 239, FBL5N): Excel exports produced manually or by
  scheduled jobs.
- **Tax** (SRI retentions): CSV or Excel downloads.
- **Operators**: manual reconciliation request spreadsheets.
- **Master data**: customer master files (CSV or Excel).

Each source has a dedicated subfolder under `data_raw/`, configured in the
loader's YAML file via `input_subfolder`.

## 2. Discovery and bootstrap

1. The orchestrator instantiates a concrete loader with its YAML path.
2. `BaseLoader.__init__` loads the YAML, creates the logger, opens a
   PostgreSQL engine (`utils.db_config.get_db_engine('raw')`) and resolves
   the input/output directories through `config.settings.PATHS`.
3. `BaseLoader.load()` lists the candidate files in the input folder:
   extensions `.xlsx`, `.xls`, `.msg`, `.csv`, `.txt`, excluding Excel lock
   files (`~$...`). Files are processed in lexical order.

For each file, the template method `run_pipeline` is invoked.

## 3. Per-file lifecycle

### Step 1 — Read
- `read_file(file_path)` is implemented by the concrete loader.
- Most loaders use `pd.read_excel(..., dtype=str)` to preserve leading
  zeros and raw identifiers.
- `GuayaquilLoader` first repairs legacy `.xls` files via
  `utils.excel_normalizer.normalize_to_temp_xlsx`, which opens the file
  through Excel COM and rewrites it as `.xlsx` in the system TEMP folder.
- `PacificardLoader` branches between Excel and `.msg` inputs; for emails
  it uses `extract_msg` to pull the Excel attachment and derives a station
  label from the uppercased subject using the YAML `station_mapping`.
- `RetencionesLoader` supports both CSV and Excel.

### Step 2 — Enrichment
After the raw DataFrame is returned, `run_pipeline` strips whitespace from
column names and attaches three auditing columns:

- `source_file` = filename being processed.
- `batch_id` = `<prefix><n>` produced by `_get_next_batch_id`. The next
  sequence number is computed via PostgreSQL `SPLIT_PART` /
  `string_to_array` over existing batch ids; a timestamp-based fallback is
  used if the query fails.
- `loaded_at` = current timestamp.

### Step 3 — Column mapping
The YAML `column_mapping` is applied via `df.rename(columns=...)`, turning
source headers (sometimes in Spanish with spaces and punctuation) into the
canonical snake_case names expected by the target table.

### Step 4 — Business rules (`specific_business_rules`)
Loader-specific cleanup is applied. Typical operations include:

- **Row filtering**: dropping subtotal / footer rows. For example,
  `BancoLoader` truncates the DataFrame at the first empty
  `fecha_transaccion`; `DatabalanceLoader` drops rows without
  `id_databalance`; `FBL5NLoader` drops rows without an account; and
  `RetencionesLoader` removes header repetition rows.
- **Date parsing**: `utils.parsers.parse_to_sql_date` handles glued digits
  (DDMMYYYY, YYYYMMDD), separated formats, Excel serials and dirty `.0`
  suffixes; `parse_bank_datetime` covers 12h/24h bank timestamps.
- **Currency parsing**: `utils.parsers.parse_currency` auto-detects Latin
  vs US number formats (by comparing the last comma and dot positions),
  strips currency tags and handles SAP trailing sign (`"100.00-"` →
  `-100.0`). Unrecognizable values fall back to `0.0`.
- **Column trimming / defaults**: e.g. `SapLoader` defaults `sociedad` to
  `'8000'` and truncates `status_partida` to twenty characters;
  `DatabalanceLoader` backfills missing `fecha_captura` from
  `fecha_voucher`.
- **Continuity gate**: `DinersClubLoader._validate_continuity_and_filter`
  compares the last loaded date (and count at that date) against the
  incoming file. If continuity is broken, the file is rejected with a
  descriptive error.

### Step 5 — Hash generation (`generate_hash_id`)
Each loader builds a canonical fingerprint by concatenating the columns
that constitute the record's natural key, applying strict formatting where
required (`_fmt_money`, `_clean_int_str`, `_clean_decimal_str`) so that
logically equal values produce identical hashes.

When intra-file duplicates are expected (e.g. twin bank transactions),
loaders add a `duplicate_rank` (cumulative count per fingerprint) before
hashing, guaranteeing uniqueness without collapsing legitimate duplicates.

The resulting hash is stored in `hash_id` (SHA-256 in most loaders, MD5 in
`FBL5NLoader`) and temporary fingerprint columns are dropped.

### Step 6 — Temporal continuity check
`BaseLoader.check_temporal_continuity` computes the minimum date in the
DataFrame against the current `MAX(...)` in the target table. If the gap
is greater than one day, a `WARN` log is emitted (non-blocking).

### Step 7 — SQL load (`load_to_sql`)
The DataFrame is persisted using a staging + INSERT strategy:

1. A uniquely named staging table `_staging_<table>_<timestamp>` is
   materialized with `df.to_sql(..., if_exists='replace')`.
2. The staging table is merged into the target table with one of three
   strategies depending on the loader:
   - **Append-only dedup** (default in `BaseLoader`): `INSERT ... ON
     CONFLICT DO NOTHING`. Requires a UNIQUE / PK on the target.
   - **True upsert** (`SapLoader`): `INSERT ... ON CONFLICT (<col>) DO
     UPDATE SET <mutable_cols> = EXCLUDED.<mutable_cols>, loaded_at =
     CURRENT_TIMESTAMP`. The conflict column is configurable in YAML.
   - **Snapshot** (`FBL5NLoader`, `ManualRequestsLoader`): `TRUNCATE TABLE
     <target> RESTART IDENTITY CASCADE` followed by a full INSERT from
     staging.
3. On success the transaction commits and `report['loaded']` is updated.
4. The staging table is **always** dropped in a `finally` block, even on
   failure, preventing garbage from accumulating across runs.

### Step 8 — File archival
- **Success**: `move_file(is_success=True)` relocates the processed file
  into `processed_files/success/<success_subfolder>/YYYY/MM/<name>_<ts><ext>`.
- **Failure**: `move_file(is_success=False)` routes to the equivalent
  `failed/...` subtree. The original exception is re-raised so the batch
  runner can log and continue.

## 4. Logging and observability

Throughout every step, the loader writes structured log records via
`utils.logger.get_logger`:

- Stdout (colorized by severity).
- Local `.txt` file under `logs/`, named after the process and timestamp.
- PostgreSQL table `biq_config.pacioli_logs`, including `log_level`,
  `process_name`, `batch_id`, `message`, `details` (JSONB), `source_file`
  and `source_line`.

If the database is unreachable, the DB sink is silently disabled after the
first failure, preserving the file sink as the authoritative fallback.

## 5. Output

The bronze layer produces:

- **Data**: rows in `biq_raw.<target_table>` (or `biq_stg.dim_customers`
  for master data) containing the cleaned source records plus the
  auditing columns `source_file`, `batch_id`, `loaded_at` and `hash_id`.
- **File archive**: every input file is relocated into
  `processed_files/success/...` or `processed_files/failed/...` with a
  timestamped filename and year/month partitioning.
- **Logs**: persistent text files under `logs/` and queryable rows in
  `biq_config.pacioli_logs`.

These three outputs together give downstream consumers a reproducible and
fully auditable starting point for silver and gold transformations.

---

## Appendix — Quick step-by-step

```
1. Drop file in data_raw/<source_subfolder>/
2. Orchestrator instantiates loader(config.yaml)
3. BaseLoader.load() discovers files
4. For each file:
   a. read_file()                 — source-specific parsing
   b. rename columns               — YAML column_mapping
   c. specific_business_rules()    — clean + normalize
   d. generate_hash_id()           — SHA-256 / MD5 fingerprint
   e. check_temporal_continuity()  — gap warning
   f. load_to_sql()                — staging + upsert / snapshot
   g. move_file(success)           — archive with timestamp
5. Logs persisted to .txt and biq_config.pacioli_logs
```
