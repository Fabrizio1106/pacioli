# PACIOLI — Bronze Layer Modules

This document lists every module analyzed across the three bronze-layer
directories: `data_loaders/`, `config/schemas/` and `utils/`. Modules are
grouped by folder and described in terms of their responsibility inside the
ingestion layer.

---

## `data_loaders/`

The ingestion entry points. Each concrete loader specializes the shared
`BaseLoader` template for a specific upstream source (bank extracts, card
settlement files, SAP exports, tax withholdings, etc.).

| Module | Description |
| --- | --- |
| `data_loaders/__init__.py` | Package marker exposing the loader classes to the orchestrator. |
| `data_loaders/base_loader.py` | Abstract `BaseLoader` template-method class that standardizes configuration loading, file discovery, batch id generation, staging-based SQL load (INSERT ... ON CONFLICT DO NOTHING), temporal continuity checks and success/failed file archival. |
| `data_loaders/banco_loader.py` | `BancoLoader` for bank account statements. Strips footer rows from the first empty date, normalizes dates/amounts and generates a SHA-256 hash id using a fingerprint + intra-file duplicate rank. |
| `data_loaders/databalance_loader.py` | `DatabalanceLoader` for Databalance voucher files. Removes subtotal rows, normalizes voucher/capture/payment dates, parses all tax and commission columns and hashes voucher identifiers. |
| `data_loaders/diners_club_loader.py` | `DinersClubLoader` for Diners settlement reports. Enforces strict temporal continuity against the target table, trims records prior to the last loaded day and hashes voucher-level fields. |
| `data_loaders/fbl5n_loader.py` | `FBL5NLoader` for SAP FBL5N customer line-item exports. Snapshot loader: TRUNCATE + staging INSERT on every run. Uses an MD5 natural key built from account, invoice reference and general ledger. |
| `data_loaders/guayaquil_loader.py` | `GuayaquilLoader` for Banco de Guayaquil merchant settlements. Transparently repairs legacy `.xls` files via the Excel COM bridge before parsing. |
| `data_loaders/manual_requests_loader.py` | `ManualRequestsLoader` for operator-provided manual reconciliation requests. Snapshot loader with TRUNCATE `RESTART IDENTITY CASCADE`. |
| `data_loaders/master_data_loader.py` | Standalone `MasterDataLoader` for the customer master dimension. Writes to `biq_stg.dim_customers`, bypassing `BaseLoader`. Truncate + append with schema-aware `to_sql`. |
| `data_loaders/pacificard_loader.py` | `PacificardLoader` accepting both Excel files and Outlook `.msg` emails. Extracts Excel attachments from emails and derives the station label from the subject line. |
| `data_loaders/retenciones_loader.py` | `RetencionesLoader` for SRI tax withholding files. Strips repeated header rows, applies YAML column mapping, normalizes dates/amounts/identifiers and hashes the configured fields. |
| `data_loaders/sap_239_loader.py` | `SapLoader` for SAP account 239 line-items. Overrides `load_to_sql` to run a true UPSERT (`INSERT ... ON CONFLICT (<key>) DO UPDATE SET ... = EXCLUDED.*`) refreshing mutable SAP attributes. |
| `data_loaders/webpos_loader.py` | `WebposLoader` for Web Point-of-Sale exports. Enforces presence of `fecha` and `clave_de_acceso`, normalizes amounts and hashes payment identifiers. |

---

## `config/schemas/`

Declarative YAML configuration consumed by each loader. These files are not
Python scripts; they act as the contract between sources and the pipeline:
target table, input/output subfolders, header row, column mapping, required
columns, date columns, hash columns and upsert policies.

| Schema | Consumer loader |
| --- | --- |
| `config/schemas/banco_239_loader.yaml` | `banco_loader.BancoLoader` (principal account 239) |
| `config/schemas/databalance_loader.yaml` | `databalance_loader.DatabalanceLoader` |
| `config/schemas/diners_club_loader.yaml` | `diners_club_loader.DinersClubLoader` |
| `config/schemas/fbl5n_loader.yaml` | `fbl5n_loader.FBL5NLoader` |
| `config/schemas/guayaquil_loader.yaml` | `guayaquil_loader.GuayaquilLoader` |
| `config/schemas/manual_requests_loader.yaml` | `manual_requests_loader.ManualRequestsLoader` |
| `config/schemas/pacificard_loader.yaml` | `pacificard_loader.PacificardLoader` |
| `config/schemas/retenciones_sri_loader.yaml` | `retenciones_loader.RetencionesLoader` |
| `config/schemas/sap_239_loader.yaml` | `sap_239_loader.SapLoader` (transitional account 239) |
| `config/schemas/webpos_loader.yaml` | `webpos_loader.WebposLoader` |

Every schema minimally defines `loader_name`, `target_table`, `batch_prefix`,
`input_subfolder`, `success_subfolder`, `failed_subfolder`, `header_row` and
`column_mapping`. Loaders requiring additional behavior declare
`required_columns`, `date_columns`, `hash_columns`, `station_mapping` or
`upsert_conflict_column` as needed.

---

## `utils/`

Cross-cutting helpers used by every loader.

| Module | Description |
| --- | --- |
| `utils/parsers.py` | Defensive parsers for dates (`parse_to_sql_date`, `parse_bank_datetime`) and monetary amounts (`parse_currency`). Handles glued digits, Excel serials, SAP trailing sign and Latin/US number formats. Returns safe defaults on failure so the ETL never aborts on a single bad row. |
| `utils/data_cleaner.py` | `DataCleaner` static helpers to normalize pandas Series: string cleanup, numeric coercion, reference extraction and strict two-decimal formatting required for stable hashing. |
| `utils/text_normalizer.py` | `normalize_text` canonicalization function: NFD decomposition, accent stripping, uppercasing and whitespace collapsing for fuzzy master-data comparisons. |
| `utils/excel_normalizer.py` | Windows-only COM bridge that repairs legacy `.xls` files by opening them with Excel and resaving as `.xlsx` in the system TEMP directory. Used by `GuayaquilLoader` before parsing. |
| `utils/db_config.py` | PostgreSQL engine factory. Loads `.env`, validates credentials, builds connection strings with a schema-pinned `search_path` and exposes `get_db_engine('config' \| 'raw' \| 'stg' \| 'gold')`. Includes a `test_connection` diagnostic. |
| `utils/logger.py` | Dual-sink logging facility. Each record is written simultaneously to a local `.txt` file and to `biq_config.pacioli_logs` in PostgreSQL. Lazy DB engine, graceful degradation on backend failure and an `exception` shortcut capturing full tracebacks. |
