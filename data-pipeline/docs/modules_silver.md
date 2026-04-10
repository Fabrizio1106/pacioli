# PACIOLI — Silver Layer: Module Catalog

> **Layer:** Silver (Staging & Reconciliation)
> **Version:** 3.1 (Orchestrator) · Last reviewed: 2026-04-06

---

## 1. Entry Point

| Module | File | Description |
|---|---|---|
| Silver Orchestrator | `main_silver_orchestrator.py` | Pipeline director for the entire silver layer. Defines execution order (Groups 1–6), handles pre/post period checks, delegates to staging commands, and tracks global metrics. |

---

## 2. Application Layer — Commands (`logic/application/commands/staging/`)

Commands are the **use-case controllers**. Each command orchestrates one staging pipeline step end-to-end: it calls extractors, domain services, and repositories in the correct sequence.

| Command | File | Responsibility |
|---|---|---|
| ProcessSAPStagingCommand | `process_sap_staging.py` | Extracts raw SAP + bank data, transforms, enriches, classifies, detects compensations, generates match hashes, and writes to `stg_sap_transactions`. The most complex command in the pipeline. |
| ProcessPortfolioCDCCommand | `process_portfolio_cdc.py` | Change-Data-Capture for the customer portfolio. Detects delta between the current portfolio snapshot and the staging table, applying inserts/updates/deletes atomically. |
| ProcessCustomerPortfolioCommand | `process_customer_portfolio.py` | Three-phase portfolio enrichment (F1 / F2 / F3). Phase 1 loads the base snapshot; Phase 2 applies VIP/Parking cascade enrichment using `CustomerPortfolioEnricherService`; Phase 3 finalizes residual matching. |
| ProcessDinersCommand | `process_diners_staging.py` | Transforms Diners Club voucher files into settlement + detail records via `DinersTransformer` and `CardAggregator`. |
| ProcessGuayaquilCommand | `process_guayaquil_staging.py` | Same as Diners but for Banco Guayaquil card settlements, using `GuayaquilTransformer`. |
| ProcessPacificardCommand | `process_pacificard_staging.py` | Same as Diners but for Pacificard, using `PacificardTransformer`. Pacificard uses a unique batch-based hash strategy (`BRAND_BATCH_AMOUNT_SEQ`). |
| ProcessParkingBreakdownCommand | `process_parking_breakdown.py` | Generates per-batch parking payment breakdowns from already-processed staging data. Reads `stg_card_details`, aggregates by settlement + batch, and writes to `stg_parking_pay_breakdown`. |
| ProcessWithholdingsCommand | `process_withholdings.py` | Transforms raw SRI withholding records and loads them to `stg_withholdings`. |
| MatchWithholdingsCommand | `match_withholdings.py` | Matches staged withholding records to portfolio invoices using the configured tolerance rules. |
| ApplyWithholdingsCommand | `apply_withholdings.py` | Applies matched withholding amounts to portfolio invoices via `WithholdingApplicationService`. Reduces `financial_amount_net` and records the applied withholding. |
| ProcessManualRequestsCommand | `process_manual_requests.py` | Loads manually submitted payment requests and enriches bank transactions based on a manual reference map. |
| ProcessBankEnrichmentCommand | `process_bank_enrichment.py` | Multi-phase bank transaction enrichment: applies card brand rules → specific text rules → cash deposit logic → smart heuristic fuzzy matching. |
| ReconcileBankTransactionsCommand | `reconcile_bank_transactions.py` | Core reconciliation: executes the CardMatcher (Phase 0), then the full cascade of Deterministic → Probabilistic → Special Case matchers for all remaining transactions. |
| UpdateBankValidationMetricsCommand | `update_bank_validation_metrics.py` | Post-reconciliation step. Synchronizes card settlement statuses, updates voucher count metrics, and computes validation KPIs per settlement. |
| ValidatePortfolioMatchesCommand | `validate_portfolio_matches.py` | Cross-validates reconciliation results against the portfolio. Identifies orphans, unmatched invoices, and amount discrepancies. |
| RestoreApprovedTransactionsCommand | `restore_approved_transactions.py` | Restores previously human-approved `MATCHED_MANUAL` transactions that were inadvertently reset by a re-run. Protects manually validated state. |

---

## 3. Domain Layer — Services (`logic/domain/services/`)

Domain services contain **pure business logic**. They are decoupled from SQL and infrastructure concerns; they receive DataFrames or typed objects and return results.

### 3.1 Transformation

| Service | File | Description |
|---|---|---|
| SAPTransformer | `transformation/sap_transformer.py` | Maps raw SAP columns to staging schema; applies column-level rules from `staging_sap_rules.yaml`. |
| DinersTransformer | `transformation/diners_transformer.py` | Normalizes Diners Club voucher files to the canonical voucher schema. |
| GuayaquilTransformer | `transformation/guayaquil_transformer.py` | Normalizes Banco Guayaquil settlement files. |
| PacificardTransformer | `transformation/pacificard_transformer.py` | Normalizes Pacificard settlement files, including batch number parsing. |
| WithholdingsTransformer | `transformation/withholdings_transformer.py` | Cleans and normalizes SRI withholding records. |
| ManualRequestsTransformer | `transformation/manual_requests_transformer.py` | Normalizes manual payment request files. |

### 3.2 Enrichment

| Service | File | Description |
|---|---|---|
| BankEnricher | `enrichment/bank_enricher.py` | Joins SAP records to bank data on `bank_ref_1`. Two levels: exact join + smart suffix matching (e.g., SAP `"438649"` → bank `"1538438649"`). |
| CardEnricher | `enrichment/card_enricher.py` | Enriches card vouchers with brand, establishment metadata, and payment date normalization. |
| ManualRequestEnricher | `enrichment/manual_request_enricher.py` | Applies manual reference mappings to identify customer IDs in bank transactions. |
| SettlementEnricher | `enrichment/settlement_enricher.py` | Enriches settlement records with portfolio-level financial data. |
| CashDepositEnricher | `enrichment/cash_deposit_enricher.py` | Classifies cash deposits as URBAPARKING, SALAS VIP, or PENDING using sequence analysis (Strategy V7): detects temporal clusters and VIP price patterns. |
| SmartHeuristicEnricher | `enrichment/smart_heuristic_enricher.py` | Phase 3 (last resort) enricher. Classifies `bank_ref_2` as ACCOUNT_NUMBER or CLIENT_NAME, then cascades: historical exact ref → historical fuzzy name → historical fuzzy ref2 → SAP master fuzzy. Uses Jaccard/rapidfuzz with adaptive thresholds for truncated names. |
| SpecificTextEnricher | `enrichment/specific_text_enricher.py` | Applies deterministic text-pattern rules from `staging_bank_enrichment_rules.yaml` to assign fixed customer IDs for well-known, hard-to-match references. |

### 3.3 Compensation & Classification

| Service | File | Description |
|---|---|---|
| IntradayCompensationDetector | `compensation/intraday_compensation_detector.py` | Detects SAP ZR documents offset by same-day, same-reference, same-amount non-ZR documents. Marks them as `is_compensated_intraday = True`. |
| SAPCompensationHandler | `compensation/sap_compensation_handler.py` | Assigns `reconcile_status` based on compensation flags: `CLOSED_IN_SOURCE_SAP` → `COMPENSATED_INTRADAY` → `PENDING`. Only PENDING documents enter reconciliation. |
| TransactionClassifier | `classification/transaction_classifier.py` | Classifies SAP transactions by type (LIQUIDACION TC, TRANSFERENCIA DIRECTA, etc.) and assigns the `brand` field for card transactions. |

### 3.4 Hashing

| Service | File | Description |
|---|---|---|
| HashGenerator | `hashing/hash_generator.py` | Generates `match_hash_key` per SAP transaction. Strategy: `BRAND_BATCH_AMOUNT_SEQ` for Pacificard; `BRAND_AMOUNT_SEQ` for all others. Validates counter ≥ 1 (v2.1 fix). |
| HistoricalContextService | `hashing/historical_context_service.py` | Pre-computes `_historical_counter` by reading the maximum existing sequence per `(brand, amount)` key from staging. Ensures hash sequences continue from the last run rather than resetting to 1. |

### 3.5 Aggregation

| Service | File | Description |
|---|---|---|
| CardAggregator | `aggregation/card_aggregator.py` | Groups card vouchers into settlement summaries (`stg_card_settlements`) and prepares detail records (`stg_card_details`). Generates `match_hash_base` (without sequence counter — counter appended by `CardRepository`). |
| ParkingBreakdownService | `aggregation/parking_breakdown_service.py` | Derives parking breakdown data from `stg_card_details`. Aggregates by settlement + batch, generates `match_hash_key = BRAND_BATCH_AMOUNT`, uses `etl_hash` with settlement_date to handle batch number recycling. |

### 3.6 Reconciliation (Matching)

| Service | File | Description |
|---|---|---|
| ReconciliationMatcherService | `reconciliation_matcher_service.py` | Unified facade for all matchers. Detects special cases and cascades: Deterministic → Probabilistic → Special Cases. |
| MatchingService | `matching_service.py` | Low-level matching primitives: EXACT_SINGLE, TOLERANCE_SINGLE, MULTI_INVOICES, PARTIAL_PAYMENT. Operates on domain value objects. |
| InvoiceMatcherService | `invoice_matcher_service.py` | Invoice-level matching logic used by portfolio reconciliation. |
| CustomerMatcherService | `customer_matcher_service.py` | Matches bank transaction references to SAP customer identities. |

### 3.7 Portfolio

| Service | File | Description |
|---|---|---|
| CustomerPortfolioEnricherService | `customer_portfolio_enricher_service.py` | Core portfolio enrichment engine (v6.0). Performs internal netting, Webpos matching, and VIP/Parking cascade matching. Optionally uses the Rust `pacioli_core` module for combinatorial search. |
| CustomerPortfolioSyncService | `customer_portfolio_sync_service.py` | Synchronizes portfolio state between runs (CDC-aware). |
| PortfolioHashService | `portfolio_hash_service.py` | Generates ETL deduplication hashes for portfolio records. |

### 3.8 Withholdings

| Service | File | Description |
|---|---|---|
| WithholdingApplicationService | `withholding_application_service.py` | SQL-free domain service. Retrieves pre-withholding amount, applies the deduction via repository, computes post-withholding balance, and returns a typed `ApplicationResult`. |
| WithholdingValidatorService | `withholding_validator_service.py` | Validates withholding records against business rules before matching. |

### 3.9 Period Management & Observability

| Service | File | Description |
|---|---|---|
| SmartPeriodClosure | `smart_period_closure.py` | Dual-lock period closure. PRE-CHECK: `ensure_periods_exist()` — creates windows only, never closes. POST-CHECK: `auto_close_periods_if_ready()` — evaluates Calendar Gate + SAP Gate using real loaded data. |
| SAPPeriodClosureValidator | `sap_period_closure_validator.py` | Validates the SAP business gate: checks if all SAP records for a period have been posted and compensated before allowing closure. |
| NewDataDetector | `new_data_detector.py` | Detects new RAW batches to decide whether to re-run a staging process or skip it (idempotency optimization). Compares last processed batch_id against available RAW batches. |
| ProcessMetricsTracker | `process_metrics_tracker.py` | Domain service tracking process lifecycle (PENDING → RUNNING → COMPLETED/FAILED). Contains `ProcessExecution` value object and SLA compliance logic. |
| HashCounterCacheManager | `hash_counter_cache_manager.py` | In-memory cache for hash sequence counters, reducing repeated DB reads during batch hash generation. |

### 3.10 Reconciliation Propagation & Synchronization

| Service | File | Description |
|---|---|---|
| ReconciliationPropagationService | `reconciliation_propagation_service.py` | Propagates reconciliation status changes from bank transactions to linked portfolio records. |
| ReconciliationReasonSynchronizer | `reconciliation_reason_synchronizer.py` | Keeps `reconcile_reason` consistent across bank and portfolio tables after status changes. |
| SmartPeriodClosure | `smart_period_closure.py` | (see Period Management above) |

---

## 4. Domain Layer — Value Objects (`logic/domain/value_objects.py`)

| Object | Description |
|---|---|
| `BankTransaction` | Typed representation of a staged bank transaction. |
| `Invoice` | Typed representation of a portfolio invoice. |
| `Match` | Result of a successful transaction-to-invoice match, carrying status, reason, and confidence score. |

---

## 5. Infrastructure Layer — Extractors (`logic/infrastructure/extractors/`)

Extractors are **read-only** database adapters. Each extractor encapsulates the SQL queries for one data domain.

| Extractor | Source Table(s) |
|---|---|
| SAPExtractor | `raw_sap_cta_239`, `raw_fbl5n` |
| BankExtractor | `raw_banco_transactions` |
| DinersExtractor | `raw_diners_vouchers` |
| GuayaquilExtractor | `raw_guayaquil_vouchers` |
| PacificardExtractor | `raw_pacificard_vouchers` |
| WithholdingsExtractor | `raw_retenciones_sri` |
| ManualRequestsExtractor | `raw_manual_requests` |
| DatabalanceExtractor | `raw_databalance` |
| CustomerPortfolioExtractor | `stg_customer_portfolio` (current snapshot) |
| BankReconciliationExtractor | `stg_bank_transactions`, `stg_customer_portfolio` (joined views for matching) |

---

## 6. Infrastructure Layer — Repositories (`logic/infrastructure/repositories/`)

Repositories are **write** adapters. They encapsulate all `INSERT`, `UPDATE`, and `DELETE` operations, keeping SQL out of the domain layer.

| Repository | Managed Table |
|---|---|
| BaseRepository | Abstract base; provides shared session and bulk-update helpers. |
| BankRepository | `stg_bank_transactions` (base writes) |
| BankTransactionRepository | `stg_bank_transactions` (status, reason, match fields) |
| BankEnrichmentRepository | `stg_bank_transactions` (enrichment fields only — update-only) |
| BankReconciliationRepository | `stg_bank_transactions`, `stg_customer_portfolio` (reconciliation result writes) |
| CardRepository | `stg_card_settlements`, `stg_card_details`, `card_hash_counters` |
| CustomerPortfolioRepository | `stg_customer_portfolio` |
| InvoiceRepository | `stg_customer_portfolio` (invoice-level reads/writes) |
| ManualRequestsRepository | `stg_manual_requests` |
| ParkingBreakdownRepository | `stg_parking_pay_breakdown` |
| WithholdingsRepository | `stg_withholdings` |
| WithholdingsOperationsRepository | `stg_withholdings`, `stg_customer_portfolio` (cross-table withholding operations) |
| ProcessMetricsRepository | `biq_config.etl_process_metrics`, `biq_config.etl_process_windows` |

---

## 7. Infrastructure Layer — Transaction Management

| Module | File | Description |
|---|---|---|
| UnitOfWork | `infrastructure/unit_of_work.py` | Context manager implementing the Unit of Work pattern. Provides a single shared session with auto-commit / auto-rollback semantics. All repositories are lazy-loaded through it. |
| BatchTracker | `infrastructure/batch_tracker.py` | Manages batch lifecycle (PENDING → RUNNING → COMPLETED / FAILED). Provides idempotency via configuration fingerprinting. Records execution duration and record counts. |

---

## 8. Reconciliation Engine — Matchers (`logic/staging/reconciliation/matchers/`)

| Matcher | Description |
|---|---|
| DeterministicMatcher | Strategies executed in confidence order: Exact Single → Exact Contiguous Multi → Tolerance Single. All yield `MATCHED` with score ≥ 90. |
| ProbabilisticMatcher | Cascade: Greedy Sequential → Subset Sum (contiguous) → Subset Sum (with gaps, penalty applied) → Best Effort (always `REVIEW`). |
| CardMatcher | Specialized for `LIQUIDACION TC` transactions. Aggregates portfolio by `settlement_id`, distinguishes confirmed invoices from suggestions, assigns `MATCHED` / `REVIEW` / `PENDING` with card-specific reasons. |
| SpecialCasesMatcher | Handles URBAPARKING (greedy exact) and Salas VIP (two-phase) as isolated strategies. |
| ScoringEngine | Computes a weighted confidence score (0–100) from: amount match (40%), tolerance match (30%), date proximity (15%), invoice continuity (10%), reference match (5%). |

---

## 9. Reconciliation Engine — Strategies (`logic/staging/reconciliation/strategies/`)

| Strategy | Description |
|---|---|
| SubsetSumSolver | Core combinatorial solver. Three algorithms: contiguous sum (O(n·k)), subset with gaps (itertools + gap penalty), best approximation. Complexity-bounded by `max_combinations_to_try`. |
| MultiPaymentStrategy | Groups same-customer payments within a 24h window. Uses permutation assignment (≤ 3 payments) or two-pass greedy with backtracking (larger groups) to find the optimal payment-to-invoice assignment. |
| SalasVIPStrategy | Two-phase VIP cash matching (v11.2): Phase 1 greedy 1-to-1; Phase 2 split detection. Enforces mono-user constraint and FIFO priority. |
| UrbaparkingStrategy | Exact-match greedy strategy for URBAPARKING (customer ID 400419). No tolerance — requires exact batch-to-invoice correspondence. |
| ResidualsReconciliationStrategy | Matches residual (unreconciled) transactions after all primary strategies have run, using a relaxed date window (±3 days, up to 5 invoices). |

---

## 10. Reconciliation Engine — Utilities (`logic/staging/reconciliation/utils/`)

| Utility | Description |
|---|---|
| `amount_helpers.py` | `is_exact_match`, `is_within_tolerance`, `sum_amounts`, `calculate_diff`. Uses integer centavo arithmetic internally to avoid floating-point errors. |
| `date_helpers.py` | `parse_date`, date proximity helpers. |

---

## 11. Configuration Rules (`config/rules/`)

| File | Governs |
|---|---|
| `reconciliation_config.yaml` | Global reconciliation: tolerance, scoring weights, confidence thresholds, matching strategies (phases 0–3), special case parameters, card settlement reasons and messages, audit settings. |
| `staging_bank_reconciliation_rules.yaml` | Business rules for `ReconcileBankTransactionsCommand`: shadow match reasons, portfolio exclusion prefixes, URBAPARKING customer ID. |
| `staging_bank_enrichment_rules.yaml` | Enrichment rules: card brand-to-customer mappings, specific text rules, heuristic thresholds (98/90/88/70), VIP prices, cash deposit classifier parameters. |
| `staging_sap_rules.yaml` | Column mapping from raw SAP schema to staging schema. |
| `staging_diners_rules.yaml` | Column mapping and normalization rules for Diners Club files. |
| `staging_guayaquil_rules.yaml` | Column mapping and normalization rules for Banco Guayaquil files. |
| `staging_pacificard_rules.yaml` | Column mapping and normalization rules for Pacificard files, including batch number extraction. |
| `staging_withholdings_rules.yaml` | Column mapping for SRI withholding records. |
| `staging_withholdings_matcher_rules.yaml` | Matching tolerance and field rules for the withholdings matcher. |
| `staging_manual_requests_rules.yaml` | Column mapping for manual request files. |
| `staging_customer_portfolio_rules.yaml` | Portfolio loading, CDC detection, and enrichment rules. |
| `staging_parking_breakdown_rules.yaml` | Extraction filters and aggregation rules for the parking breakdown service. |
| `staging_historical_training_loader_rules.yaml` | Rules for loading the historical enrichment training dataset. |
| `staging_master_data_loader_rules.yaml` | Rules for loading SAP master data (customer master, etc.). |

---

## 12. Rust Extension — pacioli_core

| Module | Location | Description |
|---|---|---|
| `pacioli_core` | `pacioli_core/src/lib.rs` | PyO3-based native extension. Exports two functions to Python: `find_invoice_combination` (subset sum with two-pointer, O(n²) worst case) and `fuzzy_batch_match` (Jaccard bigram similarity with amount pre-filter). Used as an optional performance accelerator in `CustomerPortfolioEnricherService`. |

---

## 13. Loaders (`logic/loaders/`)

| Module | Description |
|---|---|
| `historical_collections_data_processor.py` | Processes and loads the historical collections training dataset used by `SmartHeuristicEnricher` for fuzzy name matching. |
