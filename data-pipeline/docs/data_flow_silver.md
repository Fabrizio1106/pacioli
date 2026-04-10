# PACIOLI — Silver Layer: Data Flow

> **Layer:** Silver (Staging & Reconciliation)
> **Version:** 3.1 · Last reviewed: 2026-04-06

---

## Overview

The silver layer transforms raw, heterogeneous financial data into fully reconciled, auditable records. The pipeline runs as a single daily job divided into seven phases. Data flows strictly left to right: `biq_raw` → `biq_stg`. No gold-layer tables are written during silver processing.

```
biq_raw                    biq_stg                       biq_config
─────────────────          ──────────────────────        ─────────────────────
raw_sap_cta_239            stg_sap_transactions          etl_process_windows
raw_fbl5n                  stg_bank_transactions         etl_process_metrics
raw_banco_transactions  →  stg_customer_portfolio    →   etl_batch_log
raw_diners_vouchers        stg_card_settlements
raw_guayaquil_vouchers     stg_card_details
raw_pacificard_vouchers    stg_withholdings
raw_retenciones_sri        stg_manual_requests
raw_manual_requests        stg_parking_pay_breakdown
raw_databalance
raw_webpos
```

---

## Pre-Check: Period Window Guarantee

**Trigger:** Before any data is loaded.

**Actor:** `SmartPeriodClosure.ensure_periods_exist()`

**Steps:**
1. Query `biq_config.etl_process_windows` for PENDING windows.
2. If no PENDING window exists for any transactional process, create the next month's windows.
3. Never evaluate closure at this stage — RAW tables are still empty.

**Output:** At least one PENDING window exists for each transactional process (`SAP_TRANSACTIONS`, `DINERS_CARDS`, `GUAYAQUIL_CARDS`, `PACIFICARD_CARDS`, `PARKING_BREAKDOWN`).

**Business Rule:** The pipeline will not start staging without an active period. This prevents processing data with no associated accounting period.

---

## Phase 0: RAW Loading

**Trigger:** Always runs, regardless of prior completion status.

**Input:** Files in the configured input folders, one per source system.

| Loader | Input | Raw Table Written |
|---|---|---|
| SapLoader | SAP 239 export (Excel/CSV) | `raw_sap_cta_239` |
| FBL5NLoader | SAP FBL5N export | `raw_fbl5n` |
| BancoLoader | Bank statement file | `raw_banco_transactions` |
| DinersClubLoader | Diners voucher file | `raw_diners_vouchers` |
| GuayaquilLoader | Guayaquil voucher file | `raw_guayaquil_vouchers` |
| PacificardLoader | Pacificard voucher file | `raw_pacificard_vouchers` |
| DatabalanceLoader | Databalance export | `raw_databalance` |
| RetencionesLoader | SRI withholding file | `raw_retenciones_sri` |
| ManualRequestsLoader | Manual payment request file | `raw_manual_requests` |
| WebposLoader | Webpos transaction file | `raw_webpos` |

**Behavior:**
- Each loader independently checks if input files are present. Missing files are silently skipped (non-fatal).
- A loader failure does not abort the pipeline — remaining loaders continue.
- All loaders use YAML schema configs from `config/schemas/`.

---

## Phase 1 (Group 1): Foundations

### 1A — SAP Staging

**Command:** `ProcessSAPStagingCommand`
**Window:** Reads the PENDING window from `etl_process_windows['SAP_TRANSACTIONS']`.

**Input:** `raw_sap_cta_239`, `raw_fbl5n`, `raw_banco_transactions`

**Steps:**

1. **Extract** raw SAP records filtered to `window_start ≤ doc_date ≤ window_end`.
2. **Transform** — `SAPTransformer` maps raw column names to staging schema per `staging_sap_rules.yaml`.
3. **Intraday Compensation Detection** — `IntradayCompensationDetector` identifies ZR documents offset by a same-day, same-reference, same-amount non-ZR document. Marks `is_compensated_intraday = True`.
4. **Bank Enrichment** — `BankEnricher` joins SAP records to bank records on `bank_ref_1`:
   - Level 1: Exact join.
   - Level 2: Suffix smart match (SAP `"438649"` → bank `"1538438649"`).
   - Populates `bank_date`, `bank_ref_2`, `bank_description`, `bank_office_id`.
5. **Classification** — `TransactionClassifier` assigns `trans_type` (LIQUIDACION TC, TRANSFERENCIA DIRECTA, etc.) and `brand` for card transactions.
6. **Historical Context** — `HistoricalContextService.build_context()` reads `stg_sap_transactions` to compute the maximum existing sequence counter per `(brand, amount)` key.
7. **Hash Generation** — `HashGenerator` generates `match_hash_key`:
   - Pacificard with valid batch: `BRAND_BATCH_AMOUNT_SEQ` (e.g., `PACIFICARD_001032_49.68_1`)
   - All others: `BRAND_AMOUNT_SEQ` (e.g., `VISA_23.66_1`)
8. **Compensation Handler** — `SAPCompensationHandler` assigns final `reconcile_status`:
   - `is_compensated_sap = True` → `CLOSED_IN_SOURCE_SAP`
   - `is_compensated_intraday = True` → `COMPENSATED_INTRADAY`
   - Neither → `PENDING`
9. **Reconciliation Propagation** — `ReconciliationPropagationService` propagates any existing reconciliation state from previous runs to newly loaded records.
10. **Write** to `stg_sap_transactions` via `UnitOfWork`.

**Output:** `stg_sap_transactions` — normalized, enriched, classified SAP records with `match_hash_key` and `reconcile_status`.

**Business Rules:**
- Only PENDING documents proceed to bank reconciliation.
- CLOSED_IN_SOURCE_SAP documents are stored for hash sequence context but excluded from active matching.
- Hash counters must always be ≥ 1 (v2.1 fix prevents `_0` suffix).

---

### 1B — Portfolio CDC

**Command:** `ProcessPortfolioCDCCommand`

**Input:** External portfolio snapshot (Excel or API), current `stg_customer_portfolio`.

**Steps:**
1. Extract the incoming portfolio snapshot.
2. Compute delta against the current staging snapshot using MD5 hash comparison.
3. Apply inserts / updates / soft-deletes atomically.

**Output:** `stg_customer_portfolio` updated to reflect the latest portfolio snapshot.

---

### 1C — Customer Portfolio Phase 1

**Command:** `ProcessCustomerPortfolioCommand(phase=1)`

**Input:** Updated `stg_customer_portfolio`, `stg_sap_transactions`.

**Steps:**
1. Load the base portfolio snapshot.
2. Apply internal netting: SAP debit and credit pairs that cancel each other out are removed from the reconcilable balance.
3. Compute `conciliable_amount` per invoice.
4. Write enrichment metadata (ETL batch, hash).

**Output:** `stg_customer_portfolio` — Phase 1 complete; invoices have `conciliable_amount` and are ready for matching.

---

## Phase 2 (Group 2): Card Settlements

**Commands:** `ProcessDinersCommand`, `ProcessGuayaquilCommand`, `ProcessPacificardCommand`
**Windows:** Each card process reads its own PENDING window.

For each card processor, the flow is identical:

**Steps:**

1. **Extract** voucher records from the raw table filtered to the process window.
2. **Transform** — Source-specific transformer (Diners/Guayaquil/Pacificard) normalizes the voucher file to the canonical voucher schema.
3. **Aggregate** — `CardAggregator` groups vouchers by `settlement_id + brand + establishment_name + fecha_pago`:
   - Sums all amount columns (gross, net, commission, IVA, IRF).
   - Generates `match_hash_base`:
     - Pacificard: `BRAND_BATCH_NET` (e.g., `PACIFICARD_001447_178.80`)
     - Others: `BRAND_NET` (e.g., `DINERS CLUB_141.96`)
   - Generates `etl_hash` for deduplication.
4. **Counter Append** — `CardRepository` reads `card_hash_counters` and appends the sequence counter to produce the final `match_hash_key`.
5. **Write** settlement headers to `stg_card_settlements` and individual voucher details to `stg_card_details`.

**Output:**
- `stg_card_settlements` — one record per settlement, with aggregated amounts and status = `PENDING`.
- `stg_card_details` — individual voucher records linked to their settlement.

**Business Rule:** `settlement_id` is the primary matching key for card reconciliation. Its format differs by processor (Pacificard: `SVI_602_0202`; Guayaquil: `68893-0202`).

---

## Phase 3 (Group 3): Parking Breakdown

**Command:** `ProcessParkingBreakdownCommand`
**Window:** Reads the PENDING window for `PARKING_BREAKDOWN`.

**Input:** `stg_card_details` (already processed), `stg_card_settlements`.

**Note:** This phase reads from **staging**, not raw. It derives data from already-processed card data.

**Steps:**
1. Extract PARKING vouchers from `stg_card_details` joined to `stg_card_settlements` for the window period.
2. Filter by `establishment_name = 'PARKING'` (configurable in `staging_parking_breakdown_rules.yaml`).
3. Aggregate by `settlement_date + settlement_id + batch_number + brand`, summing amounts and counting vouchers.
4. Generate `match_hash_key = BRAND_BATCH_AMOUNT` (e.g., `VISA_000602_1234.56`).
5. Generate `etl_hash` including `settlement_date` to prevent collisions from recycled batch numbers.
6. Write to `stg_parking_pay_breakdown`.

**Output:** `stg_parking_pay_breakdown` — per-batch parking breakdown records used by the `UrbaparkingStrategy` during reconciliation.

---

## Phase 4 (Group 4): Tax Withholdings

### 4A — Process Withholdings

**Command:** `ProcessWithholdingsCommand`

**Input:** `raw_retenciones_sri`

**Steps:**
1. Extract raw SRI withholding records.
2. Transform via `WithholdingsTransformer` (normalize column names, clean amounts).
3. Write to `stg_withholdings`.

---

### 4B — Match Withholdings

**Command:** `MatchWithholdingsCommand`

**Input:** `stg_withholdings`, `stg_customer_portfolio`

**Steps:**
1. For each withholding record, find the matching portfolio invoice by document reference and amount (within tolerance).
2. Assign `match_status` to each withholding.

---

### 4C — Apply Withholdings

**Command:** `ApplyWithholdingsCommand`

**Input:** Matched withholding records from Step 4B.

**Steps:**
1. `WithholdingApplicationService.apply()` for each matched withholding:
   - Retrieve `conciliable_amount` from the portfolio invoice.
   - Apply withholding: `amount_after = amount_before − valor_ret_iva`.
   - Persist the updated `conciliable_amount` via `WithholdingsOperationsRepository`.
2. Return `ApplicationResult` with pre/post amounts for audit logging.

**Output:** `stg_customer_portfolio` — invoices with withholding applied, `conciliable_amount` reduced by the withheld tax.

---

## Phase 5 (Group 5): Advanced Portfolio

### 5A — Manual Requests

**Command:** `ProcessManualRequestsCommand`

**Input:** `raw_manual_requests`, `stg_bank_transactions`

**Steps:**
1. Extract manual request records.
2. Transform via `ManualRequestsTransformer`.
3. `ManualRequestEnricher` applies a fixed reference-to-customer map: bank transactions matching a manual request reference receive a pre-assigned `customer_id` with confidence 100.
4. Write enriched records to `stg_manual_requests` and update linked `stg_bank_transactions`.

**Output:** `stg_bank_transactions` — transactions matched via manual request have `enrich_customer_id` populated (Phase 0 enrichment complete).

---

### 5B — Bank Enrichment

**Command:** `ProcessBankEnrichmentCommand`

**Input:** `stg_bank_transactions` (unresolved records), enrichment data sources.

This is the most complex enrichment step. It processes only transactions **not already enriched** by Phase 0.

**Enrichment Cascade (in priority order):**

| Phase | Enricher | Method | Confidence |
|---|---|---|---|
| 1 | Card Brand Rules | `trans_type = LIQUIDACION TC` + brand YAML map | 100 |
| 2 | Specific Text Rules | Exact `bank_ref_2` text match (YAML) | 100 |
| 3 | Cash Deposit Logic | `CashDepositEnricher` — VIP price recognition + sequence analysis (Strategy V7) | 85–95 |
| 4 | Settlement Enricher | `settlement_id` linkage from card data | 100 |
| 5 | Smart Heuristic | `SmartHeuristicEnricher` — classify ref → search historical → search master | 70–98 |

**SmartHeuristicEnricher detail:**
1. Classify `bank_ref_2` as ACCOUNT_NUMBER (numeric) or CLIENT_NAME (text), or AMBIGUOUS.
2. For ACCOUNT_NUMBER: exact lookup in historical collection dataset → returns `customer_id` if found (confidence 98).
3. For CLIENT_NAME: fuzzy name match vs. historical dataset (threshold 90, 75 for names < 25 chars).
4. Fuzzy match vs. historical `referencia_2` field (threshold 70).
5. Fallback: fuzzy match vs. SAP customer master (threshold 88, 75 for truncated names).
6. Uses `rapidfuzz` if available; falls back to `difflib.SequenceMatcher`.

**Output:** `stg_bank_transactions` — `enrich_customer_id`, `enrich_customer_name`, `enrich_method`, `enrich_confidence` populated for all resolved transactions.

---

### 5C — Customer Portfolio Phase 2

**Command:** `ProcessCustomerPortfolioCommand(phase=2)`

**Input:** `stg_customer_portfolio`, `stg_card_settlements`, `stg_bank_transactions`, `raw_webpos`.

**Steps:**
1. `CustomerPortfolioEnricherService` executes the VIP and Parking cascade:
   - **Webpos Matching:** Match portfolio invoices to Webpos transactions by amount and date.
   - **VIP Invoice Matching:** For VIP/ASISTENCIAS invoices, search for matching settlement using `pacioli_core.find_invoice_combination` (Rust, optional) or pure Python. Assigns `settlement_id`.
   - **Parking Invoice Matching:** For PARKING invoices, use `pacioli_core.fuzzy_batch_match` (Rust, optional) to match batch references.
   - **Financial Distribution:** For multi-invoice matches, distribute the settlement amount proportionally across the matched invoices.
2. Mark matched invoices as `reconcile_status = 'ENRICHED'`, `reconcile_group = 'VIP_CARD'` or `'PARKING_CARD'`.
3. Flag ambiguous matches as `is_suggestion = TRUE` (reported in notes but excluded from automatic totals).

**Output:** `stg_customer_portfolio` — VIP and Parking invoices linked to `settlement_id`, with financial breakdown populated.

---

### 5D — Customer Portfolio Phase 3

**Command:** `ProcessCustomerPortfolioCommand(phase=3)`

**Input:** Remaining unmatched portfolio invoices from Phase 2.

**Steps:**
1. Apply residual matching for invoices not resolved in Phase 2.
2. Finalize `reconcile_status` for all portfolio records.

**Output:** `stg_customer_portfolio` — Phase 3 complete; all resolvable invoices have `settlement_id` and financial data.

---

## Phase 6 (Group 6): Final Reconciliation

### 6A — Bank Reconciliation

**Command:** `ReconcileBankTransactionsCommand`

**Input:** `stg_bank_transactions` (status = PENDING or REVIEW), `stg_customer_portfolio` (invoices with `conciliable_amount`).

This is the **core reconciliation step**. It matches bank transactions to portfolio invoices.

**Steps:**

1. **Card Settlements (Phase 0):**
   - `CardMatcher.reconcile_card_settlements()` processes all `LIQUIDACION TC` transactions.
   - For each `settlement_id`, aggregate confirmed portfolio invoices (`is_suggestion = FALSE`).
   - Compare `amount_total` (bank) vs. `amount_net` (portfolio, confirmed):
     - `diff ≤ $0.01` → `MATCHED / CARD_PERFECT_MATCH`
     - `diff ≤ $0.05` → `MATCHED / CARD_AMOUNT_WITHIN_TOLERANCE`
     - `diff ≤ $0.10` → `REVIEW / CARD_AMOUNT_MISMATCH_SMALL`
     - `diff > $0.10` → `REVIEW / CARD_AMOUNT_MISMATCH_LARGE`
     - No confirmed invoices → `PENDING / CARD_NO_PORTFOLIO_DATA`

2. **Standard Transactions (Phases 1–3):**
   For each remaining bank transaction:
   - Fetch the customer's open invoices from `stg_customer_portfolio`.
   - Apply `ReconciliationMatcherService.find_best_match()`:
     - **Deterministic:** Exact Single → Exact Contiguous Multi → Tolerance Single.
     - **Probabilistic:** Greedy → Subset Sum (no gaps) → Subset Sum (gaps, penalized) → Best Effort (REVIEW).
   - Persist result: `reconcile_status`, `reconcile_reason`, `match_confidence_score`, linked `port_ids`.

3. **Special Cases:**
   - `MultiPaymentStrategy`: For customers with ≥ 2 payments within 24h, groups payments and finds the optimal assignment using permutation (≤ 3 payments) or two-pass greedy with backtracking.
   - `SalasVIPStrategy`: Two-phase VIP cash deposit matching (greedy 1-to-1 + split detection). Enforces mono-user constraint.
   - `UrbaparkingStrategy`: Exact-match greedy for customer 400419 using parking breakdown records.

4. **Residuals:**
   - `ResidualsReconciliationStrategy`: A second pass over still-PENDING transactions using a ±3-day date window and up to 5 invoices.

**Output:** `stg_bank_transactions` — all records have a final `reconcile_status` (MATCHED / REVIEW / PENDING) and `reconcile_reason`. Linked `port_ids` recorded.

---

### 6B — Update Bank Validation Metrics

**Command:** `UpdateBankValidationMetricsCommand`

**Input:** `stg_bank_transactions`, `stg_card_settlements`, `stg_customer_portfolio`.

**Steps:**
1. Synchronize `stg_card_settlements.reconcile_status` based on the status of linked bank transactions (if any linked transaction is REVIEW → settlement = REVIEW).
2. Update voucher count metrics per settlement.
3. Compute and persist validation KPIs (match rate, pending rate, review rate).

**Output:** `stg_card_settlements` — status synchronized; `stg_bank_transactions` — validation metrics updated.

---

### 6C — Validate Portfolio Matches

**Command:** `ValidatePortfolioMatchesCommand`

**Input:** `stg_bank_transactions`, `stg_customer_portfolio`.

**Steps:**
1. Cross-reference reconciled bank transactions against portfolio invoices.
2. Identify orphan bank records (matched to a `port_id` that no longer exists in the portfolio).
3. Identify unmatched portfolio invoices with `conciliable_amount > 0` that have no linked bank transaction.
4. Flag amount discrepancies between `amount_total` (bank) and the sum of matched `conciliable_amount` (portfolio).
5. Write validation flags and diagnostic notes.

**Output:** `stg_bank_transactions`, `stg_customer_portfolio` — validation flags and discrepancy notes populated.

---

### 6D — Restore Approved Transactions

**Command:** `RestoreApprovedTransactionsCommand`

**Input:** `stg_bank_transactions` (records with `reconcile_status` recently changed).

**Steps:**
1. Query records that were previously `MATCHED_MANUAL` (human-approved) but now show a different status (reset by a force re-run).
2. Restore `reconcile_status = 'MATCHED_MANUAL'` for all such records.

**Output:** `stg_bank_transactions` — human-approved transactions protected from automatic re-processing.

**Business Rule:** `MATCHED_MANUAL` is a terminal state. No automated process may change it without explicit human intervention.

---

## Post-Check: Period Closure

**Trigger:** After all staging groups complete successfully.

**Actor:** `SmartPeriodClosure.auto_close_periods_if_ready()`

**Steps:**
1. For each PENDING window in `etl_process_windows`:
   - Evaluate **Calendar Gate**: is today past the configured cutoff date?
   - Evaluate **SAP Business Gate** (`SAPPeriodClosureValidator`): are all SAP records for the period posted and compensated?
2. If **both locks pass**: close the period (`status = CLOSED`), log the reason.
3. `open_next_periods()`: create the next month's PENDING windows for all closed processes.

**Output:** `biq_config.etl_process_windows` — closed periods updated; new PENDING windows created.

---

## Final Report

After the post-check, the orchestrator:
1. Clears the `NewDataDetector` cache.
2. Prints a summary: total commands / successful / skipped / failed / success rate / total duration.
3. Retrieves and prints today's process metrics from `biq_config.etl_process_metrics`.
4. Exits with code `0` (success) or `1` (failure).

---

## Data State Diagram

```
RAW (biq_raw)
    │
    │  Phase 0: Loaders
    ▼
RAW Tables
    │
    │  Phase 1A: SAP Staging
    ▼
stg_sap_transactions
    status: PENDING / COMPENSATED_INTRADAY / CLOSED_IN_SOURCE_SAP
    │
    │  Phase 1C, 5C, 5D: Portfolio Phases
    ▼
stg_customer_portfolio
    status: RAW → NETTING → ENRICHED
    │
    │  Phase 2: Card Commands
    ▼
stg_card_settlements + stg_card_details
    status: PENDING
    │
    │  Phase 3: Parking
    ▼
stg_parking_pay_breakdown
    │
    │  Phase 4: Withholdings
    ▼
stg_withholdings
    status: UNMATCHED → MATCHED
    │
    │  Phase 5B: Bank Enrichment
    ▼
stg_bank_transactions
    enrich_customer_id, enrich_confidence populated
    │
    │  Phase 6A: Bank Reconciliation
    ▼
stg_bank_transactions
    status: PENDING → MATCHED / REVIEW / PENDING
    reconcile_reason, match_confidence_score set
    │
    │  Phase 6B-6D: Validation, Restore
    ▼
Final State: All records with deterministic status
```

---

## Key Business Rules Summary

| Rule | Where Enforced |
|---|---|
| Only PENDING SAP documents enter reconciliation | `SAPCompensationHandler` |
| Hash sequence counters must be ≥ 1 | `HashGenerator` (v2.1 fix) |
| Card settlements processed before general transactions | `ReconcileBankTransactionsCommand` Phase 0 ordering |
| URBAPARKING requires exact match (no tolerance) | `UrbaparkingStrategy`, `reconciliation_config.yaml` |
| VIP deposits belong to one closure only | `SalasVIPStrategy` mono-user constraint |
| Best-effort matches always yield REVIEW (never MATCHED) | `ProbabilisticMatcher.find_best_effort_match()` |
| MATCHED_MANUAL is terminal — protected from re-runs | `RestoreApprovedTransactionsCommand` |
| Period closure requires both calendar and SAP gate | `SmartPeriodClosure` dual-lock |
| Portfolio exclusion prefixes N2-, N3-, N4- are ignored | `reconciliation_config.yaml` portfolio_exclusions |
| Withholding reduces `conciliable_amount` before reconciliation | `ApplyWithholdingsCommand` (Group 4 runs before Group 6) |
| Idempotency: skip if COMPLETED and no new RAW batch | `SilverLayerOrchestrator._run_process()` |
