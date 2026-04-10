# PACIOLI — Reconciliation Engine Reference

> **Layer:** Silver (Staging & Reconciliation)
> **Component:** `logic/staging/reconciliation/` + `pacioli_core`
> **Version:** 2.4.1 · Last reviewed: 2026-04-06

---

## 1. Purpose

The reconciliation engine matches bank transactions in `stg_bank_transactions` against open portfolio invoices in `stg_customer_portfolio`. Its design is inspired by enterprise reconciliation platforms (BlackLine) and must handle:

- **High volume:** Thousands of transactions per daily run.
- **Heterogeneous data:** Multiple card brands, cash deposits, wire transfers, SPI transfers.
- **Ambiguity:** Many-to-many relationships between payments and invoices.
- **Auditability:** Every decision is recorded with a reason code and confidence score.

---

## 2. Entry Point

```
ReconcileBankTransactionsCommand.execute()
    │
    ├── CardMatcher.reconcile_card_settlements()      # Phase 0: LIQUIDACION TC
    │
    ├── ReconciliationMatcherService.find_best_match()  # Per-customer loop
    │       ├── DeterministicMatcher
    │       ├── ProbabilisticMatcher
    │       └── SpecialCasesMatcher
    │
    ├── MultiPaymentStrategy                          # Phase 3: same-day groups
    ├── SalasVIPStrategy                              # Phase 4.5: VIP cash
    ├── UrbaparkingStrategy                           # Phase 4.5: parking
    └── ResidualsReconciliationStrategy               # Phase 5: relaxed window
```

---

## 3. Transaction Classification

Before matching, bank transactions are classified by `trans_type`:

| trans_type | Matched By | Notes |
|---|---|---|
| `LIQUIDACION TC` | CardMatcher (Phase 0) | Credit card settlements. Settlement_id is the primary key. |
| `TRANSFERENCIA DIRECTA` | ReconciliationMatcherService | Standard wire transfer. |
| `TRANSFERENCIA SPI` | ReconciliationMatcherService | SPI payment system. |
| `OTROS` | ReconciliationMatcherService | Other bank movements. |
| `TRANSFERENCIA EXTERIOR` | ReconciliationMatcherService | International transfers (higher tolerance multiplier). |
| `DEPOSITO EFECTIVO` | CashDepositEnricher (pre-reconciliation) | Classified before matching; routed by enriched customer_id. |

---

## 4. Phase 0: Card Matcher

**Handles:** All transactions where `trans_type = 'LIQUIDACION TC'`.

**Algorithm:**

For each pending bank transaction with a `settlement_id`:

1. Query `stg_customer_portfolio` for confirmed invoices (`is_suggestion = FALSE`, `reconcile_group IN ('VIP_CARD', 'PARKING_CARD')`) grouped by `settlement_id`.
2. Compare `bank.amount_total` vs. `portfolio.amount_net` (confirmed only).
3. Assign status and reason:

| Condition | Status | Reason |
|---|---|---|
| No confirmed invoices | PENDING | `CARD_NO_PORTFOLIO_DATA` |
| `diff ≤ $0.01` | MATCHED | `CARD_PERFECT_MATCH` |
| `suggestions > 0 AND diff > tolerance` | REVIEW | `CARD_HAS_SUGGESTIONS` |
| `diff ≤ $0.05` | MATCHED | `CARD_AMOUNT_WITHIN_TOLERANCE` |
| `$0.05 < diff ≤ $0.10` | REVIEW | `CARD_AMOUNT_MISMATCH_SMALL` |
| `diff > $0.10` | REVIEW | `CARD_AMOUNT_MISMATCH_LARGE` |

**Key design decision:** CardMatcher runs exclusively on `LIQUIDACION TC`. All other `trans_type` values are processed by the general matcher. This prevents card-specific logic from leaking into the standard matching flow.

---

## 5. Deterministic Matcher

**Strategy 1 — Exact Single (confidence ~100):**
- Find one invoice where `|bank_amount - conciliable_amount| = 0`.
- O(n) scan.
- Reason: `PERFECT_MATCH`.

**Strategy 2 — Exact Contiguous Multi (confidence ~92):**
- Find a contiguous window of invoices (sorted oldest-first) whose sum equals `bank_amount` exactly.
- Sliding window O(n·k) where k = `max_invoices` (default 20).
- Stops accumulating when sum exceeds target (pruning).
- Reason: `PERFECT_MATCH`.

**Strategy 3 — Tolerance Single (confidence ~95):**
- Find one invoice within `±$0.05` (configurable).
- Exact matches excluded (handled by Strategy 1).
- Reason: `PENNY_ADJUSTMENT`.

**Validation threshold:** A deterministic match is accepted only if `total_score ≥ 85`.

---

## 6. Probabilistic Matcher

Applied when no deterministic match is found. Strategies cascade in order; the first to produce a `MATCHED`-quality result wins.

**Strategy 4 — Greedy Sequential:**
- Accumulate invoices from oldest to newest until the target is reached (within tolerance).
- Delegates to `SubsetSumSolver.find_contiguous_sum()`.
- Result: MATCHED if score ≥ 90.
- Reason: `FIFO_INVOICE_MATCH`.

**Strategy 5 — Subset Sum (contiguous):**
- Same algorithm as Strategy 4 but exposes the result to gap-penalty scoring.
- Result: MATCHED if score ≥ 90.

**Strategy 6 — Subset Sum (with gaps, max_gap = 3):**
- Searches non-contiguous invoice combinations via `itertools.combinations`.
- Complexity bounded by `max_combinations_to_try` (default 5,000).
- Only runs if input has ≤ 25 invoices (performance guard).
- **Gap penalty:** each skipped invoice position reduces the score by 5, up to a maximum penalty of 15.
- Result: MATCHED if post-penalty score ≥ 90, else REVIEW.
- Reason: `SUBSET_SUM_MATCH`.

**Strategy 7 — Best Effort:**
- Returns the closest possible combination regardless of tolerance.
- **Always forces status = REVIEW** — no automatic approval regardless of score.
- Reason: `BEST_EFFORT_MATCH`.

---

## 7. Scoring Engine

Every match result carries a composite score (0–100):

| Component | Weight | Description |
|---|---|---|
| Exact amount match | 40% | `bank_amount == sum(invoice_amounts)` exactly |
| Tolerance match | 30% | `|bank_amount - sum| ≤ tolerance` |
| Date proximity | 15% | Temporal distance between payment date and invoice dates |
| Invoice continuity | 10% | No gaps in the invoice sequence index |
| Reference field match | 5% | `bank_ref_1` or `bank_ref_2` appears in invoice references |

**Score classification:**

| Range | Status |
|---|---|
| ≥ 90 | `MATCHED` |
| 60–89 | `REVIEW` |
| < 60 | `PENDING` |

---

## 8. Special Cases

### 8.1 Multi-Payment Strategy

**Trigger:** Customer has ≥ 2 bank payments within a 24-hour window (configurable).

**Algorithm:**

1. Detect multi-payment groups via `detect_multi_payments()` (groups by `enrich_customer_id` + date proximity).
2. If ≤ 3 payments and ≤ 20 invoices: try **permutation assignment** (all possible payment-to-invoice-subset combinations, up to `max_permutations = 1000`).
3. If > 3 payments or > 20 invoices: **two-pass greedy with backtracking**:
   - Pass 1: Assign payments highest-to-lowest; check for unmatched payments.
   - Pass 2: Assign payments lowest-to-highest (backtracking).
   - Accept the pass with fewer unmatched payments.

**Output:** A list of payment-to-invoice assignments, each with its own status and confidence score.

### 8.2 Salas VIP Strategy (v11.2)

**Trigger:** `enrich_customer_id = '999999'` (VIP cash deposits).

**Key constraint:** Each cash deposit belongs to exactly one VIP service closure. Splits must all belong to the same closure.

**Two-phase algorithm:**

- **Phase 1 (Greedy 1:1):** For each invoice (FIFO), find a single deposit that matches exactly within tolerance. Confirmed matches are locked.
- **Phase 2 (Split Detection):** For unmatched invoices, search for deposit combinations (up to `max_deposits_per_split = 3`) within a `split_time_window_minutes = 10` window.

**Validation:** A Phase 1 match is accepted if the match rate across the invoice set ≥ `min_match_rate = 0.70`.

**Smart reassignment:** If a Phase 1 match can be reallocated to enable a Phase 2 split that closes a different invoice, the reassignment is applied if it improves total match coverage.

### 8.3 URBAPARKING Strategy

**Trigger:** `enrich_customer_id = '400419'`.

**Algorithm:**
- Exact match required (`tolerance = 0.00`).
- Greedy sequential: for each bank transaction, find the parking breakdown batch record with the same `match_hash_key` (format `BRAND_BATCH_AMOUNT`).
- Residuals (`ResidualsReconciliationStrategy`) are applied after the greedy pass for any unmatched amounts.

---

## 9. Residuals Strategy

**Trigger:** Runs after all primary strategies as a final pass on still-PENDING transactions.

**Parameters:**
- `max_date_window_days = 3` — invoices within ±3 days of the payment date.
- `max_invoices = 5` — maximum invoices per combination.

This relaxed window catches payments made slightly outside the standard date range due to weekends, holidays, or delayed posting.

---

## 10. Rust Extension: pacioli_core

Used inside `CustomerPortfolioEnricherService` (Portfolio Phase 2) for computationally intensive matching.

### `find_invoice_combination(amounts, indices, target, tolerance, max_invoices)`

Solves the subset-sum problem for matching a settlement amount to a combination of invoices.

| Invoice count | Algorithm | Complexity |
|---|---|---|
| 1 | Linear scan | O(n) |
| 2 | HashMap with complement lookup | O(n) |
| 3 | Two-pointer on sorted array | O(n²) |

**Arithmetic:** All amounts converted to integer centavos (`× 100`, rounded) before comparison. This eliminates floating-point equality errors (e.g., `$36.00 = 3600 centavos`).

**Pre-filtering:** Invoices with `amount > target + tolerance` are discarded before the search begins. In practice this eliminates 60–80% of candidates.

### `fuzzy_batch_match(inv_batch, inv_ref, inv_amount, v_batches, v_refs, v_amounts, v_indices, threshold, tolerance)`

Finds the best-matching voucher for a given invoice using Jaccard bigram similarity.

**Algorithm:**
1. Build bigram set for the query invoice once: `O(k)` where k = string length.
2. Pre-filter voucher candidates by amount: `|v_amount - inv_amount| ≤ tolerance`. Typically eliminates > 90% of candidates.
3. For each surviving candidate, compute Jaccard similarity on batch and ref bigrams: `O(k)` per candidate.
4. Return the index of the best match if score ≥ threshold, else None.

**Why Jaccard over SequenceMatcher:**
- SequenceMatcher: O(n·m) per pair (longest common subsequence).
- Jaccard bigrams: O(n+m) per pair.
- For short strings (4–12 chars typical in batch/ref fields), correlation with edit distance is > 0.85.
- Rust's stack-allocated `HashSet<(char, char)>` is significantly faster than Python's heap-allocated set.

---

## 11. Configuration Reference

All tunable parameters are in `config/rules/reconciliation_config.yaml`.

| Parameter | Default | Description |
|---|---|---|
| `general.tolerance_threshold` | 0.05 | Amount tolerance in dollars for most matchers |
| `general.max_invoices_per_match` | 15 | Maximum invoices in one combination |
| `general.max_combinations_to_try` | 5000 | Timeout protection for combinatorial search |
| `general.recent_invoices_days` | 90 | Only consider invoices from the last N days |
| `confidence_thresholds.auto_match_minimum` | 90 | Minimum score for automatic MATCHED status |
| `confidence_thresholds.review_minimum` | 60 | Minimum score for REVIEW (below = PENDING) |
| `salas_vip_matching.min_match_rate` | 0.70 | Minimum fraction of VIP invoices that must match |
| `salas_vip_matching.max_deposits_per_split` | 3 | Maximum deposits per VIP split |
| `residuals_reconciliation.max_date_window_days` | 3 | Date window for residual matching |
| `vip_tolerance_cents` | 5 | Centavo tolerance for VIP commission absorption |

---

## 12. Audit Trail

Every reconciliation decision is persisted in `stg_bank_transactions`:

| Column | Description |
|---|---|
| `reconcile_status` | Final status: MATCHED / REVIEW / PENDING / MATCHED_MANUAL |
| `reconcile_reason` | Coded reason: e.g., `PERFECT_MATCH`, `CARD_AMOUNT_WITHIN_TOLERANCE` |
| `match_confidence_score` | Numeric score 0–100 |
| `enrich_notes` | Human-readable explanation of the reconciliation decision |
| `enrich_customer_id` | Matched customer identifier |
| `enrich_method` | Enrichment method used (e.g., `HISTORICAL_REF_MATCH`) |

---

## 13. Reason Code Taxonomy

Reason codes follow a naming convention that encodes the decision tier:

| Prefix | Tier | Example |
|---|---|---|
| `PERFECT_MATCH` | Deterministic | Exact 1:1 amount match |
| `PENNY_ADJUSTMENT` | Deterministic | Single invoice within $0.05 |
| `CARD_*` | Card Matcher | All card settlement reasons |
| `FIFO_INVOICE_MATCH` | Probabilistic | Greedy sequential |
| `SUBSET_SUM_MATCH` | Probabilistic | Non-contiguous invoice combination |
| `PARKING_BATCH_MATCH` | Special Case | URBAPARKING batch match |
| `MULTI_PAYMENT_MATCH` | Special Case | Multi-payment assignment |
| `BEST_EFFORT_MATCH` | Last Resort | Closest approximation (REVIEW only) |
| `CLOSED_IN_SOURCE_SAP` | Shadow Match | SAP-compensated, auto-closed |
| `AUTO_OFFSETTING_ENTRY` | Shadow Match | Intraday compensation |
| `REQUIRES_HUMAN_VALIDATION` | Unresolved | No automated match found |
