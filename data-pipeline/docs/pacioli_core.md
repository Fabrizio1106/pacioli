# PACIOLI — pacioli_core: Rust Extension Reference

> **Module:** `pacioli_core`
> **Location:** `pacioli_core/src/lib.rs`
> **Built with:** PyO3 0.23 + Maturin
> **Version:** 0.1.0 · Last reviewed: 2026-04-06

---

## 1. Purpose

`pacioli_core` is a native Python extension written in Rust. It accelerates two computationally expensive operations that appear in the hot path of `CustomerPortfolioEnricherService`:

1. **Subset sum matching** — finding the combination of invoices that sums to a payment amount.
2. **Fuzzy batch matching** — finding the best-matching voucher reference using string similarity.

Both operations have Python fallbacks; `pacioli_core` is **optional**. When not installed, the system continues to function correctly but is slower.

---

## 2. Build and Installation

```bash
cd pacioli_core
maturin develop --release
```

The `--release` flag activates the production compiler profile:

| Profile Setting | Value | Effect |
|---|---|---|
| `opt-level` | 3 | Maximum LLVM optimization |
| `lto` | true | Link Time Optimization (cross-crate inlining) |
| `codegen-units` | 1 | Single compilation unit — enables full LTO |
| `panic` | abort | No stack unwinding — smaller binary, faster panics |

**Python import:**
```python
from pacioli_core.pacioli_core import find_invoice_combination
from pacioli_core.pacioli_core import fuzzy_batch_match
```

---

## 3. Exported Functions

### 3.1 `find_invoice_combination`

```python
def find_invoice_combination(
    amounts: list[float],    # invoice amounts (candidates)
    indices: list[int],      # original DataFrame indices
    target: float,           # payment amount to match
    tolerance: float,        # dollar tolerance (e.g., 0.01)
    max_invoices: int,       # max invoices in combination (1, 2, or 3)
) -> list[int] | None:
```

Returns the list of original DataFrame indices whose amounts sum to `target ± tolerance`, or `None` if no combination is found.

#### Algorithm

**Integer arithmetic (centavos):**
All amounts are converted to `i64` centavos before comparison. This eliminates floating-point rounding errors in equality checks.

```
$36.00  →  3600 centavos
$1.01   →  101 centavos
$0.01   →  1 centavo (tolerance)
```

**Pre-filter:**
Invoices with `amount > target + tolerance` are removed before the search. They can never contribute to a valid combination.

**Case 1 — One invoice:** Linear scan O(n). Checks `|c - target_c| ≤ tol_c` for each candidate. This is the most common case in real data.

**Case 2 — Two invoices:** HashMap with complement lookup O(n).
For each invoice with value `c`, the complement is `target_c - c`. If the complement (±tolerance) is already in the map, a match is found in O(1).

**Case 3 — Three invoices:** Two-pointer on sorted array O(n²).
- Sort candidates ascending.
- For each element `i` (smallest), use two pointers `lo` and `hi` on the remainder to find a pair summing to `target_c - c_i`.
- Pruning: if `c_i > target_c`, break (all subsequent elements are larger).
- Total: O(n log n) sort + O(n²) loop.

**Comparison vs. naïve approaches:**

| Combinations | Naïve | pacioli_core |
|---|---|---|
| 1 invoice | O(n) | O(n) |
| 2 invoices | O(n²) | O(n) |
| 3 invoices | O(n³) | O(n²) |

---

### 3.2 `fuzzy_batch_match`

```python
def fuzzy_batch_match(
    inv_batch: str,           # invoice batch (normalized)
    inv_ref: str,             # invoice reference (normalized)
    inv_amount: float,        # invoice amount
    v_batches: list[str],     # all voucher batches
    v_refs: list[str],        # all voucher references
    v_amounts: list[float],   # all voucher amounts
    v_indices: list[int],     # original DataFrame indices
    threshold: float,         # minimum Jaccard score (e.g., 0.70)
    tolerance: float,         # amount tolerance (e.g., 0.01)
) -> int | None:
```

Returns the original DataFrame index of the best-matching voucher, or `None` if no match exceeds `threshold`.

#### Algorithm

**Bigram construction:**
A bigram is a pair of consecutive characters. Example: `"DINERS"` → `{('D','I'), ('I','N'), ('N','E'), ('E','R'), ('R','S')}`.

The query invoice's bigram sets are built once before the main loop.

**Amount pre-filter:**
For each voucher, `|v_amount - inv_amount| > tolerance` → skip immediately.
In practice this eliminates > 90% of candidates, making the Jaccard computation run on a tiny fraction of the input.

**Jaccard similarity:**

```
Jaccard(A, B) = |A ∩ B| / |A ∪ B|
             = |A ∩ B| / (|A| + |B| - |A ∩ B|)
```

The score for each candidate is the average Jaccard similarity of the batch and reference bigram sets:
```
score = (batch_sim + ref_sim) / 2
```

**Best match selection:**
The candidate with the highest `score > threshold` is returned. If no candidate exceeds `threshold`, returns `None`.

**Why Jaccard bigrams over SequenceMatcher:**

| Aspect | SequenceMatcher (Python stdlib) | Jaccard bigrams (Rust) |
|---|---|---|
| Complexity | O(n·m) per pair | O(n+m) per pair |
| Sensitivity | LCS-based (order-aware) | Set-based (order-independent) |
| Truncation handling | Poor (penalizes truncated names heavily) | Better (shared characters still score) |
| Speed | Interpreted | Compiled, stack-allocated sets |
| Typical string length | 4–20 chars (batch/ref) | Same |

---

## 4. Internal Helpers (not exported to Python)

### `bigrams(s: &str) -> HashSet<(char, char)>`

Converts a string to its character bigram set.

```rust
"DINERS" → {('D','I'), ('I','N'), ('N','E'), ('E','R'), ('R','S')}
```

Empty string or single character → empty set.

### `jaccard_score(a, b) -> f64`

Computes `|A ∩ B| / |A ∪ B|`. Special cases:
- Both empty → `1.0` (identical empty strings).
- One empty → `0.0` (no similarity possible).

---

## 5. Python Fallback Pattern

```python
try:
    from pacioli_core.pacioli_core import find_invoice_combination as _rust_fic
    from pacioli_core.pacioli_core import fuzzy_batch_match as _rust_fbm
    _RUST_AVAILABLE = True
except (ImportError, AttributeError):
    _rust = None
    _RUST_AVAILABLE = False
```

When `_RUST_AVAILABLE = False`, `CustomerPortfolioEnricherService` uses Python implementations with `itertools.combinations` (subset sum) and `difflib.SequenceMatcher` (fuzzy matching). These are functionally identical but 5–20× slower on large datasets.

---

## 6. Usage Context

`pacioli_core` is called exclusively from `CustomerPortfolioEnricherService` during Portfolio Phase 2:

- `find_invoice_combination`: Used to match a settlement amount to a combination of 1–3 invoices in the VIP and Parking matching cascades.
- `fuzzy_batch_match`: Used to match a parking voucher's batch reference against a set of settlement records when the batch number is truncated or slightly different from the stored value.

Both functions receive pre-normalized, pre-filtered inputs from the Python layer. The Rust layer performs only the combinatorial or similarity computation — no database access, no schema awareness.

---

## 7. Constraints and Limitations

- Maximum combination size: 3 invoices (`max_invoices` parameter). For combinations of 4+ invoices, the Python `SubsetSumSolver` with `itertools.combinations` is used instead.
- Input validation: empty `amounts` or mismatched `amounts`/`indices` lengths return `None` immediately.
- Integer overflow: `i64` centavos supports amounts up to ~$92 trillion. Not a practical constraint.
- Thread safety: Both functions are stateless and safe to call from multiple threads (though the Python GIL means this is not exploited in the current implementation).
