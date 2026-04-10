# Rust Core (pacioli_core)

## Table of Contents

1. [Why Rust](#why-rust)
2. [find_invoice_combination](#find_invoice_combination)
3. [fuzzy_batch_match](#fuzzy_batch_match)
4. [Performance](#performance)
5. [How to Build](#how-to-build)

---

## Why Rust

The portfolio enrichment step must find invoice combinations that sum to a target settlement amount. With hundreds of invoices per customer and a 90-day lookback window, the number of possible 2- and 3-invoice combinations grows quickly. A Python implementation using nested loops or `itertools.combinations` becomes unacceptably slow at scale — the GIL, dynamic dispatch, and object allocation overhead compound for tight inner loops over numeric data.

Rust was chosen for three reasons:
- **Zero-cost abstractions** — the compiled two-pointer loop runs at native speed with no garbage collection pauses.
- **Integer arithmetic** — amounts are converted to `i64` cents at the boundary, eliminating floating-point comparison errors that plague financial rounding.
- **PyO3 bridge** — Rust functions are exposed to Python as a native extension module with no serialization overhead. Python calls them exactly like any other function.

### How the PyO3 Bridge Works

PyO3 is a Rust crate that generates the C-extension ABI that CPython expects. The `#[pyfunction]` attribute on a Rust function and `#[pymodule]` on the registration block produce the same binary interface as a C extension written by hand.

`maturin` handles compilation and installation: it compiles the Rust crate in release mode, wraps the output `.dll` with the correct Python ABI suffix (`pacioli_core.cpython-3XX-win_amd64.pyd` on Windows), and installs it into the active Python environment's `site-packages`. After that, Python imports it as a regular module:

```python
from pacioli_core.pacioli_core import find_invoice_combination, fuzzy_batch_match
```

No data is copied across a network or serialized to JSON. Python passes its native lists directly; PyO3 converts them to Rust `Vec<f64>` / `Vec<String>` on entry and converts the result back to a Python `list` or `None` on return.

---

## find_invoice_combination

### The Problem

When a card settlement arrives for, say, $1,500.00, the pipeline must find which SAP invoices the customer is paying. Most payments cover 1–3 invoices. The function searches the available invoice pool and returns the indices of the combination that sums to the target amount within a tolerance.

**Example:**

Settlement amount: `$1,500.00`, tolerance: `$0.01`

| Index | Invoice ref | Amount |
|-------|-------------|--------|
| 0 | INV-2026-0041 | $850.00 |
| 1 | INV-2026-0038 | $650.00 |
| 2 | INV-2026-0035 | $400.00 |
| 3 | INV-2026-0029 | $900.00 |

`find_invoice_combination([850, 650, 400, 900], [0,1,2,3], 1500.00, 0.01, 3)`

- Case 1 (single): no invoice equals $1,500.00 exactly → skip.
- Case 2 (pairs): $850 + $650 = $1,500.00 → **match found at indices [0, 1]**.

Result: `[0, 1]` — the function returns on the first match found.

### Algorithm

The function runs three progressively more expensive strategies, short-circuiting as soon as a match is found:

**All arithmetic is performed in integer cents** (`i64`). Every dollar amount is multiplied by 100 and rounded to avoid floating-point comparison errors. A tolerance of `$0.01` becomes `1 cent`.

Invoices larger than `target + tolerance` are pre-filtered — they can never be part of a valid combination.

**Case 1 — Single invoice, O(n):**  
Linear scan. If any invoice amount is within tolerance of the target, return it immediately. This is the most common case in production data.

**Case 2 — Two invoices, O(n):**  
HashMap complement lookup. For each invoice with value `C` cents, the required complement is `target_c − C`. If the complement (±tolerance) already exists in the map, the pair is found. The map is built in a single pass, making this O(n) despite searching for pairs.

**Case 3 — Three invoices, O(n²):**  
Sort the filtered invoices by amount ascending. For each element `i` (the smallest of the trio), run two pointers `lo` and `hi` over the remaining subarray to find a pair summing to `target − amounts[i]`. When `lo + hi < remaining`, advance `lo` right to increase the sum. When `lo + hi > remaining`, advance `hi` left to decrease it. Each pointer moves monotonically, so the inner loop is O(n) per outer iteration.

```
sorted: [400, 650, 850, 900]  target: 1500

i=0 (400), remaining=1100
  lo=1(650), hi=3(900) → 650+900=1550 > 1100 → hi--
  lo=1(650), hi=2(850) → 650+850=1500 → within tol → FOUND [400, 650, 850]
  (but Case 2 already found [850, 650] first — Case 3 only runs if Case 2 misses)
```

### Input / Output

```
find_invoice_combination(
    amounts:      Vec<f64>,    # invoice amounts in dollars
    indices:      Vec<usize>,  # original DataFrame row indices
    target:       f64,         # settlement amount to match
    tolerance:    f64,         # dollar tolerance (e.g. 0.01)
    max_invoices: usize,       # upper bound: 1, 2, or 3
) -> Option<Vec<usize>>        # original indices of matched invoices, or None
```

### When It Is Called

`CustomerPortfolioEnricherService` imports `find_invoice_combination` at module load time with a try/except guard:

```python
try:
    from pacioli_core.pacioli_core import find_invoice_combination as _rust_fic
    _RUST_AVAILABLE = True
except (ImportError, AttributeError):
    _RUST_AVAILABLE = False
```

It is called during the card enrichment layer (Layer 3) when matching Webpos or card settlement amounts to SAP invoice combinations. The Python fallback (DP or `itertools`) is used when `_RUST_AVAILABLE` is `False` or when `max_invoices > 3`.

### Known Limitations

- **Maximum 3 invoices.** Combinations of 4 or more fall back to Python. In practice, the vast majority of settlements cover 1–2 invoices; 3 is rare.
- **No ordering guarantee on result.** The returned index list preserves insertion order for Cases 1 and 2; Case 3 returns indices in sorted-amount order, not original DataFrame order.
- **No gap support.** The function finds any combination within the pool; it does not enforce date contiguity. Contiguity constraints, if needed, must be applied by the caller.

---

## fuzzy_batch_match

### The Problem

When exact reference matching fails (truncated strings, formatting inconsistencies between the card network files and SAP), the enricher needs to find the best-matching voucher for a given invoice by comparing text references. Python's `SequenceMatcher` computes the longest common subsequence in O(n×m) per pair. With hundreds of vouchers per settlement batch this is slow enough to affect pipeline runtime.

**Example:**

Invoice batch ref: `"031826"`, SAP ref: `"DINE438649"`  
Voucher batch refs: `["031826", "031827", "031825"]`, voucher refs: `["DINE43864", "DINE43870", "DINE43855"]`

- Pre-filter by amount: only vouchers within $0.01 of the invoice amount pass through.
- Bigrams of `"031826"`: `{(0,3),(3,1),(1,8),(8,2),(2,6)}`
- Bigrams of `"031826"` vs `"031826"`: identical → Jaccard = 1.0
- Average of batch_sim (1.0) + ref_sim → best match at index 0.

### How Jaccard Similarity Works

A bigram is a pair of consecutive characters. `"DINERS"` produces the set `{(D,I), (I,N), (N,E), (E,R), (R,S)}`.

Jaccard similarity between two sets A and B:

```
Jaccard(A, B) = |A ∩ B| / |A ∪ B|
```

A score of 1.0 means identical bigram sets; 0.0 means no overlap. The function scores each voucher as the average of its batch similarity and reference similarity against the invoice, and returns the voucher with the highest score above the threshold.

Jaccard on bigrams was chosen over Python's `SequenceMatcher` because:
- Bigram construction is O(n) per string; Jaccard intersection is O(n+m) — much faster than the O(n×m) LCS approach.
- For the short strings in this domain (4–20 characters), bigram Jaccard correlates well with human-perceived string similarity.
- The computation runs entirely in Rust with stack-allocated data for short strings.

**Amount pre-filter:** Before computing any string similarity, vouchers whose amount differs from the invoice by more than `tolerance` are skipped. In practice this eliminates > 90% of candidates, so fuzzy scoring only runs on a small subset.

### Input / Output

```
fuzzy_batch_match(
    inv_batch:  &str,          # invoice batch reference (normalized)
    inv_ref:    &str,          # invoice SAP reference (normalized)
    inv_amount: f64,           # invoice amount for pre-filtering
    v_batches:  Vec<String>,   # batch refs of all voucher candidates
    v_refs:     Vec<String>,   # SAP refs of all voucher candidates
    v_amounts:  Vec<f64>,      # amounts of all voucher candidates
    v_indices:  Vec<usize>,    # original DataFrame row indices
    threshold:  f64,           # minimum Jaccard score to accept (e.g. 0.70)
    tolerance:  f64,           # dollar tolerance for amount pre-filter (e.g. 0.01)
) -> Option<usize>             # original index of best-matching voucher, or None
```

### When It Is Called

Called from `CustomerPortfolioEnricherService` during card enrichment (Layer 3) when exact batch/reference joins fail. The same `_RUST_AVAILABLE` guard applies. If Rust is unavailable, the enricher falls back to Python's `difflib.SequenceMatcher`.

### Known Limitations

- **Single best match only.** The function returns the index of the highest-scoring voucher above the threshold. It does not return ranked alternatives.
- **No positional weighting.** Bigram Jaccard treats all character positions equally. A transposition near the end of the string scores the same as one at the start.
- **Threshold is caller-controlled.** The default threshold is 0.70. Lowering it increases recall but risks false matches; raising it increases precision but may miss legitimate fuzzy matches.
- **Empty strings.** Two empty strings return Jaccard = 1.0 by convention; one empty and one non-empty return 0.0. Callers should normalize strings before passing.

---

## Performance

No formal benchmark suite exists in the codebase. The qualitative improvements are documented in source comments and the pipeline server design:

| Scenario | Before | After |
|----------|--------|-------|
| Pipeline cold start (no server) | ~2m 20s | ~44s (Pipeline Server keeps Python warm) |
| Rust hot path vs. Python nested loops | — | Rust runs `find_invoice_combination` at native speed; no GIL contention |

The release profile in `Cargo.toml` is tuned for maximum runtime performance at the cost of longer compile time:

```toml
[profile.release]
opt-level = 3       # maximum LLVM optimization passes
lto = true          # Link Time Optimization: inlines across crate boundaries
codegen-units = 1   # single codegen unit enables full LTO (slower compile)
panic = "abort"     # no stack unwinding; smaller binary, faster panic path
```

The `lto = true` + `codegen-units = 1` combination is the most impactful setting for tight numeric loops: LLVM can inline and vectorize across the entire crate boundary, not just within a single compilation unit.

---

## How to Build

### Prerequisites

- Rust toolchain ≥ 1.75 (`rustup` recommended)
- `maturin` ≥ 1.7, < 2.0 installed in the active Python environment
- Python ≥ 3.10 active environment

### Development build (installs into active Python env)

```bash
cd data-pipeline/pacioli_core
maturin develop --release
```

`--release` is required. The debug build runs several times slower and should never be used in production. `maturin develop` compiles the crate, applies the PyO3 ABI wrapper, and installs the result directly into `site-packages` — no separate `pip install` step needed.

### What gets produced

On Windows:

```
<python-env>/Lib/site-packages/pacioli_core/
    pacioli_core.cpython-3XX-win_amd64.pyd   # compiled extension
    __init__.py                               # makes it a package
```

On Linux/macOS the extension suffix is `.so` (`pacioli_core.cpython-3XX-linux-gnu.so`).

### Verify the build

```python
from pacioli_core.pacioli_core import find_invoice_combination, fuzzy_batch_match

result = find_invoice_combination([850.0, 650.0, 400.0], [0, 1, 2], 1500.0, 0.01, 3)
print(result)  # [0, 1]
```

### Raw Rust build (no Python wrapping)

```bash
cd data-pipeline/pacioli_core
cargo build --release
```

Produces `target/release/pacioli_core.dll` (Windows) but this is not directly importable by Python. Use `maturin develop` for any Python-facing build.
