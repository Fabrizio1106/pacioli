# ADR-002: Rust Extension for Combinatorial Hot-Path

## Status
Accepted

## Context

The portfolio enrichment step must find which SAP invoices a card settlement is paying. This requires searching for combinations of 1–3 invoices that sum to a target amount within a small tolerance. In the worst case — a customer with a large open invoice backlog and a 90-day lookback — the candidate pool can reach hundreds of invoices.

A pure Python implementation of this search presented two problems:

**Correctness:** Floating-point arithmetic in Python makes equality comparisons on sums of decimal amounts unreliable. `0.1 + 0.2 != 0.3` in IEEE 754. Financial data requires exact cent-level comparison. Python's `decimal.Decimal` solves this but is significantly slower than native float arithmetic.

**Performance:** Python's nested-loop combinatorial search for 3-invoice combinations is O(n³) naïve, or O(n²) with a two-pointer optimization — but even the optimized form is slow in CPython because every iteration pays the interpreter overhead: GIL acquisition, dynamic type dispatch, reference counting on temporary objects. With a 300-invoice pool and 90-day lookback, this was a measurable bottleneck in the pipeline's 18-process runtime.

A similar performance issue existed for fuzzy reference matching: the enricher must score an invoice reference against hundreds of voucher references when exact matching fails. Python's `difflib.SequenceMatcher` runs O(n×m) per pair, which compounds across the full batch.

## Decision

A Rust extension module (`pacioli_core`) was written and integrated via PyO3. It exports exactly two functions to Python:

- **`find_invoice_combination`** — finds up to 3 invoices summing to a target amount using integer cent arithmetic and a three-tier strategy (O(n) single, O(n) HashMap pair, O(n²) two-pointer trio).
- **`fuzzy_batch_match`** — scores an invoice reference against all voucher candidates using Jaccard bigram similarity, with an amount pre-filter that eliminates > 90% of candidates before any string comparison runs.

The integration is opt-in: `CustomerPortfolioEnricherService` wraps the import in a try/except block. If `pacioli_core` is not compiled or not installed, the service falls back to Python implementations (`itertools.combinations` for subset search, `difflib.SequenceMatcher` for fuzzy matching). The pipeline runs correctly either way; Rust provides the performance, not the correctness.

PyO3 was chosen as the bridge because it generates the standard CPython C-extension ABI with no runtime daemon, no socket, and no serialization. Python lists are converted to Rust `Vec<f64>` at the call boundary; the result is converted back to a Python list or `None`. There is no intermediate format.

`maturin` was chosen as the build tool because it handles the ABI suffix naming (`pacioli_core.cpython-3XX-win_amd64.pyd`), the `site-packages` installation, and the `Cargo.toml` / `pyproject.toml` coordination in a single command (`maturin develop --release`).

The release profile is set to maximum optimization: `opt-level = 3`, `lto = true`, `codegen-units = 1`, `panic = "abort"`. These settings trade compile time for runtime speed — appropriate because the extension is compiled once and run thousands of times per pipeline execution.

## Consequences

### Positive

- **Integer arithmetic eliminates rounding errors.** All amounts are converted to `i64` cents at the Rust boundary. Comparisons are exact. The `$0.01` tolerance becomes `1 cent` — no floating-point drift.
- **The hot-path runs at native speed.** The two-pointer loop for 3-invoice combinations executes without GIL, without Python object allocation, and without interpreter overhead. For large invoice pools this is the difference between a sub-second result and a multi-second stall per customer.
- **The fallback keeps the pipeline operational.** If Rust is not available (e.g., a new developer has not compiled the extension), the pipeline degrades gracefully. No process fails; only throughput is affected.
- **The boundary is contained in one domain service.** No other layer knows about Rust. Application commands and infrastructure repositories are unaffected. Replacing `pacioli_core` with a different implementation would require changing only `CustomerPortfolioEnricherService`.

### Negative / Trade-offs

- **Build step required.** Every developer and every deployment environment must compile the extension with `maturin develop --release` before running the pipeline. This adds a Rust toolchain dependency (≥ 1.75) and `maturin` (≥ 1.7, < 2.0) to the setup requirements.
- **Platform-specific binary.** The compiled `.pyd` / `.so` is not portable across operating systems or Python minor versions. The extension must be recompiled when upgrading Python or moving between Windows and Linux.
- **Maximum 3 invoices in combination.** The Rust function handles only 1–3 invoice combinations. Cases requiring 4 or more always fall back to Python. This covers the vast majority of real settlements but is a hard ceiling.
- **No test suite for the Rust code.** The extension has no `#[cfg(test)]` unit tests in `lib.rs`. Correctness is validated only through the end-to-end pipeline runs and the Python-level integration test coverage (if any).
- **Two languages to maintain.** Developers working on enrichment logic must be able to read Rust to understand what the hot-path actually does, and must recompile after any change to `lib.rs`.
