# PACIOLI — The Complete Technical Guide

*From zero to architectural mastery in one sitting.*

> This guide is written in the style of *Grokking Algorithms*: every complex idea gets a
> simple analogy first, then the code, then the math. We trace a single bank deposit —
> a **$150.00 VIP Lounge payment** — from the moment it appears in a raw bank Excel file
> all the way to a posted SAP document. By the time you finish, you will understand
> every layer of this system.

---

## Table of Contents

- [Chapter 1: The Core Problem](#chapter-1-the-core-problem)
- [Chapter 2: The Architecture & Diagrams](#chapter-2-the-architecture--diagrams)
- [Chapter 3: The Brains — Python/Rust Pipeline](#chapter-3-the-brains--pythonrust-pipeline)
- [Chapter 4: The Database Engine](#chapter-4-the-database-engine)
- [Chapter 5: The Backend Traffic Cop — Node.js](#chapter-5-the-backend-traffic-cop--nodejs)
- [Chapter 6: The Financial Interface — React](#chapter-6-the-financial-interface--react)
- [Chapter 7: The Journey to Production](#chapter-7-the-journey-to-production)
- [Chapter 8: Maintenance, Security & Troubleshooting](#chapter-8-maintenance-security--troubleshooting)

---

# Chapter 1: The Core Problem

## What Is Bank Reconciliation?

Imagine you run a hotel. Every day, hundreds of guests pay with credit cards. Visa,
Diners, Pacificard — each network sends you a lump-sum deposit into your bank account.
But that single deposit actually represents dozens of individual transactions: a $23.50
restaurant bill, a $150.00 VIP lounge charge, a $42.10 room service order.

Your job — the **reconciliation** — is to match that lump-sum bank deposit with the
individual invoices in your accounting system (SAP). Until you do this, your books don't
balance. You can't close the month. The auditor is waiting.

**This is what PACIOLI automates.**

### The Manual Pain

Before PACIOLI, analysts opened two SAP reports side by side:

| Report | What It Shows | SAP Transaction |
|--------|--------------|-----------------|
| **FBL3N** | Bank account transactions (deposits, withdrawals) | GL line items |
| **FBL5N** | Customer invoices (what each client owes) | AR open items |

The analyst would:
1. Export both reports to Excel
2. Eyeball the bank deposit amount ($4,850.00)
3. Manually search through hundreds of invoices to find the combination that sums to $4,850.00
4. Subtract the card brand commission (1.2%)
5. Subtract withholding taxes (IVA, IRF)
6. If the numbers matched, open SAP transaction **F-28** and type everything by hand
7. Repeat for the next deposit

This took **8 hours per day** for a team of 4 analysts. Each error meant the month couldn't close.

### What PACIOLI Does Instead

PACIOLI replaces steps 1–6 with software:

```
Excel files → Python pipeline → PostgreSQL → Node.js API → React UI → SAP F-28
```

The analyst still makes the final decision ("yes, this match is correct"), but PACIOLI
does all the searching, calculating, and form-filling. A task that took 8 hours now takes
45 minutes.

### Meet Our $150 VIP Lounge Deposit

Throughout this guide, we'll follow a single transaction:

> **Banco Guayaquil** receives a credit card settlement from **Diners Club** on
> **April 8, 2026**. The deposit is **$150.00**. It corresponds to a VIP lounge charge
> by customer **ACME Corp** (SAP code `C-4521`), invoice `FAC-2026-0891`,
> outstanding amount **$150.00**.

This is a simple 1:1 match — one deposit, one invoice, same amount. Perfect for learning.
Later chapters will show the hard cases: multiple invoices, partial payments, penny
rounding, and the dreaded "subset sum" problem.

---

# Chapter 2: The Architecture & Diagrams

## The Airport Analogy

Think of PACIOLI as an international airport:

| Airport Component | PACIOLI Component | Role |
|-------------------|-------------------|------|
| **Arrival Hall** | Python Data Pipeline | Raw data comes in (passengers land) |
| **Immigration** | Staging Layer (biq_stg) | Clean, validate, classify |
| **Customs** | Matching Algorithms | Check if things add up |
| **Control Tower** | Node.js API | Coordinates all movements |
| **Gate Agents** | React Frontend | Human interface for decisions |
| **Departure** | Gold Layer (biq_gold) | Approved records leave for SAP |
| **Flight Recorder** | biq_config + logs | Everything is recorded |

## Hexagonal Architecture

PACIOLI's Python pipeline uses **Hexagonal Architecture** (also called "Ports and
Adapters"). If that sounds intimidating, here's the simple version:

> **Analogy:** Think of a power strip. Your laptop has a single port. The power strip
> has many outlets. You can plug in a charger, a lamp, or a fan — the laptop doesn't
> care. The power strip (adapter) translates between the wall socket (external world)
> and your laptop (business logic).

In Hexagonal Architecture:
- The **core** (center) is pure business logic — matching algorithms, scoring, hashing.
  It knows nothing about databases, files, or APIs.
- The **ports** are interfaces — "I need to read bank transactions" or "I need to save
  a match result."
- The **adapters** are the concrete implementations — "read from PostgreSQL" or "read
  from an Excel file."

**Why does this matter?** Because you can swap the database, change the file format, or
add a new data source without touching the matching algorithms. The brain stays clean.

```
data-pipeline/
├── logic/
│   ├── domain/           ← THE CORE (pure business logic)
│   │   ├── value_objects.py
│   │   └── services/
│   │       ├── hashing/
│   │       ├── enrichment/
│   │       └── reconciliation/
│   ├── staging/          ← USE CASES (orchestrate domain operations)
│   │   └── reconciliation/
│   │       ├── matchers/
│   │       ├── strategies/
│   │       └── utils/
│   └── infrastructure/   ← ADAPTERS (database, files, external services)
│       └── repositories/
├── data_loaders/         ← ADAPTERS (file readers)
└── main_silver_orchestrator.py  ← ENTRY POINT
```

## System Architecture Diagram

```mermaid
flowchart TB
    subgraph BRONZE["Phase 0 — Bronze Layer (Raw Files)"]
        EXCEL[("📁 Excel/CSV Files<br/>SAP FBL3N, FBL5N,<br/>Card Settlements")]
        LOADERS["Python Data Loaders<br/>(10 specialized loaders)"]
        RAW[("biq_raw schema<br/>Raw tables with hash_id")]
    end

    subgraph SILVER["Phases 1-6 — Silver Layer (Staging)"]
        STG_BANK["stg_bank_transactions<br/>(43 columns)"]
        STG_PORT["stg_customer_portfolio<br/>(44 columns)"]
        STG_CARD["stg_card_settlements<br/>(19 columns)"]
        ENRICH["Bank Enricher<br/>(exact + suffix matching)"]
        MATCH["Matching Engine<br/>(Deterministic → Probabilistic)"]
        RUST["🦀 pacioli_core<br/>(Rust subset-sum + fuzzy)"]
        SCORE["Scoring Engine<br/>(weighted 0-100)"]
    end

    subgraph API["Node.js API Layer"]
        EXPRESS["Express.js Server<br/>(14 modules, 60 endpoints)"]
        JWT["JWT Auth Middleware<br/>(bcrypt + access tokens)"]
        LOCKS["Transaction Locks<br/>(5-min TTL mutex)"]
        GOLD_SVC["Gold Export Service<br/>(idempotent batch writer)"]
    end

    subgraph UI["React Frontend"]
        LOGIN["Login Page<br/>(Framer Motion)"]
        OVERVIEW["Overview Dashboard<br/>(KPI cards + queue)"]
        WORKSPACE["Workspace Panel<br/>(split-screen reconciliation)"]
        SUBMIT["Submit for Posting<br/>(Gold batch export)"]
    end

    subgraph GOLD["Gold Layer (Output)"]
        PH["payment_header<br/>(SAP F-28 header)"]
        PD["payment_detail<br/>(invoice lines)"]
        PDIFF["payment_diff<br/>(commissions/taxes)"]
    end

    RPA["🤖 RPA Robot<br/>(reads Gold, posts to SAP)"]

    EXCEL --> LOADERS --> RAW
    RAW --> STG_BANK & STG_PORT & STG_CARD
    STG_BANK --> ENRICH --> MATCH
    STG_PORT --> MATCH
    STG_CARD --> MATCH
    MATCH <--> RUST
    MATCH --> SCORE --> STG_BANK

    EXPRESS --> STG_BANK & STG_PORT
    JWT --> EXPRESS
    LOCKS --> EXPRESS
    EXPRESS --> GOLD_SVC --> PH & PD & PDIFF

    LOGIN --> EXPRESS
    OVERVIEW --> EXPRESS
    WORKSPACE --> EXPRESS
    SUBMIT --> EXPRESS

    PH --> RPA
```

## Database Entity-Relationship Diagram

```mermaid
erDiagram
    stg_bank_transactions {
        bigint    stg_id PK
        varchar   bank_ref_1
        numeric   amount_total
        varchar   reconcile_status
        varchar   settlement_id
    }

    stg_customer_portfolio {
        bigint    stg_id PK
        varchar   invoice_ref
        numeric   amount_outstanding
        varchar   reconcile_status
    }

    stg_card_settlements {
        bigint    stg_id PK
        varchar   settlement_id
        numeric   amount_gross
    }

    transaction_workitems {
        varchar   bank_ref_1 PK
        bigint    stg_id
        integer   assigned_user_id
        varchar   work_status
    }

    transaction_locks {
        varchar   bank_ref_1 PK
        integer   locked_by_id
        timestamptz expires_at
    }

    users {
        integer   id PK
        varchar   username
        varchar   role
    }

    assignment_rules {
        integer   id PK
        integer   assign_to_user_id
        integer   priority
    }

    payment_header {
        bigint    id PK
        varchar   idempotency_hash
        varchar   bank_ref_1
        numeric   amount
        varchar   rpa_status
    }

    payment_detail {
        bigint    id PK
        bigint    header_id
        varchar   invoice_ref
        numeric   amount_gross
    }

    payment_diff {
        bigint    id PK
        bigint    header_id
        varchar   sap_posting_key
        numeric   amount
    }

    stg_bank_transactions   ||--o|  transaction_workitems  : "bank_ref_1"
    stg_bank_transactions   ||--o{  payment_header         : "stg_id"
    stg_bank_transactions   }o--o|  stg_card_settlements   : "settlement_id"
    transaction_workitems   }o--||  users                  : "assigned_user_id"
    transaction_workitems   ||--o|  transaction_locks      : "bank_ref_1"
    assignment_rules        }o--||  users                  : "assign_to_user_id"
    payment_header          ||--o{  payment_detail         : "header_id"
    payment_header          ||--o{  payment_diff           : "header_id"
```

## The User's Journey

```mermaid
flowchart LR
    A["🔐 Login"] --> B["📊 Overview Dashboard"]
    B --> C{"Transaction\nAssigned?"}
    C -->|"Click row"| D["🔒 Acquire Lock"]
    D --> E["📋 Workspace Panel"]
    E --> F["Select Invoices"]
    F --> G["⚖️ Balance Bar"]
    G -->|"Balanced"| H["✅ Approve"]
    G -->|"Unbalanced"| I["🔧 Adjust\n(commission, tax, diff)"]
    I --> G
    H --> J["📤 Submit for Posting"]
    J --> K["🏦 Gold Layer"]
    K --> L["🤖 RPA → SAP F-28"]
```

---

# Chapter 3: The Brains — Python/Rust Pipeline

## How Data Enters the System

### The Loader Pattern

> **Analogy:** Think of a post office mail sorter. Every letter arrives in a different
> envelope — some handwritten, some typed, some with stickers. The sorter opens each
> envelope, reads the address, stamps it with a tracking number, and puts it in the
> correct bin. That's what a data loader does.

PACIOLI has **10 specialized loaders**, each designed to read a specific file format:

| Loader | Source File | Target Table |
|--------|-----------|-------------|
| SAP 239 | FBL3N bank extract (.xlsx) | `raw_sap_cta_239` |
| FBL5N | Customer portfolio (.xlsx) | `raw_fbl5n` |
| Banco Guayaquil | Bank statement (.xlsx) | `raw_banco_guayaquil` |
| Diners | Diners Club settlement (.xlsx) | `raw_diners` |
| Pacificard | Pacificard settlement (.xlsx) | `raw_pacificard` |
| Webpos | Datafast POS records (.xlsx) | `raw_webpos` |
| Retenciones | Tax withholdings (.xlsx) | `raw_retenciones` |
| Manual Requests | Manual reconciliation requests | `raw_manual_requests` |
| Databalance | Balance files (.xlsx) | `raw_databalance` |
| Guayaquil Settlements | Bank settlements (.xlsx) | `raw_guayaquil` |

Every loader follows the same **Template Method Pattern** — a fancy way of saying
"all loaders follow the same recipe, but each one customizes certain steps."

Here's the recipe (from `base_loader.py`):

```
Step 1: Read file → raw DataFrame (all columns as strings)
Step 2: Add metadata (source_file, batch_id, loaded_at)
Step 3: Map columns (Excel header names → database column names)
Step 4: Apply business rules (loader-specific cleaning)
Step 5: Generate hash_id (SHA-256 fingerprint for deduplication)
Step 6: Load to database (atomic UPSERT)
Step 7: Archive file (move to success/ or failed/ folder)
```

### Our $150 VIP Lounge Deposit — Step by Step

**Step 1: The file arrives.**

An analyst drops `banco_guayaquil_2026-04-08.xlsx` into the `data_raw/` folder. Row 47
of the file contains:

```
| Fecha       | Referencia   | Descripción          | Monto  |
|-------------|-------------|----------------------|--------|
| 2026-04-08  | BGQ-9981742 | DINERS SETTLEMENT    | 150.00 |
```

**Step 4: Business rules clean the data.**

The Banco Guayaquil loader (a subclass of `BaseLoader`) runs `specific_business_rules()`:

```python
def specific_business_rules(self, df):
    # Parse dates: "2026-04-08" → Python date object
    df['fecha_documento'] = df['fecha_documento'].apply(parse_to_sql_date)
    
    # Parse amounts: "150.00" → float 150.00
    df['importe_ml'] = df['importe_ml'].apply(parse_currency)
    
    # Default company code
    df['sociedad'] = '8000'
    
    return df
```

**Step 5: SHA-256 hashing creates a unique fingerprint.**

```python
def generate_hash_id(self, df):
    # Concatenate identity fields
    df['hash_source'] = (
        df['sociedad'].fillna('') +     # "8000"
        df['num_documento'].fillna('') + # "BGQ-9981742"
        df['ejercicio'].fillna('') +     # "2026"
        df['posicion'].fillna('') +      # "001"
        df['clase_documento'].fillna('') + # "ZR"
        df['importe_ml'].fillna('')      # "150.00"
    )
    
    # Hash with SHA-256
    df['hash_id'] = df['hash_source'].apply(
        lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest()
    )
    return df
```

> **Why SHA-256?** Because the same row uploaded twice must produce the same hash.
> That's why it's called *deterministic* — the same input always gives the same output.
> If row 47 produces hash `a3f8b2...`, uploading the file again tomorrow produces the
> exact same `a3f8b2...`. The UPSERT then says "I already have this row" and either
> skips or updates it. **No duplicates, ever.**

### The Math Behind SHA-256 (Simplified)

SHA-256 takes any input and produces a fixed 64-character hexadecimal string. Think of
it as a blender:

```
Input:  "8000BGQ-99817422026001ZR150.00"
         ↓ SHA-256 blender ↓
Output: "a3f8b2c91d4e7f6a..."  (64 hex chars = 256 bits)
```

Properties that matter for us:
1. **Deterministic:** Same input → same output. Always.
2. **Avalanche effect:** Change one character → completely different hash.
3. **Collision-resistant:** Two different inputs producing the same hash is
   astronomically unlikely (1 in 2^256 ≈ 10^77).

**Step 6: Atomic UPSERT loads the data.**

```sql
INSERT INTO raw_banco_guayaquil (hash_id, sociedad, num_documento, ...)
SELECT * FROM _staging_temp_table
ON CONFLICT (hash_id) DO UPDATE SET
    status_partida = EXCLUDED.status_partida,
    loaded_at = CURRENT_TIMESTAMP;
```

The `ON CONFLICT` clause is the magic — if the hash already exists, update; otherwise,
insert. This makes the loader **idempotent**: run it once or ten times, the result is
the same.

---

## The Orchestrator: 18 Processes in 6 Groups

After all raw files are loaded (Phase 0), the `SilverLayerOrchestrator` runs 18
processes in strict sequence:

```mermaid
flowchart TD
    P0["Phase 0: Raw Loaders<br/>(10 file readers)"]
    G1["Group 1: Foundations<br/>SAP Staging, Portfolio CDC, Portfolio Ph1"]
    G2["Group 2: Card Streams<br/>Diners, Guayaquil, Pacificard Staging"]
    G3["Group 3: Derived<br/>Parking Breakdown"]
    G4["Group 4: Tax<br/>Process/Match/Apply Withholdings"]
    G5["Group 5: Enrichment<br/>Manual Requests, Bank Enrichment,<br/>Portfolio Ph2/Ph3"]
    G6["Group 6: Reconciliation<br/>Bank Recon, Validation,<br/>Portfolio Matches, Restore"]

    P0 --> G1 --> G2 --> G3 --> G4 --> G5 --> G6
```

Each process is tracked in `biq_config.etl_process_windows`:

```
PENDING → RUNNING → COMPLETED (or FAILED or SKIPPED)
```

---

## The Matching Engine

This is the heart of PACIOLI. Given a bank transaction ($150.00), find which invoices
it pays.

### The Cascade Strategy

> **Analogy:** Imagine you lose your car keys. You check the obvious places first (coat
> pocket, key hook by the door). If not there, you check less obvious places (under the
> sofa cushions). If still not found, you do a full room search. You escalate effort
> only when simpler methods fail. That's the cascade.

```mermaid
flowchart TD
    A["Bank Transaction<br/>$150.00"] --> B{"Strategy 1:<br/>Exact Single Match"}
    B -->|"Found"| DONE["✅ MATCHED<br/>Score: 95-100"]
    B -->|"Not found"| C{"Strategy 2:<br/>Exact Contiguous<br/>Multi-Invoice"}
    C -->|"Found"| DONE
    C -->|"Not found"| D{"Strategy 3:<br/>Tolerance Single<br/>(±$0.05)"}
    D -->|"Found"| DONE2["✅ MATCHED<br/>Score: 70-95"]
    D -->|"Not found"| E{"Strategy 4:<br/>Greedy Sequential"}
    E -->|"Score ≥ 90"| DONE2
    E -->|"Below 90"| F{"Strategy 5:<br/>Subset Sum<br/>(no gaps)"}
    F -->|"Score ≥ 90"| DONE2
    F -->|"Below 90"| G{"Strategy 6:<br/>Subset Sum<br/>(max 3 gaps)"}
    G -->|"Score ≥ 90"| DONE2
    G -->|"Below 90"| H["⚠️ REVIEW<br/>Best approximation"]
```

### Strategy 1: Exact Single Match — Our $150 Example

This is the simplest case. The algorithm scans all open invoices looking for one whose
`conciliable_amount` equals the bank `amount_total`:

```python
# From deterministic_matcher.py
for invoice in invoices:
    if is_exact_match(bank_amount, invoice.conciliable_amount, precision=2):
        return MatchResult(
            matched_invoices=[invoice],
            method='EXACT_SINGLE',
            confidence=100,
        )
```

The `is_exact_match` function uses Python's `Decimal` type to avoid floating-point traps:

```python
def is_exact_match(amount1, amount2, precision=2):
    d1 = round(Decimal(str(amount1)), precision)  # Decimal('150.00')
    d2 = round(Decimal(str(amount2)), precision)  # Decimal('150.00')
    return d1 == d2  # True!
```

> **Why not just use `150.00 == 150.00`?** Because in floating-point arithmetic,
> `0.1 + 0.2 = 0.30000000000000004`. When you're handling real money, that rounding
> error can mean the difference between "balanced" and "investigate fraud." The `Decimal`
> type does exact arithmetic — $150.00 is always exactly $150.00.

For our VIP Lounge deposit: the bank has $150.00, the invoice has $150.00. **Exact
match. Score: 100. Status: MATCHED.**

### Strategy 2: Contiguous Multi-Invoice Match

What if the bank deposit is $350.00 and it covers three consecutive invoices?

```
Invoice 1: $100.00  (doc_date: April 1)
Invoice 2: $125.00  (doc_date: April 3)
Invoice 3: $125.00  (doc_date: April 5)
Sum:       $350.00  ✓
```

The algorithm uses a **sliding window**:

```python
for i in range(n):           # Start at each invoice
    current_sum = 0.0
    for j in range(i, min(i + max_invoices, n)):  # Extend window
        current_sum += invoices[j].amount
        
        if is_exact_match(bank_amount, current_sum):
            return indices[i:j+1]     # Found!
        
        if current_sum > bank_amount:
            break                     # Prune: sum exceeded target
```

> **Analogy:** Imagine stacking coins in a row. Start with coin 1. Add coin 2. Too
> little? Add coin 3. Too much? Stop, move the starting position forward, try again.

**Complexity:** O(n²) with early pruning. In practice, most matches are found in the
first 3-5 invoices, so the pruning makes this very fast.

### Strategy 5-6: The Subset Sum Problem

Now for the hard case. The bank deposit is $250.00 and the matching invoices are
**not consecutive**:

```
Invoice 1: $100.00  (April 1)
Invoice 2: $50.00   (April 2)  ← skip this one
Invoice 3: $80.00   (April 3)
Invoice 4: $70.00   (April 5)
```

Invoices 1 + 3 + 4 = $250.00, but invoice 2 is in between. This is the **Subset Sum
Problem** — one of the classic problems in computer science.

> **Analogy:** You're at a restaurant splitting the bill. The total is $250. You have
> bills of $100, $50, $80, and $70. Which combination adds up to exactly $250? You
> can't use the $50 bill. The answer is $100 + $80 + $70.

The naive approach (try all possible combinations) has exponential complexity:
- 10 invoices → 1,024 combinations
- 20 invoices → 1,048,576 combinations
- 25 invoices → 33,554,432 combinations

PACIOLI handles this with two optimizations:

1. **Cap at 10,000 combinations** — if not found after 10K attempts, move to best-effort
2. **Rust hot-path** — for 1-3 invoice combinations, a compiled Rust module runs the
   search at native speed

### The Rust Accelerator: `pacioli_core`

The Rust module (`data-pipeline/pacioli_core/src/lib.rs`) exposes two functions to Python:

#### `find_invoice_combination` — Subset Sum in Cents

The key insight: **convert all amounts to integer cents** to avoid floating-point errors.

```rust
// Convert to cents: $150.00 → 15000 cents
let target_c: i64 = (target * 100.0).round() as i64;
let tol_c: i64    = (tolerance * 100.0).round() as i64;
```

Then three algorithms run in cascade:

**Case 1 — Single invoice (O(n)):**
```rust
// Linear scan: does any invoice equal the target?
for &(idx, c) in &valid {
    if (c - target_c).abs() <= tol_c {
        return Ok(Some(vec![idx]));
    }
}
```

**Case 2 — Two invoices with HashMap (O(n)):**

> **Analogy:** You need two numbers that sum to 100. You have [30, 60, 70, 40].
> Look at 30 → you need 70. Is 70 in the bag? Check the HashMap in O(1). Yes!
> Done in one pass.

```rust
let mut seen: HashMap<i64, usize> = HashMap::new();
for &(idx, c) in &valid {
    let complement = target_c - c;
    // Check if we've seen the complement (with tolerance)
    for delta in [-tol_c, 0, tol_c] {
        if let Some(&other_idx) = seen.get(&(complement + delta)) {
            return Ok(Some(vec![other_idx, idx]));
        }
    }
    seen.insert(c, idx);
}
```

**Case 3 — Three invoices with two-pointer (O(n²)):**

> **Analogy:** Three friends splitting a bill. Fix one friend's contribution.
> Then use two pointers (one from the cheapest remaining, one from the most expensive)
> to find a pair that completes the total. Move the pointers inward until they meet.

```rust
sorted.sort_by_key(|&(_, c)| c);  // Sort by cents ascending

for i in 0..n-2 {
    let remaining = target_c - sorted[i].1;
    let mut lo = i + 1;
    let mut hi = n - 1;
    
    while lo < hi {
        let sum = sorted[lo].1 + sorted[hi].1;
        if (sum - remaining).abs() <= tol_c {
            return found!;  // Three indices that sum to target
        } else if sum < remaining {
            lo += 1;  // Need bigger: move left pointer right
        } else {
            hi -= 1;  // Need smaller: move right pointer left
        }
    }
}
```

**Why Rust instead of Python?**
- Python loops: ~1 million iterations/second
- Rust loops: ~1 billion iterations/second
- For combinatorial problems, **1000x speed matters**

The Rust module is compiled with `maturin develop --release` and imported as a regular
Python package: `import pacioli_core`.

#### `fuzzy_batch_match` — Jaccard Similarity

For cases where the invoice reference doesn't match exactly (typos, different formats),
PACIOLI uses **fuzzy matching** based on Jaccard similarity over character bigrams.

**What's a bigram?** Two consecutive characters:

```
"DINERS" → {(D,I), (I,N), (N,E), (E,R), (R,S)}
"DINRES" → {(D,I), (I,N), (N,R), (R,E), (E,S)}
```

**What's Jaccard similarity?**

```
Jaccard(A, B) = |A ∩ B| / |A ∪ B|

Intersection: {(D,I), (I,N)} → 2 shared bigrams
Union:        8 total unique bigrams
Jaccard = 2/8 = 0.25
```

In plain English: "what fraction of all bigrams do these two strings share?"

- Jaccard = 1.0 → identical strings
- Jaccard = 0.0 → completely different
- Threshold = 0.70 → PACIOLI considers this a match

The Rust implementation pre-filters by amount (±$0.01) before running string comparison,
eliminating 90%+ of candidates instantly.

---

## The Scoring Engine

Every match gets a score from 0 to 100. The score determines the status:

| Score | Status | Meaning |
|-------|--------|---------|
| ≥ 90 | `MATCHED` | Pipeline auto-approves |
| 60-89 | `REVIEW` | Human analyst must verify |
| < 60 | `PENDING` | No reliable match found |

### Weight Table

```python
SCORING_WEIGHTS = {
    'exact_amount_match':  45,  # Perfect amount match (0 or 45)
    'tolerance_match':     35,  # Amount within tolerance (0-35, proportional)
    'date_proximity':      10,  # Invoice date close to bank date
    'invoice_continuity':   5,  # Sequential invoice indices
    'reference_match':      5,  # Bank reference matches invoice
}
# Total possible: 100
```

### Calculating Our $150 Example's Score

```
Amount score:      45/45 (exact match, $150.00 = $150.00)
Date proximity:     9/10 (invoice dated April 5, bank April 8 = 3 days)
Continuity:         5/5  (single invoice = perfect continuity)
Reference:          5/5  (bank_ref matches settlement_id)
─────────────────────────
Total:             64/65 → normalized to 98.5/100 → MATCHED
```

### The Gap Penalty

For subset-sum matches with non-contiguous invoices, a penalty is applied:

```python
gap_penalty = min(gap_count * 5, 15)  # Max 15 point penalty
final_score = max(raw_score - gap_penalty, 0)
```

Example: A match uses invoices at positions [1, 2, 5] (gap of 2 at position 3-4):
```
Raw score: 85
Gap count: 2  →  Penalty: 10
Final score: 75  →  Status: REVIEW (not MATCHED)
```

---

## The Hash Key: Identity That Survives Re-Runs

Each bank transaction gets a deterministic **match_hash_key** that uniquely identifies
it across pipeline re-runs:

```python
# For Diners/VISA/etc:
hash_key = f"{brand}_{amount}_{sequence}"
# Example: "DINERS_150.00_1"

# For Pacificard (batch matters):
hash_key = f"PACIFICARD_{batch}_{amount}_{sequence}"
# Example: "PACIFICARD_001032_49.68_1"
```

The `sequence` comes from a **historical counter** that preserves continuity:

```
Day 1: DINERS_150.00_1, DINERS_150.00_2
Day 2: DINERS_150.00_3  ← continues from 2, not restarting at 1
```

This counter is cached in `biq_stg.hash_counter_cache` and refreshed with:

```sql
SELECT brand, amount_total,
       MAX(CAST(REGEXP_REPLACE(match_hash_key, '^.*_', '') AS INTEGER)) as last_counter
FROM biq_stg.stg_bank_transactions
WHERE doc_date < :before_date
GROUP BY brand, amount_total
```

---

# Chapter 4: The Database Engine

## Five Schemas, One Database

PACIOLI uses a single PostgreSQL 18.3 database with five schemas:

```mermaid
flowchart LR
    subgraph DB["PostgreSQL Database"]
        RAW["biq_raw<br/>🗄️ Raw files<br/>(Bronze)"]
        STG["biq_stg<br/>📊 Staging tables<br/>(Silver)"]
        AUTH["biq_auth<br/>🔐 Users, locks,<br/>workitems"]
        GOLD["biq_gold<br/>🏆 SAP F-28<br/>payment records"]
        CFG["biq_config<br/>⚙️ ETL tracking,<br/>logs"]
    end

    RAW -->|"Python pipeline"| STG
    STG -->|"Node.js API"| AUTH
    AUTH -->|"Node.js API"| GOLD
    CFG -.->|"observability"| RAW & STG
```

### Who Writes Where?

| Schema | Writer | Reader |
|--------|--------|--------|
| `biq_raw` | Python pipeline only | Python pipeline |
| `biq_stg` | Python pipeline only | Node.js API (read), Python |
| `biq_auth` | Node.js API only | Node.js API |
| `biq_gold` | Node.js API (write), RPA (update `rpa_status`) | RPA robot |
| `biq_config` | Python pipeline only | Python, Node.js API |

**This separation is critical.** The Python pipeline never touches `biq_auth` or
`biq_gold`. The Node.js API never writes to `biq_stg`. This prevents race conditions
and keeps ownership boundaries clean.

## The Silver Layer: Where Our $150 Lives

After the pipeline processes it, our VIP Lounge deposit lives in
`biq_stg.stg_bank_transactions` with 43 columns:

```sql
SELECT stg_id, bank_ref_1, amount_total, reconcile_status,
       match_hash_key, match_confidence_score, match_method,
       enrich_customer_id, enrich_customer_name
FROM biq_stg.stg_bank_transactions
WHERE bank_ref_1 = 'BGQ-9981742';
```

Result:

```
stg_id:                  47201
bank_ref_1:              BGQ-9981742
amount_total:            150.00
reconcile_status:        MATCHED
match_hash_key:          DINERS_150.00_3
match_confidence_score:  98.50
match_method:            EXACT_SINGLE
enrich_customer_id:      C-4521
enrich_customer_name:    ACME Corp
matched_portfolio_ids:   "88402"  ← CSV of matched invoice stg_ids
```

The matching invoice lives in `biq_stg.stg_customer_portfolio`:

```sql
SELECT stg_id, invoice_ref, customer_code, amount_outstanding,
       reconcile_status
FROM biq_stg.stg_customer_portfolio
WHERE stg_id = 88402;
```

```
stg_id:             88402
invoice_ref:        FAC-2026-0891
customer_code:      C-4521
amount_outstanding: 150.00
reconcile_status:   MATCHED
```

## The Gold Layer: Double-Entry Bookkeeping

> **Analogy:** The Silver Layer is a draft. The Gold Layer is the signed, notarized
> final document. You can edit a draft; you can never edit a notarized document.

The Gold Layer stores approved SAP F-28 payment records in three tables:

**`payment_header`** — one row per approved bank payment:

| Column | Our Example | Purpose |
|--------|-------------|---------|
| `idempotency_hash` | `SHA256(BGQ-9981742+...)` | Prevents duplicate export |
| `bank_ref_1` | `BGQ-9981742` | Traces back to bank transaction |
| `amount` | `150.00` | Total payment amount |
| `posting_date` | `2026-04-08` | SAP posting date |
| `period` | `4` | Fiscal month (April) |
| `bank_gl_account` | `1110213001` | GL account for bank |
| `rpa_status` | `PENDING_RPA` | Waiting for robot |
| `approved_by` | `jdoe` | Analyst username |

**`payment_detail`** — one row per invoice in the payment:

| Column | Our Example | Purpose |
|--------|-------------|---------|
| `header_id` | `→ payment_header.id` | FK to header |
| `invoice_ref` | `FAC-2026-0891` | SAP invoice number |
| `customer_code` | `C-4521` | SAP customer |
| `amount_gross` | `150.00` | Invoice amount applied |
| `is_partial_payment` | `false` | Full payment |

**`payment_diff`** — commission/tax/adjustment lines (empty for our simple example):

For a card payment with 1.2% commission, this table would contain:

```
sap_posting_key: '40'     (debit — expense)
gl_account:      '5540101004'  (bank commission account)
amount:          1.80
adjustment_type: 'final_amount_commission'
```

### The Accounting Equation

Every SAP F-28 posting must satisfy the fundamental accounting equation:

```
Bank Amount = Sum(Invoice Amounts) + Commission + Taxes + Difference

$150.00 = $150.00 + $0.00 + $0.00 + $0.00  ✓
```

The **Balance Bar** in the React UI enforces this. `canApprove` is `true` only when
the unallocated amount is exactly **zero**. Not $0.01. Not -$0.01. **Exactly zero.**

```javascript
// From reconciliation.repository.js
const canApprove = Math.abs(unallocated) < 0.005;  // Rounds to 0.00
```

### Immutability Rule

Gold records are **never deleted**. To cancel an export:

```sql
UPDATE biq_gold.payment_header
SET rpa_status = 'FAILED',
    rpa_error_message = 'Cancelado manualmente por operador'
WHERE bank_ref_1 = 'BGQ-9981742'
  AND rpa_status = 'PENDING_RPA';
```

The `rpa_status` CHECK constraint allows only: `PENDING_RPA | IN_PROGRESS | POSTED | FAILED | RETRY`.

---

# Chapter 5: The Backend Traffic Cop — Node.js

## The Express Middleware Pipeline

> **Analogy:** Imagine a nightclub. Before you reach the dance floor, you pass through:
> 1. **Security check** (Helmet — HTTP security headers)
> 2. **Guest list** (CORS — who's allowed in)
> 3. **Coat check** (JSON parser — prepare the payload)
> 4. **Bouncer counting heads** (Rate limiter — max 200/15min per IP)
> 5. **ID verification** (JWT auth — who are you?)
> 6. **VIP wristband** (Role check — what can you do?)

From `app.js`:

```javascript
app.use(helmet());           // Security headers
app.use(cors({ ... }));      // Cross-origin policy
app.use(express.json());     // Parse JSON bodies
app.use(morgan('dev'));       // Request logging
app.use(globalLimiter);      // Rate limiting: 200 req / 15 min

// Then routes:
app.use('/api/v1/auth',          authRoutes);
app.use('/api/v1/transactions',  transactionRoutes);
app.use('/api/v1/workspace',     workspaceRoutes);
// ... 14 modules total, 60 endpoints
```

## JWT Authentication

> **Analogy:** A JWT (JSON Web Token) is like a wristband at a festival. When you
> enter (login), security gives you a wristband with your name and access level
> printed on it. Every time you approach a stage (endpoint), the guard just reads
> your wristband — no need to ask security again. The wristband expires after 8 hours.

**Login flow:**

```
1. POST /api/v1/auth/login { username: "jdoe", password: "secret123" }
2. Server verifies password against bcrypt hash in biq_auth.users
3. Server signs a JWT: { sub: userId, role: "analyst", exp: 8h }
4. Client stores token in localStorage
5. Every subsequent request includes: Authorization: Bearer <token>
```

**Environment validation** (`env.js`) uses Zod schema:

```javascript
const envSchema = z.object({
  JWT_SECRET:     z.string().min(32, 'Must be at least 32 characters'),
  JWT_EXPIRES_IN: z.string().default('8h'),
  BCRYPT_ROUNDS:  z.coerce.number().min(10).max(14).default(12),
  // ... more fields
});
```

If any required env var is missing, PACIOLI refuses to start. Fail fast, not silently.

## Transaction Locks: Preventing Race Conditions

> **Analogy:** In a shared document (Google Docs), two people can edit the same
> paragraph simultaneously, causing chaos. Transaction locks are like "Edit Mode" —
> when you open a transaction, you get exclusive access for 5 minutes. If someone
> else tries to open it, they see "Locked by Jane Doe."

**How it works:**

```sql
-- Acquire lock (atomic upsert)
INSERT INTO biq_auth.transaction_locks (bank_ref_1, locked_by_id, locked_by_name, expires_at)
VALUES ('BGQ-9981742', 3, 'jdoe', NOW() + INTERVAL '5 minutes')
ON CONFLICT (bank_ref_1) DO UPDATE
  SET locked_by_id = 3, locked_by_name = 'jdoe', expires_at = NOW() + INTERVAL '5 minutes'
  WHERE transaction_locks.expires_at < NOW();  -- Only if expired!
```

The `WHERE expires_at < NOW()` clause is crucial — it means an expired lock can be
re-acquired by anyone, but an active lock blocks everyone except the owner.

**The heartbeat:** The frontend calls `PATCH /locks/:bankRef1/renew` every 4 minutes
to extend the 5-minute TTL. If the analyst's browser crashes, the lock expires
naturally after 5 minutes and the transaction becomes available again.

**On logout:** `POST /auth/logout` releases all locks for the user:

```javascript
export async function logout(req, res) {
  const { releaseAllLocksForUser } = await import('../locks/locks.repository.js');
  const released = await releaseAllLocksForUser(req.user.id);
  // ...
}
```

## The Gold Export Flow

When an admin clicks "Submit for Posting" in the React UI:

```mermaid
sequenceDiagram
    participant UI as React Frontend
    participant API as Node.js API
    participant DB as PostgreSQL
    participant RPA as RPA Robot

    UI->>API: POST /gold-export/submit
    API->>DB: Find all APPROVED workitems<br/>with doc_type IN ('ZR','SA')
    DB-->>API: 14 eligible transactions
    
    loop For each transaction
        API->>API: Generate idempotency_hash<br/>(SHA-256 of bank_ref_1 + amounts)
        API->>DB: INSERT INTO payment_header<br/>ON CONFLICT DO NOTHING
        API->>DB: INSERT INTO payment_detail<br/>(one row per invoice)
        API->>DB: INSERT INTO payment_diff<br/>(commissions, taxes, diffs)
    end
    
    API-->>UI: { exported: 14, batch_id: "BATCH-20260409-001" }
    
    Note over RPA: Later, RPA polls for<br/>rpa_status = 'PENDING_RPA'
    RPA->>DB: SELECT * FROM payment_header<br/>WHERE rpa_status = 'PENDING_RPA'<br/>ORDER BY batch_date, id
    RPA->>RPA: Post each to SAP F-28
    RPA->>DB: UPDATE rpa_status = 'POSTED'
```

### GL Account Configuration

SAP posting keys and GL accounts are configured in `gl-accounts.js` — not hardcoded in
business logic:

```javascript
export const GL_ACCOUNTS = {
  f28: {
    docClass:       'DZ',
    companyCode:    '8000',
    currency:       'USD',
    transactionSap: 'F-28',
  },
  bankGlAccounts: {
    primary: '1110213001',  // Main bank account
  },
  adjustments: {
    final_amount_commission: {
      gl_account:      '5540101004',   // Bank commission expense
      sap_posting_key: '40',           // Debit (expense)
    },
    final_amount_tax_iva: {
      gl_account:      '1140105023',   // IVA withholding
      sap_posting_key: '40',
    },
    final_amount_tax_irf: {
      gl_account:      '140104019',    // Income tax withholding
      sap_posting_key: '40',
    },
    diff_cambiario: {
      gl_account:      '3710101001',   // Exchange differential
      sap_posting_key: 'DYNAMIC',      // Resolved at runtime by sign
    },
  },
};

// Positive diff → bank paid more → credit '50'
// Negative diff → bank paid less → debit '40'
export function resolvePostingKey(adjustmentType, diffAmount = 0) {
  const config = GL_ACCOUNTS.adjustments[adjustmentType];
  if (config.sap_posting_key !== 'DYNAMIC') return config.sap_posting_key;
  return parseFloat(diffAmount) >= 0 ? '50' : '40';
}
```

---

# Chapter 6: The Financial Interface — React

## Technology Stack

| Library | Version | Purpose |
|---------|---------|---------|
| React | 19.2.4 | UI framework |
| Vite | 8.0.1 | Build tool & dev server |
| React Router | 7.13.1 | Client-side routing |
| Zustand | 5.0.12 | Client state management |
| TanStack Query | 5.94.5 | Server state & caching |
| Axios | 1.13.6 | HTTP client with JWT interceptor |
| Tailwind CSS | 4.2.2 | Utility-first styling |
| Framer Motion | 12.38.0 | Animations |
| React Hook Form + Zod | 7.72 + 4.3 | Form validation |
| Recharts | 3.8.0 | Charts & visualizations |

## Three-Tier State Management

> **Analogy:** Think of a kitchen. You have:
> 1. **The pantry** (Zustand) — long-term supplies. JWT token, user preferences.
>    Survives page refreshes because it's stored in localStorage.
> 2. **The fridge** (React Query) — server data with expiration dates. Transaction list,
>    invoice data. Automatically refreshes when "stale."
> 3. **The cutting board** (useReducer) — what you're actively working on. Selected
>    invoices, current calculation, adjustment amounts. Disappears when you leave the page.

### Tier 1: Zustand Stores

```javascript
// auth.store.js — persisted in localStorage
export const useAuthStore = create((set) => ({
  user:            null,
  token:           null,
  isAuthenticated: false,

  setAuth: ({ token, user }) => {
    localStorage.setItem('pacioli_token', token);
    localStorage.setItem('pacioli_user', JSON.stringify(user));
    set({ token, user, isAuthenticated: true });
  },

  clearAuth: () => {
    localStorage.removeItem('pacioli_token');
    localStorage.removeItem('pacioli_user');
    set({ token: null, user: null, isAuthenticated: false });
  },
}));
```

### Tier 2: React Query Configuration

```javascript
// providers.jsx
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 2 * 60 * 1000,   // Data is fresh for 2 minutes
      retry: 1,                    // One retry on failure
      refetchOnWindowFocus: false, // CRITICAL for financial data!
    },
  },
});
```

> **Why disable `refetchOnWindowFocus`?** Imagine an analyst is reviewing a transaction.
> They Alt-Tab to check an email. When they come back, React Query refetches the
> transaction list. If another analyst approved something in the meantime, the list
> shifts, and the analyst might click the wrong row. **Stability beats freshness
> for financial UIs.**

### Tier 3: Workspace Reducer

The Workspace page manages complex local state with `useReducer`:

```javascript
// Actions dispatched during reconciliation:
dispatch({ type: 'SELECT_TX',       payload: transaction });
dispatch({ type: 'TOGGLE_INVOICE',  payload: invoiceId });
dispatch({ type: 'SET_ADJUSTMENT',  payload: { field: 'commission', value: 1.80 } });
dispatch({ type: 'CALCULATION_DONE', payload: { canApprove: true, unallocated: 0 } });
dispatch({ type: 'LOCK_ACQUIRED',   payload: { expires_at: '...' } });
dispatch({ type: 'APPROVED' });
```

## The Workspace: Where Reconciliation Happens

The Workspace is PACIOLI's most complex screen — a split-screen reconciliation panel:

```
┌────────────────────────────────────────────────────────────────┐
│ HEADER: Page title, date, user avatar                         │
├──────────────────┬─────────────────────────────────────────────┤
│                  │ PORTFOLIO PANEL                              │
│  TRANSACTION     │ ┌─────────────────────────────────────────┐ │
│  QUEUE           │ │ Suggested Invoices (from pipeline)     │ │
│                  │ │ ☑ FAC-2026-0891  $150.00  ACME Corp   │ │
│  ► BGQ-9981742   │ │ ☐ FAC-2026-0895  $200.00  Beta LLC   │ │
│    $150.00       │ │ ☐ FAC-2026-0897  $75.00   Gamma Inc  │ │
│    DINERS        │ └─────────────────────────────────────────┘ │
│    ACME Corp     │                                              │
│                  │ BALANCE BAR                                   │
│  BGQ-9981743     │ ┌─────────────────────────────────────────┐ │
│    $350.00       │ │ Bank: $150 │ Invoices: $150 │ Unalloc: │ │
│    VISA          │ │ $0.00  ✅ BALANCED                      │ │
│                  │ └─────────────────────────────────────────┘ │
│  BGQ-9981744     │                                              │
│    $1,200.00     │ [🔧 Adjustments]          [✅ Approve]      │
│    PACIFICARD    │                                              │
└──────────────────┴─────────────────────────────────────────────┘
```

### The Balance Bar

The Balance Bar is the heartbeat of the reconciliation UI. It shows whether the
selected invoices + adjustments equal the bank amount:

```javascript
function BalanceBar({ calculation }) {
  const { bankAmount, invoicesTotal, unallocated, canApprove } = calculation;
  
  // Color coding:
  // Green: canApprove (unallocated === 0)
  // Yellow: within tolerance (< $0.05)
  // Red: significant difference
  
  const barColor = canApprove ? '#10B981' : '#EF4444';
  
  return (
    <div style={{ border: `1px solid ${barColor}`, borderRadius: 8, padding: '8px 14px' }}>
      <span>Bank: <strong>${fmt(bankAmount)}</strong></span>
      <span>Invoices: <strong>${fmt(invoicesTotal)}</strong></span>
      <span>Unallocated: <strong style={{ color: barColor }}>${fmt(unallocated)}</strong></span>
      <span>{canApprove ? '✅ Balanced' : '⚠️ Unbalanced'}</span>
    </div>
  );
}
```

### The Adjustment Drawer

When invoices don't perfectly match the bank amount (commissions, taxes, rounding),
the analyst opens the Adjustment Drawer to distribute the difference:

```
Bank amount:     $148.20
Invoice total:   $150.00
Difference:       -$1.80  ← Where does this $1.80 go?

Commission (5540101004):  $1.80  ← Card network fee
IVA (1140105023):         $0.00
IRF (140104019):          $0.00
Exchange diff (3710101001): $0.00
                          ─────
Adjustments total:        $1.80
Unallocated:              $0.00  ✅ Balanced!
```

### Locale-Aware Decimal Parsing

PACIOLI handles both US (`749.53`) and European (`749,53`) number formats:

```javascript
function parseDecimal(val) {
  const str = String(val).trim().replace(/\s/g, '');
  
  // European: "1.157,14" → 1157.14
  if (str.includes('.') && str.includes(',')) {
    const lastDot   = str.lastIndexOf('.');
    const lastComma = str.lastIndexOf(',');
    if (lastComma > lastDot) {
      return parseFloat(str.replace(/\./g, '').replace(',', '.'));
    }
  }
  
  // Comma-only: "749,53" → 749.53
  if (str.includes(',') && !str.includes('.')) {
    return parseFloat(str.replace(',', '.'));
  }
  
  return parseFloat(str) || 0;
}
```

## The Design System: Two Modes

PACIOLI uses a dual-theme design system:

**Vault Mode** (dark) — for navigation, login, and admin areas:
```css
--color-vault-base:    #121412;     /* Near-black background */
--color-vault-orange:  #EA6A1A;     /* Solar Orange — primary accent */
--color-vault-green:   #10B981;     /* Success / matched */
--color-vault-red:     #EF4444;     /* Error / pending */
```

**Report Mode** (light) — for data-dense financial screens:
```css
--color-report-base:    #F8FAFC;    /* Light slate background */
--color-report-surface: #FFFFFF;    /* Card backgrounds */
--color-report-text:    #0F172A;    /* Dark text for readability */
```

The mode switches automatically based on the current route — data pages get light mode
for spreadsheet-like density; navigation stays dark for focus.

## The HTTP Client: Axios with JWT Interceptor

Every API call automatically includes the JWT token:

```javascript
// api/client.js
const apiClient = axios.create({
  baseURL: import.meta.env.VITE_API_URL || 'http://localhost:3000/api/v1',
  timeout: 30000,
});

// Inject JWT on every request
apiClient.interceptors.request.use((config) => {
  const token = localStorage.getItem('pacioli_token');
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

// Auto-redirect to login on 401
apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('pacioli_token');
      window.location.href = '/login';
    }
    return Promise.reject(error);
  }
);
```

---

# Chapter 7: The Journey to Production

## What Is Docker? (For Beginners)

> **Analogy:** Imagine shipping a fragile vase. You could hand it to the delivery
> person and hope they pack it right. Or you could put it in a box with exactly the
> right foam padding, sealed with tape, and labeled "FRAGILE." Docker is that box.
> 
> A **Docker image** is the blueprint for the box (what goes inside, how it's packed).
> A **Docker container** is the actual box, running with your vase inside.

Docker solves the "works on my machine" problem. Your local Windows machine runs Node
18. The production server runs Ubuntu with Node 20. Docker doesn't care — it packages
the exact Node version, OS libraries, and application code into a sealed container.

## PACIOLI's Three Services

PACIOLI consists of three independently deployable services:

| Service | Technology | Port | Purpose |
|---------|-----------|------|---------|
| **API** | Node.js + Express | 3000 | REST endpoints |
| **Pipeline** | Python + Rust | 8000 | Data processing |
| **Frontend** | React (static files) | 80/443 | User interface |

Plus one shared dependency:

| Service | Technology | Port | Purpose |
|---------|-----------|------|---------|
| **Database** | PostgreSQL 18.3 | 5432 | All data storage |

## Deployment Architecture

```mermaid
flowchart TB
    subgraph VM["Corporate Intranet VM"]
        NGINX["Nginx Reverse Proxy<br/>:80 / :443"]
        
        subgraph CONTAINERS["Docker Containers"]
            WEB["pacioli-web<br/>(static React files)<br/>served by Nginx"]
            API_C["pacioli-api<br/>(Node.js Express)<br/>:3000"]
            PIPE["pacioli-pipeline<br/>(Python + Rust)<br/>:8000"]
            PG["postgres:18<br/>:5432"]
        end
    end

    BROWSER["🌐 Analyst's Browser"] --> NGINX
    NGINX -->|"/api/v1/*"| API_C
    NGINX -->|"/"| WEB
    API_C -->|"HTTP trigger"| PIPE
    API_C --> PG
    PIPE --> PG
```

## Docker Configuration

Since PACIOLI doesn't currently have Docker files, here is the recommended setup for
production deployment:

### Dockerfile for the API (`apps/api/Dockerfile`)

```dockerfile
FROM node:20-alpine

WORKDIR /app

# Install dependencies first (cached layer)
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# Copy application code
COPY src/ ./src/

# Environment variables are injected at runtime, not baked in
ENV NODE_ENV=production

EXPOSE 3000

CMD ["node", "src/app.js"]
```

### Dockerfile for the Pipeline (`data-pipeline/Dockerfile`)

```dockerfile
FROM python:3.11-slim

# Install Rust toolchain for compiling pacioli_core
RUN apt-get update && apt-get install -y curl build-essential && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Build Rust extension
COPY pacioli_core/ ./pacioli_core/
RUN cd pacioli_core && pip install maturin && maturin develop --release

# Copy application code
COPY . .

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Dockerfile for the Frontend (`apps/web/Dockerfile`)

```dockerfile
FROM node:20-alpine AS build

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
```

### docker-compose.yml

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:18
    restart: unless-stopped
    environment:
      POSTGRES_DB: pacioli
      POSTGRES_USER: ${DB_USER}
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./data-pipeline/sql:/docker-entrypoint-initdb.d  # Schema init
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${DB_USER}"]
      interval: 5s
      retries: 5

  api:
    build: ./apps/api
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      PORT: 3000
      NODE_ENV: production
      DB_HOST: postgres
      DB_PORT: 5432
      DB_NAME: pacioli
      DB_USER: ${DB_USER}
      DB_PASSWORD: ${DB_PASSWORD}
      JWT_SECRET: ${JWT_SECRET}
    ports:
      - "3000:3000"

  pipeline:
    build: ./data-pipeline
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      DATABASE_URL: postgresql://${DB_USER}:${DB_PASSWORD}@postgres:5432/pacioli
    volumes:
      - ./data_raw:/app/data_raw         # Raw file input
      - ./processed_files:/app/processed_files  # Archived files
    ports:
      - "8000:8000"

  web:
    build: ./apps/web
    restart: unless-stopped
    ports:
      - "80:80"

volumes:
  pgdata:
```

### Nginx Configuration

```nginx
server {
    listen 80;
    server_name pacioli.internal.company.com;

    # React SPA — serve index.html for all non-API routes
    location / {
        root /usr/share/nginx/html;
        try_files $uri $uri/ /index.html;
    }

    # API proxy
    location /api/ {
        proxy_pass http://api:3000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }

    # Health check
    location /health {
        proxy_pass http://api:3000;
    }
}
```

### Environment Variables — What to Configure

Create a `.env` file in the project root:

```bash
# DATABASE — MUST change
DB_USER=pacioli_admin
DB_PASSWORD=<generate-a-strong-password>

# JWT — MUST change (minimum 32 characters)
JWT_SECRET=<generate-with: openssl rand -hex 32>

# OPTIONAL — these have safe defaults
DB_POOL_MIN=2
DB_POOL_MAX=10
JWT_EXPIRES_IN=8h
BCRYPT_ROUNDS=12
```

### What Should NEVER Be Touched

| File/Setting | Why |
|-------------|-----|
| `gl-accounts.js` | GL account codes are governed by the SAP chart of accounts. Changing them without SAP coordination will post to wrong accounts. |
| `biq_gold` schema | Immutable audit trail. Never `DELETE` or `UPDATE` (except `rpa_status`). |
| `bcrypt` cost factor | Set to 12. Lower = insecure. Higher = login becomes noticeably slow. |
| Schema DDL files (`data-pipeline/sql/`) | Authoritative pg_dump exports. Editing these without updating the live DB creates drift. |

---

# Chapter 8: Maintenance, Security & Troubleshooting

## Reading Logs

### Pipeline Logs (Python)

All pipeline activity is logged to `biq_config.pacioli_logs`:

```sql
SELECT log_level, process_name, message, created_at
FROM biq_config.pacioli_logs
WHERE process_name = 'bank_reconciliation'
  AND log_level IN ('ERROR', 'WARNING')
ORDER BY created_at DESC
LIMIT 20;
```

Log levels: `DEBUG` → `INFO` → `WARNING` → `ERROR` → `CRITICAL`

### API Logs (Node.js)

Express uses Morgan for HTTP request logging:
- **Development:** `dev` format — concise colored output
- **Production:** `combined` format — full Apache-style logs

### Pipeline Execution History

```sql
SELECT process_name, status, records_processed, records_failed,
       execution_time_seconds, error_message,
       started_at, completed_at
FROM biq_config.etl_process_windows
WHERE periodo_mes = '2026-04'
ORDER BY created_at DESC;
```

## Common Issues & Solutions

### "No eligible transactions for Gold export"

**Cause:** Transactions exist in `stg_bank_transactions` but haven't been approved.

**Check:**
```sql
SELECT w.work_status, COUNT(*)
FROM biq_auth.transaction_workitems w
GROUP BY w.work_status;
```

If all are `PENDING_ASSIGNMENT`, run "Apply Rules" from the admin panel. If all are
`ASSIGNED`, analysts haven't started reviewing yet.

### "Analyst cannot see their queue"

**Cause:** Assignment rules haven't been applied, or the analyst's account is inactive.

**Check user:**
```sql
SELECT id, username, role, is_active FROM biq_auth.users WHERE username = '<name>';
```

**Check assignments:**
```sql
SELECT bank_ref_1, assigned_user_id, work_status
FROM biq_auth.transaction_workitems
WHERE assigned_user_id = <user_id>;
```

### "Transaction is locked by another analyst"

**Symptom:** `423 Locked` error when trying to open a transaction.

**Check:**
```sql
SELECT bank_ref_1, locked_by_name, expires_at,
       CASE WHEN expires_at > NOW() THEN 'ACTIVE' ELSE 'EXPIRED' END as status
FROM biq_auth.transaction_locks
WHERE bank_ref_1 = '<bank_ref>';
```

If the lock is expired, the next acquire attempt will succeed. If active, the analyst
must close their panel (or their browser crashed — wait 5 minutes for TTL expiry).

### "RPA is not processing Gold records"

**Check pending records:**
```sql
SELECT COUNT(*), MIN(exported_at), MAX(exported_at)
FROM biq_gold.payment_header
WHERE rpa_status = 'PENDING_RPA';
```

Records older than 1 business day indicate the RPA robot is not running. Escalate to
the RPA team.

### "Pipeline hash continuity broken"

**Symptom:** Warning in logs: "hash counter gap detected"

**Diagnosis:**
```sql
SELECT brand, amount_total, match_hash_key
FROM biq_stg.stg_bank_transactions
WHERE brand = 'DINERS' AND amount_total = 150.00
ORDER BY match_hash_key;
```

Look for gaps: `DINERS_150.00_1`, `DINERS_150.00_2`, `DINERS_150.00_4` (missing `_3`).
This usually means a raw file was loaded out of order. Rebuild the cache:

```sql
DELETE FROM biq_stg.hash_counter_cache
WHERE brand = 'DINERS' AND amount_total = 150.00;
```

Then re-run the pipeline for the affected period.

## Security Hardening Checklist

| Area | Measure | Status |
|------|---------|--------|
| **Passwords** | bcrypt cost 12, min 32-char JWT secret | ✅ Enforced in `env.js` |
| **Rate limiting** | 200 req/15min global, stricter on `/auth/login` | ✅ In `app.js` |
| **HTTP headers** | Helmet.js (15 security headers) | ✅ First middleware |
| **CORS** | Localhost in dev, explicit domain in prod | ✅ In `app.js` |
| **SQL injection** | Parameterized queries everywhere (no string concat) | ✅ All repositories |
| **Token expiry** | 8h access token, verified against DB on every request | ✅ In auth middleware |
| **Role enforcement** | `requireRole()` middleware on sensitive endpoints | ✅ Per route |
| **Input validation** | Zod schemas for env vars, React Hook Form for UI | ✅ Both layers |
| **Audit trail** | `biq_auth.audit_log` tracks login/logout/actions | ✅ In auth service |
| **Gold immutability** | No DELETE on `biq_gold`; cancel = set `FAILED` | ✅ By convention + docs |

## Monitoring Queries

**Daily health check:**

```sql
-- 1. Pipeline ran successfully today?
SELECT status, COUNT(*) FROM biq_config.etl_process_windows
WHERE created_at::date = CURRENT_DATE GROUP BY status;

-- 2. How many transactions are stuck?
SELECT work_status, COUNT(*) FROM biq_auth.transaction_workitems
GROUP BY work_status;

-- 3. Gold export backlog?
SELECT rpa_status, COUNT(*), SUM(amount)
FROM biq_gold.payment_header
GROUP BY rpa_status;

-- 4. Expired locks still hanging around?
SELECT COUNT(*) FROM biq_auth.transaction_locks
WHERE expires_at < NOW();
```

---

## Epilogue: The Complete Journey of Our $150 VIP Lounge Deposit

Let's trace our deposit one final time, end to end:

```
1. EXCEL FILE         banco_guayaquil_2026-04-08.xlsx, row 47
                      ↓
2. RAW LOADER         Cleaned, hashed (SHA-256), loaded to biq_raw
                      ↓
3. STAGING            Enriched → stg_bank_transactions (stg_id: 47201)
                      bank_ref_1: BGQ-9981742, amount: $150.00
                      ↓
4. MATCHING ENGINE    Strategy 1: Exact Single Match ✓
                      Matched to FAC-2026-0891 ($150.00)
                      Score: 98.5, Status: MATCHED
                      ↓
5. WORKITEM CREATED   biq_auth.transaction_workitems
                      bank_ref_1: BGQ-9981742, work_status: ASSIGNED
                      assigned_user_id: 3 (jdoe)
                      ↓
6. ANALYST OPENS      POST /locks/BGQ-9981742/acquire → 5-min lock
                      GET /workspace/47201/panel → bank + invoices loaded
                      ↓
7. BALANCE BAR        Bank: $150.00 | Invoices: $150.00 | Unallocated: $0.00
                      ✅ BALANCED — canApprove: true
                      ↓
8. APPROVE            POST /workspace/47201/approve
                      work_status → APPROVED
                      reconcile_status → MATCHED_MANUAL
                      Lock released
                      ↓
9. GOLD EXPORT        POST /gold-export/submit
                      → payment_header (rpa_status: PENDING_RPA)
                      → payment_detail (FAC-2026-0891, $150.00)
                      → payment_diff (empty — no adjustments)
                      ↓
10. RPA ROBOT         Reads PENDING_RPA records
                      Posts SAP F-28: DZ, company 8000, USD, GL 1110213001
                      → rpa_status: POSTED
                      → rpa_doc_number: 4900012345
                      ↓
11. DONE              The hotel's books balance. The $150 VIP Lounge charge
                      is reconciled. The auditor is happy. Go home.
```

---

*PACIOLI: Because accountants deserve software that works as precisely as they do.*
