# Architecture Overview

## Table of Contents

1. [System Layers](#system-layers)
2. [Node.js API Module Map](#nodejs-api-module-map)
3. [reconcile\_status State Machine](#reconcile_status-state-machine)
4. [work\_status State Machine](#work_status-state-machine)
5. [Gold Layer Flow](#gold-layer-flow)

---

## System Layers

```mermaid
graph TB
    subgraph FE["Frontend — React + Vite"]
        FE_PAGES["Pages<br/>Overview · Workspace · SubmitPosting<br/>Reports · DataIngestion"]
        FE_HOOKS["Hooks (TanStack Query / Zustand)<br/>useQuery · useMutation · useAuthStore · useUIStore"]
        FE_API["api/endpoints/*.api.js<br/>one file per API module"]
        FE_CLIENT["api/client.js<br/>axios instance + auth interceptor"]
        FE_PAGES --> FE_HOOKS --> FE_API --> FE_CLIENT
    end

    subgraph API["Node.js API — Express"]
        API_ROUTES["Routes<br/>requireAuth · requireRole middleware"]
        API_CTL["Controllers<br/>HTTP parsing · response shaping"]
        API_SVC["Services<br/>business logic · orchestration · UoW transactions"]
        API_REPO["Repositories<br/>all SQL · pool.query / pool.connect"]
        API_ROUTES --> API_CTL --> API_SVC --> API_REPO
    end

    subgraph PG["PostgreSQL"]
        PG_RAW[(biq_raw<br/>raw file extracts)]
        PG_STG[(biq_stg<br/>staging · matching · enrichment)]
        PG_GOLD[(biq_gold<br/>approved postings for RPA)]
        PG_AUTH[(biq_auth<br/>users · sessions · workitems · reversal requests)]
        PG_CFG[(biq_config<br/>ETL windows · assignment rules · process metrics)]
    end

    subgraph PIPE["Data Pipeline — Python"]
        P_ORC["main_silver_orchestrator.py<br/>18 processes · 6 sequential groups"]
        P_CMD["application/commands/staging/<br/>one command class per process"]
        P_DOM["domain/services/<br/>matching · enrichment · hashing · period closure"]
        P_INF["infrastructure/<br/>repositories · extractors · unit of work"]
        P_ORC --> P_CMD --> P_DOM --> P_INF
    end

    subgraph RUST["pacioli_core — Rust"]
        R1["find_invoice_combination<br/>subset sum · two-pointer O(n²)"]
        R2["fuzzy_batch_match<br/>Jaccard similarity in batch"]
    end

    subgraph SIDE["Pipeline Server — FastAPI"]
        PS_H["GET /health"]
        PS_R["POST /run  (auth: X-Pipeline-Key)"]
        PS_S["GET /status"]
        PS_L["GET /logs/stream  (SSE, auth: X-Pipeline-Key)"]
    end

    subgraph RPA["Downstream"]
        RPA_BOT["RPA Robot<br/>reads biq_gold · posts SAP F-28"]
    end

    FE_CLIENT -->|"REST / JSON"| API_ROUTES
    API_REPO --> PG_RAW & PG_STG & PG_GOLD & PG_AUTH & PG_CFG
    API_SVC -->|"POST /run"| PS_R
    P_INF --> PG_RAW & PG_STG & PG_CFG
    P_DOM -->|"PyO3 FFI"| RUST
    P_ORC --> PG_CFG
    SIDE --> P_ORC
    PG_GOLD --> RPA_BOT
```

### Layer responsibilities

| Layer | Owns | Must not |
|-------|------|----------|
| Pages / components | UI state, user interaction | Call DB or business logic directly |
| Hooks | Server state caching, mutation lifecycle | Contain business rules |
| api/endpoints | HTTP call definitions | Import from other modules |
| Controllers | Parse HTTP, shape response, call one service | Contain SQL or business logic |
| Services | Business logic, orchestrate repo calls, manage UoW transactions | Issue HTTP calls |
| Repositories | All SQL | Contain business logic or call other repos |

---

## Node.js API Module Map

| Module | Responsibility |
|--------|---------------|
| `auth` | Login with username/password, issue JWT access + refresh tokens, maintain sessions, verify tokens on all protected routes |
| `assignments` | Apply rule-based auto-assignment of pending transactions to analysts; support manual reassignment by admins |
| `gold-export` | Build and submit the Gold Layer export batch (payment headers, invoice detail lines, diff adjustment lines); return batch history |
| `ingestion` | Accept file uploads, classify files to loaders, write to `data_raw/`, trigger pipeline via Pipeline Server, stream status |
| `locks` | Acquire, renew, and release per-transaction row locks; prevent two analysts from editing the same transaction simultaneously |
| `notifications` | Manage the reversal request workflow — analysts request, admins approve or reject; notify affected parties |
| `overview` | Return daily KPI aggregates and transaction list; sync automatic pipeline matches into workitems |
| `portfolio` | Search customer invoice portfolio; load portfolio items for a given bank transaction; validate analyst selection |
| `reconciliation` | Calculate match balance (gross, net, commission, IVA, IRF, diff); validate and approve manual reconciliations |
| `reports` | Serve 7 parameterized report types (R1 Overview, R2 Bank, R3 Portfolio, R4–R5 Cards, R6 Parking, R7 Summary) with CSV export |
| `reversals` | Execute approved reversals — restore bank transaction and portfolio items to PENDING, cancel Gold entry if not yet RPA-processed |
| `transactions` | List and paginate bank transactions with filters; return transaction detail and status summary counts |
| `users` | Return the analyst directory; used by admin dropdowns for manual reassignment |
| `workspace` | Return per-analyst queue; load the full transaction panel (bank data + portfolio candidates); handle approval submission |

---

## reconcile\_status State Machine

Applies to `biq_stg.stg_bank_transactions`.

```mermaid
stateDiagram-v2
    [*] --> PENDING : Pipeline loads bank transaction

    PENDING --> MATCHED : Pipeline auto-match\n(ALGORITHM_PACIOLI)
    PENDING --> REVIEW : Pipeline flags\nambiguous / partial match
    PENDING --> MATCHED_MANUAL : Analyst approves\nin Workspace

    REVIEW --> MATCHED_MANUAL : Analyst resolves\nand approves

    MATCHED --> MATCHED_MANUAL : Analyst overrides\nauto-match

    MATCHED_MANUAL --> PENDING : Reversal executed\n(before Gold export)\nor after Gold CANCELLED
    MATCHED --> PENDING : Reversal executed\n(before Gold export)
```

### State definitions

| Status | Set by | Meaning |
|--------|--------|---------|
| `PENDING` | Pipeline (initial load); reversal execution | Awaiting reconciliation |
| `REVIEW` | Pipeline matching algorithm | Auto-match attempted but confidence below threshold or partial match |
| `MATCHED` | Pipeline (`ALGORITHM_PACIOLI`) | Fully matched automatically; workitem created and auto-approved |
| `MATCHED_MANUAL` | Analyst approval in Workspace | Manually reconciled and approved by an analyst |

> Portfolio items (`biq_stg.stg_customer_portfolio`) follow a parallel lifecycle. The pipeline sets the initial state on load (`PENDING`), then the enrichment phase updates items it successfully enriched to `ENRICHED` — this is set entirely by the pipeline and has no corresponding API transition. When an analyst approves a reconciliation, all selected items are set to `CLOSED` by the API. `CLOSED` items cannot be selected for a new reconciliation. On reversal, items are restored to `PENDING` regardless of whether they were `ENRICHED` or `CLOSED` at approval time.

---

## work\_status State Machine

Applies to `biq_auth.transaction_workitems`. One workitem exists per bank transaction.

```mermaid
stateDiagram-v2
    [*] --> PENDING_ASSIGNMENT : Workitem created\nby pipeline or sync

    PENDING_ASSIGNMENT --> ASSIGNED : Assignment rules run\nor admin reassigns

    ASSIGNED --> IN_PROGRESS : Analyst opens\ntransaction (acquires lock)
    IN_PROGRESS --> ASSIGNED : Lock expires or\nanalyst navigates away

    IN_PROGRESS --> APPROVED : Analyst submits\napproval

    APPROVED --> REVERSED : Reversal executed\nby admin/senior
```

### State definitions

| Status | Set by | Meaning |
|--------|--------|---------|
| `PENDING_ASSIGNMENT` | Pipeline sync; reversal | Created but not yet assigned to an analyst |
| `ASSIGNED` | Assignment rules engine; lock expiry; admin reassignment | Assigned to an analyst; visible in their queue |
| `IN_PROGRESS` | Lock acquisition (analyst opens transaction) | Analyst is actively working; row is locked |
| `APPROVED` | Analyst approval; auto-match sync | Reconciliation approved; eligible for Gold export |
| `REVERSED` | Reversal execution | Approval undone; bank transaction restored to PENDING |

---

## Gold Layer Flow

The Gold Layer converts approved reconciliations into structured SAP F-28 posting records consumed by the RPA robot.

```mermaid
flowchart TD
    A["Workitem: work_status = APPROVED\n(manual or ALGORITHM_PACIOLI)"]
    B["Senior analyst reviews\nSubmit for Posting page\n(getExportPreview)"]
    C{"Satisfied?"}
    D["submitForPosting called\n(atomic DB transaction)"]

    subgraph Batch["Per-transaction (within single transaction)"]
        E["Compute SHA-256 idempotency hash\nbankRef1 · date · amount · batchId"]
        F{"Hash exists\nin gold_headers?"}
        G["Skip (skipped++)"]
        H["Resolve customer\nLIQUIDACION TC → portfolio customer\nTransfer → enriched or portfolio"]
        I["Build reference text\n≤ 255 chars for SAP"]
        J["INSERT gold_headers\nrpa_status = PENDING_RPA"]
        K["INSERT gold_details\none row per matched invoice"]
        L{"diff_amount\npresent?"}
        M["INSERT gold_diffs\nadjustment line (key 40/50)"]
        N["exported++"]
    end

    O["COMMIT\nbatch written atomically"]
    P["RPA robot reads biq_gold\nWHERE rpa_status = PENDING_RPA"]
    Q["RPA posts SAP F-28\nupdates rpa_status → POSTED / FAILED"]

    subgraph Reversal["If reversal approved before RPA processes"]
        R["gold_headers SET rpa_status = CANCELLED\nWHERE rpa_status = PENDING_RPA"]
        S["bank tx → PENDING\nportfolio items → PENDING\nworkitem → REVERSED"]
    end

    A --> B --> C
    C -- "No" --> B
    C -- "Yes" --> D --> E --> F
    F -- "Yes (duplicate)" --> G
    F -- "No" --> H --> I --> J --> K --> L
    L -- "Yes" --> M --> N
    L -- "No" --> N
    N --> O --> P --> Q
    A --> Reversal
```

### Gold Layer table structure

| Table | One row per | Key fields |
|-------|-------------|------------|
| `biq_gold.gold_headers` | Bank transaction | `batch_id`, `bank_ref_1`, `amount`, `customer_code`, `rpa_status`, `idempotency_hash` |
| `biq_gold.gold_details` | Matched invoice | `header_id`, `invoice_ref`, `customer_code`, `amount_gross`, `gl_account`, `is_partial_payment` |
| `biq_gold.gold_diffs` | Diff adjustment entry | `header_id`, `diff_amount`, `posting_key` (40 = debit / 50 = credit) |

### Posting key logic

When the bank amount does not exactly equal the sum of matched invoices, a diff adjustment line balances the entry:

| Condition | Posting key | Direction |
|-----------|-------------|-----------|
| Bank overpaid (diff > 0) | 50 | Credit (haber) |
| Bank underpaid (diff < 0) | 40 | Debit (debe) |
