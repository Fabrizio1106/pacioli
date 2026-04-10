# PACIOLI — Bank Reconciliation System

PACIOLI automates daily reconciliation of bank settlements against customer invoices for an international airport treasury department. It matches card settlements, cash deposits, wire transfers, and checks against SAP accounts receivable, then exports approved reconciliations to SAP F-28 via an RPA robot.

---

## System Architecture

```mermaid
graph TB
    subgraph Sources["Source Files (data_raw/)"]
        S1[SAP FBL5N / F-239]
        S2[Card Settlements<br/>Diners · Guayaquil · Pacificard]
        S3[Bank Statement]
        S4[Withholdings / WebPOS]
    end

    subgraph Pipeline["Data Pipeline (Python · ~44s)"]
        ORC[Silver Orchestrator<br/>18 processes · 6 groups]
        CORE[pacioli_core<br/>Rust · subset sum · fuzzy match]
        ORC -->|hot-path calls| CORE
    end

    subgraph DB["PostgreSQL"]
        RAW[(biq_raw<br/>raw extracts)]
        STG[(biq_stg<br/>staging + matching)]
        GOLD[(biq_gold<br/>approved postings)]
        AUTH[(biq_auth<br/>sessions · tokens)]
        CFG[(biq_config<br/>rules · ETL windows)]
    end

    subgraph API["Node.js API (Express)"]
        direction TB
        CTL[Controllers]
        SVC[Services]
        REPO[Repositories]
        CTL --> SVC --> REPO
    end

    subgraph Frontend["React Frontend (Vite)"]
        PG1[Overview]
        PG2[Workspace]
        PG3[Submit for Posting]
        PG4[Reports]
        PG5[Data Ingestion]
    end

    subgraph Downstream
        PS[Pipeline Server<br/>FastAPI · port 8000]
        RPA[RPA Robot<br/>SAP F-28 posting]
    end

    Sources --> ORC
    ORC --> RAW --> STG
    STG --> GOLD
    REPO --> DB
    Frontend --> API
    API --> DB
    API --> PS
    PS --> ORC
    GOLD --> RPA
```

---

## Prerequisites

| Component | Requirement | Notes |
|-----------|-------------|-------|
| Node.js | ≥ 20.0.0 | API + frontend |
| npm | ≥ 10.0.0 | Bundled with Node.js 20 |
| Python | ≥ 3.10 | Data pipeline |
| Rust + Cargo | ≥ 1.75 | pacioli_core compilation |
| maturin | ≥ 1.7, < 2.0 | Python–Rust bridge |
| PostgreSQL | ≥ 15 | All schemas in one cluster |
| Git | any | — |

---

## Installation

### 1. Clone the repository

```bash
git clone <repo-url> pacioli
cd pacioli
```

### 2. Database setup

Create the five schemas and run migrations:

```bash
psql -U postgres -c "CREATE DATABASE pacioli;"
psql -U postgres -d pacioli -f database/migrations/001_create_schemas.sql
# Run remaining migration files in order
```

### 3. API (Node.js)

```bash
cd apps/api
npm install
cp .env.example .env        # fill in DB credentials and JWT_SECRET
npm run dev                 # starts on port 3000
```

**Required `.env` variables:**

```
DATABASE_URL=postgresql://user:password@localhost:5432/pacioli
JWT_SECRET=<long-random-string>
JWT_REFRESH_SECRET=<long-random-string>
```

### 4. Frontend (React + Vite)

```bash
cd apps/web
npm install
cp .env.example .env        # set VITE_API_URL
npm run dev                 # starts on port 5173
```

**Required `.env` variable:**

```
VITE_API_URL=http://localhost:3000
```

### 5. Rust core (pacioli_core)

Compile the Rust extension before running the pipeline. Requires Rust toolchain and maturin.

```bash
cd data-pipeline/pacioli_core
pip install maturin
maturin develop --release   # compiles and installs into the active Python env
```

### 6. Data pipeline

```bash
cd data-pipeline
pip install -r requirements.txt   # or: pip install -e .
cp config/settings.yaml.example config/settings.yaml   # adjust DB connection
```

### 7. Pipeline server (FastAPI sidecar)

The pipeline server keeps Python warm between runs, reducing cold-start overhead from ~2m20s to ~44s.

```bash
cd data-pipeline
pip install fastapi uvicorn
PIPELINE_API_KEY=<secret> python pipeline_server.py
# or: start_pipeline_server.bat (Windows)
# Starts on port 8000 (override with PIPELINE_PORT env var)
```

The Node.js API expects the pipeline server at `http://localhost:8000`. Set `PIPELINE_API_KEY` in the API `.env` to match.

---

## Running the system

### API

```bash
cd apps/api
npm run dev        # development (nodemon)
npm start          # production
```

### Frontend

```bash
cd apps/web
npm run dev        # development
npm run build      # production build → dist/
```

### Pipeline (manual trigger)

```bash
cd data-pipeline
python main_silver_orchestrator.py   # runs all 18 processes (~44s)
```

Or trigger via the Data Ingestion page in the UI, which calls `POST /run` on the pipeline server.

### Pipeline server

```bash
cd data-pipeline
PIPELINE_API_KEY=<secret> python pipeline_server.py
```

---

## Project structure

```
pacioli/
├── apps/
│   ├── api/                        # Node.js Express API
│   │   └── src/modules/
│   │       ├── auth/               # Login, JWT, sessions
│   │       ├── assignments/        # Auto-assignment rules
│   │       ├── gold-export/        # SAP F-28 posting export
│   │       ├── ingestion/          # File upload + pipeline trigger
│   │       ├── locks/              # Optimistic row locking
│   │       ├── notifications/      # Reversal request workflow
│   │       ├── overview/           # Dashboard + sync
│   │       ├── portfolio/          # Customer invoice search
│   │       ├── reconciliation/     # Match calculation + approval
│   │       ├── reports/            # 7 report types (R1–R7)
│   │       ├── reversals/          # Approved reversal execution
│   │       ├── transactions/       # Bank transaction listing
│   │       ├── users/              # Analyst directory
│   │       └── workspace/          # Per-analyst queue + panel
│   └── web/                        # React + Vite frontend
│       └── src/pages/
│           ├── Overview/           # Daily dashboard
│           ├── Workspace/          # Reconciliation workspace
│           ├── SubmitPosting/      # Gold export review + submit
│           ├── Reports/            # Report viewer + CSV export
│           └── DataIngestion/      # File upload + pipeline monitor
├── data-pipeline/
│   ├── main_silver_orchestrator.py # Pipeline entry point
│   ├── pipeline_server.py          # FastAPI sidecar (warm Python)
│   ├── pacioli_core/               # Rust extension (subset sum, fuzzy)
│   ├── data_loaders/               # RAW file loaders (10 sources)
│   ├── logic/
│   │   ├── application/commands/staging/   # 18 pipeline processes
│   │   ├── domain/services/               # Business logic
│   │   └── infrastructure/               # DB repositories, extractors
│   └── config/                     # settings.yaml, assignment rules
├── database/
│   └── migrations/                 # SQL migration files
└── docs/                           # Technical documentation
    ├── architecture/
    ├── data/
    └── api/
```

---

## Team and roles

| Username | Name | Role | Access |
|----------|------|------|--------|
| `analyst2` | Analyst Two | `senior_analyst` | Own queue + elevated privileges (post to Gold) |
| `analyst3` | Analyst Three | `analyst` | Assigned queue |
| `analyst4` | Analyst Four | `analyst` | Assigned queue |
| `analyst5` | Analyst Five | `analyst` | Assigned queue |
| `viewer1` | Viewer One | `viewer` | Read-only |

**Role hierarchy:**

| Role | Queue | Approve reversals | Export to Gold | Reassign transactions |
|------|-------|-------------------|----------------|-----------------------|
| `admin` | All | ✓ | ✓ | ✓ |
| `senior_analyst` | Own + all visible | ✓ | ✓ | — |
| `analyst` | Assigned only | — | — | — |
| `viewer` | Read-only | — | — | — |

---

## Current system status

| Metric | Value |
|--------|-------|
| Status | Production — deployed and actively used |
| Pipeline runtime | Under 44 seconds (full month) |
| Pipeline processes | 18 / 18 successful |
| Auto-match rate (cards) | ~79% |
| Daily manual work replaced | 5+ hours → automated |

---

## Documentation

| Document | Description |
|----------|-------------|
| [`docs/architecture/overview.md`](docs/architecture/overview.md) | Layer diagram, module map, reconcile_status state machine |
| [`docs/architecture/pipeline-algorithms.md`](docs/architecture/pipeline-algorithms.md) | 18 pipeline processes, matching algorithms, VIP Cascade |
| [`docs/architecture/rust-core.md`](docs/architecture/rust-core.md) | Rust subset sum and fuzzy matching implementation |
| [`docs/architecture/decisions/`](docs/architecture/decisions/) | Architecture Decision Records (ADR-001 through ADR-004) |
| [`docs/data/dictionary.md`](docs/data/dictionary.md) | Field-level data dictionary across all 5 schemas |
| [`docs/data/schemas.md`](docs/data/schemas.md) | Table definitions and schema relationships |
| [`docs/api/endpoints.md`](docs/api/endpoints.md) | All API endpoints with parameters and response shapes |
| [`RUNBOOK.md`](RUNBOOK.md) | Day-to-day operations: pipeline runs, common failures, recovery |
