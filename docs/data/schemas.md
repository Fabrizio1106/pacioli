# PACIOLI — Schema DDL Reference

DDL sourced from live pg_dump exports (`data-pipeline/sql/biq_*_schema.sql`).
Database: PostgreSQL 18.3. All timestamps are `timestamp without time zone`
in `biq_stg` and `biq_config`; `timestamp with time zone` in `biq_auth` and
`biq_gold`. No ORM migrations exist — these files are the authoritative source.

---

## Table of Contents

- [biq\_stg](#biq_stg)
  - [stg\_bank\_transactions](#stg_bank_transactions)
  - [stg\_customer\_portfolio](#stg_customer_portfolio)
  - [stg\_card\_settlements](#stg_card_settlements)
- [biq\_auth](#biq_auth)
  - [users](#users)
  - [assignment\_rules](#assignment_rules)
  - [transaction\_workitems](#transaction_workitems)
  - [transaction\_locks](#transaction_locks)
- [biq\_gold](#biq_gold)
  - [payment\_header](#payment_header)
  - [payment\_detail](#payment_detail)
  - [payment\_diff](#payment_diff)
- [biq\_config](#biq_config)
  - [etl\_process\_windows](#etl_process_windows)

---

## biq_stg

Staging schema. Owned by the Python pipeline. The API reads these tables
but never writes to them directly. Data is replaced on every pipeline run
for the same period.

---

### stg_bank_transactions

One row per bank transaction line from the SAP FBL3N extract. This is the
central table of the reconciliation process. Every analyst workitem
and every Gold export traces back to a row here.

```sql
CREATE TABLE biq_stg.stg_bank_transactions (
    stg_id                  bigint NOT NULL,              -- PK (sequence)
    etl_batch_id            character varying(50),
    source_system           character varying(20)  DEFAULT 'SAP_FBL3N',
    doc_date                date,
    posting_date            date,
    doc_number              character varying(50),
    doc_type                character varying(10),        -- 'ZR','SA' eligible for export
    doc_reference           character varying(100),
    amount_total            numeric(18,2),
    amount_sign             character varying(1),
    currency                character varying(5),
    sap_description         character varying(255),
    bank_date               timestamp without time zone,  -- NOT date
    bank_ref_1              character varying(100),       -- business key; join to workitems
    bank_ref_2              character varying(100),
    bank_description        character varying(255),
    bank_office_id          character varying(50),
    trans_type              character varying(50),
    global_category         character varying(50),
    brand                   character varying(50),
    batch_number            character varying(50),
    match_hash_key          character varying(255),
    is_compensated_sap      boolean DEFAULT false,
    is_compensated_intraday boolean DEFAULT false,
    reconcile_status        character varying(20)  DEFAULT 'PENDING',
    created_at              timestamp without time zone DEFAULT now() NOT NULL,
    settlement_id           character varying(100),
    establishment_name      character varying(100),
    count_voucher_bank      integer,
    count_voucher_portfolio integer,
    final_amount_gross      numeric(18,2),
    final_amount_net        numeric(18,2),
    final_amount_commission numeric(18,2),
    final_amount_tax_iva    numeric(18,2),
    final_amount_tax_irf    numeric(18,2),
    diff_adjustment         numeric(15,2),
    reconcile_reason        character varying(50),
    enrich_customer_id      character varying(50),
    enrich_customer_name    character varying(255),
    enrich_confidence_score numeric(5,2)           DEFAULT 0.00,
    enrich_inference_method character varying(50),
    enrich_notes            text,
    match_confidence_score  numeric(5,2)           DEFAULT 0.00,
    match_method            character varying(50),
    matched_portfolio_ids   text,                         -- NOT jsonb; CSV of stg_ids
    bank_ref_match          character varying(100),
    reconciled_at           timestamp without time zone,
    alternative_matches     text,                         -- NOT jsonb
    updated_at              timestamp without time zone DEFAULT now()
);
```

**Constraints**

```sql
ALTER TABLE ONLY biq_stg.stg_bank_transactions
    ADD CONSTRAINT stg_bank_transactions_pkey PRIMARY KEY (stg_id);
```

**Indexes**

```sql
CREATE INDEX idx_sbt_bank_ref_1        ON biq_stg.stg_bank_transactions USING btree (bank_ref_1);
CREATE INDEX idx_sbt_reconcile_status  ON biq_stg.stg_bank_transactions USING btree (reconcile_status);
CREATE INDEX idx_sbt_doc_type          ON biq_stg.stg_bank_transactions USING btree (doc_type);
CREATE INDEX idx_sbt_settlement_id     ON biq_stg.stg_bank_transactions USING btree (settlement_id);
CREATE INDEX idx_sbt_posting_date      ON biq_stg.stg_bank_transactions USING btree (posting_date);
```

---

### stg_customer_portfolio

One row per open invoice from the SAP FBL5N extract. The analyst selects
rows from this table when approving a reconciliation. Split payments produce
child rows via `parent_stg_id`.

```sql
CREATE TABLE biq_stg.stg_customer_portfolio (
    stg_id                bigint NOT NULL,              -- PK (sequence)
    sap_doc_number        character varying(100),
    accounting_doc        character varying(100),
    customer_code         character varying(50),
    customer_name         character varying(200),
    assignment            character varying(100),
    invoice_ref           character varying(100),
    doc_date              date,
    due_date              date,
    amount_outstanding    numeric(18,2),
    conciliable_amount    numeric(18,2),
    currency              character(3),                 -- CHAR not VARCHAR
    enrich_batch          character varying(100),
    enrich_ref            character varying(100),
    enrich_brand          character varying(100),
    enrich_user           character varying(100),
    enrich_source         character varying(100),
    reconcile_group       character varying(100),
    match_hash_key        character varying(150),
    etl_hash              character varying(150),
    reconcile_status      character varying(100),
    settlement_id         character varying(100),
    financial_amount_gross  numeric(18,2),
    financial_amount_net    numeric(18,2),
    financial_commission    numeric(18,2),
    financial_tax_iva       numeric(18,2),
    financial_tax_irf       numeric(18,2),
    match_method          character varying(100),
    match_confidence      character varying(20),        -- NOT integer
    is_suggestion         boolean DEFAULT false,
    sap_text              character varying(200),
    gl_account            character varying(20),
    internal_ref          character varying(100),
    created_at            timestamp without time zone DEFAULT now() NOT NULL,
    matched_bank_refs     text,
    partial_payment_flag  boolean DEFAULT false,
    updated_at            timestamp without time zone DEFAULT now(),
    closed_at             timestamp without time zone,
    reconciled_at         timestamp without time zone,
    is_partial_payment    boolean DEFAULT false,
    sap_residual_amount   numeric(15,2)          DEFAULT NULL,
    is_manual_residual    boolean DEFAULT false,
    parent_stg_id         bigint                        -- FK to self for split payments
);
```

**Column comments**

```sql
COMMENT ON COLUMN biq_stg.stg_customer_portfolio.is_partial_payment IS
    'TRUE si este registro aplica un pago parcial sobre la factura original.
     El RPA debe abrir la pestaña Part. Restante en SAP F-28 y colocar sap_residual_amount.';

COMMENT ON COLUMN biq_stg.stg_customer_portfolio.sap_residual_amount IS
    'Monto que debe quedar abierto en SAP cuando is_partial_payment = TRUE.
     Calculado como: amount_outstanding_original - monto_aplicado_en_este_registro.
     NULL cuando is_partial_payment = FALSE.';
```

**Constraints**

```sql
ALTER TABLE ONLY biq_stg.stg_customer_portfolio
    ADD CONSTRAINT stg_customer_portfolio_pkey PRIMARY KEY (stg_id);
```

**Indexes**

```sql
CREATE INDEX idx_scp_customer_code     ON biq_stg.stg_customer_portfolio USING btree (customer_code);
CREATE INDEX idx_scp_invoice_ref       ON biq_stg.stg_customer_portfolio USING btree (invoice_ref);
CREATE INDEX idx_scp_reconcile_status  ON biq_stg.stg_customer_portfolio USING btree (reconcile_status);
CREATE INDEX idx_scp_settlement_id     ON biq_stg.stg_customer_portfolio USING btree (settlement_id);
CREATE INDEX idx_scp_parent_stg_id     ON biq_stg.stg_customer_portfolio USING btree (parent_stg_id);
```

---

### stg_card_settlements

One row per card settlement batch from the daily Pacificard / Diners files.
Matched to `stg_bank_transactions` via `settlement_id`.

```sql
CREATE TABLE biq_stg.stg_card_settlements (
    stg_id           bigint NOT NULL,              -- PK (sequence)
    etl_batch_id     character varying(50),
    source_file      character varying(255),
    settlement_id    character varying(100),
    settlement_date  date,
    brand            character varying(50),
    batch_number     character varying(50),
    amount_gross     numeric(18,2),
    amount_net       numeric(18,2),
    amount_commission numeric(18,2),
    amount_tax_iva   numeric(18,2),
    amount_tax_irf   numeric(18,2),
    match_hash_key   character varying(255),
    reconcile_status character varying(20)  DEFAULT 'PENDING',
    created_at       timestamp without time zone DEFAULT now() NOT NULL,
    count_voucher    integer DEFAULT 0,
    establishment_name character varying(100),
    etl_hash         character varying(64),
    updated_at       timestamp without time zone DEFAULT now()
);
```

**Constraints**

```sql
ALTER TABLE ONLY biq_stg.stg_card_settlements
    ADD CONSTRAINT stg_card_settlements_pkey PRIMARY KEY (stg_id);
```

**Indexes**

```sql
CREATE INDEX idx_scs_settlement_id     ON biq_stg.stg_card_settlements USING btree (settlement_id);
CREATE INDEX idx_scs_reconcile_status  ON biq_stg.stg_card_settlements USING btree (reconcile_status);
CREATE INDEX idx_scs_brand             ON biq_stg.stg_card_settlements USING btree (brand);
```

---

## biq_auth

Authorization and workflow schema. Owned exclusively by the Node.js API.
The Python pipeline never writes here.

---

### users

System users. Role drives endpoint authorization. `password_hash` is bcrypt
cost-12. The API never returns `password_hash` to clients.

```sql
CREATE TABLE biq_auth.users (
    id            integer NOT NULL,              -- PK (sequence)
    username      character varying(50)  NOT NULL,
    email         character varying(100) NOT NULL,
    password_hash character varying(255) NOT NULL,
    full_name     character varying(100) NOT NULL,
    role          character varying(20)  DEFAULT 'analyst' NOT NULL,
    is_active     boolean DEFAULT true NOT NULL,
    created_at    timestamp with time zone DEFAULT now() NOT NULL,
    updated_at    timestamp with time zone DEFAULT now() NOT NULL,
    last_login_at timestamp with time zone,
    CONSTRAINT users_role_check CHECK (role = ANY (
        ARRAY['admin', 'analyst', 'viewer', 'senior_analyst']
    ))
);
```

**Constraints**

```sql
ALTER TABLE ONLY biq_auth.users
    ADD CONSTRAINT users_pkey        PRIMARY KEY (id);
ALTER TABLE ONLY biq_auth.users
    ADD CONSTRAINT users_username_key UNIQUE (username);
ALTER TABLE ONLY biq_auth.users
    ADD CONSTRAINT users_email_key   UNIQUE (email);
```

---

### assignment_rules

Auto-assignment rules evaluated in descending `priority` order when a new
workitem is created. The `is_default = TRUE` rule catches all transactions
that matched no other rule.

```sql
CREATE TABLE biq_auth.assignment_rules (
    id                 integer NOT NULL,          -- PK (sequence)
    rule_name          character varying(100) NOT NULL,
    description        text,
    trans_type         character varying(50),     -- filter: matches stg_bank_transactions.trans_type
    global_category    character varying(50),     -- filter: matches stg_bank_transactions.global_category
    brand              character varying(50),     -- filter: matches stg_bank_transactions.brand
    enrich_customer_id character varying(50),     -- filter: matches stg_bank_transactions.enrich_customer_id
    assign_to_user_id  integer NOT NULL,          -- FK to users.id
    priority           integer DEFAULT 0 NOT NULL,
    is_active          boolean DEFAULT true NOT NULL,
    is_default         boolean DEFAULT false NOT NULL,
    created_at         timestamp with time zone DEFAULT now(),
    created_by         character varying(50)
);
```

**Table comment**

```sql
COMMENT ON TABLE biq_auth.assignment_rules IS
    'Auto-assignment rules for bank transactions to analysts.
     Evaluated in descending priority order.
     is_default=TRUE rule catches everything that did not match.';
```

**Constraints**

```sql
ALTER TABLE ONLY biq_auth.assignment_rules
    ADD CONSTRAINT assignment_rules_pkey PRIMARY KEY (id);
ALTER TABLE ONLY biq_auth.assignment_rules
    ADD CONSTRAINT assignment_rules_assign_to_user_id_fkey
        FOREIGN KEY (assign_to_user_id) REFERENCES biq_auth.users(id);
```

**Indexes**

```sql
CREATE INDEX idx_process_config_type  ON biq_auth.assignment_rules USING btree (is_active);
CREATE INDEX idx_process_config_order ON biq_auth.assignment_rules USING btree (priority DESC);
```

---

### transaction_workitems

Operational lifecycle state for each bank transaction eligible for analyst
review. **Primary key is `bank_ref_1` (the business key), not a surrogate
integer.** This key survives daily Python pipeline snapshots because the
pipeline overwrites `stg_bank_transactions` rows but never touches this
table.

```sql
CREATE TABLE biq_auth.transaction_workitems (
    bank_ref_1           character varying(200) NOT NULL,  -- PK (business key)
    stg_id               bigint NOT NULL,
    assigned_user_id     integer,                          -- FK to users.id
    assigned_at          timestamp with time zone,
    assigned_by          character varying(50),
    work_status          character varying(30) DEFAULT 'PENDING_ASSIGNMENT' NOT NULL,
    approved_portfolio_ids text,                           -- CSV: "2838,2836"
    approved_amounts     jsonb,
    approved_by          character varying(50),
    approved_at          timestamp with time zone,
    approval_notes       text,
    diff_account_code    character varying(20),
    diff_amount          numeric(18,4),
    is_override          boolean DEFAULT false NOT NULL,
    override_reason      text,
    override_by          character varying(50),
    override_at          timestamp with time zone,
    detected_scenario    character varying(50),
    created_at           timestamp with time zone DEFAULT now() NOT NULL,
    updated_at           timestamp with time zone DEFAULT now() NOT NULL,
    approved_commission  numeric(14,2) DEFAULT NULL,
    approved_tax_iva     numeric(14,2) DEFAULT NULL,
    approved_tax_irf     numeric(14,2) DEFAULT NULL
);
```

**Table comment**

```sql
COMMENT ON TABLE biq_auth.transaction_workitems IS
    'Operational state of bank transactions.
     Exclusively owned by Node.js backend.
     bank_ref_1 as stable business key survives daily Python snapshots.
     Fallback key: sap_description when bank_ref_1 is NULL.';
```

**Constraints**

```sql
ALTER TABLE ONLY biq_auth.transaction_workitems
    ADD CONSTRAINT transaction_workitems_pkey
        PRIMARY KEY (bank_ref_1);
ALTER TABLE ONLY biq_auth.transaction_workitems
    ADD CONSTRAINT transaction_workitems_assigned_user_id_fkey
        FOREIGN KEY (assigned_user_id) REFERENCES biq_auth.users(id);
```

**Indexes**

```sql
CREATE INDEX idx_workitems_assigned_user ON biq_auth.transaction_workitems USING btree (assigned_user_id);
CREATE INDEX idx_workitems_stg_id        ON biq_auth.transaction_workitems USING btree (stg_id);
CREATE INDEX idx_workitems_work_status   ON biq_auth.transaction_workitems USING btree (work_status);
```

---

### transaction_locks

TTL-based mutex preventing two analysts from editing the same workitem
simultaneously. A lock is valid only while `expires_at > NOW()`. Expired
locks are ignored by queries; `INSERT ... ON CONFLICT DO UPDATE` re-acquires
an expired lock atomically.

```sql
CREATE TABLE biq_auth.transaction_locks (
    bank_ref_1     character varying(200) NOT NULL,  -- PK; FK to transaction_workitems
    locked_by_id   integer NOT NULL,                 -- FK to users.id
    locked_by_name character varying(100) NOT NULL,
    locked_at      timestamp with time zone DEFAULT now() NOT NULL,
    expires_at     timestamp with time zone NOT NULL,
    renewed_at     timestamp with time zone
);
```

**Table comment**

```sql
COMMENT ON TABLE biq_auth.transaction_locks IS
    'Temporary TTL locks (5 min) to prevent simultaneous editing.
     Expired locks are ignored by queries and cleaned up periodically.';
```

**Constraints**

```sql
ALTER TABLE ONLY biq_auth.transaction_locks
    ADD CONSTRAINT transaction_locks_pkey PRIMARY KEY (bank_ref_1);
ALTER TABLE ONLY biq_auth.transaction_locks
    ADD CONSTRAINT fk_lock_workitem
        FOREIGN KEY (bank_ref_1)
        REFERENCES biq_auth.transaction_workitems(bank_ref_1)
        ON DELETE CASCADE;
ALTER TABLE ONLY biq_auth.transaction_locks
    ADD CONSTRAINT transaction_locks_locked_by_id_fkey
        FOREIGN KEY (locked_by_id) REFERENCES biq_auth.users(id);
```

**Indexes**

```sql
CREATE INDEX idx_locks_expires_at ON biq_auth.transaction_locks USING btree (expires_at);
```

---

## biq_gold

Gold (output) schema. Immutable audit trail. Rows are never deleted.
The RPA robot reads from this schema and updates `rpa_status` only.
The Node.js API writes on export; the Python pipeline never touches this
schema.

---

### payment_header

One row per approved bank payment exported for SAP F-28 posting.
`idempotency_hash` prevents duplicate rows across pipeline re-runs.
RPA processes rows ordered by `batch_date ASC, id ASC`.

```sql
CREATE TABLE biq_gold.payment_header (
    id                bigint NOT NULL,              -- PK (sequence)
    idempotency_hash  character varying(64)  NOT NULL,  -- UNIQUE; prevents re-export
    batch_id          character varying(30)  NOT NULL,
    batch_date        date NOT NULL,
    transaction_sap   character varying(10)  DEFAULT 'F-28' NOT NULL,
    bank_ref_1        character varying(200) NOT NULL,
    stg_id            bigint NOT NULL,
    posting_date      date NOT NULL,
    doc_class         character varying(5)   DEFAULT 'DZ' NOT NULL,
    period            smallint NOT NULL,            -- NOT varchar; fiscal period number
    company_code      character varying(10)  DEFAULT '8000' NOT NULL,
    currency          character varying(3)   DEFAULT 'USD' NOT NULL,
    reference_text    character varying(255) NOT NULL,
    bank_gl_account   character varying(20)  DEFAULT '1110213001' NOT NULL,
    amount            numeric(18,2) NOT NULL,
    customer_code     character varying(20),
    customer_name     character varying(200),
    multi_customer    boolean DEFAULT false NOT NULL,
    match_method      character varying(50),
    match_confidence  numeric(5,2),
    reconcile_reason  character varying(100),
    approved_by       character varying(50)  NOT NULL,
    approved_at       timestamp with time zone NOT NULL,
    exported_by       character varying(50)  NOT NULL,
    exported_at       timestamp with time zone DEFAULT now() NOT NULL,
    rpa_status        character varying(20)  DEFAULT 'PENDING_RPA' NOT NULL,
    rpa_doc_number    character varying(20),
    rpa_processed_at  timestamp with time zone,
    rpa_error_message text,
    rpa_retry_count   integer DEFAULT 0 NOT NULL,
    rpa_locked_at     timestamp with time zone,
    rpa_locked_by     character varying(50),
    created_at        timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT payment_header_rpa_status_check CHECK (rpa_status = ANY (
        ARRAY['PENDING_RPA', 'IN_PROGRESS', 'POSTED', 'FAILED', 'RETRY']
    ))
);
```

**Table comment**

```sql
COMMENT ON TABLE biq_gold.payment_header IS
    'SAP F-28 payment header. One row per approved bank payment.
     idempotency_hash prevents duplicate entries across pipeline re-runs.
     transaction_sap prepared for future SAP transaction types.
     bank_gl_account default 1110213001 — future: resolved from aux table.
     RPA iterates rpa_status=PENDING_RPA ordered by batch_date ASC, id ASC.';
```

**Constraints**

```sql
ALTER TABLE ONLY biq_gold.payment_header
    ADD CONSTRAINT payment_header_pkey      PRIMARY KEY (id);
ALTER TABLE ONLY biq_gold.payment_header
    ADD CONSTRAINT uq_header_idempotency    UNIQUE (idempotency_hash);
```

**Indexes**

```sql
CREATE INDEX idx_gh_batch_id        ON biq_gold.payment_header USING btree (batch_id);
CREATE INDEX idx_gh_batch_date      ON biq_gold.payment_header USING btree (batch_date);
CREATE INDEX idx_gh_bank_ref        ON biq_gold.payment_header USING btree (bank_ref_1);
CREATE INDEX idx_gh_rpa_status      ON biq_gold.payment_header USING btree (rpa_status);
CREATE INDEX idx_gh_transaction_sap ON biq_gold.payment_header USING btree (transaction_sap);
```

---

### payment_detail

Invoice lines for a SAP F-28 payment. N rows per `header_id` — one per
invoice selected by the analyst. `is_partial_payment = TRUE` activates
Part.Rest. in SAP.

```sql
CREATE TABLE biq_gold.payment_detail (
    id                    bigint NOT NULL,         -- PK (sequence)
    header_id             bigint NOT NULL,         -- FK to payment_header.id CASCADE DELETE
    batch_id              character varying(30)  NOT NULL,
    line_number           smallint NOT NULL,
    portfolio_stg_id      bigint NOT NULL,         -- stg_customer_portfolio.stg_id (no FK)
    invoice_ref           character varying(50),
    assignment            character varying(50),
    customer_code         character varying(20)  NOT NULL,
    customer_name         character varying(200),
    amount_gross          numeric(18,2) NOT NULL,
    financial_amount_net  numeric(18,2),
    is_partial_payment    boolean DEFAULT false NOT NULL,
    sap_residual_amount   numeric(18,2),
    gl_account            character varying(20),
    created_at            timestamp with time zone DEFAULT now() NOT NULL
);
```

**Table comment**

```sql
COMMENT ON TABLE biq_gold.payment_detail IS
    'Invoice lines per SAP F-28 payment.
     N rows per header_id — one per invoice.
     is_partial_payment=TRUE activates Part.Rest. in SAP.
     Split payment Pacificard: same invoice_ref in two headers
     with complementary partial amounts.';
```

**Constraints**

```sql
ALTER TABLE ONLY biq_gold.payment_detail
    ADD CONSTRAINT payment_detail_pkey         PRIMARY KEY (id);
ALTER TABLE ONLY biq_gold.payment_detail
    ADD CONSTRAINT uq_detail_header_line       UNIQUE (header_id, line_number);
ALTER TABLE ONLY biq_gold.payment_detail
    ADD CONSTRAINT payment_detail_header_id_fkey
        FOREIGN KEY (header_id) REFERENCES biq_gold.payment_header(id)
        ON DELETE CASCADE;
```

**Indexes**

```sql
CREATE INDEX idx_gd_header_id       ON biq_gold.payment_detail USING btree (header_id);
CREATE INDEX idx_gd_invoice_ref     ON biq_gold.payment_detail USING btree (invoice_ref);
CREATE INDEX idx_gd_portfolio_stg_id ON biq_gold.payment_detail USING btree (portfolio_stg_id);
```

---

### payment_diff

Commission, tax, and adjustment lines for a SAP F-28 posting. Only has rows
when the bank reported commissions/taxes or the analyst distributed a cent
differential. `sap_posting_key` is derived at runtime from the sign of
`transaction_workitems.diff_amount`.

```sql
CREATE TABLE biq_gold.payment_diff (
    id               bigint NOT NULL,              -- PK (sequence)
    header_id        bigint NOT NULL,              -- FK to payment_header.id CASCADE DELETE
    batch_id         character varying(30)  NOT NULL,
    line_number      smallint NOT NULL,
    sap_posting_key  character varying(5)   NOT NULL,  -- '40' debit or '50' credit
    gl_account       character varying(20)  NOT NULL,
    amount           numeric(18,4) NOT NULL,
    adjustment_type  character varying(30)  NOT NULL,
    line_text        character varying(100) NOT NULL,
    created_at       timestamp with time zone DEFAULT now() NOT NULL
);
```

**Table comment**

```sql
COMMENT ON TABLE biq_gold.payment_diff IS
    'Commissions, taxes and adjustments for SAP F-28.
     sap_posting_key=40 for commissions and taxes always.
     sap_posting_key=40 or 50 for diff_cambiario based on sign at runtime.
     gl_account resolved from gl-accounts.yaml — no hardcoded values.
     Only has rows when bank reported commissions/taxes
     or analyst distributed a cent differential.';
```

**Constraints**

```sql
ALTER TABLE ONLY biq_gold.payment_diff
    ADD CONSTRAINT payment_diff_pkey        PRIMARY KEY (id);
ALTER TABLE ONLY biq_gold.payment_diff
    ADD CONSTRAINT uq_diff_header_line      UNIQUE (header_id, line_number);
ALTER TABLE ONLY biq_gold.payment_diff
    ADD CONSTRAINT payment_diff_header_id_fkey
        FOREIGN KEY (header_id) REFERENCES biq_gold.payment_header(id)
        ON DELETE CASCADE;
```

**Indexes**

```sql
CREATE INDEX idx_gdiff_header_id       ON biq_gold.payment_diff USING btree (header_id);
CREATE INDEX idx_gdiff_batch_id        ON biq_gold.payment_diff USING btree (batch_id);
CREATE INDEX idx_gdiff_adjustment_type ON biq_gold.payment_diff USING btree (adjustment_type);
```

---

## biq_config

Pipeline configuration and observability schema. Written by the Python
pipeline; read by both the pipeline and the API dashboard.

---

### etl_process_windows

One row per pipeline process per execution window. Tracks execution state
(`PENDING` → `RUNNING` → `COMPLETED` / `FAILED` / `SKIPPED`). The `notes`
column stores the `batch_id` generated by `BatchTracker`.

```sql
CREATE TABLE biq_config.etl_process_windows (
    id                     integer NOT NULL,         -- PK (sequence)
    process_name           character varying(100) NOT NULL,
    process_type           character varying(20)  NOT NULL,  -- 'TRANSACTIONAL' or 'STATEFUL'
    window_start           date,
    window_end             date,
    periodo_mes            character varying(7),
    status                 character varying(20)  DEFAULT 'PENDING' NOT NULL,
    run_id                 character varying(36),
    records_processed      integer DEFAULT 0,
    records_failed         integer DEFAULT 0,
    execution_time_seconds numeric(10,2) DEFAULT 0.00,
    config_fingerprint     character varying(64),
    error_message          text,
    notes                  text,                     -- stores batch_id from BatchTracker
    created_at             timestamp without time zone DEFAULT now() NOT NULL,
    started_at             timestamp without time zone,
    completed_at           timestamp without time zone,
    CONSTRAINT etl_process_windows_process_type_check CHECK (process_type = ANY (
        ARRAY['TRANSACTIONAL', 'STATEFUL']
    )),
    CONSTRAINT etl_process_windows_status_check CHECK (status = ANY (
        ARRAY['PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED']
    ))
);
```

**Constraints**

```sql
ALTER TABLE ONLY biq_config.etl_process_windows
    ADD CONSTRAINT pk_etl_process_windows PRIMARY KEY (id);
```

**Indexes**

```sql
CREATE INDEX idx_window_process_status ON biq_config.etl_process_windows USING btree (process_name, status);
CREATE INDEX idx_window_periodo        ON biq_config.etl_process_windows USING btree (periodo_mes);
CREATE INDEX idx_window_run_id         ON biq_config.etl_process_windows USING btree (run_id);
CREATE INDEX idx_window_created        ON biq_config.etl_process_windows USING btree (created_at);
```
