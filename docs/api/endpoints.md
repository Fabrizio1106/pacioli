# PACIOLI — REST API Endpoint Reference

**Base URL:** `/api/v1`  
**Auth scheme:** JWT Bearer token. Include `Authorization: Bearer <accessToken>` on all
protected endpoints.  
**Response envelope:**
```json
{ "status": "success" | "error", "data": {}, "message": "..." }
```
All error responses follow `{ "status": "error", "message": "..." }`.

**Roles:** `admin` · `senior_analyst` · `analyst` · `viewer`

---

## Table of Contents

1. [auth](#1-auth)
2. [transactions](#2-transactions)
3. [workspace](#3-workspace)
4. [reconciliation](#4-reconciliation)
5. [portfolio](#5-portfolio)
6. [overview](#6-overview)
7. [assignments](#7-assignments)
8. [locks](#8-locks)
9. [notifications](#9-notifications)
10. [reversals](#10-reversals)
11. [gold-export](#11-gold-export)
12. [reports](#12-reports)
13. [ingestion](#13-ingestion)
14. [users](#14-users)

---

## 1. auth

### POST /api/v1/auth/login

**Description:** Authenticates a user and returns a JWT access token and a refresh token.

**Auth:** Public

**Request body** (`application/json`):
- `username` `string` — required
- `password` `string` — required

**Response `200`:**
```json
{
  "status": "success",
  "data": {
    "accessToken": "eyJ...",
    "refreshToken": "eyJ...",
    "user": { "id": 1, "username": "jdoe", "full_name": "Jane Doe", "role": "analyst" }
  }
}
```

**Errors:**
- `400` — `username` or `password` missing
- `401` — credentials invalid
- `500` — unexpected server error

---

### GET /api/v1/auth/me

**Description:** Returns the profile of the currently authenticated user.

**Auth:** Any authenticated user

**Request:** None (identity read from JWT)

**Response `200`:**
```json
{
  "status": "success",
  "data": { "user": { "id": 1, "username": "jdoe", "full_name": "Jane Doe", "role": "analyst" } }
}
```

**Errors:**
- `401` — missing or invalid JWT

---

### POST /api/v1/auth/logout

**Description:** Logs out the current user and releases all active transaction locks they hold.

**Auth:** Any authenticated user

**Request:** None

**Response `200`:**
```json
{
  "status": "success",
  "data": { "message": "Logged out successfully", "locks_released": 2 }
}
```

**Errors:**
- `401` — missing or invalid JWT
- `500` — unexpected server error

---

## 2. transactions

### GET /api/v1/transactions

**Description:** Returns a paginated list of bank transactions with optional filters.

**Auth:** Any authenticated user

**Request query params:**
- `status` `string` — filter by `reconcile_status` (e.g. `PENDING`, `MATCHED`)
- `date_from` `string` — ISO date, lower bound on `posting_date`
- `date_to` `string` — ISO date, upper bound on `posting_date`
- `customer_id` `string` — filter by `enrich_customer_id`
- `assigned_to` `integer` — filter by `assigned_user_id`
- `work_status` `string` — filter by workitem `work_status`
- `page` `integer` — default `1`
- `limit` `integer` — default `20`, max `100`

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "stg_id": 101, "bank_ref_1": "REF-001", "amount_total": 1500.00 } ],
  "pagination": { "page": 1, "limit": 20, "total": 143 }
}
```

**Errors:**
- `401` — missing or invalid JWT
- `500` — unexpected server error

---

### GET /api/v1/transactions/summary

**Description:** Returns aggregate counts of bank transactions grouped by `reconcile_status`.

**Auth:** Any authenticated user

**Request:** None

**Response `200`:**
```json
{
  "status": "success",
  "data": { "PENDING": 42, "MATCHED": 18, "REVIEW": 7, "MATCHED_MANUAL": 5 }
}
```

**Errors:**
- `401` — missing or invalid JWT
- `500` — unexpected server error

---

### GET /api/v1/transactions/:id

**Description:** Returns full detail for a single bank transaction by its `stg_id`.

**Auth:** Any authenticated user

**Request path params:**
- `id` `integer` — `stg_id` of the transaction

**Response `200`:**
```json
{
  "status": "success",
  "data": { "stg_id": 101, "bank_ref_1": "REF-001", "amount_total": 1500.00, "doc_type": "ZR" }
}
```

**Errors:**
- `400` — `id` is not a valid integer
- `401` — missing or invalid JWT
- `404` — transaction not found
- `500` — unexpected server error

---

## 3. workspace

### GET /api/v1/workspace/my-queue

**Description:** Returns the list of transactions assigned to the authenticated analyst and pending review.

**Auth:** `admin`, `analyst`, `senior_analyst`

**Request:** None

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "stg_id": 101, "bank_ref_1": "REF-001", "work_status": "ASSIGNED", "amount_total": 1500.00 } ]
}
```

**Errors:**
- `401` — missing or invalid JWT
- `403` — role not permitted (`viewer`)
- `500` — unexpected server error

---

### GET /api/v1/workspace/my-stats

**Description:** Returns productivity statistics for the authenticated analyst (approved today, pending count, etc.).

**Auth:** `admin`, `analyst`, `senior_analyst`

**Request:** None

**Response `200`:**
```json
{
  "status": "success",
  "data": { "approved_today": 5, "pending": 12, "in_progress": 1 }
}
```

**Errors:**
- `401` — missing or invalid JWT
- `403` — role not permitted
- `500` — unexpected server error

---

### GET /api/v1/workspace/:stgId/panel

**Description:** Returns the full analyst panel for a transaction: bank data, pipeline-suggested invoices, and current balance state.

**Auth:** `admin`, `analyst`, `senior_analyst`

**Request path params:**
- `stgId` `integer`

**Request query params:**
- `search` `string` — optional free-text filter on portfolio results
- `page` `integer` — portfolio page, default `1`

**Response `200`:**
```json
{
  "status": "success",
  "transaction": { "stg_id": 101, "amount_total": 1500.00 },
  "portfolio": [ { "stg_id": 202, "invoice_ref": "FAC-001", "amount_outstanding": 1500.00 } ],
  "pagination": { "page": 1, "total": 3 }
}
```

**Errors:**
- `400` — `stgId` is not a valid integer
- `401` — missing or invalid JWT
- `403` — transaction not assigned to this analyst
- `404` — transaction not found
- `500` — unexpected server error

---

### POST /api/v1/workspace/:stgId/calculate

**Description:** Previews the balance for a given invoice selection and adjustment set without committing anything.

**Auth:** `admin`, `analyst`, `senior_analyst`

**Request path params:**
- `stgId` `integer`

**Request body** (`application/json`):
- `selected_portfolio_ids` `integer[]` — `stg_id` values of selected invoices
- `adjustments` `object`:
  - `commission` `number`
  - `tax_iva` `number`
  - `tax_irf` `number`
  - `diff_amount` `number`
  - `diff_account_code` `string`
- `is_split_payment` `boolean`
- `split_applied_amount` `number`

**Response `200`:**
```json
{
  "status": "success",
  "data": { "bank_amount": 1500.00, "allocated": 1500.00, "unallocated": 0.00, "can_approve": true }
}
```

**Errors:**
- `400` — invalid `stgId`
- `401` — missing or invalid JWT
- `403` — role not permitted
- `404` — transaction not found
- `500` — unexpected server error

---

### POST /api/v1/workspace/:stgId/approve

**Description:** Commits approval of a reconciliation match, writing the final state to all staging tables.

**Auth:** `admin`, `analyst`, `senior_analyst`

**Request path params:**
- `stgId` `integer`

**Request body** (`application/json`):
- `selected_portfolio_ids` `integer[]` — required
- `adjustments` `object` — same shape as calculate endpoint
- `is_override` `boolean` — true if analyst selected different invoices than pipeline suggested
- `override_reason` `string` — required when `is_override = true`
- `is_split_payment` `boolean`
- `split_data` `object` — split payment details when `is_split_payment = true`

**Response `200`:**
```json
{ "status": "success", "data": { "approved": true, "stg_id": 101 } }
```

**Errors:**
- `400` — invalid `stgId` or balance not zero
- `401` — missing or invalid JWT
- `403` — not the assigned analyst or role not permitted
- `404` — transaction not found
- `409` — workitem already approved
- `422` — unallocated balance is non-zero (balance gate failure)
- `423` — transaction is locked by another analyst
- `500` — unexpected server error

---

## 4. reconciliation

### GET /api/v1/reconciliation/:stgId/approved-detail

**Description:** Returns read-only detail of an approved reconciliation for display in the Done tab.

**Auth:** Any authenticated user

**Request path params:**
- `stgId` `integer`

**Response `200`:**
```json
{
  "status": "success",
  "data": { "transaction": { "stg_id": 101 }, "invoices": [ { "invoice_ref": "FAC-001" } ], "adjustments": { "commission": 12.50 } }
}
```

**Errors:**
- `400` — invalid `stgId`
- `401` — missing or invalid JWT
- `403` — analyst requesting detail for another analyst's approved item
- `404` — transaction not found or not yet approved
- `500` — unexpected server error

---

### POST /api/v1/reconciliation/:stgId/calculate

**Description:** Previews the balance calculation for the reconciliation panel; accepts both `portfolio_ids` and `selected_portfolio_ids`.

**Auth:** `admin`, `analyst`

**Request path params:**
- `stgId` `integer`

**Request body** (`application/json`):
- `portfolio_ids` `integer[]` — preferred key (alias: `selected_portfolio_ids`)
- `adjustments` `object` — accepts camelCase or snake_case keys:
  - `commission` / `taxIva` / `tax_iva` / `taxIrf` / `tax_irf` `number`
  - `diffAmount` / `diff_amount` `number`
  - `diffAccountCode` / `diff_account_code` `string`

**Response `200`:**
```json
{
  "status": "success",
  "data": { "bank_amount": 1500.00, "allocated": 1500.00, "unallocated": 0.00, "can_approve": true }
}
```

**Errors:**
- `400` — invalid `stgId` or `portfolio_ids` missing/empty
- `401` — missing or invalid JWT
- `403` — role not permitted or wrong analyst
- `404` — transaction not found
- `409` — already approved
- `422` — balance gate failure
- `423` — locked by another analyst

---

### POST /api/v1/reconciliation/:stgId/approve

**Description:** Commits the reconciliation approval; updates all staging tables and sets workitem to `APPROVED`.

**Auth:** `admin`, `analyst`

**Request path params:**
- `stgId` `integer`

**Request body** (`application/json`):
- `portfolio_ids` `integer[]` — required (alias: `selected_portfolio_ids`)
- `approval_notes` `string`
- `adjustments` `object` — same shape as calculate endpoint
- `is_override` `boolean`
- `override_reason` `string` — required when `is_override = true`

**Response `200`:**
```json
{ "status": "success", "data": { "approved": true, "stg_id": 101 } }
```

**Errors:**
- `400` — missing `portfolio_ids`, invalid `stgId`, or balance non-zero
- `401` — missing or invalid JWT
- `403` — role not permitted or wrong analyst
- `404` — transaction not found
- `409` — already approved
- `422` — balance gate failure (unallocated ≠ 0)
- `423` — locked by another analyst

---

## 5. portfolio

### GET /api/v1/portfolio/for-transaction/:stgId

**Description:** Returns the paginated list of open invoices from `stg_customer_portfolio` relevant to a specific bank transaction.

**Auth:** `admin`, `analyst`

**Request path params:**
- `stgId` `integer`

**Request query params:**
- `search` `string` — free-text filter on invoice ref / customer
- `page` `integer` — default `1`
- `limit` `integer` — default `50`, max `200`

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "stg_id": 202, "invoice_ref": "FAC-001", "customer_code": "C001", "amount_outstanding": 1500.00 } ],
  "pagination": { "page": 1, "limit": 50, "total": 12 }
}
```

**Errors:**
- `400` — invalid `stgId`
- `401` — missing or invalid JWT
- `403` — role not permitted
- `404` — transaction not found
- `500` — unexpected server error

---

### GET /api/v1/portfolio/search

**Description:** Performs a global free-text search across all open invoices in `stg_customer_portfolio`.

**Auth:** `admin`, `analyst`

**Request query params:**
- `q` `string` — required, minimum 3 characters
- `bank_amount` `number` — optional; used to rank results by proximity to bank amount
- `limit` `integer` — default `50`, max `200`

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "stg_id": 202, "invoice_ref": "FAC-001", "amount_outstanding": 1500.00 } ],
  "pagination": { "page": 1, "total": 8 }
}
```

**Errors:**
- `400` — `q` missing or fewer than 3 characters
- `401` — missing or invalid JWT
- `403` — role not permitted
- `500` — unexpected server error

---

### POST /api/v1/portfolio/validate-selection

**Description:** Validates that a set of portfolio `stg_id` values are eligible for selection (not already matched, not conflicting).

**Auth:** `admin`, `analyst`

**Request body** (`application/json`):
- `stg_ids` `integer[]` — required

**Response `200`:**
```json
{ "status": "success", "data": { "valid": true, "conflicts": [] } }
```

**Errors:**
- `400` — `stg_ids` missing or validation rule violated
- `401` — missing or invalid JWT
- `403` — role not permitted
- `409` — one or more invoices already matched
- `500` — unexpected server error

---

## 6. overview

### GET /api/v1/overview

**Description:** Returns the full transaction overview list with optional filters for the main reconciliation dashboard.

**Auth:** Any authenticated user

**Request query params:**
- `status` `string` — `ALL` (default) or a specific `reconcile_status`
- `customer` `string` — filter by customer name/code
- `date_from` `string` — ISO date
- `date_to` `string` — ISO date
- `assigned_user_id` `integer` — filter by assigned analyst

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "stg_id": 101, "bank_ref_1": "REF-001", "reconcile_status": "PENDING", "work_status": "ASSIGNED" } ]
}
```

**Errors:**
- `401` — missing or invalid JWT
- `500` — unexpected server error

---

### POST /api/v1/overview/sync-matches

**Description:** Triggers synchronization of automatic pipeline matches into workitems.

**Auth:** `admin`, `senior_analyst`

**Request:** None

**Response `200`:**
```json
{ "status": "success", "data": { "synced": 14, "skipped": 2 } }
```

**Errors:**
- `401` — missing or invalid JWT
- `403` — role not permitted
- `500` — unexpected server error

---

### PATCH /api/v1/overview/:bankRef1/note

**Description:** Updates or clears the analyst note on a transaction workitem.

**Auth:** `admin`, `senior_analyst`

**Request path params:**
- `bankRef1` `string` — URL-encoded `bank_ref_1` value

**Request body** (`application/json`):
- `note` `string | null` — pass `null` to clear

**Response `200`:**
```json
{ "status": "success", "data": { "bank_ref_1": "REF-001", "note": "Revisado con SAP" } }
```

**Errors:**
- `400` — `note` is not a string or null
- `401` — missing or invalid JWT
- `403` — role not permitted or workitem not found for this user
- `404` — transaction not found
- `500` — unexpected server error

---

## 7. assignments

All endpoints are mounted at `/api/v1/admin/assignments`.

### GET /api/v1/admin/assignments/rules

**Description:** Returns all configured assignment rules ordered by priority.

**Auth:** `admin`

**Request:** None

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "id": 1, "rule_name": "VISA rule", "priority": 10, "brand": "VISA", "assign_to_user_id": 3, "is_active": true } ]
}
```

**Errors:**
- `401` — missing or invalid JWT
- `403` — role not permitted
- `500` — unexpected server error

---

### POST /api/v1/admin/assignments/apply-rules

**Description:** Evaluates all active assignment rules against unassigned workitems and assigns them to analysts.

**Auth:** `admin`, `senior_analyst`

**Request:** None

**Response `200`:**
```json
{ "status": "success", "data": { "assigned": 8, "skipped": 1 } }
```

**Errors:**
- `401` — missing or invalid JWT
- `403` — role not permitted
- `500` — unexpected server error

---

### PATCH /api/v1/admin/assignments/:bankRef1/reassign

**Description:** Manually reassigns a single transaction workitem to a different analyst.

**Auth:** `admin`, `senior_analyst`

**Request path params:**
- `bankRef1` `string` — `bank_ref_1` of the workitem

**Request body** (`application/json`):
- `to_user_id` `integer` — required; target analyst's user ID

**Response `200`:**
```json
{ "status": "success", "data": { "bank_ref_1": "REF-001", "assigned_user_id": 5 } }
```

**Errors:**
- `400` — `to_user_id` missing
- `401` — missing or invalid JWT
- `403` — role not permitted
- `404` — workitem not found
- `500` — unexpected server error

---

## 8. locks

### POST /api/v1/locks/:bankRef1/acquire

**Description:** Acquires a 5-minute TTL lock on a transaction; re-acquires atomically if the existing lock is expired.

**Auth:** `admin`, `analyst`, `senior_analyst`

**Request path params:**
- `bankRef1` `string` — `bank_ref_1` of the workitem

**Response `200`:**
```json
{ "status": "success", "data": { "bank_ref_1": "REF-001", "expires_at": "2026-04-09T15:05:00Z" } }
```

**Errors:**
- `401` — missing or invalid JWT
- `403` — role not permitted
- `404` — workitem not found
- `409` — workitem already in a terminal state
- `423` — lock held by another analyst; response includes `{ "lockedBy": "username" }`
- `500` — unexpected server error

---

### PATCH /api/v1/locks/:bankRef1/renew

**Description:** Extends an existing lock by 5 minutes (heartbeat — called every ~4 minutes by the frontend).

**Auth:** `admin`, `analyst`, `senior_analyst`

**Request path params:**
- `bankRef1` `string`

**Response `200`:**
```json
{ "status": "success", "data": { "bank_ref_1": "REF-001", "expires_at": "2026-04-09T15:10:00Z" } }
```

**Errors:**
- `401` — missing or invalid JWT
- `403` — role not permitted
- `404` — lock not found or not owned by this user
- `500` — unexpected server error

---

### DELETE /api/v1/locks/:bankRef1/release

**Description:** Releases the lock on a transaction and resets the workitem status from `IN_PROGRESS` to `ASSIGNED`.

**Auth:** `admin`, `analyst`, `senior_analyst`

**Request path params:**
- `bankRef1` `string`

**Response `200`:**
```json
{ "status": "success", "data": { "released": true } }
```

**Errors:**
- `401` — missing or invalid JWT
- `403` — role not permitted
- `500` — unexpected server error

---

### GET /api/v1/locks/:bankRef1/status

**Description:** Returns the current lock state for a transaction.

**Auth:** `admin`, `analyst`, `senior_analyst`

**Request path params:**
- `bankRef1` `string`

**Response `200`:**
```json
{
  "status": "success",
  "data": { "locked": true, "locked_by": "jdoe", "expires_at": "2026-04-09T15:05:00Z" }
}
```

**Errors:**
- `401` — missing or invalid JWT
- `403` — role not permitted
- `500` — unexpected server error

---

## 9. notifications

### GET /api/v1/notifications/count

**Description:** Returns the count of pending notifications for the authenticated user (used for badge polling every 30 seconds).

**Auth:** Any authenticated user

**Request:** None

**Response `200`:**
```json
{ "status": "success", "data": { "pending_reversals": 2, "total": 2 } }
```

**Errors:**
- `401` — missing or invalid JWT

---

### GET /api/v1/notifications/reversals

**Description:** Returns the list of pending reversal requests visible to the authenticated user.

**Auth:** Any authenticated user

**Request:** None

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "id": 10, "stg_id": 101, "bank_ref_1": "REF-001", "requested_by_name": "Jane Doe", "status": "PENDING_APPROVAL" } ]
}
```

**Errors:**
- `401` — missing or invalid JWT

---

### GET /api/v1/notifications/approved-today

**Description:** Returns the list of transactions approved by the authenticated analyst today.

**Auth:** Any authenticated user

**Request:** None

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "stg_id": 101, "bank_ref_1": "REF-001", "approved_at": "2026-04-09T14:30:00Z" } ]
}
```

**Errors:**
- `401` — missing or invalid JWT

---

### POST /api/v1/notifications/reversals/:stgId/request

**Description:** Submits a reversal request for an approved reconciliation; requires a reason of at least 10 characters.

**Auth:** `admin`, `analyst`

**Request path params:**
- `stgId` `integer`

**Request body** (`application/json`):
- `reason` `string` — required, minimum 10 characters

**Response `201`:**
```json
{ "status": "success", "data": { "id": 10, "status": "PENDING_APPROVAL" } }
```

**Errors:**
- `400` — `reason` missing or fewer than 10 characters
- `401` — missing or invalid JWT
- `403` — role not permitted
- `404` — transaction not found or not approved
- `409` — reversal request already pending for this transaction
- `500` — unexpected server error

---

### POST /api/v1/notifications/reversals/:requestId/approve

**Description:** Approves a pending reversal request and executes the reversal.

**Auth:** `admin`

**Request path params:**
- `requestId` `integer` — `reversal_requests.id`

**Request body** (`application/json`):
- `reason` `string` — optional review note

**Response `200`:**
```json
{ "status": "success", "data": { "id": 10, "status": "APPROVED" } }
```

**Errors:**
- `400` — invalid `requestId`
- `401` — missing or invalid JWT
- `403` — role not permitted
- `404` — request not found
- `422` — request not in `PENDING_APPROVAL` state
- `500` — unexpected server error

---

### POST /api/v1/notifications/reversals/:requestId/reject

**Description:** Rejects a pending reversal request with a mandatory reason.

**Auth:** `admin`

**Request path params:**
- `requestId` `integer`

**Request body** (`application/json`):
- `reason` `string` — required, minimum 5 characters

**Response `200`:**
```json
{ "status": "success", "data": { "id": 10, "status": "REJECTED" } }
```

**Errors:**
- `400` — `reason` missing or fewer than 5 characters
- `401` — missing or invalid JWT
- `403` — role not permitted
- `404` — request not found
- `500` — unexpected server error

---

## 10. reversals

### GET /api/v1/reversals/daily

**Description:** Returns all transactions approved today, used to populate the "Processed Today" tab.

**Auth:** Any authenticated user

**Request:** None

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "stg_id": 101, "bank_ref_1": "REF-001", "approved_by": "jdoe", "approved_at": "2026-04-09T14:30:00Z" } ]
}
```

**Errors:**
- `401` — missing or invalid JWT
- `500` — unexpected server error

---

### POST /api/v1/reversals/:stgId

**Description:** Executes a reversal on an approved match, resetting the transaction and its invoices back to unmatched state.

**Auth:** `admin`, `analyst`

**Request path params:**
- `stgId` `integer`

**Request body** (`application/json`):
- `reversal_reason` `string` — optional

**Response `200`:**
```json
{ "status": "success", "data": { "reversed": true, "stg_id": 101 } }
```

**Errors:**
- `400` — invalid `stgId`
- `401` — missing or invalid JWT
- `403` — transaction not owned by this analyst
- `404` — transaction not found or not in an approved state
- `409` — Gold export already exists for this transaction (cannot reverse after export)
- `500` — unexpected server error

---

## 11. gold-export

### GET /api/v1/gold-export/preview

**Description:** Returns the list of `APPROVED` transactions eligible for the next Gold export batch.

**Auth:** `admin`, `senior_analyst`

**Request:** None

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "stg_id": 101, "bank_ref_1": "REF-001", "amount": 1500.00, "approved_by": "jdoe" } ]
}
```

**Errors:**
- `401` — missing or invalid JWT
- `403` — role not permitted
- `500` — unexpected server error

---

### POST /api/v1/gold-export/submit

**Description:** Exports all eligible `APPROVED` transactions to `biq_gold` as SAP F-28 payment records with `rpa_status = 'PENDING_RPA'`.

**Auth:** `admin`, `senior_analyst`

**Request:** None (identity read from JWT; `exported_by` set to `req.user.username`)

**Response `200`:**
```json
{
  "status": "success",
  "data": { "exported": 14, "batch_id": "BATCH-20260409-001", "batch_date": "2026-04-09" }
}
```

**Errors:**
- `400` — no eligible transactions to export
- `401` — missing or invalid JWT
- `403` — role not permitted
- `500` — unexpected server error

---

### GET /api/v1/gold-export/batches

**Description:** Returns the history of Gold export batches with aggregate amounts and record counts.

**Auth:** `admin`, `senior_analyst`

**Request:** None

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "batch_id": "BATCH-20260409-001", "batch_date": "2026-04-09", "records": 14, "total_amount": 48300.00 } ]
}
```

**Errors:**
- `401` — missing or invalid JWT
- `403` — role not permitted
- `500` — unexpected server error

---

## 12. reports

All report endpoints share the same query parameters:
- `startDate` `string` — ISO date; defaults to first day of current month
- `endDate` `string` — ISO date; defaults to today
- `status` `string` — comma-separated status values
- `brand` `string` — comma-separated brand values
- `preview` `boolean` — `true` limits rows to 200 for on-screen display (JSON endpoints only)

JSON endpoints return `{ "status", "data": [], "meta": { "showing", "total", "preview" } }`.  
Export endpoints return an `.xlsx` binary with `Content-Disposition: attachment`.

---

### GET /api/v1/reports/overview

**Description:** Returns reconciliation overview metrics (totals by status, amounts, analyst productivity).

**Auth:** Any authenticated user

**Response `200`:** `{ "status": "success", "data": { "summary": { ... }, "detail": [ ... ] } }`

---

### GET /api/v1/reports/bank

**Description:** Returns the bank reconciliation detail report (one row per bank transaction).

**Auth:** Any authenticated user

**Response `200`:** `{ "status": "success", "data": [ ... ], "meta": { "showing": 200, "total": 850, "preview": true } }`

---

### GET /api/v1/reports/portfolio

**Description:** Returns the customer portfolio status report (open invoices, matched amounts).

**Auth:** Any authenticated user

---

### GET /api/v1/reports/card-details

**Description:** Returns the card voucher detail report from `stg_card_details`.

**Auth:** Any authenticated user

---

### GET /api/v1/reports/card-settlements

**Description:** Returns the card settlement batch report from `stg_card_settlements`.

**Auth:** Any authenticated user

---

### GET /api/v1/reports/parking

**Description:** Returns the parking payment breakdown report from `stg_parking_pay_breakdown`.

**Auth:** Any authenticated user

---

### GET /api/v1/reports/summary

**Description:** Returns a consolidated summary across all reconciliation data sources.

**Auth:** Any authenticated user

---

### GET /api/v1/reports/export/overview

**Description:** Downloads the overview report as an `.xlsx` file (no row limit).

**Auth:** Any authenticated user

**Response:** `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` binary

---

### GET /api/v1/reports/export/bank

**Description:** Downloads the bank reconciliation report as `.xlsx`.

**Auth:** Any authenticated user

---

### GET /api/v1/reports/export/portfolio

**Description:** Downloads the portfolio report as `.xlsx`.

**Auth:** Any authenticated user

---

### GET /api/v1/reports/export/card-details

**Description:** Downloads the card details report as `.xlsx`.

**Auth:** Any authenticated user

---

### GET /api/v1/reports/export/card-settlements

**Description:** Downloads the card settlements report as `.xlsx`.

**Auth:** Any authenticated user

---

### GET /api/v1/reports/export/parking

**Description:** Downloads the parking breakdown report as `.xlsx`.

**Auth:** Any authenticated user

---

### GET /api/v1/reports/export/summary

**Description:** Downloads the summary report as `.xlsx`.

**Auth:** Any authenticated user

Export endpoints accept the same `startDate`, `endDate`, `status`, and `brand` query parameters as their JSON counterparts but never accept `preview` — the full dataset is always exported.

**Errors (all report endpoints):**
- `401` — missing or invalid JWT
- `500` — unexpected server error

---

## 13. ingestion

### GET /api/v1/ingestion/loader-status

**Description:** Returns health metadata for each data loader (last loaded date, record count, amount sum).

**Auth:** Any authenticated user

**Request:** None

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "loader_id": "sap_fbl3n", "last_date": "2026-04-08", "record_count": 1420, "status": "ok" } ]
}
```

**Errors:**
- `401` — missing or invalid JWT
- `500` — unexpected server error

---

### POST /api/v1/ingestion/classify

**Description:** Accepts uploaded files in memory and returns the detected loader classification for each file, without moving any files.

**Auth:** `admin`, `analyst`, `senior_analyst`

**Request:** `multipart/form-data`
- `files` — one or more files; accepted types: `.xlsx`, `.xls`, `.csv`, `.txt`, `.msg`; max 50 MB each, up to 20 files per request

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "originalName": "reporte.xlsx", "detected_loader": "sap_fbl3n", "confidence": "high" } ]
}
```

**Errors:**
- `400` — no files received
- `401` — missing or invalid JWT
- `403` — role not permitted
- `500` — unexpected server error

---

### POST /api/v1/ingestion/upload

**Description:** Moves confirmed files to their respective `data_raw/` folders based on user-confirmed loader assignments.

**Auth:** `admin`, `analyst`, `senior_analyst`

**Request:** `multipart/form-data`
- `files` — one or more files (same constraints as classify)
- `assignments` `string` — JSON-encoded array of `[{ "originalName": "reporte.xlsx", "loaderId": "sap_fbl3n" }]`; files without a `loaderId` are silently skipped

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "originalName": "reporte.xlsx", "success": true, "destination": "data_raw/sap_fbl3n/" } ],
  "summary": { "success": 2, "failed": 0, "total": 2 }
}
```

**Errors:**
- `400` — no files, invalid `assignments` JSON, or no valid loader assignments
- `401` — missing or invalid JWT
- `403` — role not permitted
- `500` — unexpected server error

---

### GET /api/v1/ingestion/scan-folders

**Description:** Scans the `data_raw/` directories and returns files already present (placed manually, not via upload).

**Auth:** `admin`, `analyst`, `senior_analyst`

**Request:** None

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "loader_id": "sap_fbl3n", "files": [ "fbl3n_2026-04-08.xlsx" ] } ]
}
```

**Errors:**
- `401` — missing or invalid JWT
- `403` — role not permitted
- `500` — unexpected server error

---

### POST /api/v1/ingestion/run-pipeline

**Description:** Triggers the Python pipeline orchestrator; rejects with `409` if a run is already in progress.

**Auth:** `admin`, `analyst`, `senior_analyst`

**Request:** None

**Response `202`** (started):
```json
{ "status": "success", "data": { "started": true, "run_id": "abc-123" } }
```

**Response `409`** (already running):
```json
{ "status": "success", "data": { "started": false, "message": "Pipeline already running" } }
```

**Errors:**
- `401` — missing or invalid JWT
- `403` — role not permitted
- `500` — unexpected server error

---

### GET /api/v1/ingestion/pipeline-status

**Description:** Returns the current pipeline run state and per-process progress from `etl_process_windows`.

**Auth:** Any authenticated user

**Request:** None

**Response `200`:**
```json
{
  "status": "success",
  "data": { "running": true, "current_process": "stg_bank_transactions", "progress": [ { "name": "stg_bank_transactions", "status": "COMPLETED" } ] }
}
```

**Errors:**
- `401` — missing or invalid JWT
- `500` — unexpected server error

---

### GET /api/v1/ingestion/history

**Description:** Returns the pipeline execution history for the last 7 days from `etl_process_windows`.

**Auth:** Any authenticated user

**Request:** None

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "periodo_mes": "2026-04", "status": "COMPLETED", "records_processed": 1420, "completed_at": "2026-04-08T06:45:00" } ]
}
```

**Errors:**
- `401` — missing or invalid JWT
- `500` — unexpected server error

---

## 14. users

### GET /api/v1/users/analysts

**Description:** Returns the list of active users with roles `admin`, `analyst`, or `senior_analyst`; used to populate assignment dropdowns across the application.

**Auth:** Any authenticated user

**Request:** None

**Response `200`:**
```json
{
  "status": "success",
  "data": [ { "id": 3, "username": "jdoe", "full_name": "Jane Doe", "role": "analyst" } ]
}
```

**Errors:**
- `401` — missing or invalid JWT
- `500` — unexpected server error
