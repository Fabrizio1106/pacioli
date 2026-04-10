// src/modules/transactions/transactions.repository.js
import { pool } from '../../config/database.js';

// ─────────────────────────────────────────────
// PAGINATED LIST — Active workspace transactions
// Only shows what analysts need to work on:
//   doc_type IN (ZR, SA)
//   not compensated
//   status PENDING or REVIEW
//   joined with workitem operational state
// ─────────────────────────────────────────────
export async function findTransactions({
  status,
  dateFrom,
  dateTo,
  customerId,
  assignedTo,      // user ID — filters to analyst's own transactions
  workStatus,      // workitem work_status filter
  page  = 1,
  limit = 20,
}) {
  const conditions = [
    `t.doc_type IN ('ZR', 'SA')`,
    `t.is_compensated_sap = FALSE`,
    `t.is_compensated_intraday = FALSE`,
  ];
  const params = [];
  let   p = 1;

  if (status) {
    const statusList = Array.isArray(status) ? status : status.split(',');
    conditions.push(`t.reconcile_status = ANY($${p})`);
    params.push(statusList);
    p++;
  } else {
    // Default: only show actionable statuses
    conditions.push(`t.reconcile_status IN ('PENDING', 'REVIEW')`);
  }

  if (dateFrom) {
    conditions.push(`t.bank_date >= $${p}`);
    params.push(dateFrom);
    p++;
  }

  if (dateTo) {
    conditions.push(`t.bank_date <= $${p}`);
    params.push(dateTo);
    p++;
  }

  if (customerId) {
    conditions.push(`t.enrich_customer_id = $${p}`);
    params.push(customerId);
    p++;
  }

  if (assignedTo) {
    conditions.push(`w.assigned_user_id = $${p}`);
    params.push(assignedTo);
    p++;
  }

  if (workStatus) {
    const wsList = Array.isArray(workStatus) ? workStatus : workStatus.split(',');
    conditions.push(`w.work_status = ANY($${p})`);
    params.push(wsList);
    p++;
  } else {
    // Default: only show actionable workitem states
    conditions.push(`(w.work_status IN ('ASSIGNED', 'IN_PROGRESS', 'APPROVED', 'REVERSED') OR w.work_status IS NULL)`);
  }

  const offset = (page - 1) * limit;
  params.push(limit);
  params.push(offset);

  const where = conditions.join(' AND ');

  const dataQuery = `
    SELECT
      t.stg_id,
      t.doc_date,
      t.bank_date,
      t.doc_number,
      t.doc_reference,
      t.amount_total,
      t.currency,
      t.bank_description,
      t.trans_type,
      t.global_category,
      t.establishment_name,
      t.brand,
      t.enrich_customer_id,
      t.enrich_customer_name,
      t.enrich_confidence_score,
      t.reconcile_status,
      t.reconcile_reason,
      t.match_confidence_score,
      t.match_method,
      t.matched_portfolio_ids,
      t.reconciled_at,
      t.updated_at,
      -- Workitem operational state
      w.bank_ref_1,
      w.work_status,
      w.assigned_user_id,
      w.assigned_by,
      w.assigned_at,
      w.detected_scenario,
      -- Lock info — is someone else working on this?
      lk.locked_by_name,
      lk.locked_by_id,
      lk.expires_at      AS lock_expires_at,
      -- Lock is only valid if not expired
      CASE
        WHEN lk.expires_at > NOW() THEN TRUE
        ELSE FALSE
      END                AS is_locked
    FROM biq_stg.stg_bank_transactions t
    LEFT JOIN biq_auth.transaction_workitems w
      ON w.bank_ref_1 =
         COALESCE(NULLIF(TRIM(t.bank_ref_1), ''), t.sap_description)
    LEFT JOIN biq_auth.transaction_locks lk
      ON lk.bank_ref_1 = w.bank_ref_1
      AND lk.expires_at > NOW()
    WHERE ${where}
    ORDER BY t.bank_date DESC, t.stg_id DESC
    LIMIT $${p} OFFSET $${p + 1}
  `;

  const countQuery = `
    SELECT COUNT(*) AS total
    FROM biq_stg.stg_bank_transactions t
    LEFT JOIN biq_auth.transaction_workitems w
      ON w.bank_ref_1 =
         COALESCE(NULLIF(TRIM(t.bank_ref_1), ''), t.sap_description)
    WHERE ${where}
  `;

  const [dataResult, countResult] = await Promise.all([
    pool.query(dataQuery, params),
    pool.query(countQuery, params.slice(0, -2)),
  ]);

  return {
    rows:  dataResult.rows,
    total: parseInt(countResult.rows[0].total, 10),
  };
}

// ─────────────────────────────────────────────
// FULL DETAIL — All columns for the calculator
// ─────────────────────────────────────────────
export async function findTransactionById(stgId) {
  const query = `
    SELECT
      t.*,
      w.bank_ref_1,
      w.work_status,
      w.assigned_user_id,
      w.assigned_by,
      w.assigned_at,
      w.detected_scenario,
      w.approved_by      AS workitem_approved_by,
      w.approved_at      AS workitem_approved_at,
      lk.locked_by_name,
      lk.locked_by_id,
      lk.expires_at      AS lock_expires_at,
      CASE
        WHEN lk.expires_at > NOW() THEN TRUE
        ELSE FALSE
      END                AS is_locked
    FROM biq_stg.stg_bank_transactions t
    LEFT JOIN biq_auth.transaction_workitems w
      ON w.bank_ref_1 =
         COALESCE(NULLIF(TRIM(t.bank_ref_1), ''), t.sap_description)
    LEFT JOIN biq_auth.transaction_locks lk
      ON lk.bank_ref_1 = w.bank_ref_1
      AND lk.expires_at > NOW()
    WHERE t.stg_id = $1
  `;
  const result = await pool.query(query, [stgId]);
  return result.rows[0] || null;
}

// ─────────────────────────────────────────────
// STATUS SUMMARY — Dashboard KPIs
// ─────────────────────────────────────────────
export async function getStatusSummary() {
  const query = `
    SELECT
      t.reconcile_status                         AS status,
      COUNT(*)::integer                          AS count,
      COALESCE(SUM(t.amount_total), 0)           AS total_amount,
      COALESCE(AVG(t.match_confidence_score), 0) AS avg_confidence
    FROM biq_stg.stg_bank_transactions t
    WHERE t.doc_type IN ('ZR', 'SA')
      AND t.reconcile_status NOT IN ('CLOSED_IN_SOURCE_SAP', 'COMPENSATED_INTRADAY')
    GROUP BY t.reconcile_status
    ORDER BY count DESC
  `;
  const result = await pool.query(query);
  return result.rows;
}