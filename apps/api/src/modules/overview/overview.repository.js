// src/modules/overview/overview.repository.js
import { pool } from '../../config/database.js';

export async function getOverviewData({ status, customer, dateFrom, dateTo, assignedUserId }) {
  // Construir condiciones dinámicas
  const conditions = [
    `t.doc_type IN ('ZR', 'SA')`,
    `t.is_compensated_sap = FALSE`,
    `t.is_compensated_intraday = FALSE`,
  ];
  const params = [];
  let p = 1;

  if (status && status !== 'ALL') {
  if (status === 'MATCHED') {
    conditions.push(`t.reconcile_status IN ('MATCHED','MATCHED_MANUAL')`);
  } else {
    conditions.push(`t.reconcile_status = $${p}`);
    params.push(status);
    p++;
  }
}

  if (customer) {
    conditions.push(`(
      LOWER(t.enrich_customer_name) LIKE LOWER($${p})
      OR t.enrich_customer_id = $${p}
    )`);
    params.push(`%${customer}%`);
    p++;
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

  if (assignedUserId) {
    conditions.push(`w.assigned_user_id = $${p}`);
    params.push(parseInt(assignedUserId, 10));
    p++;
  }

  const where = conditions.join(' AND ');

  // CTE: una sola query que hace TODO el trabajo
  // CTE = Common Table Expression — como una vista temporal
  // Primero trae las filas, luego las agrupa, luego calcula el summary
  const query = `
    WITH base AS (
      SELECT
        t.stg_id,
        t.doc_type,
        t.bank_date::date                              AS bank_date,
        t.sap_description,
        t.amount_total,
        t.trans_type,
        t.enrich_customer_id,
        t.enrich_customer_name,
        t.reconcile_status,
        t.bank_ref_1,
        t.doc_number,
        t.doc_reference,
        t.bank_description,
        t.brand,
        t.settlement_id,
        t.match_confidence_score,
        t.reconcile_reason,
        w.assigned_user_id,
        w.work_status,
        w.detected_scenario,
        w.approval_notes,
        u.full_name                                    AS assigned_user_name,
        u.username                                     AS assigned_username
      FROM biq_stg.stg_bank_transactions t
      LEFT JOIN biq_auth.transaction_workitems w
        ON w.bank_ref_1 =
           COALESCE(NULLIF(TRIM(t.bank_ref_1), ''), t.sap_description)
      LEFT JOIN biq_auth.users u
        ON u.id = w.assigned_user_id
      WHERE ${where}
    ),
    summary AS (
      SELECT
        COUNT(*) FILTER (WHERE reconcile_status = 'PENDING')::integer  AS pending_count,
        COALESCE(SUM(amount_total) FILTER (WHERE reconcile_status = 'PENDING'), 0)  AS pending_amount,
        COUNT(*) FILTER (WHERE reconcile_status = 'REVIEW')::integer   AS review_count,
        COALESCE(SUM(amount_total) FILTER (WHERE reconcile_status = 'REVIEW'), 0)   AS review_amount,
        COUNT(*) FILTER (WHERE reconcile_status IN ('MATCHED','MATCHED_MANUAL'))::integer AS matched_count,
        COALESCE(SUM(amount_total) FILTER (WHERE reconcile_status IN ('MATCHED','MATCHED_MANUAL')), 0) AS matched_amount,
        COUNT(*)::integer                                               AS total_count,
        COALESCE(SUM(amount_total), 0)                                  AS total_amount
      FROM base
    ),
    grouped AS (
      SELECT
        bank_date,
        COUNT(*)::integer        AS day_count,
        SUM(amount_total)        AS day_subtotal,
        json_agg(
          json_build_object(
            'stg_id',              stg_id,
            'doc_type',            doc_type,
            'bank_date',           bank_date,
            'sap_description',     sap_description,
            'amount_total',        amount_total,
            'trans_type',          trans_type,
            'enrich_customer_id',  enrich_customer_id,
            'enrich_customer_name', enrich_customer_name,
            'reconcile_status',    reconcile_status,
            'bank_ref_1',          bank_ref_1,
            'doc_number',          doc_number,
            'brand',               brand,
            'match_confidence_score', match_confidence_score,
            'reconcile_reason',    reconcile_reason,
            'assigned_user_id',    assigned_user_id,
            'assigned_user_name',  assigned_user_name,
            'assigned_username',   assigned_username,
            'work_status',         work_status,
            'detected_scenario',   detected_scenario,
            'analyst_note',        approval_notes
          )
          ORDER BY stg_id DESC
        )                        AS rows
      FROM base
      GROUP BY bank_date
      ORDER BY bank_date ASC
    )
    SELECT
      (SELECT row_to_json(s) FROM summary s)  AS summary,
      json_agg(
        json_build_object(
          'date',     g.bank_date,
          'count',    g.day_count,
          'subtotal', g.day_subtotal,
          'rows',     g.rows
        )
      )                                        AS groups
    FROM grouped g
  `;

  const result = await pool.query(query, params);
  return result.rows[0];
}

export async function findMatchedWithoutWorkitem() {
  const result = await pool.query(`
    SELECT
      t.stg_id,
      t.bank_ref_1,
      t.sap_description,
      t.reconcile_status,
      t.match_method,
      t.match_confidence_score,
      t.matched_portfolio_ids,
      t.amount_total,
      t.trans_type
    FROM biq_stg.stg_bank_transactions t
    WHERE t.reconcile_status = 'MATCHED'
      AND t.doc_type IN ('ZR','SA')
      AND t.is_compensated_sap = FALSE
      AND t.is_compensated_intraday = FALSE
      AND NOT EXISTS (
        SELECT 1 FROM biq_gold.payment_header gh
        WHERE gh.bank_ref_1 = COALESCE(NULLIF(TRIM(t.bank_ref_1),''), t.sap_description)
          AND gh.rpa_status != 'CANCELLED'
      )
    ORDER BY t.bank_date ASC
  `);
  return result.rows;
}
 
export async function createAutoApprovedWorkitems(transactions, syncedBy) {
  if (!transactions.length) return 0;
  let synced = 0;
  for (const tx of transactions) {
    const bankRef1 = (tx.bank_ref_1 || tx.sap_description).trim();
    try {
      await pool.query(`
        INSERT INTO biq_auth.transaction_workitems
          (bank_ref_1, stg_id, assigned_user_id, work_status,
           detected_scenario, approved_by, approved_at, approved_portfolio_ids)
        VALUES (
          $1, $2,
          NULL,
          'APPROVED', 'AUTO_MATCHED', 'ALGORITHM_PACIOLI', NOW(), $3
        )
        ON CONFLICT (bank_ref_1) DO UPDATE
          SET work_status            = 'APPROVED',
              approved_by            = 'ALGORITHM_PACIOLI',
              approved_at            = NOW(),
              approved_portfolio_ids = EXCLUDED.approved_portfolio_ids,
              assigned_user_id       = NULL,
              updated_at             = NOW()
      `, [bankRef1, tx.stg_id, tx.matched_portfolio_ids || null]);
      synced++;
    } catch {
      // Skip silently
    }
  }
  return synced;
}

export async function updateAnalystNote({ bankRef1, note }) {
  const result = await pool.query(
    `UPDATE biq_auth.transaction_workitems
        SET approval_notes = $2,
            updated_at     = NOW()
      WHERE bank_ref_1 = $1
  RETURNING bank_ref_1, approval_notes`,
    [bankRef1, note ?? null]
  );
  return result.rows[0];
}