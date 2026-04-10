// src/modules/notifications/notifications.repository.js
import { pool } from '../../config/database.js';

export async function findPendingReversals({ userId, canApproveReversals }) {
  if (canApproveReversals) {
    const result = await pool.query(`
      SELECT r.*, t.amount_total, t.trans_type,
             t.reconcile_status AS current_status
      FROM biq_auth.reversal_requests r
      JOIN biq_stg.stg_bank_transactions t ON t.stg_id = r.stg_id
      WHERE r.status = 'PENDING_APPROVAL'
      ORDER BY r.requested_at ASC
    `);
    return result.rows;
  }
  const result = await pool.query(`
    SELECT r.*, t.amount_total, t.trans_type,
           t.reconcile_status AS current_status
    FROM biq_auth.reversal_requests r
    JOIN biq_stg.stg_bank_transactions t ON t.stg_id = r.stg_id
    WHERE r.requested_by_id = $1
      AND r.status IN ('PENDING_APPROVAL','APPROVED','REJECTED')
      AND r.requested_at > NOW() - INTERVAL '7 days'
    ORDER BY r.requested_at DESC
  `, [userId]);
  return result.rows;
}

export async function countPendingReversals({ canApproveReversals, userId }) {
  if (canApproveReversals) {
    const result = await pool.query(`
      SELECT COUNT(*)::integer AS count
      FROM biq_auth.reversal_requests WHERE status = 'PENDING_APPROVAL'
    `);
    return result.rows[0].count;
  }
  const result = await pool.query(`
    SELECT COUNT(*)::integer AS count
    FROM biq_auth.reversal_requests
    WHERE requested_by_id = $1 AND status = 'PENDING_APPROVAL'
  `, [userId]);
  return result.rows[0].count;
}

export async function createReversalRequest({
  stgId, bankRef1, requestedById, requestedByName,
  reason, goldRpaStatus, goldBatchId,
}) {
  const existing = await pool.query(`
    SELECT id FROM biq_auth.reversal_requests
    WHERE stg_id = $1 AND status = 'PENDING_APPROVAL' LIMIT 1
  `, [stgId]);

  if (existing.rows.length > 0) {
    throw Object.assign(
      new Error('A reversal request for this transaction is already pending approval'),
      { status: 409 }
    );
  }

  const result = await pool.query(`
    INSERT INTO biq_auth.reversal_requests
      (stg_id, bank_ref_1, requested_by_id, requested_by_name,
       request_reason, gold_rpa_status, gold_batch_id)
    VALUES ($1,$2,$3,$4,$5,$6,$7)
    RETURNING *
  `, [stgId, bankRef1, requestedById, requestedByName,
      reason, goldRpaStatus, goldBatchId]);

  return result.rows[0];
}

export async function approveReversal({
  requestId, reviewedById, reviewedByName, reviewReason,
}) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const reqResult = await client.query(`
      SELECT * FROM biq_auth.reversal_requests
      WHERE id = $1 AND status = 'PENDING_APPROVAL'
    `, [requestId]);

    if (!reqResult.rows[0]) {
      throw Object.assign(
        new Error('Reversal request not found or already processed'),
        { status: 404 }
      );
    }

    const req = reqResult.rows[0];

    // 1. Revert bank transaction
    // NOTE: matched_portfolio_ids is intentionally NOT cleared
    // Python's algorithm suggestions must survive a reversal so the
    // analyst can re-select the same invoices without losing context
    await client.query(`
      UPDATE biq_stg.stg_bank_transactions
      SET
        reconcile_status = CASE
          WHEN matched_portfolio_ids IS NOT NULL THEN 'REVIEW'
          ELSE 'PENDING'
        END,
        match_method           = NULL,
        match_confidence_score = NULL,
        reconciled_at          = NULL,
        updated_at             = NOW()
      WHERE stg_id = $1
    `, [req.stg_id]);

    // 1B. Close split children — invalidated when parent is reversed
    await client.query(`
      UPDATE biq_stg.stg_customer_portfolio
      SET reconcile_status   = 'CLOSED',
          conciliable_amount = 0,
          updated_at         = NOW()
      WHERE parent_stg_id IN (
        SELECT stg_id FROM biq_stg.stg_customer_portfolio
        WHERE settlement_id = $1
          AND is_manual_residual = FALSE
      )
      AND is_manual_residual = TRUE
    `, [req.bank_ref_1]);

    // 2A. Revert portfolio items by settlement_id
    await client.query(`
      UPDATE biq_stg.stg_customer_portfolio
      SET
        reconcile_status   = 'PENDING',
        conciliable_amount = amount_outstanding,
        settlement_id      = NULL,
        match_method       = NULL,
        match_confidence   = NULL,
        reconciled_at      = NULL,
        closed_at          = NULL,
        updated_at         = NOW()
      WHERE settlement_id = $1
        AND reconcile_status = 'CLOSED'
        AND is_manual_residual = FALSE
    `, [req.bank_ref_1]);

    // 2B. Fallback — use approved_portfolio_ids from workitem
    const workitemResult = await client.query(`
      SELECT approved_portfolio_ids
      FROM biq_auth.transaction_workitems
      WHERE bank_ref_1 = $1
    `, [req.bank_ref_1]);

    const rawIds = workitemResult.rows[0]?.approved_portfolio_ids;
    if (rawIds) {
      const ids = rawIds.toString()
        .replace(/[\[\]\s]/g, '').split(',')
        .map(id => parseInt(id.trim(), 10))
        .filter(id => !isNaN(id));

      if (ids.length > 0) {
        await client.query(`
          UPDATE biq_stg.stg_customer_portfolio
          SET
            reconcile_status   = 'PENDING',
            conciliable_amount = amount_outstanding,
            settlement_id      = NULL,
            match_method       = NULL,
            match_confidence   = NULL,
            reconciled_at      = NULL,
            closed_at          = NULL,
            updated_at         = NOW()
          WHERE stg_id = ANY($1::bigint[])
            AND reconcile_status = 'CLOSED'
            AND is_manual_residual = FALSE
        `, [ids]);
      }
    }

    // 3. Revert workitem — reset approval data but keep assignment
    await client.query(`
      UPDATE biq_auth.transaction_workitems
      SET
        work_status            = 'ASSIGNED',
        approved_portfolio_ids = NULL,
        approved_by            = NULL,
        approved_at            = NULL,
        approved_commission    = NULL,
        approved_tax_iva       = NULL,
        approved_tax_irf       = NULL,
        diff_amount            = NULL,
        diff_account_code      = NULL,
        updated_at             = NOW()
      WHERE bank_ref_1 = $1
    `, [req.bank_ref_1]);

    // 4. Mark reversal approved
    await client.query(`
      UPDATE biq_auth.reversal_requests
      SET
        status           = 'APPROVED',
        reviewed_by_id   = $2,
        reviewed_by_name = $3,
        review_reason    = $4,
        reviewed_at      = NOW(),
        updated_at       = NOW()
      WHERE id = $1
    `, [requestId, reviewedById, reviewedByName, reviewReason || null]);

    await client.query('COMMIT');

    // 5. Cancel Gold AFTER commit — separate connection, non-critical
    // Uses stg_id (bigint) — confirmed column exists in biq_gold.payment_header
    // Does NOT include updated_at — that column does not exist in payment_header
    try {
      await pool.query(`
        UPDATE biq_gold.payment_header
        SET rpa_status = 'CANCELLED'
        WHERE stg_id = $1
          AND rpa_status = 'PENDING_RPA'
      `, [req.stg_id]);
    } catch (goldErr) {
      // Log the actual error — never swallow silently
      console.error('[approveReversal] Gold cancellation failed:', goldErr.message);
    }

    return { success: true, bankRef1: req.bank_ref_1 };

  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

export async function rejectReversal({
  requestId, reviewedById, reviewedByName, reviewReason,
}) {
  const result = await pool.query(`
    UPDATE biq_auth.reversal_requests
    SET
      status           = 'REJECTED',
      reviewed_by_id   = $2,
      reviewed_by_name = $3,
      review_reason    = $4,
      reviewed_at      = NOW(),
      updated_at       = NOW()
    WHERE id = $1 AND status = 'PENDING_APPROVAL'
    RETURNING *
  `, [requestId, reviewedById, reviewedByName, reviewReason || null]);

  if (!result.rows[0]) {
    throw Object.assign(new Error('Request not found or already processed'), { status: 404 });
  }
  return result.rows[0];
}

export async function findTransactionForReversal(stgId) {
  const result = await pool.query(`
    SELECT
      t.stg_id,
      COALESCE(NULLIF(TRIM(t.bank_ref_1),''), t.sap_description) AS bank_ref_1,
      t.reconcile_status,
      t.amount_total,
      t.trans_type
    FROM biq_stg.stg_bank_transactions t
    WHERE t.stg_id = $1
  `, [stgId]);
  return result.rows[0] || null;
}

export async function findGoldHeaderByBankRef(bankRef1) {
  const result = await pool.query(`
    SELECT rpa_status, batch_id
    FROM biq_gold.payment_header
    WHERE bank_ref_1 = $1
    LIMIT 1
  `, [bankRef1]);
  return result.rows[0] || null;
}

export async function findApprovedToday({ userId, isAdmin = false }) {
  const query = isAdmin
    ? `SELECT
         t.stg_id, t.bank_ref_1, t.sap_description,
         t.amount_total, t.currency, t.trans_type, t.reconcile_status,
         w.approved_by, w.approved_at, w.approved_portfolio_ids,
         w.work_status, w.detected_scenario,
         r.id     AS reversal_request_id,
         r.status AS reversal_status
       FROM biq_stg.stg_bank_transactions t
       JOIN biq_auth.transaction_workitems w
         ON w.bank_ref_1 = COALESCE(NULLIF(TRIM(t.bank_ref_1),''), t.sap_description)
       LEFT JOIN biq_auth.reversal_requests r
         ON r.stg_id = t.stg_id AND r.status = 'PENDING_APPROVAL'
       WHERE w.work_status = 'APPROVED'
         AND w.approved_at >= CURRENT_DATE
         AND w.approved_at <  CURRENT_DATE + INTERVAL '1 day'
       ORDER BY w.approved_at DESC`
    : `SELECT
         t.stg_id, t.bank_ref_1, t.sap_description,
         t.amount_total, t.currency, t.trans_type, t.reconcile_status,
         w.approved_by, w.approved_at, w.approved_portfolio_ids,
         w.work_status, w.detected_scenario,
         r.id     AS reversal_request_id,
         r.status AS reversal_status
       FROM biq_stg.stg_bank_transactions t
       JOIN biq_auth.transaction_workitems w
         ON w.bank_ref_1 = COALESCE(NULLIF(TRIM(t.bank_ref_1),''), t.sap_description)
       LEFT JOIN biq_auth.reversal_requests r
         ON r.stg_id = t.stg_id AND r.status = 'PENDING_APPROVAL'
       WHERE w.assigned_user_id = $1
         AND w.work_status = 'APPROVED'
         AND w.approved_at >= CURRENT_DATE
         AND w.approved_at <  CURRENT_DATE + INTERVAL '1 day'
       ORDER BY w.approved_at DESC`;

  const params = isAdmin ? [] : [userId];
  const result = await pool.query(query, params);
  return result.rows;
}