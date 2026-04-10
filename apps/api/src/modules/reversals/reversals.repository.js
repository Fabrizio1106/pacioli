// src/modules/reversals/reversals.repository.js
import { pool } from '../../config/database.js';

export async function findWorkitemByStgId(stgId) {
  const result = await pool.query(`
    SELECT w.bank_ref_1, w.work_status, w.assigned_user_id,
           w.approved_by, w.approved_portfolio_ids, w.approved_at
    FROM biq_auth.transaction_workitems w
    WHERE w.stg_id = $1
  `, [stgId]);
  return result.rows[0] || null;
}

// ─────────────────────────────────────────────
// REVERSE MATCH
// Undoes exactly what approveMatch did — atomically
//   1. Bank transaction → back to original status
//   2. Portfolio items  → back to PENDING
//   3. Workitem         → REVERSED
// ─────────────────────────────────────────────
export async function reverseMatch({
  bankRef1,
  stgId,
  portfolioIds,
  reversedBy,
  reversalReason,
}) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // ── Step 1: Restore bank transaction ─────────────────────────────────────
    // REVIEW if it had algorithm suggestions, PENDING otherwise
    const txResult = await client.query(`
      SELECT match_confidence_score
      FROM biq_stg.stg_bank_transactions
      WHERE stg_id = $1
    `, [stgId]);

    const originalStatus = (txResult.rows[0]?.match_confidence_score > 0) ? 'REVIEW' : 'PENDING';

    await client.query(`
      UPDATE biq_stg.stg_bank_transactions
      SET reconcile_status = $1,
          reconciled_at    = NULL,
          updated_at       = NOW()
      WHERE stg_id = $2
    `, [originalStatus, stgId]);

    // ── Step 2: Detect split vs standard ─────────────────────────────────────
    // A split reversal is identified when any of the portfolioIds is a child
    // (has parent_stg_id set). In a split, approved_portfolio_ids = Child-A's stg_id.
    // We check ALL statuses — Child-A is MATCHED, not CLOSED.
    const splitCheckResult = await client.query(`
      SELECT stg_id, parent_stg_id
      FROM biq_stg.stg_customer_portfolio
      WHERE stg_id = ANY($1::bigint[])
        AND parent_stg_id IS NOT NULL
      LIMIT 1
    `, [portfolioIds]);

    const isSplit = splitCheckResult.rows.length > 0;

    if (isSplit) {
      // ── SPLIT REVERSAL ────────────────────────────────────────────────────
      //
      // Before reversal:
      //   Parent (2397): CLOSED, conciliable_amount=0, amount_outstanding=49181.34
      //   Child-A (3178): MATCHED, settlement_id=1584555835 (the bank ref)
      //   Child-B (3179): PENDING, conciliable_amount=48215.34
      //
      // After reversal:
      //   Parent (2397): PENDING, conciliable_amount=49181.34 (restored from amount_outstanding)
      //   Child-A (3178): CLOSED, settlement_id=NULL (audit record only)
      //   Child-B (3179): CLOSED (audit record only)

      const childA      = splitCheckResult.rows[0];
      const parentStgId = childA.parent_stg_id;

      // 2a. Read parent's amount_outstanding — this is the original invoice value
      // It was never modified during the split, so it's always the correct restore value
      const parentResult = await client.query(`
        SELECT amount_outstanding
        FROM biq_stg.stg_customer_portfolio
        WHERE stg_id = $1
      `, [parentStgId]);

      const originalConciliableAmount = parseFloat(parentResult.rows[0]?.amount_outstanding || 0);

      // 2b. Close ALL children of this parent (Child-A + Child-B)
      // Clear settlement_id from Child-A — it no longer belongs to this bank payment
      // Keep all other data intact for audit trail
      await client.query(`
        UPDATE biq_stg.stg_customer_portfolio
        SET reconcile_status = 'CLOSED',
            settlement_id    = NULL,
            closed_at        = NOW(),
            updated_at       = NOW()
        WHERE parent_stg_id = $1
          AND is_manual_residual = TRUE
      `, [parentStgId]);

      // 2c. Restore parent to PENDING with original conciliable_amount
      // Clear reconcile timestamps — it's back in the open queue
      await client.query(`
        UPDATE biq_stg.stg_customer_portfolio
        SET reconcile_status   = 'PENDING',
            conciliable_amount = $2,
            reconciled_at      = NULL,
            closed_at          = NULL,
            updated_at         = NOW()
        WHERE stg_id = $1
      `, [parentStgId, originalConciliableAmount]);

    } else {
      // ── STANDARD REVERSAL ─────────────────────────────────────────────────
      // portfolioIds = original invoice stg_ids (CLOSED after approval)
      // Restore conciliable_amount from amount_outstanding — the field that
      // holds the original invoice value and is never modified
      await client.query(`
        UPDATE biq_stg.stg_customer_portfolio
        SET reconcile_status   = 'PENDING',
            conciliable_amount = amount_outstanding,
            settlement_id      = NULL,
            reconciled_at      = NULL,
            closed_at          = NULL,
            updated_at         = NOW()
        WHERE stg_id = ANY($1::bigint[])
      `, [portfolioIds]);
    }

    // ── Step 3: Mark workitem as REVERSED ────────────────────────────────────
    // Preserve all approval data (approved_by, approved_at, portfolio_ids)
    // so auditors can reconstruct exactly what was reversed
    await client.query(`
      UPDATE biq_auth.transaction_workitems
      SET work_status = 'REVERSED',
          updated_at  = NOW()
      WHERE bank_ref_1 = $1
    `, [bankRef1]);

    await client.query('COMMIT');
    return { originalStatus, isSplit };

  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// ─────────────────────────────────────────────
// GET DAILY APPROVED
// All APPROVED transactions for today — shown in
// "Processed Today" tab for reversal review
// ─────────────────────────────────────────────
export async function findDailyApproved({ userId, isAdmin }) {
  // Admin sees all approved today
  // Analyst sees only their own
  const userFilter = isAdmin
    ? ''
    : `AND w.assigned_user_id = ${parseInt(userId, 10)}`;

  const result = await pool.query(`
    SELECT
      t.stg_id,
      t.bank_date,
      t.doc_number,
      t.amount_total,
      t.currency,
      t.bank_description,
      t.trans_type,
      t.enrich_customer_id,
      t.enrich_customer_name,
      t.reconcile_status,
      w.bank_ref_1,
      w.work_status,
      w.approved_by,
      w.approved_at,
      w.approved_portfolio_ids,
      w.approval_notes,
      w.diff_account_code,
      w.diff_amount,
      w.is_override,
      w.assigned_user_id
    FROM biq_stg.stg_bank_transactions t
    JOIN biq_auth.transaction_workitems w
      ON w.bank_ref_1 = COALESCE(NULLIF(TRIM(t.bank_ref_1), ''), t.sap_description)
    WHERE w.work_status = 'APPROVED'
      AND w.approved_at >= CURRENT_DATE
      AND t.doc_type IN ('ZR', 'SA')
      ${userFilter}
    ORDER BY w.approved_at DESC
  `);

  return result.rows;
}