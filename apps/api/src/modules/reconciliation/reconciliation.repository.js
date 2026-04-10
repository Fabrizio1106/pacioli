// src/modules/reconciliation/reconciliation.repository.js
import { pool } from '../../config/database.js';

export async function findWorkitemByStgId(stgId) {
  const result = await pool.query(`
    SELECT w.bank_ref_1, w.work_status, w.assigned_user_id
    FROM biq_auth.transaction_workitems w
    WHERE w.stg_id = $1
  `, [stgId]);
  return result.rows[0] || null;
}

// ─────────────────────────────────────────────
// APPROVE MATCH
// Atomic transaction — all or nothing:
//   1. Update bank transaction → MATCHED_MANUAL
//   2. Update portfolio items  → CLOSED
//   3. Update workitem         → APPROVED
// ─────────────────────────────────────────────
export async function approveMatch({
  bankRef1,
  stgId,
  portfolioIds,
  approvedBy,
  approvalNotes,
  diffAccountCode,
  diffAmount,
  commission,
  taxIva,
  taxIrf,
  isOverride,
  overrideReason,
}) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // Step 1: Update bank transaction status
    await client.query(`
      UPDATE biq_stg.stg_bank_transactions
      SET
        reconcile_status = 'MATCHED_MANUAL',
        reconciled_at    = NOW(),
        updated_at       = NOW()
      WHERE stg_id = $1
    `, [stgId]);

    // Step 2: Close all selected portfolio items
    // CLOSED = permanently taken, cannot be selected again
    await client.query(`
      UPDATE biq_stg.stg_customer_portfolio
      SET
        reconcile_status = 'CLOSED',
        reconciled_at    = NOW(),
        closed_at        = NOW(),
        updated_at       = NOW()
      WHERE stg_id = ANY($1::bigint[])
        AND reconcile_status IN ('PENDING', 'ENRICHED', 'REVIEW')
    `, [portfolioIds]);

    // Step 3: Update workitem to APPROVED with full approval data.
    // approved_commission / approved_tax_iva / approved_tax_irf are persisted
    // so the Gold Layer uses analyst-approved values as source of truth.
    // diff_amount is stored with the sign that makes the entry balance:
    //   positive → bank overpaid  → posting key 50 (credit / haber)
    //   negative → bank underpaid → posting key 40 (debit  / debe)
    await client.query(`
      UPDATE biq_auth.transaction_workitems
      SET
        work_status            = 'APPROVED',
        approved_portfolio_ids = $1,
        approved_by            = $2,
        approved_at            = NOW(),
        approval_notes         = $3,
        diff_account_code      = $4,
        diff_amount            = $5,
        approved_commission    = $6,
        approved_tax_iva       = $7,
        approved_tax_irf       = $8,
        is_override            = $9,
        override_reason        = $10,
        updated_at             = NOW()
      WHERE bank_ref_1 = $11
    `, [
      portfolioIds.join(','),
      approvedBy,
      approvalNotes   || null,
      diffAccountCode || null,
      // Store signed diff_amount so Gold knows direction (40 / 50)
      // Input: absolute value from analyst; sign derived from balance direction
      diffAmount      || null,
      commission      || null,
      taxIva          || null,
      taxIrf          || null,
      isOverride      || false,
      overrideReason  || null,
      bankRef1,
    ]);

    await client.query('COMMIT');

  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// ─────────────────────────────────────────────
// GET APPROVED DETAIL
// Read-only. No lock or work_status validation.
// Returns full bank data + enriched invoice list
// for the DONE tab detail panel.
//
// approved_portfolio_ids is stored as CSV string
// ("2838,2836") — we parse it in SQL with
// string_to_array so we avoid a round-trip.
// ─────────────────────────────────────────────
export async function getApprovedDetail({ stgId, userId, isAdmin }) {
  // Step 1: Bank + workitem data
  const bankResult = await pool.query(`
    SELECT
      t.stg_id,
      t.bank_date,
      t.doc_number,
      t.amount_total,
      t.currency,
      t.trans_type,
      t.bank_ref_1                    AS bank_ref_1_tx,
      t.bank_ref_2,
      t.bank_description,
      t.brand,
      t.settlement_id,
      t.enrich_notes,
      t.enrich_customer_id            AS customer_code,
      t.enrich_customer_name          AS customer_name,
      w.bank_ref_1,
      w.approved_at,
      w.approved_by,
      w.approved_portfolio_ids,
      w.diff_amount,
      w.diff_account_code,
      w.approved_commission,
      w.approved_tax_iva,
      w.approved_tax_irf,
      w.is_override,
      w.assigned_user_id
    FROM biq_stg.stg_bank_transactions t
    JOIN biq_auth.transaction_workitems w
      ON w.bank_ref_1 = COALESCE(NULLIF(TRIM(t.bank_ref_1), ''), t.sap_description)
    WHERE t.stg_id = $1
      AND w.work_status = 'APPROVED'
  `, [stgId]);

  if (!bankResult.rows[0]) {
    throw Object.assign(
      new Error(`Approved transaction stg_id=${stgId} not found`),
      { status: 404 }
    );
  }

  const row = bankResult.rows[0];

  // Enforce visibility: analyst sees only their own approvals
  if (!isAdmin && row.assigned_user_id !== userId) {
    throw Object.assign(
      new Error('You do not have permission to view this transaction'),
      { status: 403 }
    );
  }

  // Step 2: Invoice details — parse CSV ids in SQL
  // approved_portfolio_ids: "2838,2836" → bigint[]
  let invoices = [];
  if (row.approved_portfolio_ids) {
    const invoiceResult = await pool.query(`
      SELECT
        p.stg_id,
        p.invoice_ref,
        p.customer_code,
        p.customer_name,
        p.conciliable_amount,
        p.amount_outstanding,
        p.is_partial_payment,
        p.doc_date,
        p.due_date,
        p.assignment
      FROM biq_stg.stg_customer_portfolio p
      WHERE p.stg_id = ANY(
        string_to_array($1, ',')::bigint[]
      )
      ORDER BY p.doc_date ASC
    `, [row.approved_portfolio_ids]);

    invoices = invoiceResult.rows.map(p => ({
      stgId:             p.stg_id,
      invoiceRef:        p.invoice_ref,
      customerCode:      p.customer_code,
      customerName:      p.customer_name,
      // amount_outstanding = valor original al momento del cierre
      // conciliable_amount se pone a 0 al aprobar — no es útil aquí
      amount:            parseFloat(p.amount_outstanding || 0),
      isPartialPayment:  p.is_partial_payment || false,
      docDate:           p.doc_date,
      dueDate:           p.due_date,
      assignment:        p.assignment,
    }));
  }

  // Step 3: Check for pending reversal request
  const reversalResult = await pool.query(`
    SELECT id
    FROM biq_auth.reversal_requests
    WHERE stg_id = $1
      AND status = 'PENDING'
    LIMIT 1
  `, [stgId]);

  const hasPendingReversal = reversalResult.rows.length > 0;

  return {
    bank: {
      stgId:          row.stg_id,
      bankRef1:       row.bank_ref_1,
      bankRef2:       row.bank_ref_2       || null,
      bankDate:       row.bank_date,
      amountTotal:    parseFloat(row.amount_total || 0),
      currency:       row.currency,
      transType:      row.trans_type,
      customerName:   row.customer_name    || null,
      customerCode:   row.customer_code    || null,
      brand:          row.brand            || null,
      settlementId:   row.settlement_id    || null,
      enrichNotes:    row.enrich_notes     || null,
      approvedAt:     row.approved_at,
      approvedBy:     row.approved_by,
      isOverride:     row.is_override      || false,
      diffAmount:     parseFloat(row.diff_amount      || 0) || null,
      diffAccountCode: row.diff_account_code           || null,
      commission:     parseFloat(row.approved_commission || 0) || null,
      taxIva:         parseFloat(row.approved_tax_iva    || 0) || null,
      taxIrf:         parseFloat(row.approved_tax_irf    || 0) || null,
    },
    invoices,
    hasPendingReversal,
  };
}

// ─────────────────────────────────────────────
// CALCULATE BALANCE
// Computes the true unallocated amount after applying
// all analyst adjustments (commission, taxIva, taxIrf, diffAmount).
//
// KEY DESIGN DECISION:
// canApprove = true ONLY when unallocated === 0 exactly.
// There is no "auto-absorb" for small differences.
// Every cent must be explicitly distributed by the analyst.
// This guarantees every SAP entry is balanced.
//
// Unallocated sign convention (matches SAP posting direction):
//   unallocated > 0 → bank overpaid  → diff goes to HABER (posting key 50)
//   unallocated < 0 → bank underpaid → diff goes to DEBE  (posting key 40)
// ─────────────────────────────────────────────
export async function calculateBalance({
  stgId,
  portfolioIds,
  adjustments = {},
}) {
  // Get bank transaction amount
  const bankResult = await pool.query(`
    SELECT amount_total, currency
    FROM biq_stg.stg_bank_transactions
    WHERE stg_id = $1
  `, [stgId]);

  if (!bankResult.rows[0]) {
    throw Object.assign(
      new Error(`Bank transaction stg_id=${stgId} not found`),
      { status: 404 }
    );
  }

  const bankAmount = parseFloat(bankResult.rows[0].amount_total);
  const currency   = bankResult.rows[0].currency;

  // Get sum of selected portfolio items
  const portfolioResult = await pool.query(`
    SELECT
      stg_id,
      invoice_ref,
      customer_code,
      customer_name,
      conciliable_amount,
      reconcile_status
    FROM biq_stg.stg_customer_portfolio
    WHERE stg_id = ANY($1::bigint[])
  `, [portfolioIds]);

  const selectedItems  = portfolioResult.rows;
  const invoicesTotal  = selectedItems.reduce(
    (sum, row) => sum + parseFloat(row.conciliable_amount || 0),
    0
  );

  // Extract adjustments — analyst always enters absolute values
  const commission = Math.abs(parseFloat(adjustments.commission) || 0);
  const taxIva     = Math.abs(parseFloat(adjustments.taxIva)     || 0);
  const taxIrf     = Math.abs(parseFloat(adjustments.taxIrf)     || 0);
  const diffAmount = Math.abs(parseFloat(adjustments.diffAmount) || 0);

  // Total allocated = invoices + all adjustment lines
  // Adjustments reduce what the bank needs to cover
  // (commission/tax/diff are deducted from the bank payment before matching)
  const totalAllocated = Math.round(
    (invoicesTotal + commission + taxIva + taxIrf + diffAmount) * 10000
  ) / 10000;

  // unallocated: what remains after applying all lines
  // Positive → bank paid more than allocated (overpayment)
  // Negative → bank paid less than allocated (underpayment)
  const unallocated = Math.round((bankAmount - totalAllocated) * 10000) / 10000;
  const absUnallocated = Math.abs(unallocated);

  // Balance status — used only for UI messaging, NOT for canApprove gate
  let balanceStatus;
  if (absUnallocated === 0) {
    balanceStatus = 'BALANCED';
  } else if (unallocated > 0) {
    // Bank overpaid — analyst needs to add a credit line (posting key 50 / haber)
    balanceStatus = absUnallocated <= 0.05 ? 'NEEDS_CREDIT_CENTS' : 'OVER';
  } else {
    // Bank underpaid — analyst needs to add a debit line (posting key 40 / debe)
    balanceStatus = absUnallocated <= 0.05 ? 'NEEDS_DEBIT_CENTS' : 'UNDER';
  }

  // Posting key hint for the UI — tells the analyst which direction to distribute
  // 50 = credit (haber): bank overpaid, diff goes to income/liability account
  // 40 = debit  (debe):  bank underpaid, diff goes to expense/asset account
  const postingKeyHint = unallocated > 0 ? '50' : '40';

  return {
    bankAmount,
    invoicesTotal:   Math.round(invoicesTotal  * 100) / 100,
    commission,
    taxIva,
    taxIrf,
    diffAmount,
    totalAllocated:  Math.round(totalAllocated * 100) / 100,
    unallocated:     Math.round(unallocated    * 100) / 100,
    absUnallocated:  Math.round(absUnallocated * 100) / 100,
    balanceStatus,
    postingKeyHint,
    currency,
    // canApprove: strictly zero — every cent must be distributed
    canApprove:      absUnallocated === 0,
    selectedItems:   selectedItems.map(r => ({
      id:                r.stg_id,
      invoiceRef:        r.invoice_ref,
      customerCode:      r.customer_code,
      customerName:      r.customer_name,
      conciliableAmount: parseFloat(r.conciliable_amount),
      status:            r.reconcile_status,
    })),
  };
}