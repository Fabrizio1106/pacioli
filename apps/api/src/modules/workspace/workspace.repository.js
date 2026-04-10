// src/modules/workspace/workspace.repository.js
import { pool } from '../../config/database.js';

export async function findMyQueue({ userId, isAdmin = false }) {
  const result = await pool.query(`
    SELECT
      t.stg_id,
      t.doc_type,
      t.bank_date::date          AS bank_date,
      t.doc_number,
      t.bank_ref_1,
      t.bank_ref_2,
      t.sap_description,
      t.enrich_notes,
      t.amount_total,
      t.currency,
      t.trans_type,
      t.global_category,
      t.establishment_name,
      t.brand,
      t.settlement_id,
      t.enrich_customer_id,
      t.enrich_customer_name,
      t.enrich_confidence_score,
      t.reconcile_status,
      t.reconcile_reason,
      t.match_method,
      t.match_confidence_score,
      t.matched_portfolio_ids,
      t.final_amount_gross,
      t.final_amount_net,
      t.final_amount_commission,
      t.final_amount_tax_iva,
      t.final_amount_tax_irf,
      t.diff_adjustment,
      w.work_status,
      w.approval_notes           AS analyst_note,
      w.bank_ref_1               AS workitem_bank_ref,
      w.detected_scenario,
      lk.locked_by_name,
      lk.locked_by_id,
      lk.expires_at              AS lock_expires_at,
      CASE WHEN lk.expires_at > NOW() THEN TRUE ELSE FALSE END AS is_locked
    FROM biq_stg.stg_bank_transactions t
    JOIN biq_auth.transaction_workitems w
      ON w.bank_ref_1 = COALESCE(NULLIF(TRIM(t.bank_ref_1),''), t.sap_description)
    LEFT JOIN biq_auth.transaction_locks lk
      ON lk.bank_ref_1 = w.bank_ref_1
      AND lk.expires_at > NOW()
    WHERE (w.assigned_user_id = $1 OR $2 = TRUE)
      AND t.reconcile_status IN ('PENDING', 'REVIEW')
      AND t.doc_type IN ('ZR', 'SA')
      AND t.is_compensated_sap = FALSE
      AND t.is_compensated_intraday = FALSE
    ORDER BY t.bank_date ASC, t.stg_id ASC
  `, [userId, isAdmin]);
  return result.rows;
}

export async function findActiveLockForUser(userId) {
  const result = await pool.query(`
    SELECT lk.bank_ref_1, lk.expires_at, t.stg_id
    FROM biq_auth.transaction_locks lk
    JOIN biq_auth.transaction_workitems w ON w.bank_ref_1 = lk.bank_ref_1
    JOIN biq_stg.stg_bank_transactions t
      ON COALESCE(NULLIF(TRIM(t.bank_ref_1),''), t.sap_description) = lk.bank_ref_1
    WHERE lk.locked_by_id = $1
      AND lk.expires_at > NOW()
    LIMIT 1
  `, [userId]);
  return result.rows[0] || null;
}

export async function approveReconciliation({
  stgId,
  bankRef1,
  selectedPortfolioIds,
  adjustments,
  isOverride,
  overrideReason,
  isSplitPayment,
  splitData,
  approvedBy,
  approvedUserId,
  updatedBankCustomer,
  bankSettlementId,
  approvedPortfolioIdsJson,
}) {
  const client = await pool.connect();

  // Use bank_ref_1 as settlement_id fallback for TRANSFER transactions
  const effectiveSettlementId = bankSettlementId || bankRef1;

  // Extract analyst-approved adjustments
  // These override the bank's calculated values in Gold export
  const approvedCommission = parseFloat(adjustments?.commission)  || 0;
  const approvedTaxIva     = parseFloat(adjustments?.taxIva)      || 0;
  const approvedTaxIrf     = parseFloat(adjustments?.taxIrf)      || 0;
  const approvedDiffAmount = parseFloat(adjustments?.diffAmount)  || 0;
  const diffAccountCode    = adjustments?.diffAccountCode         || null;

  try {
    await client.query('BEGIN');

    // ── Step 1: Update bank transaction ──────────────────────────────
    if (updatedBankCustomer) {
      await client.query(`
        UPDATE biq_stg.stg_bank_transactions
        SET reconcile_status       = 'MATCHED_MANUAL',
            enrich_customer_id     = $2,
            enrich_customer_name   = $3,
            reconcile_reason       = 'MANUAL_MATCH',
            match_method           = 'MANUAL_MATCH',
            match_confidence_score = 100,
            matched_portfolio_ids  = $4,
            reconciled_at          = NOW(),
            updated_at             = NOW()
        WHERE stg_id = $1
      `, [stgId, updatedBankCustomer.customerCode,
          updatedBankCustomer.customerName, approvedPortfolioIdsJson]);
    } else {
      await client.query(`
        UPDATE biq_stg.stg_bank_transactions
        SET reconcile_status       = 'MATCHED_MANUAL',
            reconcile_reason       = 'MANUAL_MATCH',
            match_method           = 'MANUAL_MATCH',
            match_confidence_score = 100,
            matched_portfolio_ids  = $2,
            reconciled_at          = NOW(),
            updated_at             = NOW()
        WHERE stg_id = $1
      `, [stgId, approvedPortfolioIdsJson]);
    }

    // ── Step 2: Portfolio items ───────────────────────────────────────
    if (isSplitPayment && splitData) {

      // Close parent — sin settlement_id:
      // El settlement_id pertenece solo a Child-A (la hija MATCHED).
      // El padre es el registro original cerrado por referencia histórica.
      await client.query(`
        UPDATE biq_stg.stg_customer_portfolio
        SET reconcile_status   = 'CLOSED',
            conciliable_amount = 0,
            match_method       = 'SPLIT_PAYMENT',
            match_confidence   = 100,
            reconciled_at      = NOW(),
            closed_at          = NOW(),
            updated_at         = NOW()
        WHERE stg_id = $1
      `, [splitData.parentStgId]);

      // Create Child-A: MATCHED — applied amount
      await client.query(`
        INSERT INTO biq_stg.stg_customer_portfolio (
          invoice_ref, sap_doc_number, accounting_doc,
          customer_code, customer_name, assignment,
          doc_date, due_date,
          amount_outstanding, conciliable_amount,
          currency, gl_account,
          financial_amount_gross, financial_amount_net,
          financial_commission, financial_tax_iva, financial_tax_irf,
          settlement_id, is_partial_payment,
          sap_residual_amount,
          reconcile_status, reconciled_at,
          match_method, match_confidence, sap_text,
          is_manual_residual, parent_stg_id,
          created_at, updated_at
        )
        SELECT
          invoice_ref, sap_doc_number, accounting_doc,
          customer_code, customer_name, assignment,
          doc_date, due_date,
          $2, 0,
          currency, gl_account,
          $3, $4, $5, $6, $7,
          $8, TRUE,
          $9,
          'MATCHED', NOW(),
          'SPLIT_PAYMENT', 100, sap_text,
          TRUE, stg_id,
          NOW(), NOW()
        FROM biq_stg.stg_customer_portfolio
        WHERE stg_id = $1
      `, [
        splitData.parentStgId,
        splitData.appliedAmount,
        splitData.financialGross  || 0,
        splitData.financialNet    || 0,
        splitData.commission      || 0,
        splitData.taxIva          || 0,
        splitData.taxIrf          || 0,
        effectiveSettlementId,
        splitData.residualAmount,
      ]);

      const childAResult = await client.query(`
        SELECT stg_id FROM biq_stg.stg_customer_portfolio
        WHERE parent_stg_id = $1
          AND reconcile_status = 'MATCHED'
          AND is_manual_residual = TRUE
        ORDER BY created_at DESC LIMIT 1
      `, [splitData.parentStgId]);

      const childAStgId = childAResult.rows[0]?.stg_id;

      // Create Child-B: PENDING — residual (no settlement_id)
      await client.query(`
        INSERT INTO biq_stg.stg_customer_portfolio (
          invoice_ref, sap_doc_number, accounting_doc,
          customer_code, customer_name, assignment,
          doc_date, due_date,
          amount_outstanding, conciliable_amount,
          currency, gl_account,
          settlement_id, is_partial_payment,
          sap_residual_amount, reconcile_status,
          match_method, sap_text,
          is_manual_residual, parent_stg_id,
          created_at, updated_at
        )
        SELECT
          invoice_ref, sap_doc_number, accounting_doc,
          customer_code, customer_name, assignment,
          doc_date, due_date,
          $2, $2, currency, gl_account,
          NULL, TRUE, NULL, 'PENDING',
          'SPLIT_RESIDUAL', sap_text,
          TRUE, stg_id, NOW(), NOW()
        FROM biq_stg.stg_customer_portfolio
        WHERE stg_id = $1
      `, [splitData.parentStgId, splitData.residualAmount]);

      // ── Update workitem with approved adjustments ──────────────────
      await client.query(`
        UPDATE biq_auth.transaction_workitems
        SET work_status            = 'APPROVED',
            approved_portfolio_ids = $1,
            approved_by            = $2,
            approved_at            = NOW(),
            is_override            = $3,
            override_reason        = $4,
            approved_commission    = $5,
            approved_tax_iva       = $6,
            approved_tax_irf       = $7,
            diff_amount            = $8,
            diff_account_code      = $9,
            updated_at             = NOW()
        WHERE bank_ref_1 = $10
      `, [
        String(childAStgId),
        approvedBy,
        isOverride || false,
        overrideReason || null,
        approvedCommission,
        approvedTaxIva,
        approvedTaxIrf,
        approvedDiffAmount,
        diffAccountCode,
        bankRef1,
      ]);

    } else {

      // ── STANDARD FLOW ──────────────────────────────────────────────
      await client.query(`
        UPDATE biq_stg.stg_customer_portfolio
        SET reconcile_status   = 'CLOSED',
            conciliable_amount = 0,
            settlement_id      = $2,
            match_method       = 'MANUAL_MATCH',
            match_confidence   = 100,
            reconciled_at      = NOW(),
            closed_at          = NOW(),
            updated_at         = NOW()
        WHERE stg_id = ANY($1::bigint[])
          AND reconcile_status IN ('PENDING', 'ENRICHED', 'REVIEW')
      `, [selectedPortfolioIds, effectiveSettlementId]);

      // ── Update workitem with approved adjustments ──────────────────
      await client.query(`
        UPDATE biq_auth.transaction_workitems
        SET work_status            = 'APPROVED',
            approved_portfolio_ids = $1,
            approved_by            = $2,
            approved_at            = NOW(),
            diff_account_code      = $3,
            diff_amount            = $4,
            is_override            = $5,
            override_reason        = $6,
            approved_commission    = $7,
            approved_tax_iva       = $8,
            approved_tax_irf       = $9,
            updated_at             = NOW()
        WHERE bank_ref_1 = $10
      `, [
        selectedPortfolioIds.join(','),
        approvedBy,
        diffAccountCode,
        approvedDiffAmount || null,
        isOverride     || false,
        overrideReason || null,
        approvedCommission || null,
        approvedTaxIva     || null,
        approvedTaxIrf     || null,
        bankRef1,
      ]);
    }

    // ── Step 3: Release lock ──────────────────────────────────────────
    await client.query(`
      DELETE FROM biq_auth.transaction_locks WHERE bank_ref_1 = $1
    `, [bankRef1]);

    await client.query('COMMIT');
    return { success: true };

  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// ─────────────────────────────────────────────
// FIND WORKITEM BY STG_ID
// Used by getPanel() and approve() to verify
// assignment and lock ownership before acting.
// ─────────────────────────────────────────────
export async function findWorkitemByStgId(stgId) {
  const result = await pool.query(`
    SELECT w.bank_ref_1, w.work_status, w.assigned_user_id
    FROM biq_auth.transaction_workitems w
    WHERE w.stg_id = $1
  `, [stgId]);
  return result.rows[0] || null;
}

// ─────────────────────────────────────────────
// FIND BANK TRANSACTION FOR CALCULATION
// Fetches the fields needed by calculate() to
// derive effective bank amount and TC detection.
// ─────────────────────────────────────────────
export async function findBankTransactionForCalculation(stgId) {
  const result = await pool.query(`
    SELECT amount_total, currency, trans_type,
           final_amount_commission, final_amount_tax_iva,
           final_amount_tax_irf, diff_adjustment
    FROM biq_stg.stg_bank_transactions
    WHERE stg_id = $1
  `, [stgId]);
  return result.rows[0] || null;
}

// ─────────────────────────────────────────────
// FIND PORTFOLIO ITEMS BY IDS
// Fetches selected invoices for balance summing
// in calculate(). Returns only the columns needed.
// ─────────────────────────────────────────────
export async function findPortfolioItemsById(stgIds) {
  if (!stgIds || stgIds.length === 0) return [];
  const result = await pool.query(`
    SELECT stg_id, invoice_ref, customer_code, customer_name,
           conciliable_amount, reconcile_status, gl_account
    FROM biq_stg.stg_customer_portfolio
    WHERE stg_id = ANY($1::bigint[])
  `, [stgIds]);
  return result.rows;
}

// ─────────────────────────────────────────────
// FIND BANK SETTLEMENT ID
// Single-column lookup used in approve() to pass
// settlement_id into approveReconciliation().
// ─────────────────────────────────────────────
export async function findBankSettlementId(stgId) {
  const result = await pool.query(`
    SELECT settlement_id
    FROM biq_stg.stg_bank_transactions
    WHERE stg_id = $1
  `, [stgId]);
  return result.rows[0]?.settlement_id || null;
}

// ─────────────────────────────────────────────
// FIND PORTFOLIO CUSTOMER
// Fetches customer identity from a single invoice —
// used in approve() to backfill unknown bank records.
// ─────────────────────────────────────────────
export async function findPortfolioCustomer(stgId) {
  const result = await pool.query(`
    SELECT customer_code, customer_name
    FROM biq_stg.stg_customer_portfolio
    WHERE stg_id = $1
  `, [stgId]);
  return result.rows[0] || null;
}

export async function findMyStats({ userId, isAdmin = false }) {
  if (isAdmin) {
    const result = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE t.reconcile_status = 'PENDING')::integer                     AS pending_count,
        COUNT(*) FILTER (WHERE t.reconcile_status = 'REVIEW')::integer                     AS review_count,
        COUNT(*) FILTER (WHERE t.reconcile_status IN ('MATCHED_MANUAL','MATCHED'))::integer AS approved_count,
        COUNT(*)::integer                                                                    AS total_count,
        COALESCE(SUM(t.amount_total) FILTER (WHERE t.reconcile_status = 'PENDING'), 0)     AS pending_amount,
        COALESCE(SUM(t.amount_total) FILTER (WHERE t.reconcile_status = 'REVIEW'), 0)             AS review_amount,
        COALESCE(SUM(t.amount_total) FILTER (WHERE t.reconcile_status IN ('MATCHED_MANUAL','MATCHED')), 0) AS approved_amount,
        COALESCE(SUM(t.amount_total), 0)                                                    AS total_amount
      FROM biq_stg.stg_bank_transactions t
      JOIN biq_auth.transaction_workitems w
        ON w.bank_ref_1 = COALESCE(NULLIF(TRIM(t.bank_ref_1),''), t.sap_description)
      WHERE t.doc_type IN ('ZR','SA')
        AND t.is_compensated_sap = FALSE
        AND t.is_compensated_intraday = FALSE
    `);
    return result.rows[0];
  }
  const result = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE t.reconcile_status = 'PENDING')::integer                     AS pending_count,
      COUNT(*) FILTER (WHERE t.reconcile_status = 'REVIEW')::integer                     AS review_count,
      COUNT(*) FILTER (WHERE t.reconcile_status IN ('MATCHED_MANUAL','MATCHED'))::integer AS approved_count,
      COUNT(*)::integer                                                                    AS total_count,
      COALESCE(SUM(t.amount_total) FILTER (WHERE t.reconcile_status = 'PENDING'), 0)     AS pending_amount,
      COALESCE(SUM(t.amount_total) FILTER (WHERE t.reconcile_status = 'REVIEW'), 0)             AS review_amount,
      COALESCE(SUM(t.amount_total) FILTER (WHERE t.reconcile_status IN ('MATCHED_MANUAL','MATCHED')), 0) AS approved_amount,
      COALESCE(SUM(t.amount_total), 0)                                                    AS total_amount
    FROM biq_stg.stg_bank_transactions t
    JOIN biq_auth.transaction_workitems w
      ON w.bank_ref_1 = COALESCE(NULLIF(TRIM(t.bank_ref_1),''), t.sap_description)
    WHERE t.doc_type IN ('ZR','SA')
      AND t.is_compensated_sap = FALSE
      AND t.is_compensated_intraday = FALSE
      AND w.assigned_user_id = $1
  `, [userId]);
  return result.rows[0];
}