// src/modules/gold-export/gold-export.repository.js
import { pool } from '../../config/database.js';

export async function findReadyForExport() {
  const result = await pool.query(`
    SELECT
      t.stg_id,
      t.bank_ref_1,
      t.sap_description,
      t.doc_date,
      t.bank_date,
      t.doc_number,
      t.amount_total,
      t.currency,
      t.trans_type,
      t.global_category,
      t.establishment_name,
      t.brand,
      t.settlement_id,
      t.enrich_customer_id,
      t.enrich_customer_name,
      t.reconcile_status,
      t.reconcile_reason,
      t.match_method,
      t.match_confidence_score,
      -- Bank-calculated values (Python algorithm output)
      -- Used only as fallback if analyst did not override
      t.final_amount_gross,
      t.final_amount_net,
      t.final_amount_commission    AS bank_commission,
      t.final_amount_tax_iva       AS bank_tax_iva,
      t.final_amount_tax_irf       AS bank_tax_irf,
      t.diff_adjustment            AS bank_diff_adjustment,
      t.matched_portfolio_ids,
      -- Workitem data — analyst-approved values (source of truth for Gold)
      w.work_status,
      w.approved_by,
      w.approved_at,
      w.approved_portfolio_ids     AS workitem_portfolio_ids,
      -- Analyst-approved adjustments — these override bank values
      w.approved_commission,
      w.approved_tax_iva,
      w.approved_tax_irf,
      w.diff_amount                AS approved_diff_amount,
      w.diff_account_code          AS approved_diff_account,
      w.is_override
    FROM biq_stg.stg_bank_transactions t
    JOIN biq_auth.transaction_workitems w
      ON w.bank_ref_1 = COALESCE(NULLIF(TRIM(t.bank_ref_1),''), t.sap_description)
    WHERE w.work_status = 'APPROVED'
      AND t.doc_type IN ('ZR','SA')
      AND t.is_compensated_sap = FALSE
      AND t.is_compensated_intraday = FALSE
      AND NOT EXISTS (
        SELECT 1 FROM biq_gold.payment_header gh
        WHERE gh.bank_ref_1 = COALESCE(NULLIF(TRIM(t.bank_ref_1),''), t.sap_description)
          AND gh.rpa_status != 'CANCELLED'
      )
    ORDER BY t.bank_date ASC, t.stg_id ASC
  `);
  return result.rows;
}

export async function findPortfolioItemsForExport(portfolioIds) {
  if (!portfolioIds || portfolioIds.length === 0) return [];
  const result = await pool.query(`
    SELECT
      p.stg_id,
      p.invoice_ref,
      p.assignment,
      p.customer_code,
      p.customer_name,
      p.amount_outstanding,
      p.conciliable_amount,
      p.financial_amount_net,
      p.financial_amount_gross,
      p.financial_commission,
      p.financial_tax_iva,
      p.financial_tax_irf,
      p.gl_account,
      -- Regla unificada para is_partial_payment en Gold:
      -- TRUE solo cuando hay un sap_residual_amount real > 0 que el RPA debe registrar.
      -- Esto cubre correctamente:
      --   1. Split manual: hija MATCHED tiene sap_residual_amount = residuo real
      --   2. TC Parking con residuo real: sap_residual_amount > 0
      -- Y excluye correctamente:
      --   3. TC Parking sin residuo: partial_payment_flag=TRUE pero sap_residual_amount IS NULL
      --      → el voucher va completo, el RPA NO debe abrir part_rest
      CASE
        WHEN p.sap_residual_amount IS NOT NULL
         AND p.sap_residual_amount > 0
        THEN TRUE
        ELSE FALSE
      END AS effective_is_partial_payment,
      p.sap_residual_amount,
      p.reconcile_status
    FROM biq_stg.stg_customer_portfolio p
    WHERE p.stg_id = ANY($1::bigint[])
    ORDER BY p.stg_id ASC
  `, [portfolioIds]);
  return result.rows;
}

export async function insertGoldHeader(client, header) {
  const result = await client.query(`
    INSERT INTO biq_gold.payment_header (
      idempotency_hash, batch_id, batch_date,
      transaction_sap, bank_ref_1, stg_id,
      posting_date, doc_class, period, company_code, currency,
      reference_text, bank_gl_account, amount,
      customer_code, customer_name, multi_customer,
      match_method, match_confidence, reconcile_reason,
      approved_by, approved_at, exported_by, exported_at,
      rpa_status
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
      $15,$16,$17,$18,$19,$20,$21,$22,$23,NOW(),'PENDING_RPA'
    )
    ON CONFLICT (idempotency_hash) DO NOTHING
    RETURNING id
  `, [
    header.idempotencyHash, header.batchId,       header.batchDate,
    header.transactionSap,  header.bankRef1,       header.stgId,
    header.postingDate,     header.docClass,       header.period,
    header.companyCode,     header.currency,       header.referenceText,
    header.bankGlAccount,   header.amount,
    header.customerCode,    header.customerName,   header.multiCustomer,
    header.matchMethod,     header.matchConfidence, header.reconcileReason,
    header.approvedBy,      header.approvedAt,     header.exportedBy,
  ]);
  return result.rows[0]?.id || null;
}

export async function insertGoldDetails(client, headerId, batchId, details) {
  for (const d of details) {
    await client.query(`
      INSERT INTO biq_gold.payment_detail (
        header_id, batch_id, line_number,
        portfolio_stg_id, invoice_ref, assignment,
        customer_code, customer_name,
        amount_gross, financial_amount_net,
        is_partial_payment, sap_residual_amount, gl_account
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
    `, [
      headerId, batchId, d.lineNumber,
      d.portfolioStgId, d.invoiceRef,  d.assignment,
      d.customerCode,   d.customerName,
      d.amountGross,    d.financialAmountNet || null,
      d.isPartialPayment || false,
      d.sapResidualAmount || null,
      d.glAccount,
    ]);
  }
}

export async function insertGoldDiffs(client, headerId, batchId, diffs) {
  for (const d of diffs) {
    await client.query(`
      INSERT INTO biq_gold.payment_diff (
        header_id, batch_id, line_number,
        sap_posting_key, gl_account,
        amount, adjustment_type, line_text
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    `, [
      headerId, batchId, d.lineNumber,
      d.sapPostingKey, d.glAccount,
      d.amount, d.adjustmentType, d.lineText,
    ]);
  }
}

export async function getBatchHistory() {
  const result = await pool.query(`
    SELECT
      batch_id,
      batch_date,
      COUNT(*)::integer  AS count,
      SUM(amount)        AS total_amount,
      MAX(rpa_status)    AS rpa_status,
      MIN(exported_at)   AS submitted_at,
      MAX(approved_by)   AS submitted_by
    FROM biq_gold.payment_header
    GROUP BY batch_id, batch_date
    ORDER BY MIN(exported_at) DESC
    LIMIT 50
  `);
  return result.rows;
}

export async function getNextBatchId(batchDate) {
  const result = await pool.query(`
    SELECT COUNT(DISTINCT batch_id)::integer AS count
    FROM biq_gold.payment_header
    WHERE batch_date = $1
  `, [batchDate]);
  const seq     = (result.rows[0].count || 0) + 1;
  const dateStr = batchDate.replace(/-/g, '');
  return `PACIOLI-${dateStr}-${String(seq).padStart(3,'0')}`;
}