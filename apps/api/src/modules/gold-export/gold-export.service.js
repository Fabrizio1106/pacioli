// src/modules/gold-export/gold-export.service.js
import crypto         from 'crypto';
import { pool }       from '../../config/database.js';
import { GL_ACCOUNTS, resolvePostingKey } from '../../config/gl-accounts.js';
import * as repo      from './gold-export.repository.js';

function parseIds(raw) {
  if (!raw) return [];
  return raw.toString()
    .replace(/[\[\]\s]/g, '')
    .split(',')
    .map(id => parseInt(id.trim(), 10))
    .filter(id => !isNaN(id));
}

function buildIdempotencyHash(bankRef1, bankDate, amount, batchId) {
  const dateStr = String(bankDate).split('T')[0];
  const raw     = `${bankRef1}|${dateStr}|${amount}|${batchId}`;
  return crypto.createHash('sha256').update(raw).digest('hex');
}

// ─────────────────────────────────────────────────────────────────────────────
// BUILD REFERENCE TEXT
// overrideCustomerName: passed from submitForPosting when bank has no customer
// For transfers where enrich_customer_name is NULL, we use the first portfolio
// item's customer name so the reference text is complete for SAP/RPA traceability
// ─────────────────────────────────────────────────────────────────────────────
function buildReferenceText(tx, overrideCustomerName = null) {
  const bankRef1     = (tx.bank_ref_1 || tx.sap_description || '').trim();
  const estName      = (tx.establishment_name || '').toUpperCase().trim();
  const brand        = (tx.brand || '').toUpperCase().trim();
  const settlement   = (tx.settlement_id || '').trim();
  // Use override (from portfolio) when bank has no customer identified
  const customerName = overrideCustomerName || (tx.enrich_customer_name || '').trim();

  let text;

  if (tx.trans_type === 'LIQUIDACION TC') {
    if (estName.includes('PARKING')) {
      text = `${bankRef1} TC ${brand} PARKING ${settlement}`;
    } else if (estName.includes('VIP') || estName.includes('ASISTENCIA')) {
      text = `${bankRef1} TC ${brand} SV ${settlement}`;
    } else {
      text = `${bankRef1} TC ${brand} ${settlement}`;
    }
  } else {
    const ref      = bankRef1.substring(0, 20);
    const maxCust  = 50 - ref.length - 7;
    const custPart = customerName.substring(0, maxCust > 0 ? maxCust : 0);
    text = `${ref} COBRO ${custPart}`;
  }

  return text.trim().substring(0, 50);
}

// ─────────────────────────────────────────────────────────────────────────────
// BUILD DIFF LINES
// Uses analyst-approved adjustments from workitem as source of truth
// Falls back to bank values ONLY for auto-matched (ALGORITHM_PACIOLI)
// ─────────────────────────────────────────────────────────────────────────────
function buildDiffLines(tx, startLineNumber) {
  const diffs      = [];
  let   lineNumber = startLineNumber;
  const bankRef1   = tx.bank_ref_1 || tx.sap_description;
  const estName    = (tx.establishment_name || '').toUpperCase().trim();
  const brand      = (tx.brand || '').toUpperCase().trim();
  const settlement = tx.settlement_id || '';
  const isTC       = tx.trans_type === 'LIQUIDACION TC';

  const textPrefix = isTC
    ? `TC ${brand} ${estName.includes('VIP') ? 'SV' : estName.includes('PARKING') ? 'PARKING' : ''} ${settlement}`.trim()
    : (tx.enrich_customer_name || '').substring(0, 20);

  const isManual = tx.approved_by !== 'ALGORITHM_PACIOLI';

  const commission = isManual
    ? (parseFloat(tx.approved_commission) || 0)
    : (parseFloat(tx.bank_commission)     || 0);

  const taxIva = isManual
    ? (parseFloat(tx.approved_tax_iva) || 0)
    : (parseFloat(tx.bank_tax_iva)     || 0);

  const taxIrf = isManual
    ? (parseFloat(tx.approved_tax_irf) || 0)
    : (parseFloat(tx.bank_tax_irf)     || 0);

  const diffAmount     = parseFloat(tx.approved_diff_amount) || 0;
  const diffAccountRaw = tx.approved_diff_account || null;

  if (commission > 0) {
    const cfg = GL_ACCOUNTS.adjustments.final_amount_commission;
    diffs.push({
      lineNumber:     lineNumber++,
      sapPostingKey:  cfg.sap_posting_key,
      glAccount:      cfg.gl_account,
      amount:         commission,
      adjustmentType: 'final_amount_commission',
      lineText:       `COMMISSION ${textPrefix}`.substring(0, 100),
    });
  }

  if (taxIva > 0) {
    const cfg = GL_ACCOUNTS.adjustments.final_amount_tax_iva;
    diffs.push({
      lineNumber:     lineNumber++,
      sapPostingKey:  cfg.sap_posting_key,
      glAccount:      cfg.gl_account,
      amount:         taxIva,
      adjustmentType: 'final_amount_tax_iva',
      lineText:       `TAX IVA ${textPrefix}`.substring(0, 100),
    });
  }

  if (taxIrf > 0) {
    const cfg = GL_ACCOUNTS.adjustments.final_amount_tax_irf;
    diffs.push({
      lineNumber:     lineNumber++,
      sapPostingKey:  cfg.sap_posting_key,
      glAccount:      cfg.gl_account,
      amount:         taxIrf,
      adjustmentType: 'final_amount_tax_irf',
      lineText:       `TAX IRF ${textPrefix}`.substring(0, 100),
    });
  }

  if (Math.abs(diffAmount) > 0) {
    const cfg        = GL_ACCOUNTS.adjustments.diff_cambiario;
    const postingKey = resolvePostingKey('diff_cambiario', diffAmount);
    const glAccount  = diffAccountRaw || cfg.gl_account;
    diffs.push({
      lineNumber:     lineNumber++,
      sapPostingKey:  postingKey,
      glAccount,
      amount:         Math.abs(diffAmount),
      adjustmentType: 'diff_cambiario',
      lineText:       `DIFF ${textPrefix} ${bankRef1}`.substring(0, 100),
    });
  }

  return diffs;
}

/**
 * Returns a summary of all transactions ready for Gold Layer export.
 *
 * Fetches approved transactions not yet posted and produces a breakdown
 * by count, total amount, match type (manual vs algorithm), and transaction
 * type. Also returns the next batch ID so the admin can preview exactly
 * what will be committed before calling submitForPosting.
 *
 * @returns {Promise<object>} Preview summary including totalAmount, manualCount,
 *   autoCount, byTransType array, nextBatchId, and a transactions list.
 */
export async function getExportPreview() {
  const transactions = await repo.findReadyForExport();

  const manual      = transactions.filter(t => t.approved_by !== 'ALGORITHM_PACIOLI');
  const automatic   = transactions.filter(t => t.approved_by === 'ALGORITHM_PACIOLI');
  const totalAmount = transactions.reduce((s, t) => s + parseFloat(t.amount_total || 0), 0);

  const typeMap = {};
  for (const t of transactions) {
    const key = t.trans_type || 'OTHER';
    if (!typeMap[key]) typeMap[key] = { transType: key, count: 0, amount: 0 };
    typeMap[key].count++;
    typeMap[key].amount += parseFloat(t.amount_total || 0);
  }
  const byTransType = Object.values(typeMap).sort((a, b) => b.count - a.count);

  const batchDate   = new Date().toISOString().split('T')[0];
  const nextBatchId = await repo.getNextBatchId(batchDate);

  return {
    totalTransactions: transactions.length,
    totalCount:        transactions.length,
    totalAmount:       Math.round(totalAmount * 100) / 100,
    manualApprovals:   manual.length,
    manualCount:       manual.length,
    autoMatched:       automatic.length,
    autoCount:         automatic.length,
    byTransType,
    nextBatchId,
    transactions: transactions.map(t => ({
      stgId:        t.stg_id,
      bankRef1:     t.bank_ref_1 || t.sap_description,
      bankDate:     t.bank_date,
      amount:       parseFloat(t.amount_total),
      customerName: t.enrich_customer_name,
      transType:    t.trans_type,
      approvedBy:   t.approved_by,
      approvedAt:   t.approved_at,
    })),
  };
}

/**
 * Atomically builds and posts the Gold Layer export batch.
 *
 * Iterates all transactions ready for export and inserts a payment header,
 * invoice detail lines, and adjustment diff lines for each one within a
 * single database transaction. Per-transaction errors are captured and
 * reported without aborting the batch. Duplicate submissions are rejected
 * via a SHA-256 idempotency hash keyed on bankRef1, date, amount, and batchId.
 *
 * @param {object} params
 * @param {string} params.exportedBy - Username of the admin submitting the batch.
 * @returns {Promise<object>} Batch result with batchId, batchDate, exported count,
 *   skipped count, errors array, totalAmount, and per-item status list.
 * @throws {400} If there are no transactions ready for export.
 */
export async function submitForPosting({ exportedBy }) {
  const transactions = await repo.findReadyForExport();

  if (transactions.length === 0) {
    throw Object.assign(new Error('No transactions ready for posting'), { status: 400 });
  }

  const batchDate = new Date().toISOString().split('T')[0];
  const batchId   = await repo.getNextBatchId(batchDate);
  const client    = await pool.connect();

  let exported = 0;
  let skipped  = 0;
  const errors = [];
  const items  = [];

  try {
    await client.query('BEGIN');

    for (const tx of transactions) {
      const bankRef1 = tx.bank_ref_1 || tx.sap_description;
      try {
        const idempotencyHash = buildIdempotencyHash(bankRef1, tx.bank_date, tx.amount_total, batchId);

        const rawIds       = tx.workitem_portfolio_ids || tx.matched_portfolio_ids;
        const portfolioIds = parseIds(rawIds);

        if (portfolioIds.length === 0) {
          errors.push({ bankRef1, reason: 'No portfolio IDs found' });
          items.push({ bankRef1, transType: tx.trans_type, amount: parseFloat(tx.amount_total), error: true });
          continue;
        }

        const portfolioItems = await repo.findPortfolioItemsForExport(portfolioIds);

        if (portfolioItems.length === 0) {
          errors.push({ bankRef1, reason: 'Portfolio items not found' });
          items.push({ bankRef1, transType: tx.trans_type, amount: parseFloat(tx.amount_total), error: true });
          continue;
        }

        // ── CUSTOMER RESOLUTION ──────────────────────────────────────────────
        // For LIQUIDACION TC: bank customer = card processor, use portfolio customer
        // For transfers: use portfolio customer, multi_customer flag if multiple
        const uniqueCustomers = [...new Set(portfolioItems.map(p => p.customer_code))];
        const multiCustomer   = uniqueCustomers.length > 1;
        const isTC            = tx.trans_type === 'LIQUIDACION TC';

        let headerCustomerCode, headerCustomerName;

        if (isTC) {
          headerCustomerCode = portfolioItems[0].customer_code;
          headerCustomerName = portfolioItems[0].customer_name;
        } else {
          headerCustomerCode = multiCustomer ? tx.enrich_customer_id   : portfolioItems[0].customer_code;
          headerCustomerName = multiCustomer ? tx.enrich_customer_name : portfolioItems[0].customer_name;
        }

        const bankDate = new Date(tx.bank_date);
        const period   = bankDate.getMonth() + 1;

        // ── REFERENCE TEXT ───────────────────────────────────────────────────
        // Pass headerCustomerName as override so transfers without a bank-identified
        // customer still produce a complete reference: "1584555844 COBRO MERAMEXAIR S.A."
        // instead of the incomplete "1584555844 COBRO "
        const referenceText = buildReferenceText(tx, headerCustomerName).substring(0, 255);

        const header = {
          idempotencyHash,
          batchId,
          batchDate,
          transactionSap:  GL_ACCOUNTS.f28.transactionSap,
          bankRef1,
          stgId:           tx.stg_id,
          postingDate:     bankDate.toISOString().split('T')[0],
          docClass:        GL_ACCOUNTS.f28.docClass,
          period,
          companyCode:     GL_ACCOUNTS.f28.companyCode,
          currency:        GL_ACCOUNTS.f28.currency,
          referenceText,
          bankGlAccount:   GL_ACCOUNTS.bankGlAccounts.primary,
          amount:          parseFloat(tx.amount_total),
          customerCode:    headerCustomerCode,
          customerName:    headerCustomerName,
          multiCustomer,
          matchMethod:     tx.match_method,
          matchConfidence: parseFloat(tx.match_confidence_score) || null,
          reconcileReason: tx.reconcile_reason,
          approvedBy:      tx.approved_by,
          approvedAt:      tx.approved_at,
          exportedBy,
        };

        const headerId = await repo.insertGoldHeader(client, header);

        if (!headerId) {
          skipped++;
          items.push({ bankRef1, transType: tx.trans_type, amount: parseFloat(tx.amount_total), error: false });
          continue;
        }

        const detailLines = portfolioItems.map((item, index) => ({
          lineNumber:         index + 1,
          portfolioStgId:     item.stg_id,
          invoiceRef:         item.invoice_ref,
          assignment:         item.assignment,
          customerCode:       item.customer_code,
          customerName:       item.customer_name,
          amountGross:        parseFloat(item.amount_outstanding || item.conciliable_amount),
          financialAmountNet: item.financial_amount_net ? parseFloat(item.financial_amount_net) : null,
          isPartialPayment:   item.effective_is_partial_payment || false, // regla: sap_residual_amount > 0
          sapResidualAmount:  item.sap_residual_amount   ? parseFloat(item.sap_residual_amount) : null,
          glAccount:          item.gl_account,
        }));

        await repo.insertGoldDetails(client, headerId, batchId, detailLines);

        const diffLines = buildDiffLines(tx, detailLines.length + 1);
        if (diffLines.length > 0) {
          await repo.insertGoldDiffs(client, headerId, batchId, diffLines);
        }

        items.push({ bankRef1, transType: tx.trans_type, amount: parseFloat(tx.amount_total), error: false });
        exported++;

      } catch (txError) {
        errors.push({ bankRef1, reason: txError.message });
        items.push({ bankRef1, transType: tx.trans_type, amount: parseFloat(tx.amount_total), error: true });
      }
    }

    await client.query('COMMIT');

  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }

  return {
    batchId,
    batchDate,
    exported,
    skipped,
    errors,
    totalProcessed: transactions.length,
    totalAmount:    items.filter(i => !i.error).reduce((s, i) => s + i.amount, 0),
    items,
  };
}

/**
 * Returns the Gold Layer export batch history ordered by submission date.
 *
 * Returns up to the 50 most recent batches. Each entry includes the batch ID,
 * date, transaction count, total amount, RPA processing status, and the
 * submitting user. Intended for the admin batch history view.
 *
 * @returns {Promise<object[]>} Array of batch summary DTOs ordered by submitted date descending.
 */
export async function getBatchHistory() {
  const rows = await repo.getBatchHistory();
  return rows.map(b => ({
    batchId:     b.batch_id,
    batchDate:   b.batch_date,
    count:       b.count,
    totalAmount: parseFloat(b.total_amount) || 0,
    rpaStatus:   b.rpa_status,
    submittedAt: b.submitted_at,
    submittedBy: b.submitted_by,
  }));
}