// src/modules/workspace/workspace.service.js
import * as repo      from './workspace.repository.js';
import * as lockRepo  from '../locks/locks.repository.js';
import * as auditRepo from '../auth/auth.repository.js';
import { loadPortfolioForTransaction } from '../portfolio/portfolio.service.js';

/**
 * Returns the transaction queue for the calling user.
 *
 * Admins receive all active transactions; analysts receive only those
 * assigned to them. Raw DB rows are mapped to the client DTO shape
 * including lock state and financial breakdown.
 *
 * @param {object} params
 * @param {object} params.user - Authenticated user ({ id, role }).
 * @returns {Promise<object[]>} Array of queue item DTOs ordered by bank date ascending.
 */
export async function getMyQueue({ user }) {
  const rows = await repo.findMyQueue({
    userId:  user.id,
    isAdmin: user.role === 'admin',
  });

  return rows.map(r => ({
    id:                  r.stg_id,
    docType:             r.doc_type,
    bankDate:            r.bank_date,
    docNumber:           r.doc_number,
    bankRef1:            r.bank_ref_1 || r.sap_description,
    bankRef2:            r.bank_ref_2 || null,
    sapDescription:      r.sap_description,
    enrichNotes:         r.enrich_notes || null,
    amountTotal:         parseFloat(r.amount_total) || 0,
    currency:            r.currency,
    transType:           r.trans_type,
    globalCategory:      r.global_category,
    establishmentName:   r.establishment_name,
    brand:               r.brand,
    settlementId:        r.settlement_id,
    customerCode:        r.enrich_customer_id,
    customerName:        r.enrich_customer_name,
    confidenceScore:     parseFloat(r.enrich_confidence_score) || 0,
    reconcileStatus:     r.reconcile_status,
    reconcileReason:     r.reconcile_reason,
    matchMethod:         r.match_method,
    matchConfidence:     parseFloat(r.match_confidence_score) || 0,
    financialAmounts: {
      gross:      parseFloat(r.final_amount_gross)      || 0,
      net:        parseFloat(r.final_amount_net)        || 0,
      commission: parseFloat(r.final_amount_commission) || 0,
      taxIva:     parseFloat(r.final_amount_tax_iva)    || 0,
      taxIrf:     parseFloat(r.final_amount_tax_irf)    || 0,
      diffAdj:    parseFloat(r.diff_adjustment)         || 0,
    },
    workStatus:       r.work_status,
    analystNote:      r.analyst_note || null,
    detectedScenario: r.detected_scenario,
    isLocked:         r.is_locked,
    lockedBy:         r.locked_by_name,
    lockExpiresAt:    r.lock_expires_at,
  }));
}

/**
 * Loads the invoice selection panel for a transaction under review.
 *
 * Verifies the workitem is assigned to the calling user before delegating
 * to the portfolio service. The returned panel includes scenario detection,
 * suggested and complementary invoice lists, pagination, and UI configuration.
 *
 * @param {object}      params
 * @param {number}      params.stgId    - Staging ID of the bank transaction.
 * @param {object}      params.user     - Authenticated user ({ id, role }).
 * @param {string|null} [params.search] - Free-text search filter for invoices.
 * @param {number}      [params.page]   - Page number for complementary invoice list (default 1).
 * @returns {Promise<object>} Panel data: scenario, bankContext, suggestedItems, complementaryItems, pagination, uiConfig.
 * @throws {403} If the transaction is not assigned to the calling user.
 * @throws {404} If the transaction is not found in workitems.
 */
export async function getPanel({ stgId, user, search = null, page = 1 }) {
  const workitem = await repo.findWorkitemByStgId(stgId);
  if (!workitem) throw Object.assign(new Error('Transaction not found'), { status: 404 });
  if (workitem.assigned_user_id !== user.id && user.role !== 'admin') {
    throw Object.assign(new Error('This transaction is not assigned to you'), { status: 403 });
  }

  return loadPortfolioForTransaction({ stgId, search, page });
}

/**
 * Computes the workspace balance for a transaction using TC-aware logic.
 *
 * For card settlements (LIQUIDACION TC), commission and withholdings are
 * added to the bank side (they were deducted by the processor before transfer).
 * For transfers and deposits, diffAmount is applied as a signed adjustment.
 * canApprove is true only when unallocated is exactly zero.
 *
 * @param {object}      params
 * @param {number}      params.stgId                  - Staging ID of the bank transaction.
 * @param {number[]}    [params.selectedPortfolioIds]  - Staging IDs of the selected invoices.
 * @param {object}      [params.adjustments]           - Analyst adjustments.
 * @param {number}      [params.adjustments.commission]    - Commission amount (absolute).
 * @param {number}      [params.adjustments.taxIva]        - IVA withholding (absolute).
 * @param {number}      [params.adjustments.taxIrf]        - IRF withholding (absolute).
 * @param {number}      [params.adjustments.diffAmount]    - Exchange/differential amount (signed).
 * @param {boolean}     [params.isSplitPayment]        - True when only part of the bank amount is being applied.
 * @param {number|null} [params.splitAppliedAmount]    - The portion being applied in a split (overrides invoice sum).
 * @returns {Promise<object>} Balance breakdown including effectiveBankAmount, unallocated, canApprove, isCents, and postingKeyHint.
 * @throws {404} If the bank transaction is not found.
 */
export async function calculate({
  stgId, selectedPortfolioIds, adjustments,
  isSplitPayment, splitAppliedAmount,
}) {
  const bank = await repo.findBankTransactionForCalculation(stgId);
  if (!bank) throw Object.assign(new Error('Transaction not found'), { status: 404 });

  const bankAmount = parseFloat(bank.amount_total);
  const isTC       = bank.trans_type === 'LIQUIDACION TC';
 
  let invoicesTotal = 0;
  let selectedItems = [];
 
  if (isSplitPayment && splitAppliedAmount) {
    invoicesTotal = parseFloat(splitAppliedAmount) || 0;
  } else if (selectedPortfolioIds?.length > 0) {
    selectedItems = await repo.findPortfolioItemsById(selectedPortfolioIds);
    invoicesTotal = selectedItems.reduce((s, r) => s + (parseFloat(r.conciliable_amount) || 0), 0);
  }
 
  const adj        = adjustments || {};
  const commission = parseFloat(adj.commission) || 0;
  const taxIva     = parseFloat(adj.taxIva)     || 0;
  const taxIrf     = parseFloat(adj.taxIrf)     || 0;
  const diffAmount = parseFloat(adj.diffAmount) || 0;
 
  // ── BALANCE CALCULATION ────────────────────────────────────────────────────
  //
  // LIQUIDACION TC (card settlements):
  //   Commission and withholdings are DEBIT accounts (posting key 40)
  //   They complete the debit side of the journal entry
  //   Effective bank = bankAmount + commission + taxIva + taxIrf
  //   Because: the card processor deducted these before transferring bankAmount
  //   Journal: DEBIT bank 94.65 + DEBIT commission 4.72 + DEBIT IVA 2.67
  //            CREDIT portfolio 102.03
  //
  // TRANSFERENCIA / DEPOSITO:
  //   Effective bank = bankAmount (no deductions)
  //   diffAmount = exchange difference (debit if positive, credit if negative)
  //   Journal: DEBIT bank 100 + DEBIT/CREDIT exchange diff
  //            CREDIT portfolio
  //
  let effectiveBankAmount;
  let unallocated;
 
  if (isTC) {
    // For TC: bank + retentions + commission = invoices
    effectiveBankAmount = bankAmount + commission + taxIva + taxIrf;
    unallocated = Math.round((effectiveBankAmount - invoicesTotal) * 10000) / 10000;
  } else {
    // For transfers: bank + diffAmount (signed) = invoices
    // diffAmount > 0 means extra debit (adds to bank side)
    // diffAmount < 0 would mean credit adjustment
    effectiveBankAmount = bankAmount + diffAmount;
    unallocated = Math.round((effectiveBankAmount - invoicesTotal) * 10000) / 10000;
  }
 
  const absUnalloc = Math.abs(unallocated);
 
  // Auto-absorb cent differences (Golden Rule: <= $0.05)
  // For TC: absorb into commission
  // For transfers: absorb into diff_adjustment
  const absUnallocRounded = Math.round(absUnalloc * 10000) / 10000;

  // ── GATE DE APROBACIÓN — ESTRICTO ────────────────────────────────────────
  // canApprove = true ÚNICAMENTE cuando unallocated === 0.
  // No existe absorción automática de centavos. Toda diferencia, sin importar
  // el monto, debe ser distribuida manualmente por el analista en Adjustments.
  // Esto garantiza que cada asiento SAP quede perfectamente cuadrado.
  const canApprove = absUnallocRounded === 0;

  // isCents: señal para el UI — diferencia pequeña, va a Exchange Diff
  const isCents = absUnallocRounded > 0 && absUnallocRounded <= 0.05;

  // postingKeyHint: dirección contable para el analista
  //   unallocated > 0 → banco pagó de más → HABER (posting key 50 / credit)
  //   unallocated < 0 → banco pagó de menos → DEBE  (posting key 40 / debit)
  const postingKeyHint = unallocated >= 0 ? '50' : '40';

  let balanceStatus;
  if (absUnallocRounded === 0)              balanceStatus = 'BALANCED';
  else if (isCents && unallocated > 0)      balanceStatus = 'NEEDS_CREDIT_CENTS';
  else if (isCents && unallocated < 0)      balanceStatus = 'NEEDS_DEBIT_CENTS';
  else if (unallocated > 0)                 balanceStatus = 'OVER';
  else                                      balanceStatus = 'UNDER';

  return {
    bankAmount,
    effectiveBankAmount: Math.round(effectiveBankAmount * 100) / 100,
    invoicesTotal:       Math.round(invoicesTotal * 100)       / 100,
    commission, taxIva, taxIrf, diffAmount,
    isTC,
    unallocated:    Math.round(unallocated    * 100) / 100,
    absUnallocated: Math.round(absUnalloc     * 100) / 100,
    balanceStatus,
    canApprove,
    isCents,
    postingKeyHint,
    selectedItems: selectedItems.map(r => ({
      id:                r.stg_id,
      invoiceRef:        r.invoice_ref,
      customerCode:      r.customer_code,
      customerName:      r.customer_name,
      conciliableAmount: parseFloat(r.conciliable_amount),
      status:            r.reconcile_status,
      glAccount:         r.gl_account,
    })),
  };
}

/**
 * Executes the workspace reconciliation approval for a bank transaction.
 *
 * Validates lock ownership, override justification, and a strict zero-unallocated
 * gate before committing. Supports both standard and split-payment flows.
 * On success:
 * - Bank transaction  → MATCHED_MANUAL
 * - Selected invoices → CLOSED (standard) or split into Child-A/Child-B (split)
 * - Workitem          → APPROVED
 * - Lock              → released (via approveReconciliation)
 * - Audit log         → written
 *
 * @param {object}      params
 * @param {number}      params.stgId                  - Staging ID of the bank transaction.
 * @param {number[]}    [params.selectedPortfolioIds]  - Staging IDs of the invoices to close.
 * @param {object}      [params.adjustments]           - Analyst adjustments (same shape as calculate).
 * @param {boolean}     [params.isOverride]            - True when approving outside normal match rules.
 * @param {string}      [params.overrideReason]        - Required justification when isOverride is true (min 20 chars).
 * @param {boolean}     [params.isSplitPayment]        - True when applying only part of the bank amount.
 * @param {object|null} [params.splitData]             - Split details ({ parentStgId, appliedAmount, residualAmount, financialGross, financialNet, commission, taxIva, taxIrf }).
 * @param {object}      params.user                   - Authenticated user ({ id, username, role }).
 * @param {string}      params.ipAddress              - Caller IP address for the audit log.
 * @returns {Promise<object>} Confirmation with stgId, bankRef1, approvedBy, approvedAt, balanceStatus, isCents, postingKeyHint.
 * @throws {400} If override reason is missing or too short.
 * @throws {403} If the transaction is not assigned to the calling user.
 * @throws {404} If the transaction is not found in workitems.
 * @throws {409} If the workitem is not in IN_PROGRESS status.
 * @throws {422} If the balance is not exactly zero after all adjustments.
 * @throws {423} If the lock has expired.
 */
export async function approve({
  stgId, selectedPortfolioIds, adjustments,
  isOverride, overrideReason,
  isSplitPayment, splitData,
  user, ipAddress,
}) {
  const workitem = await repo.findWorkitemByStgId(stgId);
  if (!workitem) throw Object.assign(new Error('Transaction not found'), { status: 404 });
  if (workitem.assigned_user_id !== user.id && user.role !== 'admin') {
    throw Object.assign(new Error('Transaction not assigned to you'), { status: 403 });
  }
  if (workitem.work_status !== 'IN_PROGRESS') {
    throw Object.assign(new Error('Transaction must be locked before approving.'), { status: 409 });
  }

  const lock = await lockRepo.getLockStatus(workitem.bank_ref_1);
  if (!lock) throw Object.assign(new Error('Lock expired. Please reopen the transaction.'), { status: 423 });

  if (isOverride && (!overrideReason || overrideReason.trim().length < 20)) {
    throw Object.assign(new Error('Override requires a justification of at least 20 characters'), { status: 400 });
  }

  const calcPayload = isSplitPayment
    ? { stgId, selectedPortfolioIds: [], adjustments, isSplitPayment, splitAppliedAmount: splitData?.appliedAmount }
    : { stgId, selectedPortfolioIds, adjustments };

  const balance = await calculate(calcPayload);
  if (!balance.canApprove) {
    throw Object.assign(
      new Error(`Cannot approve: $${balance.unallocated} unallocated.`),
      { status: 422 }
    );
  }

  // Get bank settlement_id for portfolio enrichment
  const bankSettlementId = await repo.findBankSettlementId(stgId);

  // Build approved portfolio IDs JSON for bank update
  const approvedIds = isSplitPayment ? [] : (selectedPortfolioIds || []);
  const approvedPortfolioIdsJson = JSON.stringify(approvedIds);

  // Determine if unknown customer was identified via override
  let updatedBankCustomer = null;
  if (selectedPortfolioIds?.length > 0) {
    // Always grab the customer from first selected portfolio item
    // This enriches the bank record when customer was unknown
    // and updates it for traceability on manual matches
    const customer = await repo.findPortfolioCustomer(selectedPortfolioIds[0]);
    if (customer) {
      updatedBankCustomer = {
        customerCode: customer.customer_code,
        customerName: customer.customer_name,
      };
    }
  }

  await repo.approveReconciliation({
    stgId,
    bankRef1:              workitem.bank_ref_1,
    selectedPortfolioIds:  selectedPortfolioIds || [],
    adjustments,
    isOverride:            isOverride    || false,
    overrideReason:        overrideReason || null,
    isSplitPayment:        isSplitPayment || false,
    splitData:             splitData      || null,
    approvedBy:            user.username,
    approvedUserId:        user.id,
    updatedBankCustomer,
    bankSettlementId,
    approvedPortfolioIdsJson,
  });

  await auditRepo.writeAuditLog({
    userId:   user.id,
    username: user.username,
    action:   isOverride ? 'MATCH_APPROVED_OVERRIDE' : 'MATCH_APPROVED',
    resource: `transaction/${workitem.bank_ref_1}`,
    detail: {
      stg_id:          stgId,
      portfolio_ids:   selectedPortfolioIds,
      bank_amount:     balance.bankAmount,
      invoices_total:  balance.invoicesTotal,
      adjustments,
      is_split:        isSplitPayment || false,
      is_override:     isOverride     || false,
      override_reason: overrideReason || null,
      settlement_id:   bankSettlementId,
    },
    ipAddress,
  });

  return {
    approved:      true,
    stgId,
    bankRef1:      workitem.bank_ref_1,
    approvedBy:    user.username,
    approvedAt:    new Date().toISOString(),
    balanceStatus: balance.balanceStatus,
    isCents:       balance.isCents,
    postingKeyHint: balance.postingKeyHint,
  };
}

/**
 * Returns dashboard statistics for the calling user's transaction portfolio.
 *
 * Admins receive counts and amounts across all transactions; analysts
 * receive only those assigned to them. Counts are grouped by reconcile status.
 *
 * @param {object} params
 * @param {object} params.user - Authenticated user ({ id, role }).
 * @returns {Promise<object>} Counts and amounts by status: pending, review, approved, and total.
 */
export async function getMyStats({ user }) {
  return repo.findMyStats({
    userId:  user.id,
    isAdmin: user.role === 'admin',
  });
}