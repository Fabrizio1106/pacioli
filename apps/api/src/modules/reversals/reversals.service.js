// src/modules/reversals/reversals.service.js
import * as repo      from './reversals.repository.js';
import * as auditRepo from '../auth/auth.repository.js';

// ─────────────────────────────────────────────
// GET DAILY APPROVED — "Processed Today" tab
// ─────────────────────────────────────────────
/**
 * Returns all approved transactions for today for the reversal review tab.
 *
 * Admins see all approvals across all analysts; analysts see only their own.
 * Raw DB rows are mapped to the client DTO shape including parsed portfolio
 * ID array and signed diff amount.
 *
 * @param {object} params
 * @param {object} params.user - Authenticated user ({ id, role }).
 * @returns {Promise<object[]>} Array of today's approved transaction DTOs
 *   including approvedPortfolioIds, diffAmount, and isOverride fields.
 */
export async function getDailyApproved({ user }) {
  const rows = await repo.findDailyApproved({
    userId:  user.id,
    isAdmin: user.role === 'admin',
  });

  return rows.map(row => ({
    stgId:           row.stg_id,
    bankRef1:        row.bank_ref_1,
    bankDate:        row.bank_date,
    docNumber:       row.doc_number,
    amountTotal:     parseFloat(row.amount_total),
    currency:        row.currency,
    bankDescription: row.bank_description,
    transType:       row.trans_type,
    customerCode:    row.enrich_customer_id,
    customerName:    row.enrich_customer_name,
    reconcileStatus: row.reconcile_status,
    workStatus:      row.work_status,
    approvedBy:      row.approved_by,
    approvedAt:      row.approved_at,
    approvedPortfolioIds: row.approved_portfolio_ids
      ? row.approved_portfolio_ids.split(',').map(Number)
      : [],
    approvalNotes:   row.approval_notes,
    diffAccountCode: row.diff_account_code,
    diffAmount:      row.diff_amount ? parseFloat(row.diff_amount) : null,
    isOverride:      row.is_override,
  }));
}

/**
 * Reverses an approved reconciliation, restoring all affected records.
 *
 * Only the analyst who originally approved the transaction or an admin
 * may execute a reversal. On success, atomically:
 * - Bank transaction  → REVIEW (if it had algorithm suggestions) or PENDING
 * - Portfolio items   → PENDING (standard) or parent restored + children closed (split)
 * - Workitem          → REVERSED
 * - Audit log         → written with full snapshot of the original approval
 *
 * @param {object} params
 * @param {number} params.stgId          - Staging ID of the bank transaction to reverse.
 * @param {string} params.reversalReason - Mandatory reason for the reversal (min 5 characters).
 * @param {object} params.user           - Authenticated user ({ id, username, role }).
 * @param {string} params.ipAddress      - Caller IP address for the audit log.
 * @returns {Promise<object>} Reversal confirmation with bankRef1, stgId, portfolioIds,
 *   restoredBankStatus, restoredPortfolioStatus, reversedBy, reversedAt, and reason.
 * @throws {400} If reversalReason is missing or fewer than 5 characters,
 *   or if no portfolio IDs are found on the workitem.
 * @throws {403} If the caller is neither the original approver nor an admin.
 * @throws {404} If the transaction is not found in workitems.
 * @throws {409} If the workitem is not in APPROVED status.
 */
export async function reverseMatch({
  stgId,
  reversalReason,
  user,
  ipAddress,
}) {
  // Step 1: Get workitem
  const workitem = await repo.findWorkitemByStgId(stgId);

  if (!workitem) {
    throw Object.assign(
      new Error('Transaction not found in workitems'),
      { status: 404 }
    );
  }

  // Step 2: Verify it's in APPROVED status
  if (workitem.work_status !== 'APPROVED') {
    throw Object.assign(
      new Error(`Cannot reverse: transaction is in status ${workitem.work_status}`),
      { status: 409 }
    );
  }

  // Step 3: Authorization — only approver or admin can reverse
  const isApprover = workitem.approved_by === user.username;
  const isAdmin    = user.role === 'admin';

  if (!isApprover && !isAdmin) {
    throw Object.assign(
      new Error('Only the analyst who approved or an admin can reverse this transaction'),
      { status: 403 }
    );
  }

  // Step 4: Reversal reason is mandatory
  if (!reversalReason || reversalReason.trim().length < 5) {
    throw Object.assign(
      new Error('A reversal reason of at least 5 characters is required'),
      { status: 400 }
    );
  }

  // Step 5: Parse portfolio IDs from workitem
  const portfolioIds = workitem.approved_portfolio_ids
    ? workitem.approved_portfolio_ids.split(',').map(Number).filter(Boolean)
    : [];

  if (portfolioIds.length === 0) {
    throw Object.assign(
      new Error('No portfolio IDs found to reverse'),
      { status: 400 }
    );
  }

  // Step 6: Execute reversal atomically
  const { originalStatus } = await repo.reverseMatch({
    bankRef1:       workitem.bank_ref_1,
    stgId,
    portfolioIds,
    reversedBy:     user.username,
    reversalReason,
  });

  // Step 7: Audit trail — full snapshot of what was reversed
  await auditRepo.writeAuditLog({
    userId:   user.id,
    username: user.username,
    action:   'MATCH_REVERSED',
    resource: `transaction/${workitem.bank_ref_1}`,
    detail: {
      stg_id:           stgId,
      portfolio_ids:    portfolioIds,
      original_status:  originalStatus,
      reversal_reason:  reversalReason,
      original_approval: {
        approved_by: workitem.approved_by,
        approved_at: workitem.approved_at,
      },
    },
    ipAddress,
  });

  return {
    reversed:        true,
    bankRef1:        workitem.bank_ref_1,
    stgId,
    portfolioIds,
    restoredBankStatus:      originalStatus,
    restoredPortfolioStatus: 'PENDING',
    reversedBy:      user.username,
    reversedAt:      new Date().toISOString(),
    reason:          reversalReason,
  };
}