// src/modules/reconciliation/reconciliation.service.js
import * as repo      from './reconciliation.repository.js';
import * as lockRepo  from '../locks/locks.repository.js';
import * as auditRepo from '../auth/auth.repository.js';
import { GL_ACCOUNTS } from '../../config/gl-accounts.js';

/**
 * Returns the full detail view of an approved bank transaction.
 *
 * Enforces visibility: analysts see only their own approvals,
 * admins see all. No lock is required — this is read-only.
 *
 * @param {object} params
 * @param {number} params.stgId  - Staging ID of the bank transaction.
 * @param {object} params.user   - Authenticated user ({ id, role }).
 * @returns {Promise<object>} Bank data, matched invoices, and pending reversal flag.
 * @throws {403} If an analyst requests a transaction not assigned to them.
 * @throws {404} If no approved transaction exists for the given stgId.
 */
export async function getApprovedDetail({ stgId, user }) {
  return repo.getApprovedDetail({
    stgId,
    userId:  user.id,
    isAdmin: user.role === 'admin',
  });
}

/**
 * Computes the live balance preview for a transaction under review.
 *
 * Applies all analyst adjustments (commission, tax, diff amount) against
 * the bank total to produce the unallocated remainder. The workitem must
 * be IN_PROGRESS and assigned to the calling user before the calculation
 * is delegated to the repository.
 *
 * @param {object}   params
 * @param {number}   params.stgId          - Staging ID of the bank transaction.
 * @param {number[]} params.portfolioIds   - Staging IDs of the selected invoices.
 * @param {object}   [params.adjustments]  - Analyst adjustments.
 * @param {number}   [params.adjustments.commission]      - Commission amount (absolute).
 * @param {number}   [params.adjustments.taxIva]          - IVA withholding (absolute).
 * @param {number}   [params.adjustments.taxIrf]          - IRF withholding (absolute).
 * @param {number}   [params.adjustments.diffAmount]      - Exchange/differential amount (absolute).
 * @param {string}   [params.adjustments.diffAccountCode] - GL account for the differential.
 * @param {object}   params.user           - Authenticated user ({ id }).
 * @returns {Promise<object>} Balance breakdown including unallocated amount and canApprove flag.
 * @throws {403} If the transaction is not assigned to the calling user.
 * @throws {404} If the transaction is not found in workitems.
 * @throws {409} If the workitem is not in IN_PROGRESS status.
 */
export async function calculateBalance({ stgId, portfolioIds, adjustments = {}, user }) {

  const item = await repo.findWorkitemByStgId(stgId);
  if (!item) {
    throw Object.assign(
      new Error('Transaction not found in workitems'),
      { status: 404 }
    );
  }

  if (item.work_status !== 'IN_PROGRESS') {
    throw Object.assign(
      new Error('Transaction must be locked before calculating. Please open the transaction first.'),
      { status: 409 }
    );
  }

  if (item.assigned_user_id !== user.id) {
    throw Object.assign(
      new Error('You are not assigned to this transaction'),
      { status: 403 }
    );
  }

  // Pass full adjustments so calculateBalance includes every line
  // in the unallocated computation
  return repo.calculateBalance({ stgId, portfolioIds, adjustments });
}

/**
 * Executes the full reconciliation approval for a bank transaction.
 *
 * Validates lock ownership, override justification, GL account validity,
 * and a strict zero-unallocated gate before committing. On success:
 * - Bank transaction → MATCHED_MANUAL
 * - Selected invoices → CLOSED
 * - Workitem         → APPROVED
 * - Lock             → released
 * - Audit log        → written
 *
 * The signed diff_amount stored in the workitem encodes posting direction
 * for the Gold Layer: positive = credit (key 50 / haber),
 * negative = debit (key 40 / debe).
 *
 * @param {object}   params
 * @param {number}   params.stgId            - Staging ID of the bank transaction.
 * @param {number[]} params.portfolioIds      - Staging IDs of the invoices to close.
 * @param {string}   [params.approvalNotes]   - Free-text note from the analyst.
 * @param {object}   [params.adjustments]     - Analyst adjustments (same shape as calculateBalance).
 * @param {boolean}  [params.isOverride]      - True when approving outside normal match rules.
 * @param {string}   [params.overrideReason]  - Required justification when isOverride is true (min 10 chars).
 * @param {object}   params.user              - Authenticated user ({ id, username }).
 * @param {string}   params.ipAddress         - Caller IP address for the audit log.
 * @returns {Promise<object>} Confirmation with bankRef1, stgId, portfolioIds, amounts, approvedBy, approvedAt.
 * @throws {400} If override reason is missing or too short, or if GL account is absent/invalid.
 * @throws {403} If the transaction is not assigned to the calling user.
 * @throws {404} If the transaction is not found in workitems.
 * @throws {409} If the workitem is not in IN_PROGRESS status.
 * @throws {422} If the balance is not exactly zero after all adjustments.
 * @throws {423} If the lock has expired.
 */
export async function approveMatch({
  stgId,
  portfolioIds,
  approvalNotes,
  adjustments = {},
  isOverride,
  overrideReason,
  user,
  ipAddress,
}) {
  // Step 1: Get workitem and verify lock ownership
  const workitem = await repo.findWorkitemByStgId(stgId);
  if (!workitem) {
    throw Object.assign(
      new Error('Transaction not found in workitems'),
      { status: 404 }
    );
  }

  if (workitem.work_status !== 'IN_PROGRESS') {
    throw Object.assign(
      new Error('Transaction must be locked (IN_PROGRESS) before approving'),
      { status: 409 }
    );
  }

  if (workitem.assigned_user_id !== user.id) {
    throw Object.assign(
      new Error('You are not assigned to this transaction'),
      { status: 403 }
    );
  }

  // Step 2: Verify lock is still active
  const lock = await lockRepo.getLockStatus(workitem.bank_ref_1);
  if (!lock) {
    throw Object.assign(
      new Error('Lock expired. Please reopen the transaction to continue.'),
      { status: 423 }
    );
  }

  // Step 3: Validate override has justification note
  if (isOverride && (!overrideReason || overrideReason.trim().length < 10)) {
    throw Object.assign(
      new Error('Override requires a justification note of at least 10 characters'),
      { status: 400 }
    );
  }

  // Step 4: Validate diff GL account when diffAmount is set
  const diffAmountAbs = Math.abs(parseFloat(adjustments.diffAmount) || 0);
  const diffAccountCode = adjustments.diffAccountCode || null;

  if (diffAmountAbs > 0) {
    if (!diffAccountCode) {
      throw Object.assign(
        new Error('A GL account is required when there is a differential amount'),
        { status: 400 }
      );
    }
    const validAccounts = Object.values(GL_ACCOUNTS.adjustments).map(a => a.gl_account);
    if (!validAccounts.includes(diffAccountCode)) {
      throw Object.assign(
        new Error(`Invalid GL account code: ${diffAccountCode}`),
        { status: 400 }
      );
    }
  }

  // Step 5: Final balance check with full adjustments — must be exactly BALANCED
  // This is the authoritative gate: unallocated must be 0.00 to proceed.
  const balance = await repo.calculateBalance({ stgId, portfolioIds, adjustments });

  if (!balance.canApprove) {
    const direction = balance.unallocated > 0
      ? `$${balance.absUnallocated.toFixed(2)} overpaid — distribute to credit (posting key 50)`
      : `$${balance.absUnallocated.toFixed(2)} underpaid — distribute to debit (posting key 40)`;
    throw Object.assign(
      new Error(
        `Cannot approve: ${direction}. ` +
        `Open Adjustments and distribute the remaining amount before approving.`
      ),
      { status: 422 }
    );
  }

  // Step 6: Derive signed diff_amount for Gold Layer
  // Gold's resolvePostingKey uses the sign to determine 40 vs 50.
  // Convention: positive = credit (50/haber), negative = debit (40/debe)
  // We derive the sign from the RAW balance (before diffAmount was applied)
  // so Gold knows the original direction even after the entry is balanced.
  let signedDiffAmount = null;
  if (diffAmountAbs > 0) {
    const balanceBeforeDiff = await repo.calculateBalance({
      stgId,
      portfolioIds,
      adjustments: {
        ...adjustments,
        diffAmount: 0,   // compute balance without the diff to get the raw direction
      },
    });
    // If bank overpaid before diff → diff is a credit (positive)
    // If bank underpaid before diff → diff is a debit (negative)
    signedDiffAmount = balanceBeforeDiff.unallocated >= 0
      ? diffAmountAbs
      : -diffAmountAbs;
  }

  // Step 7: Commit the approval atomically
  await repo.approveMatch({
    bankRef1:        workitem.bank_ref_1,
    stgId,
    portfolioIds,
    approvedBy:      user.username,
    approvalNotes,
    diffAccountCode,
    diffAmount:      signedDiffAmount,
    commission:      Math.abs(parseFloat(adjustments.commission) || 0) || null,
    taxIva:          Math.abs(parseFloat(adjustments.taxIva)     || 0) || null,
    taxIrf:          Math.abs(parseFloat(adjustments.taxIrf)     || 0) || null,
    isOverride,
    overrideReason,
  });

  // Step 8: Release lock — transaction is now APPROVED
  await lockRepo.releaseLock({
    bankRef1: workitem.bank_ref_1,
    userId:   user.id,
  });

  // Step 9: Audit trail
  await auditRepo.writeAuditLog({
    userId:    user.id,
    username:  user.username,
    action:    'MATCH_APPROVED',
    resource:  `transaction/${workitem.bank_ref_1}`,
    detail: {
      stg_id:           stgId,
      portfolio_ids:    portfolioIds,
      bank_amount:      balance.bankAmount,
      invoices_total:   balance.invoicesTotal,
      commission:       balance.commission,
      tax_iva:          balance.taxIva,
      tax_irf:          balance.taxIrf,
      diff_amount:      signedDiffAmount,
      diff_account:     diffAccountCode || null,
      is_override:      isOverride      || false,
    },
    ipAddress,
  });

  return {
    approved:       true,
    bankRef1:       workitem.bank_ref_1,
    stgId,
    portfolioIds,
    bankAmount:     balance.bankAmount,
    invoicesTotal:  balance.invoicesTotal,
    unallocated:    balance.unallocated,
    approvedBy:     user.username,
    approvedAt:     new Date().toISOString(),
  };
}