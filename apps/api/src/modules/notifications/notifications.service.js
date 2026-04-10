// src/modules/notifications/notifications.service.js
import * as repo from './notifications.repository.js';

function canApproveReversals(user) {
  return user.role === 'admin';
}

function toReversalDTO(row) {
  return {
    id:              row.id,
    stgId:           row.stg_id,
    bankRef1:        row.bank_ref_1,
    amountTotal:     parseFloat(row.amount_total) || 0,
    transType:       row.trans_type,
    currentStatus:   row.current_status,
    requestedById:   row.requested_by_id,
    requestedByName: row.requested_by_name,
    requestReason:   row.request_reason,
    requestedAt:     row.requested_at,
    reviewedByName:  row.reviewed_by_name,
    reviewReason:    row.review_reason,
    reviewedAt:      row.reviewed_at,
    status:          row.status,
    goldRpaStatus:   row.gold_rpa_status  || null,
    goldBatchId:     row.gold_batch_id    || null,
    alreadyPosted:   row.gold_rpa_status === 'POSTED',
  };
}

/**
 * Returns all pending reversal requests visible to the calling user.
 *
 * Admins see all pending requests across all analysts.
 * Analysts see only requests they submitted themselves.
 *
 * @param {object} params
 * @param {object} params.user - Authenticated user ({ id, role }).
 * @returns {Promise<object[]>} Array of reversal request DTOs including
 *   request status, Gold RPA status, and alreadyPosted flag.
 */
export async function getPendingReversals({ user }) {
  const rows = await repo.findPendingReversals({
    userId:              user.id,
    canApproveReversals: canApproveReversals(user),
  });
  return rows.map(toReversalDTO);
}

/**
 * Returns the count of pending reversal requests for the notification badge.
 *
 * Applies the same visibility scoping as getPendingReversals:
 * admins see all, analysts see only their own.
 *
 * @param {object} params
 * @param {object} params.user - Authenticated user ({ id, role }).
 * @returns {Promise<{ count: number }>} Total number of pending reversal requests.
 */
export async function getNotificationCount({ user }) {
  const count = await repo.countPendingReversals({
    userId:              user.id,
    canApproveReversals: canApproveReversals(user),
  });
  return { count };
}

/**
 * Submits a reversal request for a matched bank transaction.
 *
 * Validates that the transaction is in a reversible status (MATCHED or
 * MATCHED_MANUAL). Silently enriches the request with Gold Layer batch
 * and RPA status if the transaction has already been posted — a missing
 * Gold record is not treated as an error.
 *
 * @param {object} params
 * @param {number} params.stgId  - Staging ID of the bank transaction to reverse.
 * @param {string} params.reason - Analyst-provided reason for the reversal request.
 * @param {object} params.user   - Authenticated user ({ id, username, fullName }).
 * @returns {Promise<object>} Reversal request DTO including Gold batch info if available.
 * @throws {400} If the transaction is not in a reversible status.
 * @throws {404} If the bank transaction is not found.
 */
export async function requestReversal({ stgId, reason, user }) {
  // Simple query — no Gold join needed until Gold is populated
  const tx = await repo.findTransactionForReversal(stgId);
  if (!tx) throw Object.assign(new Error('Transaction not found'), { status: 404 });

  if (!['MATCHED', 'MATCHED_MANUAL'].includes(tx.reconcile_status)) {
    throw Object.assign(
      new Error('Only matched transactions can be reversed'),
      { status: 400 }
    );
  }

  // Check if Gold record exists for this transaction (optional — no crash if not)
  let goldRpaStatus = null;
  let goldBatchId   = null;
  try {
    const goldRow = await repo.findGoldHeaderByBankRef(tx.bank_ref_1);
    if (goldRow) {
      goldRpaStatus = goldRow.rpa_status;
      goldBatchId   = goldRow.batch_id;
    }
  } catch {
    // Gold table may be empty — not an error
  }

  const request = await repo.createReversalRequest({
    stgId,
    bankRef1:        tx.bank_ref_1,
    requestedById:   user.id,
    requestedByName: user.fullName || user.username,
    reason,
    goldRpaStatus,
    goldBatchId,
  });

  return toReversalDTO({
    ...request,
    amount_total:   tx.amount_total,
    trans_type:     tx.trans_type,
    current_status: tx.reconcile_status,
  });
}

/**
 * Approves a pending reversal request. Admin only.
 *
 * Records the reviewing admin's identity and optional justification note.
 * The actual transaction reversal is executed separately by the reversals module.
 *
 * @param {object} params
 * @param {number} params.requestId - ID of the reversal request to approve.
 * @param {string} [params.reason]  - Optional admin note explaining the approval decision.
 * @param {object} params.user      - Authenticated user ({ id, username, fullName, role }).
 * @returns {Promise<object>} Updated reversal request row from the repository.
 * @throws {403} If the calling user is not an admin.
 */
export async function approveReversal({ requestId, reason, user }) {
  if (!canApproveReversals(user)) {
    throw Object.assign(new Error('You are not authorized to approve reversals'), { status: 403 });
  }
  return repo.approveReversal({
    requestId,
    reviewedById:   user.id,
    reviewedByName: user.fullName || user.username,
    reviewReason:   reason || null,
  });
}

/**
 * Rejects a pending reversal request. Admin only.
 *
 * Records the reviewing admin's identity and optional justification note.
 * The transaction remains in its current approved/matched state.
 *
 * @param {object} params
 * @param {number} params.requestId - ID of the reversal request to reject.
 * @param {string} [params.reason]  - Optional admin note explaining the rejection decision.
 * @param {object} params.user      - Authenticated user ({ id, username, fullName, role }).
 * @returns {Promise<object>} Updated reversal request row from the repository.
 * @throws {403} If the calling user is not an admin.
 */
export async function rejectReversal({ requestId, reason, user }) {
  if (!canApproveReversals(user)) {
    throw Object.assign(new Error('You are not authorized to reject reversals'), { status: 403 });
  }
  return repo.rejectReversal({
    requestId,
    reviewedById:   user.id,
    reviewedByName: user.fullName || user.username,
    reviewReason:   reason || null,
  });
}

/**
 * Returns all transactions approved today for the "Processed Today" tab.
 *
 * Admins see all approvals across all analysts; analysts see only their own.
 * Each entry includes reconcile status, approval metadata, and the current
 * state of any associated reversal request.
 *
 * @param {object} params
 * @param {object} params.user - Authenticated user ({ id, role }).
 * @returns {Promise<object[]>} Array of today's approved transaction DTOs
 *   including hasPendingReversal and reversalStatus fields.
 */
export async function getApprovedToday({ user }) {
  const rows = await repo.findApprovedToday({
    userId:  user.id,
    isAdmin: user.role === 'admin',
  });
  return rows.map(r => ({
    id:                   r.stg_id,
    bankRef1:             r.bank_ref_1 || r.sap_description,
    amountTotal:          parseFloat(r.amount_total) || 0,
    currency:             r.currency,
    transType:            r.trans_type,
    reconcileStatus:      r.reconcile_status,
    approvedBy:           r.approved_by,
    approvedAt:           r.approved_at,
    approvedPortfolioIds: r.approved_portfolio_ids,
    detectedScenario:     r.detected_scenario,
    reversalRequestId:    r.reversal_request_id,
    reversalStatus:       r.reversal_status,
    hasPendingReversal:   !!r.reversal_request_id,
  }));
}