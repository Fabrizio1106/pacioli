// src/modules/assignments/assignments.service.js
import * as repo        from './assignments.repository.js';
import * as syncService from '../../shared/services/sync.service.js';
import * as auditRepo   from '../auth/auth.repository.js';

/**
 * Syncs workitems and applies assignment rules in a single admin-triggered run.
 *
 * Executes two sequential steps:
 *   1. Sync — reconciles the workitem table with the current pipeline snapshot,
 *      creating new workitems and marking compensated transactions.
 *   2. Assign — applies active rules in priority order (specific rules first,
 *      default rule last) to all unassigned workitems.
 *
 * Both steps and their outcomes are recorded in the audit log.
 *
 * @param {string} appliedBy - Username of the admin triggering the run.
 * @param {string} ipAddress - Caller IP address for the audit log.
 * @returns {Promise<object>} Result with sync summary, assigned count, and rules_applied count.
 */
export async function applyAssignmentRules(appliedBy, ipAddress) {
  // Step 1: Sync workitems with today's snapshot
  const syncResult = await syncService.syncWorkitemsWithSnapshot();

  // Step 2: Load active rules ordered by priority
  const rules = await repo.findActiveRules();

  // Separate default rule from specific rules
  const specificRules = rules.filter(r => !r.is_default);
  const defaultRule   = rules.find(r => r.is_default);

  // Step 3: Apply specific rules first, then default
  const orderedRules = defaultRule
    ? [...specificRules, defaultRule]
    : specificRules;

  const assigned = await repo.applyRulesToWorkitems(orderedRules, appliedBy);

  // Step 4: Audit log
  await auditRepo.writeAuditLog({
    username:  appliedBy,
    action:    'ASSIGNMENT_RULES_APPLIED',
    resource:  'transactions',
    detail:    {
      new_workitems: syncResult.new_workitems,
      compensated:   syncResult.compensated,
      assigned,
    },
    ipAddress,
  });

  return {
    sync:     syncResult,
    assigned,
    rules_applied: orderedRules.length,
  };
}

/**
 * Manually reassigns a transaction to a different analyst. Admin only.
 *
 * Updates the workitem's assigned user and records the reassignment
 * in the audit log with the acting admin's identity.
 *
 * @param {object} params
 * @param {string} params.bankRef1  - Bank reference identifier of the workitem to reassign.
 * @param {number} params.toUserId  - User ID of the analyst to assign the transaction to.
 * @param {object} params.byUser    - Authenticated admin performing the reassignment ({ id, username }).
 * @param {string} params.ipAddress - Caller IP address for the audit log.
 * @returns {Promise<object>} Updated workitem row from the repository.
 * @throws {404} If the transaction is not found in workitems.
 */
export async function reassignTransaction({
  bankRef1, toUserId, byUser, ipAddress
}) {
  const updated = await repo.reassignTransaction({
    bankRef1,
    toUserId,
    byUsername: byUser.username,
  });

  if (!updated) {
    throw Object.assign(
      new Error('Transaction not found in workitems'),
      { status: 404 }
    );
  }

  await auditRepo.writeAuditLog({
    userId:    byUser.id,
    username:  byUser.username,
    action:    'TRANSACTION_REASSIGNED',
    resource:  `transaction/${bankRef1}`,
    detail:    { bank_ref_1: bankRef1, to_user_id: toUserId },
    ipAddress,
  });

  return updated;
}

/**
 * Returns all active assignment rules for the admin rules UI.
 *
 * Rules are returned in priority order as stored in the repository.
 * This is a direct pass-through with no transformation applied.
 *
 * @returns {Promise<object[]>} Array of active assignment rule rows.
 */
export async function getRules() {
  return repo.findActiveRules();
}