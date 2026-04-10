// src/modules/locks/locks.service.js
import * as repo from './locks.repository.js';

const LOCK_DURATION_MINUTES = 5;

/**
 * Acquires an exclusive 5-minute lock on a bank transaction.
 *
 * Accepts two calling conventions to support both the frontend heartbeat hook
 * (passes userId/userName directly) and the controller (passes a user object):
 *   - acquireLock({ bankRef1, userId, userName })
 *   - acquireLock({ bankRef1, user })
 *
 * Before acquiring, any existing lock held by the same user is automatically
 * released to prevent self-lock conflicts when switching between transactions.
 * On success the workitem is marked IN_PROGRESS.
 *
 * @param {object}      params
 * @param {string}      params.bankRef1   - Bank reference identifier of the transaction to lock.
 * @param {number}      [params.userId]   - User ID (direct calling convention).
 * @param {string}      [params.userName] - Display name (direct calling convention).
 * @param {object}      [params.user]     - Authenticated user ({ id, fullName, username }) (controller calling convention).
 * @returns {Promise<{ locked: true, bankRef1: string, expiresAt: Date }>} Lock confirmation and expiry time.
 * @throws {423} If the transaction is currently locked by a different user.
 */
export async function acquireLock({ bankRef1, userId, userName, user }) {
  // Accept both calling conventions:
  // From frontend hook: acquireLock({ bankRef1, userId, userName })
  // From controller:    acquireLock({ bankRef1, user })
  const resolvedUserId   = userId   || user?.id;
  const resolvedUserName = userName || user?.fullName || user?.username;

  // Auto-release any existing lock held by THIS user
  // Prevents "locked by another analyst" when same user switches transactions
  await repo.releaseAllLocksForUser(resolvedUserId);

  const expiresAt = new Date(Date.now() + LOCK_DURATION_MINUTES * 60 * 1000);

  const lock = await repo.createLock({
    bankRef1,
    lockedById:   resolvedUserId,
    lockedByName: resolvedUserName,
    expiresAt,
  });

  if (!lock) {
    // Someone else locked it between our release and acquire
    const existing = await repo.getLockStatus(bankRef1);
    if (existing && existing.locked_by_id !== resolvedUserId) {
      throw Object.assign(
        new Error(`Transaction is locked by ${existing.locked_by_name}`),
        { status: 423, lockedBy: existing.locked_by_name }
      );
    }
  }

  // Mark workitem as IN_PROGRESS
  await repo.setWorkitemInProgress(bankRef1);

  return { locked: true, bankRef1, expiresAt };
}

/**
 * Releases the lock on a bank transaction and resets the workitem to ASSIGNED.
 *
 * Accepts the same dual calling convention as acquireLock.
 * Safe to call even if the lock has already expired — the delete is a no-op
 * in that case and the workitem status is still corrected.
 *
 * @param {object} params
 * @param {string} params.bankRef1  - Bank reference identifier of the transaction to unlock.
 * @param {number} [params.userId]  - User ID (direct calling convention).
 * @param {object} [params.user]    - Authenticated user ({ id }) (controller calling convention).
 * @returns {Promise<{ released: true }>} Release confirmation.
 */
export async function releaseLock({ bankRef1, userId, user }) {
  const resolvedUserId = userId || user?.id;
  await repo.deleteLock({ bankRef1, userId: resolvedUserId });
  await repo.setWorkitemAssigned(bankRef1);
  return { released: true };
}

/**
 * Extends an active lock by 5 minutes from the current time.
 *
 * Called by the frontend heartbeat to keep the lock alive while an analyst
 * is actively working on a transaction. Accepts the same dual calling
 * convention as acquireLock.
 *
 * @param {object} params
 * @param {string} params.bankRef1  - Bank reference identifier of the transaction whose lock to renew.
 * @param {number} [params.userId]  - User ID (direct calling convention).
 * @param {object} [params.user]    - Authenticated user ({ id }) (controller calling convention).
 * @returns {Promise<{ renewed: true, expiresAt: Date }>} Renewal confirmation and new expiry time.
 * @throws {404} If no active lock exists for the given bankRef1 and user.
 */
export async function renewLock({ bankRef1, userId, user }) {
  const resolvedUserId = userId || user?.id;
  const expiresAt = new Date(Date.now() + LOCK_DURATION_MINUTES * 60 * 1000);
  const renewed   = await repo.renewLock({ bankRef1, userId: resolvedUserId, expiresAt });
  if (!renewed) {
    throw Object.assign(new Error('Lock not found or expired'), { status: 404 });
  }
  return { renewed: true, expiresAt };
}

/**
 * Returns the current lock state for a bank transaction.
 *
 * A direct pass-through to the repository. Returns null if no active
 * lock exists, so callers can use a simple truthiness check.
 *
 * @param {string} bankRef1 - Bank reference identifier of the transaction to check.
 * @returns {Promise<object|null>} Lock record ({ locked_by_id, locked_by_name, expires_at })
 *   if an active lock exists, or null if the transaction is unlocked.
 */
export async function getLockStatus(bankRef1) {
  return repo.getLockStatus(bankRef1);
}