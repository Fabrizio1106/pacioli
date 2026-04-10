// src/modules/locks/locks.repository.js
// Adds releaseAllLocksForUser for zombie lock prevention
import { pool } from '../../config/database.js';

export async function createLock({ bankRef1, lockedById, lockedByName, expiresAt }) {
  const result = await pool.query(`
    INSERT INTO biq_auth.transaction_locks
      (bank_ref_1, locked_by_id, locked_by_name, expires_at)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (bank_ref_1) DO UPDATE
      SET locked_by_id   = EXCLUDED.locked_by_id,
          locked_by_name = EXCLUDED.locked_by_name,
          expires_at     = EXCLUDED.expires_at
      WHERE biq_auth.transaction_locks.expires_at < NOW()
    RETURNING *
  `, [bankRef1, lockedById, lockedByName, expiresAt]);
  return result.rows[0] || null;
}

export async function deleteLock({ bankRef1, userId }) {
  await pool.query(`
    DELETE FROM biq_auth.transaction_locks
    WHERE bank_ref_1 = $1
      AND (locked_by_id = $2 OR expires_at < NOW())
  `, [bankRef1, userId]);
}

// Release ALL locks held by a specific user
// Called before acquiring a new lock to prevent zombie locks
export async function releaseAllLocksForUser(userId) {
  const result = await pool.query(`
    DELETE FROM biq_auth.transaction_locks
    WHERE locked_by_id = $1
    RETURNING bank_ref_1
  `, [userId]);

  // Reset any workitems that were IN_PROGRESS for these locks
  const releasedRefs = result.rows.map(r => r.bank_ref_1);
  if (releasedRefs.length > 0) {
    await pool.query(`
      UPDATE biq_auth.transaction_workitems
      SET work_status = 'ASSIGNED', updated_at = NOW()
      WHERE bank_ref_1 = ANY($1::text[])
        AND work_status = 'IN_PROGRESS'
    `, [releasedRefs]);
  }
  return releasedRefs.length;
}

export async function renewLock({ bankRef1, userId, expiresAt }) {
  const result = await pool.query(`
    UPDATE biq_auth.transaction_locks
    SET expires_at = $3, renewed_at = NOW()
    WHERE bank_ref_1 = $1
      AND locked_by_id = $2
      AND expires_at > NOW()
    RETURNING *
  `, [bankRef1, userId, expiresAt]);
  return result.rows[0] || null;
}

export async function getLockStatus(bankRef1) {
  const result = await pool.query(`
    SELECT * FROM biq_auth.transaction_locks
    WHERE bank_ref_1 = $1 AND expires_at > NOW()
  `, [bankRef1]);
  return result.rows[0] || null;
}

export async function setWorkitemInProgress(bankRef1) {
  await pool.query(`
    UPDATE biq_auth.transaction_workitems
    SET work_status = 'IN_PROGRESS', updated_at = NOW()
    WHERE bank_ref_1 = $1
      AND work_status = 'ASSIGNED'
  `, [bankRef1]);
}

export async function setWorkitemAssigned(bankRef1) {
  await pool.query(`
    UPDATE biq_auth.transaction_workitems
    SET work_status = 'ASSIGNED', updated_at = NOW()
    WHERE bank_ref_1 = $1
      AND work_status = 'IN_PROGRESS'
  `, [bankRef1]);
}