// src/modules/assignments/assignments.repository.js
import { pool } from '../../config/database.js';

// Get all active rules ordered by priority descending
export async function findActiveRules() {
  const result = await pool.query(`
    SELECT
      r.*,
      u.username    AS assign_to_username,
      u.full_name   AS assign_to_full_name
    FROM biq_auth.assignment_rules r
    JOIN biq_auth.users u ON u.id = r.assign_to_user_id
    WHERE r.is_active = TRUE
    ORDER BY r.priority DESC, r.id ASC
  `);
  return result.rows;
}

// Apply rules: assign user_id to all PENDING_ASSIGNMENT workitems
// Uses priority order — first matching rule wins
export async function applyRulesToWorkitems(rules, appliedBy) {
  const client = await pool.connect();
  let   totalAssigned = 0;

  try {
    await client.query('BEGIN');

    for (const rule of rules) {
      // Build WHERE conditions dynamically
      // We join with stg_bank_transactions to apply rule criteria
      const joinConditions  = [];
      const params          = [rule.assign_to_user_id, appliedBy];
      let   p               = 3;

      if (rule.trans_type) {
        joinConditions.push(`t.trans_type = $${p}`);
        params.push(rule.trans_type);
        p++;
      }

      if (rule.brand) {
        joinConditions.push(`t.brand = $${p}`);
        params.push(rule.brand);
        p++;
      }

      if (rule.enrich_customer_id) {
        joinConditions.push(`t.enrich_customer_id = $${p}`);
        params.push(rule.enrich_customer_id);
        p++;
      }

      // Build the full WHERE clause
      // Always filter for unassigned workitems only
      const ruleWhere = joinConditions.length > 0
        ? `AND ${joinConditions.join(' AND ')}`
        : ''; // default rule: no extra conditions, catches everything

      const query = `
        UPDATE biq_auth.transaction_workitems w
        SET
          assigned_user_id = $1,
          assigned_by      = $2,
          assigned_at      = NOW(),
          work_status      = 'ASSIGNED',
          updated_at       = NOW()
        FROM biq_stg.stg_bank_transactions t
        WHERE w.stg_id = t.stg_id
          AND w.work_status = 'PENDING_ASSIGNMENT'
          ${ruleWhere}
      `;

      const result = await client.query(query, params);
      totalAssigned += result.rowCount;
    }

    await client.query('COMMIT');
    return totalAssigned;

  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// Manual reassignment — admin only
export async function reassignTransaction({ bankRef1, toUserId, byUsername }) {
  const result = await pool.query(`
    UPDATE biq_auth.transaction_workitems
    SET
      assigned_user_id = $1,
      assigned_by      = $2,
      assigned_at      = NOW(),
      work_status      = 'ASSIGNED',
      updated_at       = NOW()
    WHERE bank_ref_1 = $3
    RETURNING *
  `, [toUserId, byUsername, bankRef1]);

  return result.rows[0] || null;
}