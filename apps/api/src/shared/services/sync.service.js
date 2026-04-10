// src/shared/services/sync.service.js
// Bridge between Python Silver Layer and Node.js operational state
// Called automatically when admin applies assignment rules

import { pool } from '../../config/database.js';

// ─────────────────────────────────────────────
// MAIN SYNC FUNCTION
// Synchronizes transaction_workitems with current snapshot
// Safe to call multiple times — fully idempotent
// ─────────────────────────────────────────────
export async function syncWorkitemsWithSnapshot() {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // STEP 1: Insert new transactions not yet in workitems
    // Only doc_type ZR and SA — the reconcilable ones
    const insertResult = await client.query(`
      INSERT INTO biq_auth.transaction_workitems (bank_ref_1, stg_id, work_status)
      SELECT
        -- Use bank_ref_1 as primary stable key
        -- Fall back to sap_description if bank_ref_1 is NULL
        COALESCE(NULLIF(TRIM(t.bank_ref_1), ''), t.sap_description) AS bank_ref_1,
        t.stg_id,
        'PENDING_ASSIGNMENT'
      FROM biq_stg.stg_bank_transactions t
      WHERE t.doc_type IN ('ZR', 'SA')
        AND t.is_compensated_sap = FALSE
        AND t.is_compensated_intraday = FALSE
        AND t.reconcile_status IN ('PENDING', 'REVIEW')
        -- Only insert if not already tracked
        AND NOT EXISTS (
          SELECT 1
          FROM biq_auth.transaction_workitems w
          WHERE w.bank_ref_1 =
            COALESCE(NULLIF(TRIM(t.bank_ref_1), ''), t.sap_description)
        )
      ON CONFLICT (bank_ref_1) DO NOTHING
    `);

    const newCount = insertResult.rowCount;

    // STEP 2: Update stg_id for existing workitems
    // The snapshot may have renumbered stg_id — bank_ref_1 stays stable
    await client.query(`
      UPDATE biq_auth.transaction_workitems w
      SET
        stg_id     = t.stg_id,
        updated_at = NOW()
      FROM biq_stg.stg_bank_transactions t
      WHERE w.bank_ref_1 =
          COALESCE(NULLIF(TRIM(t.bank_ref_1), ''), t.sap_description)
        AND w.stg_id != t.stg_id
        AND t.doc_type IN ('ZR', 'SA')
    `);

    // STEP 3: Mark as COMPENSATED when pipeline closed them
    // These exit the active workspace view
    const compensatedResult = await client.query(`
      UPDATE biq_auth.transaction_workitems w
      SET
        work_status = 'COMPENSATED',
        updated_at  = NOW()
      FROM biq_stg.stg_bank_transactions t
      WHERE w.bank_ref_1 =
          COALESCE(NULLIF(TRIM(t.bank_ref_1), ''), t.sap_description)
        AND w.work_status NOT IN ('COMPENSATED', 'APPROVED')
        AND (t.is_compensated_sap = TRUE OR t.is_compensated_intraday = TRUE)
    `);

    const compensatedCount = compensatedResult.rowCount;

    await client.query('COMMIT');

    return {
      new_workitems:   newCount,
      compensated:     compensatedCount,
      synced_at:       new Date().toISOString(),
    };

  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// ─────────────────────────────────────────────
// GET SYNC STATUS
// Summary of current workitem states
// ─────────────────────────────────────────────
export async function getSyncStatus() {
  const result = await pool.query(`
    SELECT
      work_status,
      COUNT(*)::integer AS count
    FROM biq_auth.transaction_workitems
    GROUP BY work_status
    ORDER BY count DESC
  `);

  return result.rows;
}