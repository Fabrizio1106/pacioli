// src/modules/overview/overview.service.js
import * as auditRepo from '../auth/auth.repository.js';
import * as repo from './overview.repository.js';

/**
 * Returns the overview dashboard data grouped by date with status summary.
 *
 * Applies safe zero defaults when the repository returns no summary rows
 * (e.g. empty dataset or filtered to a user with no transactions). Raw DB
 * rows are mapped to the client DTO shape with all numeric fields parsed.
 *
 * @param {object} filters                  - Filter parameters forwarded to the repository.
 * @param {number} [filters.assignedUserId] - If set, restricts results to a specific analyst.
 * @returns {Promise<object>} Dashboard data with summary counts/amounts by status,
 *   transactions grouped by date, grandTotal, and totalCount.
 */
export async function getOverview(filters) {
  const raw = await repo.getOverviewData(filters); // filters incluye assignedUserId

  const summary = raw.summary || {
    pending_count: 0,  pending_amount: 0,
    review_count:  0,  review_amount:  0,
    matched_count: 0,  matched_amount: 0,
    total_count:   0,  total_amount:   0,
  };

  const groups = (raw.groups || []).map(g => ({
    date:     g.date,
    count:    g.count,
    subtotal: parseFloat(g.subtotal) || 0,
    rows:     (g.rows || []).map(r => ({
      id:                  r.stg_id,
      docType:             r.doc_type,
      bankDate:            r.bank_date,
      sapDescription:      r.sap_description,
      amountTotal:         parseFloat(r.amount_total) || 0,
      transType:           r.trans_type,
      enrichCustomerId:    r.enrich_customer_id,
      enrichCustomerName:  r.enrich_customer_name,
      reconcileStatus:     r.reconcile_status,
      bankRef1:            r.bank_ref_1,
      docNumber:           r.doc_number,
      brand:               r.brand,
      matchConfidence:     parseFloat(r.match_confidence_score) || 0,
      reconcileReason:     r.reconcile_reason,
      assignedUserId:      r.assigned_user_id,
      assignedUserName:    r.assigned_user_name,
      assignedUsername:    r.assigned_username,
      workStatus:          r.work_status,
      detectedScenario:    r.detected_scenario,
      analystNote:         r.analyst_note || null,
    })),
  }));

  const grandTotal = parseFloat(summary.total_amount) || 0;

  return {
    summary: {
      pending: {
        count:  summary.pending_count,
        amount: parseFloat(summary.pending_amount) || 0,
      },
      review: {
        count:  summary.review_count,
        amount: parseFloat(summary.review_amount) || 0,
      },
      matched: {
        count:  summary.matched_count,
        amount: parseFloat(summary.matched_amount) || 0,
      },
      total: {
        count:  summary.total_count,
        amount: grandTotal,
      },
    },
    groups,
    grandTotal,
    totalCount: summary.total_count,
  };
}

/**
 * Creates workitems for algorithm-matched transactions not yet in the export queue.
 *
 * Finds transactions marked as automatically matched by the pipeline that have
 * no corresponding workitem, and creates auto-approved workitems for them so
 * they appear in the Gold export queue. Intended to be run by an admin after
 * the pipeline completes if automatic matches were not captured by the regular sync.
 *
 * @param {object} params
 * @param {object} params.user - Authenticated user ({ username }).
 * @returns {Promise<object>} Result with found count, synced count, and a human-readable message.
 */
export async function syncAutomaticMatches({ user }) {
  const pending  = await repo.findMatchedWithoutWorkitem();
  const created  = await repo.createAutoApprovedWorkitems(pending, user.username);
  return {
    found:   pending.length,
    synced:  created,
    message: created === 0
      ? 'No new automatic matches to sync'
      : `Synced ${created} automatic matches — ready for Submit for Posting`,
  };
}

/**
 * Updates or clears the analyst note on a transaction workitem. Admin and senior analyst only.
 *
 * Passing null or an empty string for note clears the existing note.
 * Notes are trimmed before saving and cannot exceed 500 characters.
 * All updates are recorded in the audit log regardless of whether
 * the note was set or cleared.
 *
 * @param {object}      params
 * @param {string}      params.bankRef1 - Bank reference identifier of the workitem to update.
 * @param {string|null} params.note     - New note text, or null to clear the existing note.
 * @param {object}      params.user     - Authenticated user ({ id, username, role }).
 * @returns {Promise<{ bankRef1: string, note: string|null }>} Updated bank reference and note value.
 * @throws {400} If the note exceeds 500 characters.
 * @throws {403} If the calling user is not an admin or senior analyst.
 */
export async function updateAnalystNote({ bankRef1, note, user }) {
  // Solo admin puede escribir notas — los analistas solo leen
  if (!['admin', 'senior_analyst'].includes(user.role)) {
    throw Object.assign(
      new Error('Only administrators can write analyst notes'),
      { status: 403 }
    );
  }
  if (note && note.trim().length > 500) {
    throw Object.assign(
      new Error('Note cannot exceed 500 characters'),
      { status: 400 }
    );
  }
  const result = await repo.updateAnalystNote({
    bankRef1,
    note: note ? note.trim() : null,
  });

  await auditRepo.writeAuditLog({
    userId:   user.id,
    username: user.username,
    action:   'NOTE_UPDATED',
    resource: `transaction/${bankRef1}`,
    detail:   { note_length: note?.length || 0, cleared: !note },
  });

  return { bankRef1: result.bank_ref_1, note: result.approval_notes };
}