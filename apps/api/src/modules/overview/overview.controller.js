// src/modules/overview/overview.controller.js
import * as service from './overview.service.js';

export async function getOverview(req, res) {
  try {
    const filters = {
      status:         req.query.status          || 'ALL',
      customer:       req.query.customer        || null,
      dateFrom:       req.query.date_from       || null,
      dateTo:         req.query.date_to         || null,
      assignedUserId: req.query.assigned_user_id || null,
    };

    const data = await service.getOverview(filters);
    return res.status(200).json({ status: 'success', data });

  } catch (err) {
    console.error('[overview.controller]', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

export async function syncMatches(req, res) {
  try {
    const data = await service.syncAutomaticMatches({ user: req.user });
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    console.error('[overview] syncMatches:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// PATCH /api/v1/overview/:bankRef1/note
export async function updateNote(req, res) {
  try {
    const bankRef1 = decodeURIComponent(req.params.bankRef1);
    const { note } = req.body;

    if (note !== undefined && note !== null && typeof note !== 'string') {
      return res.status(400).json({ status: 'error', message: 'note must be a string or null' });
    }

    const data = await service.updateAnalystNote({
      bankRef1,
      note: note || null,
      user: req.user,
    });

    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    if ([400, 403, 404].includes(err.status)) {
      return res.status(err.status).json({ status: 'error', message: err.message });
    }
    console.error('[overview] updateNote:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}