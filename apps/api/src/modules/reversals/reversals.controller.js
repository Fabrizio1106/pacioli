// src/modules/reversals/reversals.controller.js
import * as service from './reversals.service.js';

// GET /api/v1/reversals/daily
export async function getDaily(req, res) {
  try {
    const data = await service.getDailyApproved({ user: req.user });
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    console.error('[reversals.controller] getDaily:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// POST /api/v1/reversals/:stgId
export async function reverse(req, res) {
  try {
    const stgId = parseInt(req.params.stgId, 10);

    if (isNaN(stgId)) {
      return res.status(400).json({ status: 'error', message: 'Invalid stgId' });
    }

    const { reversal_reason } = req.body;

    const result = await service.reverseMatch({
      stgId,
      reversalReason: reversal_reason,
      user:           req.user,
      ipAddress:      req.ip,
    });

    return res.status(200).json({ status: 'success', data: result });

  } catch (err) {
    if ([400, 403, 404, 409].includes(err.status)) {
      return res.status(err.status).json({ status: 'error', message: err.message });
    }
    console.error('[reversals.controller] reverse:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}