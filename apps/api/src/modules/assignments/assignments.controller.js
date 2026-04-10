// src/modules/assignments/assignments.controller.js
import * as service from './assignments.service.js';

// POST /api/v1/admin/assignments/apply-rules
export async function applyRules(req, res) {
  try {
    const result = await service.applyAssignmentRules(
      req.user.username,
      req.ip
    );
    return res.status(200).json({ status: 'success', data: result });
  } catch (err) {
    console.error('[assignments.controller] applyRules:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// PATCH /api/v1/admin/assignments/:bankRef1/reassign
export async function reassign(req, res) {
  try {
    const { to_user_id } = req.body;
    if (!to_user_id) {
      return res.status(400).json({
        status: 'error', message: 'to_user_id is required'
      });
    }

    const result = await service.reassignTransaction({
      bankRef1:  req.params.bankRef1,
      toUserId:  to_user_id,
      byUser:    req.user,
      ipAddress: req.ip,
    });

    return res.status(200).json({ status: 'success', data: result });
  } catch (err) {
    if (err.status === 404) {
      return res.status(404).json({ status: 'error', message: err.message });
    }
    console.error('[assignments.controller] reassign:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// GET /api/v1/admin/assignments/rules
export async function getRules(req, res) {
  try {
    const data = await service.getRules();
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    console.error('[assignments.controller] getRules:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}