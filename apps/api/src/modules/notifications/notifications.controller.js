// src/modules/notifications/notifications.controller.js
import * as service from './notifications.service.js';

export async function getPendingReversals(req, res) {
  try {
    const data = await service.getPendingReversals({ user: req.user });
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

export async function getCount(req, res) {
  try {
    const data = await service.getNotificationCount({ user: req.user });
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

export async function requestReversal(req, res) {
  try {
    const stgId  = parseInt(req.params.stgId, 10);
    const { reason } = req.body;
    if (!reason || reason.trim().length < 10) {
      return res.status(400).json({ status: 'error', message: 'Reason must be at least 10 characters' });
    }
    const data = await service.requestReversal({ stgId, reason, user: req.user });
    return res.status(201).json({ status: 'success', data });
  } catch (err) {
    if ([400, 404, 409].includes(err.status)) {
      return res.status(err.status).json({ status: 'error', message: err.message });
    }
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

export async function approveReversal(req, res) {
  try {
    const requestId = parseInt(req.params.requestId, 10);
    const { reason } = req.body;
    const data = await service.approveReversal({ requestId, reason, user: req.user });
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    if ([400, 403, 404, 422].includes(err.status)) {
      return res.status(err.status).json({ status: 'error', message: err.message });
    }
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

export async function rejectReversal(req, res) {
  try {
    const requestId = parseInt(req.params.requestId, 10);
    const { reason } = req.body;
    if (!reason || reason.trim().length < 5) {
      return res.status(400).json({ status: 'error', message: 'Rejection reason is required' });
    }
    const data = await service.rejectReversal({ requestId, reason, user: req.user });
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    if ([400, 403, 404].includes(err.status)) {
      return res.status(err.status).json({ status: 'error', message: err.message });
    }
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

export async function getApprovedToday(req, res) {
  try {
    const data = await service.getApprovedToday({ user: req.user });
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    return res.status(500).json({ status: 'error', message: err.message });
  }
}