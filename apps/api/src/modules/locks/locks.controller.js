// src/modules/locks/locks.controller.js
import * as service from './locks.service.js';

// POST /api/v1/locks/:bankRef1/acquire
export async function acquire(req, res) {
  try {
    const result = await service.acquireLock({
      bankRef1: req.params.bankRef1,
      user:     req.user,
    });
    return res.status(200).json({ status: 'success', data: result });

  } catch (err) {
    if (err.status === 423) {
      return res.status(423).json({
        status:   'error',
        message:  err.message,
        lockedBy: err.lockedBy,
      });
    }
    if (err.status === 404 || err.status === 409) {
      return res.status(err.status).json({
        status: 'error', message: err.message
      });
    }
    console.error('[locks.controller] acquire:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// PATCH /api/v1/locks/:bankRef1/renew
export async function renew(req, res) {
  try {
    const result = await service.renewLock({
      bankRef1: req.params.bankRef1,
      user:     req.user,
    });
    return res.status(200).json({ status: 'success', data: result });

  } catch (err) {
    if (err.status === 404) {
      return res.status(404).json({ status: 'error', message: err.message });
    }
    console.error('[locks.controller] renew:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// DELETE /api/v1/locks/:bankRef1/release
export async function release(req, res) {
  try {
    const result = await service.releaseLock({
      bankRef1: req.params.bankRef1,
      user:     req.user,
    });
    return res.status(200).json({ status: 'success', data: result });

  } catch (err) {
    console.error('[locks.controller] release:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// GET /api/v1/locks/:bankRef1/status
export async function status(req, res) {
  try {
    const result = await service.getLockStatus(req.params.bankRef1);
    return res.status(200).json({ status: 'success', data: result });

  } catch (err) {
    console.error('[locks.controller] status:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}