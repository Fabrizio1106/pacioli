// src/modules/workspace/workspace.controller.js
import * as service from './workspace.service.js';

// GET /api/v1/workspace/my-queue
export async function getMyQueue(req, res) {
  try {
    const data = await service.getMyQueue({ user: req.user });
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    console.error('[workspace] getMyQueue:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// GET /api/v1/workspace/:stgId/panel
export async function getPanel(req, res) {
  try {
    const stgId = parseInt(req.params.stgId, 10);
    if (isNaN(stgId)) {
      return res.status(400).json({ status: 'error', message: 'Invalid stgId' });
    }
    const data = await service.getPanel({
      stgId,
      user:   req.user,
      search: req.query.search || null,  // ← agregar
      page:   parseInt(req.query.page, 10) || 1,
    });
    return res.status(200).json({ status: 'success', ...data });
  } catch (err) {
    if ([403, 404].includes(err.status)) {
      return res.status(err.status).json({ status: 'error', message: err.message });
    }
    console.error('[workspace] getPanel:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// POST /api/v1/workspace/:stgId/calculate
export async function calculate(req, res) {
  try {
    const stgId = parseInt(req.params.stgId, 10);
    if (isNaN(stgId)) {
      return res.status(400).json({ status: 'error', message: 'Invalid stgId' });
    }
    const {
      selected_portfolio_ids,
      adjustments,
      is_split_payment,
      split_applied_amount,
    } = req.body;

    const result = await service.calculate({
      stgId,
      selectedPortfolioIds: (selected_portfolio_ids || []).map(Number),
      adjustments,
      isSplitPayment:       is_split_payment    || false,
      splitAppliedAmount:   split_applied_amount || null,
    });

    return res.status(200).json({ status: 'success', data: result });
  } catch (err) {
    if (err.status === 404) {
      return res.status(404).json({ status: 'error', message: err.message });
    }
    console.error('[workspace] calculate:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// POST /api/v1/workspace/:stgId/approve
export async function approve(req, res) {
  try {
    const stgId = parseInt(req.params.stgId, 10);
    if (isNaN(stgId)) {
      return res.status(400).json({ status: 'error', message: 'Invalid stgId' });
    }
    const {
      selected_portfolio_ids,
      adjustments,
      is_override,
      override_reason,
      is_split_payment,
      split_data,
    } = req.body;

    const result = await service.approve({
      stgId,
      selectedPortfolioIds: (selected_portfolio_ids || []).map(Number),
      adjustments,
      isOverride:     is_override      || false,
      overrideReason: override_reason  || null,
      isSplitPayment: is_split_payment || false,
      splitData:      split_data       || null,
      user:           req.user,
      ipAddress:      req.ip,
    });

    return res.status(200).json({ status: 'success', data: result });
  } catch (err) {
    if ([400, 403, 404, 409, 422, 423].includes(err.status)) {
      return res.status(err.status).json({ status: 'error', message: err.message });
    }
    console.error('[workspace] approve:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

export async function getMyStats(req, res) {
  try {
    const data = await service.getMyStats({ user: req.user });
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    return res.status(500).json({ status: 'error', message: err.message });
  }
}