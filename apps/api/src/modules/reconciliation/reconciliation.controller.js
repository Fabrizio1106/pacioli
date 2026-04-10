// src/modules/reconciliation/reconciliation.controller.js
import * as service from './reconciliation.service.js';

// GET /api/v1/reconciliation/:stgId/approved-detail
// Read-only detail for the DONE tab panel.
// Returns full bank data + enriched invoice list.
// No lock required — analyst can only see their own approvals.
export async function getApprovedDetail(req, res) {
  try {
    const stgId = parseInt(req.params.stgId, 10);

    if (isNaN(stgId)) {
      return res.status(400).json({ status: 'error', message: 'Invalid stgId' });
    }

    const result = await service.getApprovedDetail({
      stgId,
      user: req.user,
    });

    return res.status(200).json({ status: 'success', data: result });

  } catch (err) {
    if ([400, 403, 404].includes(err.status)) {
      return res.status(err.status).json({ status: 'error', message: err.message });
    }
    console.error('[reconciliation.controller] getApprovedDetail:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// POST /api/v1/reconciliation/:stgId/calculate
// Preview the balance before approving.
// Receives full adjustments so the BalanceBar reflects
// exactly what will be posted (including diffAmount).
// Acepta selected_portfolio_ids (legacy) o portfolio_ids (nuevo)
export async function calculate(req, res) {
  try {
    const stgId = parseInt(req.params.stgId, 10);
    const {
      portfolio_ids,
      selected_portfolio_ids,
      adjustments = {},
    } = req.body;

    if (isNaN(stgId)) {
      return res.status(400).json({ status: 'error', message: 'Invalid stgId' });
    }

    // Acepta ambas keys para compatibilidad con el hook useCalculate existente
    const ids = portfolio_ids || selected_portfolio_ids;

    if (!Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({
        status: 'error', message: 'portfolio_ids array is required'
      });
    }

    const result = await service.calculateBalance({
      stgId,
      portfolioIds: ids.map(Number),
      adjustments: {
        // Acepta tanto camelCase (frontend) como snake_case — ambos formatos
        commission:      parseFloat(adjustments.commission)                       || 0,
        taxIva:          parseFloat(adjustments.taxIva   || adjustments.tax_iva)  || 0,
        taxIrf:          parseFloat(adjustments.taxIrf   || adjustments.tax_irf)  || 0,
        diffAmount:      parseFloat(adjustments.diffAmount || adjustments.diff_amount) || 0,
        diffAccountCode: adjustments.diffAccountCode || adjustments.diff_account_code || null,
      },
      user: req.user,
    });

    return res.status(200).json({ status: 'success', data: result });

  } catch (err) {
    if ([400, 403, 404, 409, 422, 423].includes(err.status)) {
      return res.status(err.status).json({ status: 'error', message: err.message });
    }
    console.error('[reconciliation.controller] calculate:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// POST /api/v1/reconciliation/:stgId/approve
// Commit the approval — closes invoices and updates all states.
// adjustments must produce unallocated === 0 or the service rejects it.
// Acepta selected_portfolio_ids (legacy) o portfolio_ids (nuevo)
export async function approve(req, res) {
  try {
    const stgId = parseInt(req.params.stgId, 10);
    const {
      portfolio_ids,
      selected_portfolio_ids,
      approval_notes,
      adjustments = {},
      is_override,
      override_reason,
    } = req.body;

    if (isNaN(stgId)) {
      return res.status(400).json({ status: 'error', message: 'Invalid stgId' });
    }

    const ids = portfolio_ids || selected_portfolio_ids;

    if (!Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({
        status: 'error', message: 'portfolio_ids array is required'
      });
    }

    const result = await service.approveMatch({
      stgId,
      portfolioIds:  ids.map(Number),
      approvalNotes: approval_notes || null,
      adjustments: {
        commission:      parseFloat(adjustments.commission)                            || 0,
        taxIva:          parseFloat(adjustments.taxIva      || adjustments.tax_iva)    || 0,
        taxIrf:          parseFloat(adjustments.taxIrf      || adjustments.tax_irf)    || 0,
        diffAmount:      parseFloat(adjustments.diffAmount  || adjustments.diff_amount) || 0,
        diffAccountCode: adjustments.diffAccountCode || adjustments.diff_account_code  || null,
      },
      isOverride:     is_override      || false,
      overrideReason: override_reason  || null,
      user:           req.user,
      ipAddress:      req.ip,
    });

    return res.status(200).json({ status: 'success', data: result });

  } catch (err) {
    if ([400, 403, 404, 409, 422, 423].includes(err.status)) {
      return res.status(err.status).json({ status: 'error', message: err.message });
    }
    console.error('[reconciliation.controller] approve:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}