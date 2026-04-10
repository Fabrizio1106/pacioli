// src/modules/portfolio/portfolio.controller.js
import * as service from './portfolio.service.js';

// GET /api/v1/portfolio/for-transaction/:stgId
export async function getForTransaction(req, res) {
  try {
    const stgId = parseInt(req.params.stgId, 10);

    if (isNaN(stgId)) {
      return res.status(400).json({
        status: 'error', message: 'Invalid stgId'
      });
    }

    const result = await service.loadPortfolioForTransaction({
      stgId,
      page:  parseInt(req.query.page,  10) || 1,
      limit: Math.min(parseInt(req.query.limit, 10) || 50, 200),
      search: req.query.search || null,
    });

    return res.status(200).json({ status: 'success', ...result });

  } catch (err) {
    if (err.status === 404) {
      return res.status(404).json({ status: 'error', message: err.message });
    }
    console.error('[portfolio.controller] getForTransaction:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// GET /api/v1/portfolio/search?q=texto&bank_amount=105.66&limit=50
// Búsqueda global de texto libre en toda la cartera activa.
// Disponible para todos los escenarios — el frontend decide cuándo mostrarlo.
export async function searchPortfolio(req, res) {
  try {
    const q = req.query.q?.trim();

    if (!q || q.length < 3) {
      return res.status(400).json({
        status: 'error',
        message: 'Query must be at least 3 characters',
      });
    }

    const result = await service.searchPortfolio({
      query:      q,
      bankAmount: req.query.bank_amount ? parseFloat(req.query.bank_amount) : null,
      limit:      Math.min(parseInt(req.query.limit, 10) || 50, 200),
    });

    return res.status(200).json({ status: 'success', ...result });

  } catch (err) {
    console.error('[portfolio.controller] searchPortfolio:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// POST /api/v1/portfolio/validate-selection
export async function validateSelection(req, res) {
  try {
    const { stg_ids } = req.body;
    const result = await service.validateSelection(stg_ids);
    return res.status(200).json({ status: 'success', data: result });

  } catch (err) {
    if (err.status === 400 || err.status === 409) {
      return res.status(err.status).json({ status: 'error', message: err.message });
    }
    console.error('[portfolio.controller] validateSelection:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}