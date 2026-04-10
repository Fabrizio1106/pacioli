// src/modules/transactions/transactions.controller.js
import * as service from './transactions.service.js';

// GET /api/v1/transactions
export async function list(req, res) {
  try {
    const filters = {
      status:     req.query.status,
      dateFrom:   req.query.date_from,
      dateTo:     req.query.date_to,
      customerId: req.query.customer_id,
      assignedTo: req.query.assigned_to,
      workStatus: req.query.work_status,
      page:       parseInt(req.query.page,  10) || 1,
      limit:      Math.min(parseInt(req.query.limit, 10) || 20, 100),
    };

    const result = await service.listTransactions(filters);

    return res.status(200).json({
      status: 'success',
      ...result, // spread: incluye data[] y pagination{}
    });

  } catch (err) {
    console.error('[transactions.controller] list:', err);
    return res.status(500).json({
      status:  'error',
      message: 'Error al obtener transacciones',
    });
  }
}

// GET /api/v1/transactions/summary
export async function summary(req, res) {
  try {
    const data = await service.getStatusSummary();
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    console.error('[transactions.controller] summary:', err);
    return res.status(500).json({ status: 'error', message: 'Error al obtener resumen' });
  }
}

// GET /api/v1/transactions/:id
export async function detail(req, res) {
  try {
    const stgId = parseInt(req.params.id, 10);

    if (isNaN(stgId)) {
      return res.status(400).json({
        status:  'error',
        message: 'ID de transacción inválido',
      });
    }

    const data = await service.getTransactionDetail(stgId);
    return res.status(200).json({ status: 'success', data });

  } catch (err) {
    if (err.status === 404) {
      return res.status(404).json({ status: 'error', message: err.message });
    }
    console.error('[transactions.controller] detail:', err);
    return res.status(500).json({ status: 'error', message: 'Error al obtener transacción' });
  }
}