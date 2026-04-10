// src/modules/reports/reports.controller.js
import * as service from './reports.service.js';
import * as excel   from './reports.excel.js';

// ─────────────────────────────────────────────────────────────────────────────
// HELPER — Parsear filtros comunes del query string
// ─────────────────────────────────────────────────────────────────────────────
function parseFilters(query) {
  const {
    startDate, endDate,
    status, brand,
    preview,
  } = query;

  return {
    startDate: startDate || new Date(new Date().getFullYear(), new Date().getMonth(), 1)
                              .toISOString().split('T')[0],
    endDate:   endDate   || new Date().toISOString().split('T')[0],
    status:    status    ? (Array.isArray(status) ? status : status.split(',')) : [],
    brand:     brand     ? (Array.isArray(brand)  ? brand  : brand.split(','))  : [],
    preview:   preview === 'true' || preview === true,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPER — Enviar Excel como descarga
// ─────────────────────────────────────────────────────────────────────────────
async function sendExcel(res, wb, filename) {
  res.setHeader('Content-Type',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.setHeader('Cache-Control', 'no-cache');
  await wb.xlsx.write(res);
  res.end();
}

// ─────────────────────────────────────────────────────────────────────────────
// R1 — OVERVIEW
// ─────────────────────────────────────────────────────────────────────────────
export async function getOverview(req, res) {
  try {
    const f    = parseFilters(req.query);
    const data = await service.getOverviewReport(f);
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    console.error('[reports] getOverview:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

export async function exportOverview(req, res) {
  try {
    const f    = parseFilters({ ...req.query, preview: false });
    const data = await service.getOverviewReport(f);
    const wb   = await excel.buildExcel('bank', 'Overview Report',
                   f.startDate, f.endDate, data.detail);
    await sendExcel(res, wb, excel.buildFileName('overview', f.startDate, f.endDate));
  } catch (err) {
    console.error('[reports] exportOverview:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// R2 — BANK RECONCILIATION
// ─────────────────────────────────────────────────────────────────────────────
export async function getBank(req, res) {
  try {
    const f    = parseFilters(req.query);
    const rows = await service.getBankReport(f);
    const total = await service.getRowCount('bank', f);
    return res.status(200).json({
      status: 'success',
      data:   rows,
      meta:   { showing: rows.length, total, preview: f.preview },
    });
  } catch (err) {
    console.error('[reports] getBank:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

export async function exportBank(req, res) {
  try {
    const f    = parseFilters({ ...req.query, preview: false });
    const rows = await service.getBankReport(f);
    const wb   = await excel.buildExcel('bank', 'Bank Reconciliation',
                   f.startDate, f.endDate, rows);
    await sendExcel(res, wb, excel.buildFileName('bank', f.startDate, f.endDate));
  } catch (err) {
    console.error('[reports] exportBank:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// R3 — PORTFOLIO
// ─────────────────────────────────────────────────────────────────────────────
export async function getPortfolio(req, res) {
  try {
    const f    = parseFilters(req.query);
    const rows = await service.getPortfolioReport(f);
    const total = await service.getRowCount('portfolio', f);
    return res.status(200).json({
      status: 'success',
      data:   rows,
      meta:   { showing: rows.length, total, preview: f.preview },
    });
  } catch (err) {
    console.error('[reports] getPortfolio:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

export async function exportPortfolio(req, res) {
  try {
    const f    = parseFilters({ ...req.query, preview: false });
    const rows = await service.getPortfolioReport(f);
    const wb   = await excel.buildExcel('portfolio', 'Portfolio Report',
                   f.startDate, f.endDate, rows);
    await sendExcel(res, wb, excel.buildFileName('portfolio', f.startDate, f.endDate));
  } catch (err) {
    console.error('[reports] exportPortfolio:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// R4 — CARD DETAILS
// ─────────────────────────────────────────────────────────────────────────────
export async function getCardDetails(req, res) {
  try {
    const f    = parseFilters(req.query);
    const rows = await service.getCardDetailsReport(f);
    const total = await service.getRowCount('card-details', f);
    return res.status(200).json({
      status: 'success',
      data:   rows,
      meta:   { showing: rows.length, total, preview: f.preview },
    });
  } catch (err) {
    console.error('[reports] getCardDetails:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

export async function exportCardDetails(req, res) {
  try {
    const f    = parseFilters({ ...req.query, preview: false });
    const rows = await service.getCardDetailsReport(f);
    const wb   = await excel.buildExcel('card-details', 'Card Details',
                   f.startDate, f.endDate, rows);
    await sendExcel(res, wb, excel.buildFileName('card-details', f.startDate, f.endDate));
  } catch (err) {
    console.error('[reports] exportCardDetails:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// R5 — CARD SETTLEMENTS
// ─────────────────────────────────────────────────────────────────────────────
export async function getCardSettlements(req, res) {
  try {
    const f    = parseFilters(req.query);
    const rows = await service.getCardSettlementsReport(f);
    const total = await service.getRowCount('card-settlements', f);
    return res.status(200).json({
      status: 'success',
      data:   rows,
      meta:   { showing: rows.length, total, preview: f.preview },
    });
  } catch (err) {
    console.error('[reports] getCardSettlements:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

export async function exportCardSettlements(req, res) {
  try {
    const f    = parseFilters({ ...req.query, preview: false });
    const rows = await service.getCardSettlementsReport(f);
    const wb   = await excel.buildExcel('card-settlements', 'Card Settlements',
                   f.startDate, f.endDate, rows);
    await sendExcel(res, wb, excel.buildFileName('card-settlements', f.startDate, f.endDate));
  } catch (err) {
    console.error('[reports] exportCardSettlements:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// R6 — PARKING BREAKDOWN
// ─────────────────────────────────────────────────────────────────────────────
export async function getParking(req, res) {
  try {
    const f    = parseFilters(req.query);
    const rows = await service.getParkingReport(f);
    const total = await service.getRowCount('parking', f);
    return res.status(200).json({
      status: 'success',
      data:   rows,
      meta:   { showing: rows.length, total, preview: f.preview },
    });
  } catch (err) {
    console.error('[reports] getParking:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

export async function exportParking(req, res) {
  try {
    const f    = parseFilters({ ...req.query, preview: false });
    const rows = await service.getParkingReport(f);
    const wb   = await excel.buildExcel('parking', 'Parking Breakdown',
                   f.startDate, f.endDate, rows);
    await sendExcel(res, wb, excel.buildFileName('parking', f.startDate, f.endDate));
  } catch (err) {
    console.error('[reports] exportParking:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// R7 — SUMMARY
// ─────────────────────────────────────────────────────────────────────────────
export async function getSummary(req, res) {
  try {
    const f    = parseFilters(req.query);
    const data = await service.getSummaryReport(f);
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    console.error('[reports] getSummary:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

export async function exportSummary(req, res) {
  try {
    const f    = parseFilters(req.query);
    const data = await service.getSummaryReport(f);
    const wb   = await excel.buildSummaryExcel(f.startDate, f.endDate, data);
    await sendExcel(res, wb, excel.buildFileName('summary', f.startDate, f.endDate));
  } catch (err) {
    console.error('[reports] exportSummary:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}