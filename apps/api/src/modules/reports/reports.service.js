// src/modules/reports/reports.service.js
import * as repo from './reports.repository.js';

// ─────────────────────────────────────────────────────────────────────────────
// R1 — OVERVIEW SUMMARY
// Replica los KPIs y la lista del Overview
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Returns KPI aggregates and transaction detail rows for the Overview report.
 * Runs both queries in parallel via Promise.all.
 * @param {object} params
 * @param {string} params.startDate - ISO date string, inclusive range start.
 * @param {string} params.endDate   - ISO date string, inclusive range end.
 * @param {boolean} params.preview  - When true, detail rows are capped at 200.
 * @returns {Promise<object>} `{ kpis, detail }` — kpis is a single aggregate
 *   row; detail is an array of bank transaction rows ordered by date.
 */
export async function getOverviewReport({ startDate, endDate, preview }) {
  const [kpis, detail] = await Promise.all([
    repo.findOverviewKpis(startDate, endDate),
    repo.findOverviewDetail(startDate, endDate, preview),
  ]);
  return { kpis, detail };
}

// ─────────────────────────────────────────────────────────────────────────────
// R2 — BANK RECONCILIATION
// Partidas abiertas del banco (no compensadas)
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Returns bank transaction rows for the Bank Reconciliation report.
 * Excludes compensated (SAP and intraday) transactions. Optionally filters
 * by one or more reconcile_status values.
 * @param {object}   params
 * @param {string}   params.startDate  - ISO date string, inclusive range start.
 * @param {string}   params.endDate    - ISO date string, inclusive range end.
 * @param {string[]} [params.status]   - Reconcile status filter (e.g. ['PENDING','REVIEW']).
 *   When omitted all statuses are returned.
 * @param {boolean}  params.preview    - When true, result is capped at 200 rows.
 * @returns {Promise<object[]>} Array of bank transaction rows.
 */
export async function getBankReport({ startDate, endDate, status, preview }) {
  return repo.findBankReport(startDate, endDate, status, preview);
}

// ─────────────────────────────────────────────────────────────────────────────
// R3 — PORTFOLIO (Cartera Conciliada)
// Excluye CLOSED por defecto — la tabla es histórica y CLOSED sería enorme
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Returns portfolio rows for the Portfolio report.
 * Excludes CLOSED items by default; passing a status filter overrides the default.
 * @param {object}   params
 * @param {string}   params.startDate  - ISO date string, inclusive range start.
 * @param {string}   params.endDate    - ISO date string, inclusive range end.
 * @param {string[]} [params.status]   - Reconcile status filter. Omit to use
 *   the default exclusion of CLOSED items.
 * @param {boolean}  params.preview    - When true, result is capped at 200 rows.
 * @returns {Promise<object[]>} Array of portfolio rows.
 */
export async function getPortfolioReport({ startDate, endDate, status, preview }) {
  return repo.findPortfolioReport(startDate, endDate, status, preview);
}

// ─────────────────────────────────────────────────────────────────────────────
// R4 — CARD DETAILS (Vouchers individuales)
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Returns individual voucher rows for the Card Details report.
 * Both brand and status filters are optional and combinable.
 * @param {object}   params
 * @param {string}   params.startDate  - ISO date string, inclusive range start (voucher_date).
 * @param {string}   params.endDate    - ISO date string, inclusive range end.
 * @param {string[]} [params.brand]    - Card brand filter (e.g. ['VISA','MASTERCARD']).
 * @param {string[]} [params.status]   - Reconcile status filter.
 * @param {boolean}  params.preview    - When true, result is capped at 200 rows.
 * @returns {Promise<object[]>} Array of card detail voucher rows.
 */
export async function getCardDetailsReport({ startDate, endDate, brand, status, preview }) {
  return repo.findCardDetailsReport(startDate, endDate, brand, status, preview);
}

// ─────────────────────────────────────────────────────────────────────────────
// R5 — CARD SETTLEMENTS (Liquidaciones agrupadas)
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Returns grouped settlement rows for the Card Settlements report.
 * Both brand and status filters are optional and combinable.
 * @param {object}   params
 * @param {string}   params.startDate  - ISO date string, inclusive range start (settlement_date).
 * @param {string}   params.endDate    - ISO date string, inclusive range end.
 * @param {string[]} [params.brand]    - Card brand filter.
 * @param {string[]} [params.status]   - Reconcile status filter.
 * @param {boolean}  params.preview    - When true, result is capped at 200 rows.
 * @returns {Promise<object[]>} Array of card settlement rows.
 */
export async function getCardSettlementsReport({ startDate, endDate, brand, status, preview }) {
  return repo.findCardSettlementsReport(startDate, endDate, brand, status, preview);
}

// ─────────────────────────────────────────────────────────────────────────────
// R6 — PARKING BREAKDOWN
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Returns parking pay breakdown rows for the Parking report.
 * Optionally filtered by brand.
 * @param {object}   params
 * @param {string}   params.startDate  - ISO date string, inclusive range start (settlement_date).
 * @param {string}   params.endDate    - ISO date string, inclusive range end.
 * @param {string[]} [params.brand]    - Card brand filter.
 * @param {boolean}  params.preview    - When true, result is capped at 200 rows.
 * @returns {Promise<object[]>} Array of parking breakdown rows.
 */
export async function getParkingReport({ startDate, endDate, brand, preview }) {
  return repo.findParkingReport(startDate, endDate, brand, preview);
}

// ─────────────────────────────────────────────────────────────────────────────
// R7 — RECONCILIATION SUMMARY (Resumen Global Ejecutivo)
// Solo totales — sin detalle de filas
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Returns executive summary totals across bank, portfolio, and card settlements.
 * All three queries run in parallel via Promise.all. Returns aggregate counts
 * and amounts only — no detail rows.
 * @param {object} params
 * @param {string} params.startDate - ISO date string, inclusive range start.
 * @param {string} params.endDate   - ISO date string, inclusive range end.
 * @returns {Promise<object>} `{ bank, portfolio, cards }` — each is an array of
 *   rows grouped by reconcile_status (cards also grouped by brand).
 */
export async function getSummaryReport({ startDate, endDate }) {
  const [bank, portfolio, cards] = await Promise.all([
    repo.findSummaryBank(startDate, endDate),
    repo.findSummaryPortfolio(startDate, endDate),
    repo.findSummaryCards(startDate, endDate),
  ]);
  return { bank, portfolio, cards };
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPER — Conteo total sin LIMIT (para mostrar "200 of X rows" en el preview)
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Returns the total unfiltered row count for a given report type, ignoring
 * the preview LIMIT. Used to render "200 of X rows" in the UI.
 * @param {string} reportType - One of: 'bank', 'portfolio', 'card-details',
 *   'card-settlements', 'parking'. Unknown types return 0.
 * @param {object} filters
 * @param {string} filters.startDate - ISO date string, inclusive range start.
 * @param {string} filters.endDate   - ISO date string, inclusive range end.
 * @returns {Promise<number>} Total row count, or 0 for unknown reportType.
 */
export async function getRowCount(reportType, filters) {
  const { startDate, endDate, status, brand } = filters;
  const row = await repo.findReportRowCount(reportType, startDate, endDate);
  return row ? parseInt(row.count) : 0;
}
