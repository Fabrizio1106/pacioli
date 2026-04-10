// src/modules/reports/reports.repository.js
import { pool } from '../../config/database.js';

// Appended only in preview mode — limits result set to 200 rows
function limitClause(preview) {
  return preview ? 'LIMIT 200' : '';
}

export async function findOverviewKpis(startDate, endDate) {
  const result = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE reconcile_status = 'PENDING')                       AS pending_count,
      COALESCE(SUM(amount_total) FILTER (WHERE reconcile_status = 'PENDING'), 0) AS pending_amount,
      COUNT(*) FILTER (WHERE reconcile_status = 'REVIEW')                        AS review_count,
      COALESCE(SUM(amount_total) FILTER (WHERE reconcile_status = 'REVIEW'), 0)  AS review_amount,
      COUNT(*) FILTER (WHERE reconcile_status IN ('MATCHED','MATCHED_MANUAL'))   AS matched_count,
      COALESCE(SUM(amount_total) FILTER (
        WHERE reconcile_status IN ('MATCHED','MATCHED_MANUAL')), 0)              AS matched_amount,
      COUNT(*)                                                                    AS total_count,
      COALESCE(SUM(amount_total), 0)                                             AS total_amount
    FROM biq_stg.stg_bank_transactions
    WHERE doc_date BETWEEN $1 AND $2
      AND is_compensated_sap      = FALSE
      AND is_compensated_intraday = FALSE
  `, [startDate, endDate]);
  return result.rows[0];
}

export async function findOverviewDetail(startDate, endDate, preview) {
  const result = await pool.query(`
    SELECT
      doc_date, doc_type, sap_description, bank_ref_2, amount_total,
      trans_type, global_category, brand, reconcile_status,
      enrich_customer_id, enrich_customer_name
    FROM biq_stg.stg_bank_transactions
    WHERE doc_date BETWEEN $1 AND $2
      AND is_compensated_sap      = FALSE
      AND is_compensated_intraday = FALSE
    ORDER BY doc_date ASC, reconcile_status ASC
    ${limitClause(preview)}
  `, [startDate, endDate]);
  return result.rows;
}

export async function findBankReport(startDate, endDate, status, preview) {
  const params = [startDate, endDate];
  let statusSQL = '';
  if (status && status.length > 0) {
    const placeholders = status.map((_, i) => `$${i + 3}`).join(', ');
    statusSQL = `AND reconcile_status IN (${placeholders})`;
    params.push(...status);
  }
  const result = await pool.query(`
    SELECT
      stg_id, doc_date, doc_type, amount_total, sap_description, bank_ref_2,
      trans_type, global_category, brand, batch_number, match_hash_key,
      is_compensated_sap, is_compensated_intraday, reconcile_status, settlement_id,
      establishment_name, count_voucher_bank, count_voucher_portfolio,
      final_amount_gross, final_amount_net, final_amount_commission,
      final_amount_tax_iva, final_amount_tax_irf, diff_adjustment, reconcile_reason,
      enrich_customer_id, enrich_customer_name, enrich_notes
    FROM biq_stg.stg_bank_transactions
    WHERE doc_date BETWEEN $1 AND $2
      AND is_compensated_sap      = FALSE
      AND is_compensated_intraday = FALSE
      ${statusSQL}
    ORDER BY doc_date ASC, reconcile_status ASC
    ${limitClause(preview)}
  `, params);
  return result.rows;
}

export async function findPortfolioReport(startDate, endDate, status, preview) {
  const params = [startDate, endDate];
  let statusSQL = "AND reconcile_status != 'CLOSED'"; // default: exclude CLOSED
  if (status && status.length > 0) {
    const placeholders = status.map((_, i) => `$${i + 3}`).join(', ');
    statusSQL = `AND reconcile_status IN (${placeholders})`;
    params.push(...status);
  }
  const result = await pool.query(`
    SELECT
      stg_id, customer_code, customer_name, assignment, invoice_ref,
      doc_date, due_date, amount_outstanding, conciliable_amount,
      enrich_batch, enrich_ref, enrich_brand, enrich_user, enrich_source,
      reconcile_group, match_hash_key, reconcile_status, settlement_id,
      financial_amount_gross, financial_amount_net, financial_commission,
      financial_tax_iva, financial_tax_irf, match_method
    FROM biq_stg.stg_customer_portfolio
    WHERE doc_date BETWEEN $1 AND $2
      ${statusSQL}
    ORDER BY doc_date ASC, reconcile_status ASC
    ${limitClause(preview)}
  `, params);
  return result.rows;
}

export async function findCardDetailsReport(startDate, endDate, brand, status, preview) {
  const params = [startDate, endDate];
  let brandSQL  = '';
  let statusSQL = '';
  let idx       = 3;
  if (brand && brand.length > 0) {
    const placeholders = brand.map((_, i) => `$${idx + i}`).join(', ');
    brandSQL = `AND brand IN (${placeholders})`;
    params.push(...brand);
    idx += brand.length;
  }
  if (status && status.length > 0) {
    const placeholders = status.map((_, i) => `$${idx + i}`).join(', ');
    statusSQL = `AND reconcile_status IN (${placeholders})`;
    params.push(...status);
  }
  const result = await pool.query(`
    SELECT
      stg_id, settlement_id, voucher_date, card_number, auth_code,
      voucher_ref, batch_number, amount_gross, amount_net, amount_commission,
      amount_tax_iva, amount_tax_irf, brand, establishment_code,
      establishment_name, voucher_hash_key, reconcile_status
    FROM biq_stg.stg_card_details
    WHERE voucher_date BETWEEN $1 AND $2
      ${brandSQL}
      ${statusSQL}
    ORDER BY voucher_date ASC, brand ASC
    ${limitClause(preview)}
  `, params);
  return result.rows;
}

export async function findCardSettlementsReport(startDate, endDate, brand, status, preview) {
  const params = [startDate, endDate];
  let brandSQL  = '';
  let statusSQL = '';
  let idx       = 3;
  if (brand && brand.length > 0) {
    const placeholders = brand.map((_, i) => `$${idx + i}`).join(', ');
    brandSQL = `AND brand IN (${placeholders})`;
    params.push(...brand);
    idx += brand.length;
  }
  if (status && status.length > 0) {
    const placeholders = status.map((_, i) => `$${idx + i}`).join(', ');
    statusSQL = `AND reconcile_status IN (${placeholders})`;
    params.push(...status);
  }
  const result = await pool.query(`
    SELECT
      stg_id, settlement_id, settlement_date, brand, batch_number,
      amount_gross, amount_net, amount_commission, amount_tax_iva, amount_tax_irf,
      match_hash_key, reconcile_status, count_voucher, establishment_name
    FROM biq_stg.stg_card_settlements
    WHERE settlement_date BETWEEN $1 AND $2
      ${brandSQL}
      ${statusSQL}
    ORDER BY settlement_date ASC, brand ASC
    ${limitClause(preview)}
  `, params);
  return result.rows;
}

export async function findParkingReport(startDate, endDate, brand, preview) {
  const params = [startDate, endDate];
  let brandSQL = '';
  if (brand && brand.length > 0) {
    const placeholders = brand.map((_, i) => `$${i + 3}`).join(', ');
    brandSQL = `AND brand IN (${placeholders})`;
    params.push(...brand);
  }
  const result = await pool.query(`
    SELECT
      stg_id, settlement_date, settlement_id, batch_number, brand,
      amount_gross, amount_commission, amount_tax_iva, amount_tax_irf, amount_net,
      count_voucher, match_hash_key, reconcile_status
    FROM biq_stg.stg_parking_pay_breakdown
    WHERE settlement_date BETWEEN $1 AND $2
      ${brandSQL}
    ORDER BY settlement_date ASC, brand ASC
    ${limitClause(preview)}
  `, params);
  return result.rows;
}

export async function findSummaryBank(startDate, endDate) {
  const result = await pool.query(`
    SELECT
      reconcile_status,
      COUNT(*)::integer              AS count,
      COALESCE(SUM(amount_total), 0) AS total_amount
    FROM biq_stg.stg_bank_transactions
    WHERE doc_date BETWEEN $1 AND $2
      AND is_compensated_sap      = FALSE
      AND is_compensated_intraday = FALSE
    GROUP BY reconcile_status
    ORDER BY reconcile_status
  `, [startDate, endDate]);
  return result.rows;
}

export async function findSummaryPortfolio(startDate, endDate) {
  const result = await pool.query(`
    SELECT
      reconcile_status,
      COUNT(*)::integer                    AS count,
      COALESCE(SUM(conciliable_amount), 0) AS total_amount
    FROM biq_stg.stg_customer_portfolio
    WHERE doc_date BETWEEN $1 AND $2
    GROUP BY reconcile_status
    ORDER BY reconcile_status
  `, [startDate, endDate]);
  return result.rows;
}

export async function findSummaryCards(startDate, endDate) {
  const result = await pool.query(`
    SELECT
      brand,
      reconcile_status,
      COUNT(*)::integer              AS count,
      COALESCE(SUM(amount_gross), 0) AS total_gross,
      COALESCE(SUM(amount_net),   0) AS total_net
    FROM biq_stg.stg_card_settlements
    WHERE settlement_date BETWEEN $1 AND $2
    GROUP BY brand, reconcile_status
    ORDER BY brand, reconcile_status
  `, [startDate, endDate]);
  return result.rows;
}

export async function findReportRowCount(reportType, startDate, endDate) {
  const queries = {
    bank: {
      sql:    `SELECT COUNT(*) FROM biq_stg.stg_bank_transactions
               WHERE doc_date BETWEEN $1 AND $2
                 AND is_compensated_sap = FALSE AND is_compensated_intraday = FALSE`,
      params: [startDate, endDate],
    },
    portfolio: {
      sql:    `SELECT COUNT(*) FROM biq_stg.stg_customer_portfolio
               WHERE doc_date BETWEEN $1 AND $2 AND reconcile_status != 'CLOSED'`,
      params: [startDate, endDate],
    },
    'card-details': {
      sql:    `SELECT COUNT(*) FROM biq_stg.stg_card_details
               WHERE voucher_date BETWEEN $1 AND $2`,
      params: [startDate, endDate],
    },
    'card-settlements': {
      sql:    `SELECT COUNT(*) FROM biq_stg.stg_card_settlements
               WHERE settlement_date BETWEEN $1 AND $2`,
      params: [startDate, endDate],
    },
    parking: {
      sql:    `SELECT COUNT(*) FROM biq_stg.stg_parking_pay_breakdown
               WHERE settlement_date BETWEEN $1 AND $2`,
      params: [startDate, endDate],
    },
  };
  const q = queries[reportType];
  if (!q) return null;
  const result = await pool.query(q.sql, q.params);
  return result.rows[0];
}
