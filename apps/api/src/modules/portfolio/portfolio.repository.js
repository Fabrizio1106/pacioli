// src/modules/portfolio/portfolio.repository.js
import { pool } from '../../config/database.js';

export async function findPortfolioByIds(stgIds) {
  if (!stgIds || stgIds.length === 0) return [];
  const result = await pool.query(`
    SELECT p.*, NULL::numeric AS amount_diff, NULL::numeric AS amount_pct
    FROM biq_stg.stg_customer_portfolio p
    WHERE p.stg_id = ANY($1::bigint[])
      AND p.reconcile_status IN ('PENDING', 'ENRICHED', 'REVIEW')
    ORDER BY p.match_confidence DESC NULLS LAST, p.due_date ASC
  `, [stgIds]);
  return result.rows;
}

export async function findPortfolioByCustomer({
  customerCode, excludeIds = [], bankAmount = null, page = 1, limit = 50,
}) {
  const offset      = (page - 1) * limit;
  const hasExclude  = excludeIds.length > 0;
  const hasAmount   = bankAmount !== null && bankAmount !== undefined;
  let query, countQuery, params, countParams;

  if (!hasExclude && !hasAmount) {
    query = `SELECT p.*, NULL::numeric AS amount_diff, NULL::numeric AS amount_pct
      FROM biq_stg.stg_customer_portfolio p
      WHERE p.customer_code = $1 AND p.reconcile_status IN ('PENDING','ENRICHED','REVIEW')
      ORDER BY p.due_date ASC LIMIT $2 OFFSET $3`;
    params = [customerCode, limit, offset];
    countQuery = `SELECT COUNT(*)::integer AS total FROM biq_stg.stg_customer_portfolio p
      WHERE p.customer_code = $1 AND p.reconcile_status IN ('PENDING','ENRICHED','REVIEW')`;
    countParams = [customerCode];
  } else if (hasExclude && !hasAmount) {
    query = `SELECT p.*, NULL::numeric AS amount_diff, NULL::numeric AS amount_pct
      FROM biq_stg.stg_customer_portfolio p
      WHERE p.customer_code = $1 AND p.reconcile_status IN ('PENDING','ENRICHED','REVIEW')
        AND p.stg_id != ALL($2::bigint[])
      ORDER BY p.due_date ASC LIMIT $3 OFFSET $4`;
    params = [customerCode, excludeIds, limit, offset];
    countQuery = `SELECT COUNT(*)::integer AS total FROM biq_stg.stg_customer_portfolio p
      WHERE p.customer_code = $1 AND p.reconcile_status IN ('PENDING','ENRICHED','REVIEW')
        AND p.stg_id != ALL($2::bigint[])`;
    countParams = [customerCode, excludeIds];
  } else if (!hasExclude && hasAmount) {
    query = `SELECT p.*,
        ABS(p.conciliable_amount - $2) AS amount_diff,
        ROUND((p.conciliable_amount / NULLIF($2,0) * 100)::numeric,2) AS amount_pct
      FROM biq_stg.stg_customer_portfolio p
      WHERE p.customer_code = $1 AND p.reconcile_status IN ('PENDING','ENRICHED','REVIEW')
      ORDER BY ABS(p.conciliable_amount - $2) ASC, p.due_date ASC LIMIT $3 OFFSET $4`;
    params = [customerCode, parseFloat(bankAmount), limit, offset];
    countQuery = `SELECT COUNT(*)::integer AS total FROM biq_stg.stg_customer_portfolio p
      WHERE p.customer_code = $1 AND p.reconcile_status IN ('PENDING','ENRICHED','REVIEW')`;
    countParams = [customerCode];
  } else {
    query = `SELECT p.*,
        ABS(p.conciliable_amount - $3) AS amount_diff,
        ROUND((p.conciliable_amount / NULLIF($3,0) * 100)::numeric,2) AS amount_pct
      FROM biq_stg.stg_customer_portfolio p
      WHERE p.customer_code = $1 AND p.reconcile_status IN ('PENDING','ENRICHED','REVIEW')
        AND p.stg_id != ALL($2::bigint[])
      ORDER BY ABS(p.conciliable_amount - $3) ASC, p.due_date ASC LIMIT $4 OFFSET $5`;
    params = [customerCode, excludeIds, parseFloat(bankAmount), limit, offset];
    countQuery = `SELECT COUNT(*)::integer AS total FROM biq_stg.stg_customer_portfolio p
      WHERE p.customer_code = $1 AND p.reconcile_status IN ('PENDING','ENRICHED','REVIEW')
        AND p.stg_id != ALL($2::bigint[])`;
    countParams = [customerCode, excludeIds];
  }

  const [dataResult, countResult] = await Promise.all([
    pool.query(query, params),
    pool.query(countQuery, countParams),
  ]);
  return { rows: dataResult.rows, total: countResult.rows[0].total };
}

export async function findPortfolioByGlAccount({
  glAccount, customerCode = null, excludeIds = [],
  bankAmount = null, page = 1, limit = 100,
}) {
  const offset      = (page - 1) * limit;
  const hasCustomer = customerCode !== null;
  const hasExclude  = excludeIds.length > 0;
  const hasAmount   = bankAmount !== null;
  let query, params;

  if (!hasCustomer && !hasExclude && !hasAmount) {
    query  = `SELECT p.*, NULL::numeric AS amount_diff, NULL::numeric AS amount_pct
      FROM biq_stg.stg_customer_portfolio p
      WHERE p.reconcile_status IN ('PENDING','ENRICHED','REVIEW') AND p.gl_account=$1
      ORDER BY p.due_date ASC LIMIT $2 OFFSET $3`;
    params = [glAccount, limit, offset];
  } else if (hasCustomer && !hasExclude && !hasAmount) {
    query  = `SELECT p.*, NULL::numeric AS amount_diff, NULL::numeric AS amount_pct
      FROM biq_stg.stg_customer_portfolio p
      WHERE p.reconcile_status IN ('PENDING','ENRICHED','REVIEW') AND p.gl_account=$1 AND p.customer_code=$2
      ORDER BY p.due_date ASC LIMIT $3 OFFSET $4`;
    params = [glAccount, customerCode, limit, offset];
  } else if (!hasCustomer && hasExclude && !hasAmount) {
    query  = `SELECT p.*, NULL::numeric AS amount_diff, NULL::numeric AS amount_pct
      FROM biq_stg.stg_customer_portfolio p
      WHERE p.reconcile_status IN ('PENDING','ENRICHED','REVIEW') AND p.gl_account=$1
        AND p.stg_id != ALL($2::bigint[])
      ORDER BY p.due_date ASC LIMIT $3 OFFSET $4`;
    params = [glAccount, excludeIds, limit, offset];
  } else if (hasCustomer && hasExclude && !hasAmount) {
    query  = `SELECT p.*, NULL::numeric AS amount_diff, NULL::numeric AS amount_pct
      FROM biq_stg.stg_customer_portfolio p
      WHERE p.reconcile_status IN ('PENDING','ENRICHED','REVIEW') AND p.gl_account=$1
        AND p.customer_code=$2 AND p.stg_id != ALL($3::bigint[])
      ORDER BY p.due_date ASC LIMIT $4 OFFSET $5`;
    params = [glAccount, customerCode, excludeIds, limit, offset];
  } else if (!hasCustomer && !hasExclude && hasAmount) {
    query  = `SELECT p.*, ABS(p.conciliable_amount-$2) AS amount_diff, NULL::numeric AS amount_pct
      FROM biq_stg.stg_customer_portfolio p
      WHERE p.reconcile_status IN ('PENDING','ENRICHED','REVIEW') AND p.gl_account=$1
      ORDER BY ABS(p.conciliable_amount-$2) ASC LIMIT $3 OFFSET $4`;
    params = [glAccount, parseFloat(bankAmount), limit, offset];
  } else if (hasCustomer && !hasExclude && hasAmount) {
    query  = `SELECT p.*, ABS(p.conciliable_amount-$3) AS amount_diff, NULL::numeric AS amount_pct
      FROM biq_stg.stg_customer_portfolio p
      WHERE p.reconcile_status IN ('PENDING','ENRICHED','REVIEW') AND p.gl_account=$1 AND p.customer_code=$2
      ORDER BY ABS(p.conciliable_amount-$3) ASC LIMIT $4 OFFSET $5`;
    params = [glAccount, customerCode, parseFloat(bankAmount), limit, offset];
  } else if (!hasCustomer && hasExclude && hasAmount) {
    query  = `SELECT p.*, ABS(p.conciliable_amount-$3) AS amount_diff, NULL::numeric AS amount_pct
      FROM biq_stg.stg_customer_portfolio p
      WHERE p.reconcile_status IN ('PENDING','ENRICHED','REVIEW') AND p.gl_account=$1
        AND p.stg_id != ALL($2::bigint[])
      ORDER BY ABS(p.conciliable_amount-$3) ASC LIMIT $4 OFFSET $5`;
    params = [glAccount, excludeIds, parseFloat(bankAmount), limit, offset];
  } else {
    query  = `SELECT p.*, ABS(p.conciliable_amount-$4) AS amount_diff, NULL::numeric AS amount_pct
      FROM biq_stg.stg_customer_portfolio p
      WHERE p.reconcile_status IN ('PENDING','ENRICHED','REVIEW') AND p.gl_account=$1
        AND p.customer_code=$2 AND p.stg_id != ALL($3::bigint[])
      ORDER BY ABS(p.conciliable_amount-$4) ASC LIMIT $5 OFFSET $6`;
    params = [glAccount, customerCode, excludeIds, parseFloat(bankAmount), limit, offset];
  }

  const result = await pool.query(query, params);
  return result.rows;
}

export async function findPortfolioUniversal({ bankAmount, page = 1, limit = 50, search = null }) {
  const offset    = (page - 1) * limit;
  const hasSearch = search && search.trim().length >= 2;

  if (hasSearch) {
    const s = `%${search.trim()}%`;
    const [dataResult, countResult] = await Promise.all([
      pool.query(`
        SELECT p.*,
          ABS(p.conciliable_amount - $1) AS amount_diff,
          ROUND((p.conciliable_amount / NULLIF($1,0) * 100)::numeric,2) AS amount_pct
        FROM biq_stg.stg_customer_portfolio p
        WHERE p.reconcile_status IN ('PENDING','ENRICHED','REVIEW')
          AND p.conciliable_amount > 0
          AND (LOWER(p.customer_name) ILIKE LOWER($2)
            OR p.invoice_ref ILIKE $2
            OR p.assignment  ILIKE $2
            OR p.customer_code = $2)
        ORDER BY ABS(p.conciliable_amount - $1) ASC
        LIMIT $3 OFFSET $4
      `, [parseFloat(bankAmount), s, limit, offset]),
      pool.query(`
        SELECT COUNT(*)::integer AS total
        FROM biq_stg.stg_customer_portfolio p
        WHERE p.reconcile_status IN ('PENDING','ENRICHED','REVIEW')
          AND p.conciliable_amount > 0
          AND (LOWER(p.customer_name) ILIKE LOWER($1)
            OR p.invoice_ref ILIKE $1
            OR p.assignment  ILIKE $1
            OR p.customer_code = $1)
      `, [s]),
    ]);
    return { rows: dataResult.rows, total: countResult.rows[0].total };
  }

  const [dataResult, countResult] = await Promise.all([
    pool.query(`
      SELECT p.*,
        ABS(p.conciliable_amount - $1) AS amount_diff,
        ROUND((p.conciliable_amount / NULLIF($1,0) * 100)::numeric,2) AS amount_pct
      FROM biq_stg.stg_customer_portfolio p
      WHERE p.reconcile_status IN ('PENDING','ENRICHED','REVIEW')
        AND p.conciliable_amount > 0
      ORDER BY ABS(p.conciliable_amount - $1) ASC
      LIMIT $2 OFFSET $3
    `, [parseFloat(bankAmount), limit, offset]),
    pool.query(`
      SELECT COUNT(*)::integer AS total
      FROM biq_stg.stg_customer_portfolio p
      WHERE p.reconcile_status IN ('PENDING','ENRICHED','REVIEW')
        AND p.conciliable_amount > 0
    `),
  ]);
  return { rows: dataResult.rows, total: countResult.rows[0].total };
}

// ── findPortfolioBySearch — búsqueda global por texto libre ───────────────
// Busca en TODA la cartera por customer_name, invoice_ref, assignment, customer_code.
// Sin filtro de customer — cruza todo el portfolio activo.
// Ordena: coincidencias exactas en invoice_ref primero, luego por proximidad de monto.
export async function findPortfolioBySearch({ query, bankAmount = null, limit = 50 }) {
  const s      = `%${query.trim()}%`;
  const amount = bankAmount ? parseFloat(bankAmount) : 0;

  const result = await pool.query(`
    SELECT
      p.*,
      ABS(p.conciliable_amount - $2) AS amount_diff,
      ROUND((p.conciliable_amount / NULLIF($2, 0) * 100)::numeric, 2) AS amount_pct
    FROM biq_stg.stg_customer_portfolio p
    WHERE p.reconcile_status IN ('PENDING', 'ENRICHED', 'REVIEW')
      AND p.conciliable_amount > 0
      AND (
        p.customer_name  ILIKE $1 OR
        p.invoice_ref    ILIKE $1 OR
        p.assignment     ILIKE $1 OR
        p.customer_code  ILIKE $1
      )
    ORDER BY
      CASE
        WHEN p.invoice_ref   ILIKE $1 THEN 3
        WHEN p.assignment    ILIKE $1 THEN 2
        WHEN p.customer_name ILIKE $1 THEN 1
        ELSE 0
      END DESC,
      ABS(p.conciliable_amount - $2) ASC
    LIMIT $3
  `, [s, amount, limit]);

  return { rows: result.rows, total: result.rows.length };
}

export async function findBankTransactionById(stgId) {
  const result = await pool.query(`
    SELECT * FROM biq_stg.stg_bank_transactions WHERE stg_id = $1
  `, [stgId]);
  return result.rows[0] || null;
}

export async function findTcSuggestions(settlementId, excludeIds) {
  const result = await pool.query(`
    SELECT *,
           NULL::numeric AS amount_diff,
           NULL::numeric AS amount_pct
    FROM biq_stg.stg_customer_portfolio
    WHERE settlement_id = $1
      AND is_suggestion = TRUE
      AND reconcile_status IN ('PENDING', 'ENRICHED', 'REVIEW')
      AND stg_id != ALL($2::bigint[])
    ORDER BY match_confidence DESC NULLS LAST
  `, [settlementId, excludeIds.length > 0 ? excludeIds : [0]]);
  return result.rows;
}

export async function validatePortfolioItemsSelectable(stgIds) {
  const SELECTABLE = ['PENDING', 'ENRICHED', 'REVIEW'];
  const result = await pool.query(`
    SELECT stg_id, reconcile_status, customer_code, conciliable_amount
    FROM biq_stg.stg_customer_portfolio
    WHERE stg_id = ANY($1::bigint[])
  `, [stgIds]);
  const invalid = result.rows.filter(r => !SELECTABLE.includes(r.reconcile_status));
  return { valid: invalid.length === 0, invalid, rows: result.rows };
}