// src/modules/transactions/transactions.service.js
import * as repo from './transactions.repository.js';

// ─────────────────────────────────────────────
// Transforma una fila cruda de DB al formato que
// el frontend espera. Esto es nuestro DTO.
// ─────────────────────────────────────────────
function toTransactionDTO(row) {
  return {
    id:             row.stg_id,
    bankRef1:       row.bank_ref_1,
    docDate:        row.doc_date,
    bankDate:       row.bank_date,
    docNumber:      row.doc_number,
    docReference:   row.doc_reference,
    amountTotal:    parseFloat(row.amount_total) || 0,
    currency:       row.currency || 'USD',
    bankDescription: row.bank_description,
    transType:      row.trans_type,
    globalCategory: row.global_category,
    establishmentName: row.establishment_name,

    // Enriquecimiento del pipeline Python
    customer: {
      id:              row.enrich_customer_id,
      name:            row.enrich_customer_name,
      confidenceScore: parseFloat(row.enrich_confidence_score) || 0,
    },

    // Estado de conciliación
    reconciliation: {
      status:          row.reconcile_status,
      reason:          row.reconcile_reason,
      confidenceScore: parseFloat(row.match_confidence_score) || 0,
      method:          row.match_method,
      reconciledAt:    row.reconciled_at,
    },

    updatedAt: row.updated_at,
  };
}

// ─────────────────────────────────────────────
// Transforma el detalle completo — todas las columnas
// ─────────────────────────────────────────────
function toTransactionDetailDTO(row) {
  return {
    // Todo lo del DTO básico más...
    ...toTransactionDTO(row),

    // Campos adicionales solo en detalle
    sourceSystem:    row.source_system,
    docType:         row.doc_type,
    amountSign:      row.amount_sign,
    sapDescription:  row.sap_description,
    bankRef1:        row.bank_ref_1,
    bankRef2:        row.bank_ref_2,
    bankOfficeId:    row.bank_office_id,
    brand:           row.brand,
    settlementId:    row.settlement_id,
    batchNumber:     row.batch_number,
    isCompensatedSap:      row.is_compensated_sap,
    isCompensatedIntraday: row.is_compensated_intraday,

    // Montos finales calculados por el pipeline
    amounts: {
      gross:      parseFloat(row.final_amount_gross)      || 0,
      net:        parseFloat(row.final_amount_net)        || 0,
      commission: parseFloat(row.final_amount_commission) || 0,
      taxIva:     parseFloat(row.final_amount_tax_iva)    || 0,
      taxIrf:     parseFloat(row.final_amount_tax_irf)    || 0,
      diffAdjustment: parseFloat(row.diff_adjustment)     || 0,
    },

    // Matches sugeridos por el algoritmo
    matchedPortfolioIds: row.matched_portfolio_ids 
      ? row.matched_portfolio_ids.split(',').map(s => s.trim()).filter(Boolean)
      : [],
    alternativeMatches: row.alternative_matches || null,

    enrich: {
      customerId:       row.enrich_customer_id,
      customerName:     row.enrich_customer_name,
      confidenceScore:  parseFloat(row.enrich_confidence_score) || 0,
      inferenceMethod:  row.enrich_inference_method,
      notes:            row.enrich_notes,
    },
  };
}

/**
 * Returns a paginated list of bank transactions matching the given filters.
 *
 * Raw DB rows are mapped to the basic transaction DTO shape. Pagination
 * metadata is computed from the total row count and the requested page/limit.
 * Defaults to page 1 with a limit of 20 if not specified in filters.
 *
 * @param {object} filters         - Filter and pagination parameters forwarded to the repository.
 * @param {number} [filters.page]  - Page number (default 1).
 * @param {number} [filters.limit] - Results per page (default 20).
 * @returns {Promise<object>} Paginated result with data array and pagination metadata
 *   ({ total, page, limit, totalPages, hasNextPage, hasPrevPage }).
 */
export async function listTransactions(filters) {
  const { rows, total } = await repo.findTransactions(filters);

  const limit      = parseInt(filters.limit, 10) || 20;
  const page       = parseInt(filters.page,  10) || 1;
  const totalPages = Math.ceil(total / limit);

  return {
    data: rows.map(toTransactionDTO),
    pagination: {
      total,
      page,
      limit,
      totalPages,
      hasNextPage: page < totalPages,
      hasPrevPage: page > 1,
    },
  };
}

/**
 * Returns the full detail view of a single bank transaction.
 *
 * Extends the basic transaction DTO with source system fields, bank references,
 * compensation flags, pipeline-calculated financial amounts, algorithm match
 * suggestions, and enrichment metadata.
 *
 * @param {number} stgId - Staging ID of the bank transaction to retrieve.
 * @returns {Promise<object>} Full transaction detail DTO.
 * @throws {404} If no transaction exists with the given stgId.
 */
export async function getTransactionDetail(stgId) {
  const row = await repo.findTransactionById(stgId);

  if (!row) {
    const error  = new Error('Transacción no encontrada');
    error.status = 404;
    throw error;
  }

  return toTransactionDetailDTO(row);
}

/**
 * Returns transaction counts and amounts grouped by reconcile status.
 *
 * Intended for dashboard summary widgets. Each entry includes the status label,
 * transaction count, total amount, and average algorithm confidence score.
 *
 * @returns {Promise<Array<{ status: string, count: number, totalAmount: number, avgConfidence: number }>>}
 *   One entry per reconcile status present in the dataset.
 */
export async function getStatusSummary() {
  const rows = await repo.getStatusSummary();

  return rows.map(row => ({
    status:        row.status,
    count:         parseInt(row.count, 10),
    totalAmount:   parseFloat(row.total_amount) || 0,
    avgConfidence: parseFloat(row.avg_confidence) || 0,
  }));
}