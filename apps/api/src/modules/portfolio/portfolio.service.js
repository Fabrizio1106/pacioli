// src/modules/portfolio/portfolio.service.js
import * as repo from './portfolio.repository.js';

export const SCENARIOS = {
  REVIEW_TRANSFER_MATCHED:     'REVIEW_TRANSFER_MATCHED',
  REVIEW_CARD_PARKING:         'REVIEW_CARD_PARKING',
  REVIEW_CARD_VIP_ASSISTANCE:  'REVIEW_CARD_VIP_ASSISTANCE',
  PENDING_WITH_SUGGESTIONS:    'PENDING_WITH_SUGGESTIONS',
  PENDING_DEPOSIT_NORMAL:      'PENDING_DEPOSIT_NORMAL',
  PENDING_DEPOSIT_NO_INVOICES: 'PENDING_DEPOSIT_NO_INVOICES',
  PENDING_UNKNOWN_CLIENT:      'PENDING_UNKNOWN_CLIENT',
};

const GL_ACCOUNTS = {
  PARKING:        '1120114029',
  VIP_ASSISTANCE: '1120114035',
};

const UNKNOWN_CUSTOMER_CODES = ['999998', '999999', null, undefined, ''];

function parseMatchedIds(raw) {
  if (!raw) return [];
  try {
    const cleaned = raw.toString().replace(/[\[\]\s]/g, '');
    if (!cleaned) return [];
    return cleaned.split(',')
      .map(id => parseInt(id.trim(), 10))
      .filter(id => !isNaN(id));
  } catch {
    return [];
  }
}

function detectScenario(bankTx) {
  const status      = bankTx.reconcile_status;
  const transType   = (bankTx.trans_type          || '').trim();
  const estName     = (bankTx.establishment_name  || '').trim().toUpperCase();
  const customerId  = bankTx.enrich_customer_id;
  const matchedIds  = parseMatchedIds(bankTx.matched_portfolio_ids);
  const enrichNotes = (bankTx.enrich_notes        || '').toLowerCase();

  if (status === 'REVIEW') {
    if (transType === 'LIQUIDACION TC') {
      if (estName.includes('PARKING')) {
        return SCENARIOS.REVIEW_CARD_PARKING;
      }
      if (estName.includes('VIP') || estName.includes('ASISTENCIA')) {
        return SCENARIOS.REVIEW_CARD_VIP_ASSISTANCE;
      }
      return SCENARIOS.REVIEW_CARD_PARKING;
    }
    return SCENARIOS.REVIEW_TRANSFER_MATCHED;
  }

  if (status === 'PENDING') {
    if (transType === 'DEPOSITO EFECTIVO'          ||
        transType === 'DEPOSITO CHEQUE'             ||
        transType === 'DEPOSITO DIFERIDO EFECTIVO') {
      if (enrichNotes.includes('sin facturas')) {
        return SCENARIOS.PENDING_DEPOSIT_NO_INVOICES;
      }
      return SCENARIOS.PENDING_DEPOSIT_NORMAL;
    }

    if (UNKNOWN_CUSTOMER_CODES.includes(customerId) || !customerId) {
      return SCENARIOS.PENDING_UNKNOWN_CLIENT;
    }

    if (matchedIds.length > 0) {
      return SCENARIOS.PENDING_WITH_SUGGESTIONS;
    }

    return SCENARIOS.PENDING_DEPOSIT_NORMAL;
  }

  return SCENARIOS.PENDING_UNKNOWN_CLIENT;
}

/**
 * Maps a raw portfolio DB row to the client-facing invoice DTO shape.
 *
 * Exported so other modules (workspace, portfolio panel) can reuse
 * the same mapping without duplicating field transforms. All numeric
 * fields are parsed with parseFloat; missing values default to zero or null.
 *
 * @param {object}  row             - Raw DB row from stg_customer_portfolio.
 * @param {boolean} [isPreSelected] - Whether to flag this item as pre-selected in the UI (default false).
 * @returns {object} Portfolio item DTO with normalized financial amounts and metadata.
 */
export function toPortfolioItemDTO(row, isPreSelected = false) {
  return {
    id:                row.stg_id,
    sapDocNumber:      row.sap_doc_number,
    accountingDoc:     row.accounting_doc,
    customerCode:      row.customer_code,
    customerName:      row.customer_name,
    invoiceRef:        row.invoice_ref,
    assignment:        row.assignment,
    docDate:           row.doc_date,
    dueDate:           row.due_date,
    amountOutstanding: parseFloat(row.amount_outstanding) || 0,
    conciliableAmount: parseFloat(row.conciliable_amount) || 0,
    currency:          row.currency?.trim() || 'USD',
    status:            row.reconcile_status,
    glAccount:         row.gl_account,
    settlementId:      row.settlement_id,
    isSuggestion:      row.is_suggestion    || false,
    matchConfidence:   row.match_confidence ? parseInt(row.match_confidence, 10) : null,
    matchMethod:       row.match_method     || null,
    sapText:           row.sap_text,
    isPartialPayment:  row.partial_payment_flag || false,
    financialAmounts: {
      gross:      parseFloat(row.financial_amount_gross) || 0,
      net:        parseFloat(row.financial_amount_net)   || 0,
      commission: parseFloat(row.financial_commission)   || 0,
      taxIva:     parseFloat(row.financial_tax_iva)      || 0,
      taxIrf:     parseFloat(row.financial_tax_irf)      || 0,
    },
    amountDiff: row.amount_diff !== null && row.amount_diff !== undefined
      ? parseFloat(row.amount_diff) : null,
    amountPct: row.amount_pct !== null && row.amount_pct !== undefined
      ? parseFloat(row.amount_pct) : null,
    preSelected: isPreSelected,
    updatedAt:   row.updated_at,
  };
}

function buildPagination(total, page, limit) {
  const totalPages = Math.ceil(total / limit);
  return {
    total,
    page:        parseInt(page,  10),
    limit:       parseInt(limit, 10),
    totalPages,
    hasNextPage: page < totalPages,
    hasPrevPage: page > 1,
  };
}

async function loadTcSuggestions(settlementId, excludeIds) {
  if (!settlementId) return [];
  try {
    return await repo.findTcSuggestions(settlementId, excludeIds);
  } catch {
    return [];
  }
}

/**
 * Searches all active invoices by free text across the full portfolio.
 *
 * Matches against customer name, invoice reference, assignment, and customer
 * code with no scenario or customer scoping. Results are ordered by match
 * priority (invoice ref > assignment > customer name) then by amount proximity
 * to bankAmount. Intended for the global search section available in all scenarios.
 *
 * @param {object}      params
 * @param {string}      params.query        - Free-text search term (min 1 character).
 * @param {number|null} [params.bankAmount] - Bank transaction amount used for proximity sorting.
 * @param {number}      [params.limit]      - Maximum results to return (default 50).
 * @returns {Promise<{ rows: object[], total: number }>} Matched portfolio item DTOs and result count.
 */
export async function searchPortfolio({ query, bankAmount, limit = 50 }) {
  const { rows } = await repo.findPortfolioBySearch({ query, bankAmount, limit });
  return {
    rows:  rows.map(r => toPortfolioItemDTO(r, false)),
    total: rows.length,
  };
}

/**
 * Loads the full invoice selection panel for a bank transaction.
 *
 * Detects the transaction scenario from bank status, trans_type, establishment
 * name, and algorithm suggestions, then fetches the appropriate suggested and
 * complementary invoice lists. The seven supported scenarios are:
 *   - REVIEW_TRANSFER_MATCHED       — algorithm-matched transfer
 *   - REVIEW_CARD_PARKING           — card settlement, parking invoices
 *   - REVIEW_CARD_VIP_ASSISTANCE    — card settlement, VIP/assistance invoices
 *   - PENDING_WITH_SUGGESTIONS      — low-confidence algorithm suggestions
 *   - PENDING_DEPOSIT_NORMAL        — deposit with known customer
 *   - PENDING_DEPOSIT_NO_INVOICES   — deposit flagged with no open invoices
 *   - PENDING_UNKNOWN_CLIENT        — unidentified customer, universal search
 *
 * @param {object} params
 * @param {number} params.stgId   - Staging ID of the bank transaction.
 * @param {number} [params.page]  - Page number for complementary invoice list (default 1).
 * @param {number} [params.limit] - Page size for complementary invoice list (default 50).
 * @returns {Promise<object>} Panel data: scenario, bankContext, suggestedItems,
 *   complementaryItems, pagination, and uiConfig.
 * @throws {404} If the bank transaction is not found.
 */
export async function loadPortfolioForTransaction({ stgId, page = 1, limit = 50 }) {
  const bankTx = await repo.findBankTransactionById(stgId);
  if (!bankTx) {
    throw Object.assign(
      new Error(`Bank transaction stg_id=${stgId} not found`),
      { status: 404 }
    );
  }

  const matchedIds = parseMatchedIds(bankTx.matched_portfolio_ids);
  const scenario   = detectScenario(bankTx);
  const bankAmount = parseFloat(bankTx.amount_total);

  const bankContext = {
    stgId:             bankTx.stg_id,
    bankRef1:          bankTx.bank_ref_1 || bankTx.sap_description,
    amountTotal:       bankAmount,
    currency:          bankTx.currency,
    transType:         bankTx.trans_type,
    globalCategory:    bankTx.global_category,
    establishmentName: bankTx.establishment_name,
    brand:             bankTx.brand,
    settlementId:      bankTx.settlement_id,
    customerCode:      bankTx.enrich_customer_id,
    customerName:      bankTx.enrich_customer_name,
    confidenceScore:   parseFloat(bankTx.enrich_confidence_score) || 0,
    matchMethod:       bankTx.match_method,
    matchConfidence:   parseFloat(bankTx.match_confidence_score)  || 0,
    reconcileReason:   bankTx.reconcile_reason,
    algorithmNote:     bankTx.enrich_notes,
    matchedIds,
  };

  let suggestedItems     = [];
  let complementaryItems = [];
  let totalComplementary = 0;
  let uiConfig           = {};

  switch (scenario) {

    case SCENARIOS.REVIEW_TRANSFER_MATCHED: {
      if (matchedIds.length > 0) {
        const suggested = await repo.findPortfolioByIds(matchedIds);
        suggestedItems  = suggested.map(r => toPortfolioItemDTO(r, true));
      }
      if (bankTx.enrich_customer_id &&
          !UNKNOWN_CUSTOMER_CODES.includes(bankTx.enrich_customer_id)) {
        const { rows, total } = await repo.findPortfolioByCustomer({
          customerCode: bankTx.enrich_customer_id,
          excludeIds:   matchedIds,
          bankAmount,
          page,
          limit,
        });
        complementaryItems = rows.map(r => toPortfolioItemDTO(r, false));
        totalComplementary = total;
      }
      uiConfig = {
        allowUniversalSearch: false,
        requireOverrideNote:  false,
        showInfoBanner:       false,
        infoBannerMessage:    null,
        glAccountFilter:      null,
      };
      break;
    }

    case SCENARIOS.REVIEW_CARD_PARKING: {
      if (matchedIds.length > 0) {
        const suggested = await repo.findPortfolioByIds(matchedIds);
        suggestedItems  = suggested.map(r => toPortfolioItemDTO(r, true));
      }
      const tcSuggestions = await loadTcSuggestions(bankTx.settlement_id, matchedIds);
      suggestedItems = [...suggestedItems, ...tcSuggestions.map(r => toPortfolioItemDTO(r, false))];
      const allExcludeIds = [...matchedIds, ...tcSuggestions.map(r => r.stg_id)];
      const rows = await repo.findPortfolioByGlAccount({
        glAccount:    GL_ACCOUNTS.PARKING,
        customerCode: bankTx.enrich_customer_id,
        excludeIds:   allExcludeIds,
        bankAmount,
        page,
        limit,
      });
      complementaryItems = rows.map(r => toPortfolioItemDTO(r, false));
      totalComplementary = rows.length;
      uiConfig = {
        allowUniversalSearch: false,
        requireOverrideNote:  false,
        showInfoBanner:       true,
        infoBannerMessage:    `Parking invoices for ${bankTx.enrich_customer_name}. Select the missing vouchers.`,
        glAccountFilter:      GL_ACCOUNTS.PARKING,
      };
      break;
    }

    case SCENARIOS.REVIEW_CARD_VIP_ASSISTANCE: {
      if (matchedIds.length > 0) {
        const suggested = await repo.findPortfolioByIds(matchedIds);
        suggestedItems  = suggested.map(r => toPortfolioItemDTO(r, true));
      }
      const tcSuggestions = await loadTcSuggestions(bankTx.settlement_id, matchedIds);
      suggestedItems = [...suggestedItems, ...tcSuggestions.map(r => toPortfolioItemDTO(r, false))];
      const allExcludeIds = [...matchedIds, ...tcSuggestions.map(r => r.stg_id)];
      const rows = await repo.findPortfolioByGlAccount({
        glAccount:  GL_ACCOUNTS.VIP_ASSISTANCE,
        excludeIds: allExcludeIds,
        bankAmount,
        page,
        limit,
      });
      complementaryItems = rows.map(r => toPortfolioItemDTO(r, false));
      totalComplementary = rows.length;
      uiConfig = {
        allowUniversalSearch: false,
        requireOverrideNote:  false,
        showInfoBanner:       suggestedItems.length === 0,
        infoBannerMessage:    suggestedItems.length === 0
          ? 'No algorithm suggestions available. Select from VIP/Assistance invoices below.'
          : null,
        glAccountFilter: GL_ACCOUNTS.VIP_ASSISTANCE,
      };
      break;
    }

    case SCENARIOS.PENDING_WITH_SUGGESTIONS: {
      if (matchedIds.length > 0) {
        const suggested = await repo.findPortfolioByIds(matchedIds);
        suggestedItems  = suggested.map(r => toPortfolioItemDTO(r, false));
      }
      if (bankTx.enrich_customer_id &&
          !UNKNOWN_CUSTOMER_CODES.includes(bankTx.enrich_customer_id)) {
        const { rows, total } = await repo.findPortfolioByCustomer({
          customerCode: bankTx.enrich_customer_id,
          excludeIds:   matchedIds,
          bankAmount,
          page,
          limit,
        });
        complementaryItems = rows.map(r => toPortfolioItemDTO(r, false));
        totalComplementary = total;
      }
      uiConfig = {
        allowUniversalSearch: false,
        requireOverrideNote:  false,
        showInfoBanner:       true,
        infoBannerMessage:    bankTx.enrich_notes ||
          'Algorithm found possible matches with low confidence. Please verify before approving.',
        glAccountFilter: null,
      };
      break;
    }

    case SCENARIOS.PENDING_DEPOSIT_NORMAL: {
      const { rows, total } = await repo.findPortfolioByCustomer({
        customerCode: bankTx.enrich_customer_id,
        excludeIds:   [],
        bankAmount,
        page,
        limit,
      });
      complementaryItems = rows.map(r => toPortfolioItemDTO(r, false));
      totalComplementary = total;
      uiConfig = {
        allowUniversalSearch: false,
        requireOverrideNote:  false,
        showInfoBanner:       false,
        infoBannerMessage:    null,
        glAccountFilter:      null,
      };
      break;
    }

    case SCENARIOS.PENDING_DEPOSIT_NO_INVOICES: {
      if (bankTx.enrich_customer_id &&
          !UNKNOWN_CUSTOMER_CODES.includes(bankTx.enrich_customer_id)) {
        const { rows, total } = await repo.findPortfolioByCustomer({
          customerCode: bankTx.enrich_customer_id,
          excludeIds:   [],
          bankAmount,
          page,
          limit,
        });
        complementaryItems = rows.map(r => toPortfolioItemDTO(r, false));
        totalComplementary = total;
      }
      uiConfig = {
        allowUniversalSearch: true,
        requireOverrideNote:  true,
        showInfoBanner:       true,
        infoBannerMessage:    bankTx.enrich_notes ||
          'No invoices found for this customer. A justification note is required if you proceed.',
        glAccountFilter: null,
      };
      break;
    }

    case SCENARIOS.PENDING_UNKNOWN_CLIENT:
    default: {
      const { rows, total } = await repo.findPortfolioUniversal({
        bankAmount,
        page,
        limit,
      });
      complementaryItems = rows.map(r => toPortfolioItemDTO(r, false));
      totalComplementary = total;
      uiConfig = {
        allowUniversalSearch: true,
        requireOverrideNote:  false,
        showInfoBanner:       true,
        infoBannerMessage:    'Customer not identified. Showing all open invoices ordered by amount proximity.',
        glAccountFilter:      null,
      };
      break;
    }
  }

  return {
    scenario,
    bankContext,
    suggestedItems,
    complementaryItems,
    pagination: buildPagination(totalComplementary, page, limit),
    uiConfig,
  };
}

/**
 * Validates that all selected invoices are still available for reconciliation.
 *
 * Checks that each invoice is in a selectable status (PENDING, ENRICHED, or REVIEW).
 * Guards against concurrent selection conflicts where another analyst may have
 * claimed an invoice between the panel load and the approval submission.
 *
 * @param {number[]} stgIds - Staging IDs of the invoices to validate.
 * @returns {Promise<{ valid: true, items: object[] }>} Validation result and full invoice rows.
 * @throws {400} If the stgIds array is empty.
 * @throws {409} If any invoice is no longer in a selectable status.
 */
export async function validateSelection(stgIds) {
  if (!Array.isArray(stgIds) || stgIds.length === 0) {
    throw Object.assign(
      new Error('At least one invoice must be selected'),
      { status: 400 }
    );
  }
  const validation = await repo.validatePortfolioItemsSelectable(stgIds);
  if (!validation.valid) {
    const list = validation.invalid
      .map(i => `stg_id ${i.stg_id} (${i.reconcile_status})`)
      .join(', ');
    throw Object.assign(
      new Error(`The following invoices are no longer available: ${list}`),
      { status: 409 }
    );
  }
  return { valid: true, items: validation.rows };
}