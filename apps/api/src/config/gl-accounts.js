// src/config/gl-accounts.js
// General Ledger accounts for SAP F-28 Gold Layer
// Edit this file to update accounts without touching business logic

export const GL_ACCOUNTS = {

  // Fixed SAP F-28 configuration
  f28: {
    docClass:       'DZ',
    companyCode:    '8000',
    currency:       'USD',
    transactionSap: 'F-28',
  },

  // Bank GL accounts
  // Default: 1110213001 covers all current transactions
  // Future: 1110213002 for auxiliary account (resolved from pipeline table)
  bankGlAccounts: {
    primary:   '1110213001',
    secondary: '1110213002',
  },

  // Adjustment accounts for payment_diff table
  // sap_posting_key:
  //   '40' = debit (expense/withholding) — always for commissions and taxes
  //   '50' = credit (income)             — possible for diff_cambiario
  //   'DYNAMIC' = calculated at runtime based on diff sign
  adjustments: {
    final_amount_commission: {
      gl_account:      '5540101004',
      sap_posting_key: '40',
      description:     'Bank card commission',
    },
    final_amount_tax_iva: {
      gl_account:      '1140105023',
      sap_posting_key: '40',
      description:     'VAT withholding',
    },
    final_amount_tax_irf: {
      gl_account:      '140104019',
      sap_posting_key: '40',
      description:     'Income tax withholding (IRF)',
    },
    diff_cambiario: {
      gl_account:      '3710101001',
      // Runtime logic in gold-export.service.js:
      // diff_adjustment > 0 → '50' (income, bank paid more)
      // diff_adjustment < 0 → '40' (expense, bank paid less)
      sap_posting_key: 'DYNAMIC',
      description:     'Exchange rate differential',
    },
    other_income: {
      gl_account:      '3610401016',
      sap_posting_key: '40',
      description:     'Other income',
    },
  },

};

// Helper: resolve sap_posting_key at runtime for diff_cambiario
export function resolvePostingKey(adjustmentType, diffAmount = 0) {
  const config = GL_ACCOUNTS.adjustments[adjustmentType];
  if (!config) throw new Error(`Unknown adjustment type: ${adjustmentType}`);

  if (config.sap_posting_key !== 'DYNAMIC') {
    return config.sap_posting_key;
  }

  // DYNAMIC: diff_cambiario
  // Positive diff → bank paid more → income → credit '50'
  // Negative diff → bank paid less → expense → debit '40'
  return parseFloat(diffAmount) >= 0 ? '50' : '40';
}
