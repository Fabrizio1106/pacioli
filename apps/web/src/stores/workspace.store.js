// src/stores/workspace.store.js
import { create } from 'zustand';

export const useWorkspaceStore = create((set, get) => ({
  // ─── Transacción abierta ──────────────────
  activeTransaction:  null,  // objeto completo de la transacción bancaria
  portfolioPanel:     null,  // respuesta del endpoint for-transaction

  // ─── Selección de facturas ────────────────
  selectedInvoiceIds: [],    // array de stg_ids seleccionados

  // ─── Calculadora ──────────────────────────
  calculatorResult:   null,  // respuesta del endpoint calculate

  // ─── Distribución de diferencial ─────────
  diffAccountCode:    null,
  diffAmount:         null,

  // ─── Acciones ─────────────────────────────

  // Abrir una transacción en el workspace
  openTransaction: (transaction) => set({
    activeTransaction:  transaction,
    portfolioPanel:     null,
    selectedInvoiceIds: [],
    calculatorResult:   null,
    diffAccountCode:    null,
    diffAmount:         null,
  }),

  // Guardar la respuesta del panel de cartera
  setPortfolioPanel: (panel) => {
    // Pre-seleccionar las facturas sugeridas automáticamente
    const preSelected = panel.suggestedItems
      .filter(item => item.preSelected)
      .map(item => item.id);

    set({
      portfolioPanel:     panel,
      selectedInvoiceIds: preSelected,
    });
  },

  // Toggle de selección de una factura
  toggleInvoice: (invoiceId) => {
    const current = get().selectedInvoiceIds;
    const isSelected = current.includes(invoiceId);

    set({
      selectedInvoiceIds: isSelected
        ? current.filter(id => id !== invoiceId)
        : [...current, invoiceId],
      // Resetear calculadora cuando cambia la selección
      calculatorResult: null,
    });
  },

  // Guardar resultado de la calculadora
  setCalculatorResult: (result) => set({ calculatorResult: result }),

  // Guardar distribución de diferencial
  setDiffDistribution: ({ accountCode, amount }) => set({
    diffAccountCode: accountCode,
    diffAmount:      amount,
  }),

  // Limpiar workspace al cerrar
  closeTransaction: () => set({
    activeTransaction:  null,
    portfolioPanel:     null,
    selectedInvoiceIds: [],
    calculatorResult:   null,
    diffAccountCode:    null,
    diffAmount:         null,
  }),
}));