// src/pages/Workspace/useWorkspaceReducer.js

export const initialState = {
  activeTx:           null,
  panel:              null,
  selectedIds:        [],
  adjustments: {
    commission:       '',
    taxIva:           '',
    taxIrf:           '',
    diffAccountCode:  '',
    diffAmount:       '',
  },
  isSplitPayment:     false,
  splitAppliedAmount: '',
  splitParentStgId:   null,
  isOverride:         false,
  overrideReason:     '',
  drawerOpen:         false,
  calculation:        null,
  lockActive:         false,
  isLoadingPanel:     false,
  isApproving:        false,
  error:              null,
};

export function workspaceReducer(state, action) {
  switch (action.type) {

    case 'SELECT_TX':
      return { ...initialState, activeTx: action.payload };

    case 'CLEAR_TX':
      return { ...initialState };

    case 'PANEL_LOADED': {
      const panel = action.payload;
      const preSelected = panel.suggestedItems
        .filter(item => item.preSelected)
        .map(item => item.id);
      return {
        ...state,
        panel,
        selectedIds:    preSelected,
        isLoadingPanel: false,
        error:          null,
      };
    }

    case 'PANEL_LOADING':
      return { ...state, isLoadingPanel: true, error: null };

    case 'PANEL_ERROR':
      return { ...state, isLoadingPanel: false, error: action.payload };

    case 'TOGGLE_INVOICE': {
      const id = action.payload;
      const isSelected = state.selectedIds.includes(id);
      return {
        ...state,
        selectedIds: isSelected
          ? state.selectedIds.filter(x => x !== id)
          : [...state.selectedIds, id],
        calculation: null,
      };
    }

    case 'SET_SELECTED_IDS':
      return { ...state, selectedIds: action.payload, calculation: null };

    case 'SET_ADJUSTMENT':
      // NO reseteamos calculation aquí — mantener el último cálculo válido
      // mientras el debounce espera antes de disparar el nuevo request.
      // Esto evita que el botón Approve parpadee/desaparezca en cada keystroke.
      return {
        ...state,
        adjustments: { ...state.adjustments, [action.key]: action.value },
      };

    case 'SET_ADJUSTMENTS_BULK':
      return {
        ...state,
        adjustments: { ...state.adjustments, ...action.payload },
        calculation: null,
      };

    case 'ENABLE_SPLIT':
      return {
        ...state,
        isSplitPayment:     true,
        splitParentStgId:   action.payload.parentStgId,
        splitAppliedAmount: '',
        drawerOpen:         true,
      };

    case 'SET_SPLIT_AMOUNT':
      return { ...state, splitAppliedAmount: action.payload, calculation: null };

    case 'DISABLE_SPLIT':
      return {
        ...state,
        isSplitPayment:     false,
        splitParentStgId:   null,
        splitAppliedAmount: '',
      };

    case 'SET_OVERRIDE':
      return {
        ...state,
        isOverride:     action.payload,
        overrideReason: action.payload ? state.overrideReason : '',
      };

    case 'SET_OVERRIDE_REASON':
      return { ...state, overrideReason: action.payload };

    case 'OPEN_DRAWER':
      return { ...state, drawerOpen: true };

    case 'CLOSE_DRAWER':
      return { ...state, drawerOpen: false };

    case 'CALCULATION_DONE':
      return { ...state, calculation: action.payload };

    case 'LOCK_ACQUIRED':
      return { ...state, lockActive: true };

    case 'LOCK_RELEASED':
      return { ...state, lockActive: false };

    case 'APPROVING':
      return { ...state, isApproving: true, error: null };

    case 'APPROVED':
      return { ...initialState };

    case 'APPROVE_ERROR':
      return { ...state, isApproving: false, error: action.payload };

    case 'SET_ERROR':
      return { ...state, error: action.payload };

    case 'CLEAR_ERROR':
      return { ...state, error: null };

    default:
      return state;
  }
}