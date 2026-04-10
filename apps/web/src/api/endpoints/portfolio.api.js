// src/api/endpoints/portfolio.api.js
import apiClient from '../client.js';

// Cargar panel de cartera para una transacción específica
export async function getPortfolioForTransactionRequest(stgId, params = {}) {
  const response = await apiClient.get(
    `/portfolio/for-transaction/${stgId}`,
    { params }
  );
  return response.data;
}

// Validar selección antes de aprobar
export async function validateSelectionRequest(stgIds) {
  const response = await apiClient.post('/portfolio/validate-selection', {
    stg_ids: stgIds,
  });
  return response.data;
}