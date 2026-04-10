// src/api/endpoints/reconciliation.api.js
import apiClient from '../client.js';

// Calcular balance — preview antes de aprobar
export async function calculateBalanceRequest(stgId, portfolioIds) {
  const response = await apiClient.post(
    `/reconciliation/${stgId}/calculate`,
    { portfolio_ids: portfolioIds }
  );
  return response.data;
}

// Aprobar match
export async function approveMatchRequest(stgId, payload) {
  const response = await apiClient.post(
    `/reconciliation/${stgId}/approve`,
    payload
  );
  return response.data;
}

// Revertir match
export async function reverseMatchRequest(stgId, reason) {
  const response = await apiClient.post(`/reversals/${stgId}`, {
    reversal_reason: reason,
  });
  return response.data;
}

// Transacciones aprobadas hoy
export async function getDailyApprovedRequest() {
  const response = await apiClient.get('/reversals/daily');
  return response.data;
}