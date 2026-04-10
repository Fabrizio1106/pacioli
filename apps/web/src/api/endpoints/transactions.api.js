// src/api/endpoints/transactions.api.js
import apiClient from '../client.js';

// Lista paginada con filtros
export async function getTransactionsRequest(params = {}) {
  const response = await apiClient.get('/transactions', { params });
  return response.data;
}

// Resumen de estados para dashboard
export async function getTransactionsSummaryRequest() {
  const response = await apiClient.get('/transactions/summary');
  return response.data;
}

// Detalle completo de una transacción
export async function getTransactionDetailRequest(stgId) {
  const response = await apiClient.get(`/transactions/${stgId}`);
  return response.data;
}