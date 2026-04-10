// src/api/endpoints/gold-export.api.js
import apiClient from '../client.js';

// Preview de lo que se exportará
export async function getExportPreviewRequest() {
  const response = await apiClient.get('/gold-export/preview');
  return response.data;
}

// Submit for Posting — el botón principal
export async function submitForPostingRequest() {
  const response = await apiClient.post('/gold-export/submit');
  return response.data;
}

// Historial de batches
export async function getBatchHistoryRequest(date) {
  const response = await apiClient.get('/gold-export/batches', {
    params: date ? { date } : {},
  });
  return response.data;
}