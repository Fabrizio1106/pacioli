import apiClient from '../client.js';

export async function getOverviewRequest(params = {}) {
  const response = await apiClient.get('/overview', { params });
  return response.data;
}

export async function getAnalystsRequest() {
  const response = await apiClient.get('/users/analysts');
  return response.data;
}

export async function syncMatchesRequest() {
  const response = await apiClient.post('/overview/sync-matches');
  return response.data;
}

export async function updateAnalystNoteRequest(bankRef1, note) {
  const response = await apiClient.patch(
    `/overview/${encodeURIComponent(bankRef1)}/note`,
    { note }
  );
  return response.data;
}

export async function reassignTransactionRequest(bankRef1, toUserId) {
  const response = await apiClient.patch(
    `/admin/assignments/${encodeURIComponent(bankRef1)}/reassign`,
    { to_user_id: toUserId }
  );
  return response.data;
}