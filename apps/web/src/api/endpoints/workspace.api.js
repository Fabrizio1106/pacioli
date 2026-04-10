// src/api/endpoints/workspace.api.js
import apiClient from '../client.js';

export async function getMyQueueRequest() {
  const response = await apiClient.get('/workspace/my-queue');
  return response.data;
}

export async function getPanelRequest(stgId) {
  const response = await apiClient.get(`/workspace/${stgId}/panel`);
  return response.data;
}

export async function calculateRequest(stgId, payload) {
  const response = await apiClient.post(`/workspace/${stgId}/calculate`, payload);
  return response.data;
}

export async function approveRequest(stgId, payload) {
  const response = await apiClient.post(`/workspace/${stgId}/approve`, payload);
  return response.data;
}

export async function acquireLockRequest(bankRef1) {
  const response = await apiClient.post(`/locks/${encodeURIComponent(bankRef1)}/acquire`);
  return response.data;
}

export async function releaseLockRequest(bankRef1) {
  const response = await apiClient.delete(`/locks/${encodeURIComponent(bankRef1)}`);
  return response.data;
}

export async function renewLockRequest(bankRef1) {
  const response = await apiClient.patch(`/locks/${encodeURIComponent(bankRef1)}/renew`);
  return response.data;
}