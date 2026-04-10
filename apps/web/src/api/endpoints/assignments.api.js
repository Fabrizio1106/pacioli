// src/api/endpoints/assignments.api.js
import apiClient from '../client.js';

export async function applyAssignmentRulesRequest() {
  const response = await apiClient.post('/admin/assignments/apply-rules');
  return response.data;
}
