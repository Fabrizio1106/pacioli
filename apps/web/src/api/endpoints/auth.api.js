// src/api/endpoints/auth.api.js
import apiClient from '../client.js';

// Login — recibe credenciales, retorna token y usuario
export async function loginRequest({ username, password }) {
  const response = await apiClient.post('/auth/login', {
    username,
    password,
  });
  return response.data;
}

// Me — obtener perfil del usuario actual
export async function getMeRequest() {
  const response = await apiClient.get('/auth/me');
  return response.data;
}

// Logout
export async function logoutRequest() {
  const response = await apiClient.post('/auth/logout');
  return response.data;
}