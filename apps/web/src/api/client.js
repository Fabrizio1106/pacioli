// src/api/client.js
import axios from 'axios';

const BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:3000/api/v1';

const apiClient = axios.create({
  baseURL: BASE_URL,
  timeout: 30000,
  // NO ponemos Content-Type global aquí.
  // Para JSON lo agrega el interceptor.
  // Para FormData el navegador lo pone automáticamente con el boundary correcto.
});

// ─────────────────────────────────────────────
// INTERCEPTOR DE REQUEST
// ─────────────────────────────────────────────
apiClient.interceptors.request.use(
  (config) => {
    // JWT
    const token = localStorage.getItem('pacioli_token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }

    // Content-Type: solo forzar JSON cuando el body NO es FormData.
    // Si es FormData, dejar que el navegador ponga
    // "multipart/form-data; boundary=----WebKitFormBoundary..."
    // automáticamente — multer lo necesita así.
    if (!(config.data instanceof FormData)) {
      config.headers['Content-Type'] = 'application/json';
    }

    return config;
  },
  (error) => Promise.reject(error)
);

// ─────────────────────────────────────────────
// INTERCEPTOR DE RESPONSE
// ─────────────────────────────────────────────
apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('pacioli_token');
      localStorage.removeItem('pacioli_user');
      window.location.href = '/login';
    }
    return Promise.reject(error);
  }
);

export default apiClient;