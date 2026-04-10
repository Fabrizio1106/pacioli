// src/stores/auth.store.js
import { create } from 'zustand';

export const useAuthStore = create((set) => ({
  // ─── Estado inicial ───────────────────────
  user:          null,   // objeto con id, username, role, fullName
  token:         null,   // JWT string
  isAuthenticated: false,

  // ─── Inicializar desde localStorage ───────
  // Llamado al arrancar la app para restaurar sesión
  initAuth: () => {
    const token = localStorage.getItem('pacioli_token');
    const user  = localStorage.getItem('pacioli_user');

    if (token && user) {
      set({
        token,
        user:            JSON.parse(user),
        isAuthenticated: true,
      });
    }
  },

  // ─── Login ────────────────────────────────
  setAuth: ({ token, user }) => {
    // Guardar en localStorage para persistir entre recargas
    localStorage.setItem('pacioli_token', token);
    localStorage.setItem('pacioli_user', JSON.stringify(user));

    set({ token, user, isAuthenticated: true });
  },

  // ─── Logout ───────────────────────────────
  clearAuth: () => {
    localStorage.removeItem('pacioli_token');
    localStorage.removeItem('pacioli_user');

    set({ token: null, user: null, isAuthenticated: false });
  },
}));