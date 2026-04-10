// src/stores/ui.store.js
// UI preferences store — persists across sessions via localStorage
import { create } from 'zustand';

const STORAGE_KEY = 'pacioli_ui_prefs';

function loadPrefs() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch {
    return {};
  }
}

function savePrefs(prefs) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(prefs));
  } catch {}
}

export const useUIStore = create((set, get) => ({
  // Sidebar — expanded by default (first time), then remembers preference
  sidebarExpanded: loadPrefs().sidebarExpanded ?? true,

  toggleSidebar: () => {
    const next = !get().sidebarExpanded;
    savePrefs({ ...loadPrefs(), sidebarExpanded: next });
    set({ sidebarExpanded: next });
  },

  setSidebar: (value) => {
    savePrefs({ ...loadPrefs(), sidebarExpanded: value });
    set({ sidebarExpanded: value });
  },
}));