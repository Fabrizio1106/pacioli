// src/components/layout/AppLayout.jsx
import Sidebar     from './Sidebar.jsx';
import Header      from './Header.jsx';
import { useUIStore }  from '../../stores/ui.store.js';
import { useLocation } from 'react-router-dom';

// Páginas con tablas densas de datos — fondo claro tipo Excel
const DATA_MODE_ROUTES = ['/', '/workspace', '/submit-posting', '/reports', '/ingestion'];

export default function AppLayout({ children }) {
  const { sidebarExpanded } = useUIStore();
  const location            = useLocation();

  const sidebarWidth = sidebarExpanded ? 'ml-56' : 'ml-14';
  const isDataMode   = DATA_MODE_ROUTES.includes(location.pathname);

  // En modo datos el contenedor raíz también usa el fondo de reportes
  // para que no aparezca la franja oscura entre sidebar y contenido
  const rootBg = isDataMode ? 'var(--color-report-base)' : 'var(--color-vault-base)';

  return (
    <div
      className="h-screen flex overflow-hidden"
      style={{ backgroundColor: rootBg }}
    >
      <Sidebar />

      <div
        className={`flex-1 ${sidebarWidth} flex flex-col overflow-hidden
                    transition-all duration-200 ease-in-out`}
        style={{ backgroundColor: rootBg }}
      >
        <Header />

        <main
          className={`flex-1 overflow-y-auto p-6 ${isDataMode ? 'data-mode' : ''}`}
          style={isDataMode ? {
            backgroundColor: 'var(--color-report-base)',
            color:           'var(--color-report-text)',
          } : {
            backgroundColor: 'var(--color-vault-base)',
            color:           'var(--color-vault-text)',
          }}
        >
          {children}
        </main>
      </div>
    </div>
  );
}