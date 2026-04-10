// src/components/layout/Header.jsx
import { useLocation }  from 'react-router-dom';
import { useAuthStore } from '../../stores/auth.store.js';
import NotificationBell from './NotificationBell.jsx';

const PAGE_META = {
  '/':               { title: 'Overview',    sub: 'Transaction queue and daily reconciliation status' },
  '/workspace':      { title: 'Reconciliation Workspace', sub: 'Review, distribute and approve bank transactions' },
  '/submit-posting': { title: 'Submit for Posting',    sub: 'Export approved transactions to SAP F-28 format' },
  '/ingestion':      { title: 'Data Ingestion',        sub: 'Upload source files and run the automation pipeline' },
  '/reports':        { title: 'Report Center',         sub: 'Export reconciliation data to Excel spreadsheets' },
};

export default function Header() {
  const user     = useAuthStore(s => s.user);
  const location = useLocation();
  const meta     = PAGE_META[location.pathname] || { title: 'PACIOLI', sub: '' };
  
  const isDataMode = ['/', '/workspace', '/submit-posting', '/reports', '/ingestion'].includes(location.pathname);

  const today = new Date().toLocaleDateString('en-US', {
    weekday: 'long', year: 'numeric', month: 'long', day: 'numeric',
  });

  const initials = (user?.fullName || user?.username || 'U')
    .split(' ').map(w => w[0]).slice(0, 2).join('').toUpperCase();

  return (
    <header
      className="shrink-0 h-16 flex items-center justify-between px-8 sticky top-0 z-40 transition-all"
      style={{
        backgroundColor: isDataMode ? 'var(--color-report-surface)' : 'var(--color-vault-surface)',
        borderBottom:    '1px solid ' + (isDataMode ? 'var(--color-report-border)' : 'var(--color-vault-border)'),
      }}
    >
      {/* ── IZQUIERDA: Títulos ────────────────────────────────────────────── */}
      <div className="flex flex-col min-w-0 justify-center">
        <h1 
          className="font-bold truncate" 
          style={{ 
            color: isDataMode ? 'var(--color-report-text)' : 'var(--color-vault-text)', 
            fontSize: '1.15rem', letterSpacing: '-0.01em' 
          }}
        >
          {meta.title}
        </h1>
        {meta.sub && (
          <span className="truncate hidden md:block" style={{ 
            color: isDataMode ? 'var(--color-report-text2)' : 'var(--color-vault-text2)', 
            fontSize: '0.78rem', marginTop: '1px' 
          }}>
            {meta.sub}
          </span>
        )}
      </div>

      {/* ── DERECHA: Herramientas y Usuario ───────────────────────────────── */}
      <div className="flex items-center gap-4 shrink-0">
        
        {/* Fecha (Solo desktop) */}
        <span className="hidden lg:block font-medium" style={{ 
          color: isDataMode ? 'var(--color-report-text2)' : 'var(--color-vault-text3)', 
          fontSize: '0.75rem' 
        }}>
          {today}
        </span>

        {/* Divisor Vertical */}
        <div className="w-px h-5 hidden lg:block" style={{ 
          background: isDataMode ? 'var(--color-report-border)' : 'var(--color-vault-border)' 
        }} />

        {/* Campana de Notificaciones */}
        <NotificationBell user={user} />

        {/* Divisor Vertical */}
        <div className="w-px h-5" style={{ 
          background: isDataMode ? 'var(--color-report-border)' : 'var(--color-vault-border)' 
        }} />

        {/* Rol y Avatar Corporativo */}
        <div className="flex items-center gap-2.5">
          <div className="text-right hidden sm:block">
             <p className="font-semibold" style={{ 
               color: isDataMode ? 'var(--color-report-text)' : 'var(--color-vault-text)', 
               fontSize: '0.8rem', lineHeight: 1.2 
             }}>
                {user?.fullName || user?.username}
             </p>
             <p className="uppercase font-bold" style={{ 
               color: isDataMode ? 'var(--color-report-text2)' : 'var(--color-vault-text2)', 
               fontSize: '0.65rem', letterSpacing: '0.05em' 
             }}>
                {user?.role}
             </p>
          </div>
          
          {/* Avatar Squircle Claro/Oscuro */}
          <div
            className="w-9 h-9 rounded-lg flex items-center justify-center font-bold font-mono shadow-sm"
            style={{
              background: isDataMode ? 'var(--color-report-base)' : 'var(--color-vault-orange-dim)',
              color:      isDataMode ? 'var(--color-report-accent)' : 'var(--color-vault-orange-soft)',
              border:     '1px solid ' + (isDataMode ? 'var(--color-report-border)' : 'var(--color-vault-border)'),
              fontSize:   '0.8rem',
            }}
            title={user?.fullName || user?.username}
          >
            {initials}
          </div>
        </div>
      </div>
    </header>
  );
}