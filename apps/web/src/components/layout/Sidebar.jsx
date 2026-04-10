// src/components/layout/Sidebar.jsx
import { NavLink, useNavigate } from 'react-router-dom';
import {
  LayoutDashboard, GitMerge, Upload,
  SendToBack, FileBarChart, LogOut,
  ChevronLeft, ChevronRight,
} from 'lucide-react';
import { useAuthStore }  from '../../stores/auth.store.js';
import { useUIStore }    from '../../stores/ui.store.js';
import { logoutRequest } from '../../api/endpoints/auth.api.js';

const POSTING_ROLES = ['admin', 'senior_analyst'];

const navItems = [
  { to: '/',               icon: LayoutDashboard, label: 'Overview',           end: true },
  { to: '/workspace',      icon: GitMerge,        label: 'Reconciliation'                },
  { to: '/submit-posting', icon: SendToBack,      label: 'Submit for Posting', roles: POSTING_ROLES },
  { to: '/ingestion',      icon: Upload,          label: 'Data Ingestion'                },
  { to: '/reports',        icon: FileBarChart,    label: 'Reports'                       },
];

export default function Sidebar() {
  const navigate   = useNavigate();
  const clearAuth  = useAuthStore(s => s.clearAuth);
  const user       = useAuthStore(s => s.user);
  const { sidebarExpanded, toggleSidebar } = useUIStore();

  async function handleLogout() {
    try { await logoutRequest(); } catch (err) { console.warn('[logout] server call failed:', err.message); }
    clearAuth();
    navigate('/login');
  }

  const initials = (user?.fullName || user?.username || 'U')
    .split(' ').map(w => w[0]).slice(0, 2).join('').toUpperCase();

  return (
    <aside
      className={`h-screen flex flex-col fixed left-0 top-0 z-50
                  transition-all duration-300 ease-in-out
                  ${sidebarExpanded ? 'w-60' : 'w-16'}`}
      style={{
        backgroundColor: 'var(--color-vault-sidebar)',
        borderRight:     '1px solid var(--color-vault-border)',
        boxShadow:       '4px 0 24px rgba(0,0,0,0.25)',
      }}
    >
      {/* ── LOGO ────────────────────────────────────────────────────────── */}
      <div
        className={`flex items-center h-16 shrink-0
                    ${sidebarExpanded ? 'px-5 justify-between' : 'px-0 justify-center'}`}
        style={{ borderBottom: '1px solid var(--color-vault-border)' }}
      >
        {sidebarExpanded && (
          <div className="flex flex-col mt-1">
            <span style={{ 
              fontFamily: 'var(--font-sans)', fontWeight: 800, fontSize: '1rem', 
              letterSpacing: '0.35em', color: 'var(--color-vault-text)', lineHeight: 1 
            }}>
              P A C I O L I
            </span>
            <span style={{ 
              fontFamily: 'var(--font-sans)', fontWeight: 500, fontSize: '0.62rem', 
              letterSpacing: '0.12em', textTransform: 'uppercase', color: 'var(--color-vault-orange-soft)', 
              marginTop: '4px' 
            }}>
              Reconciliation System
            </span>
          </div>
        )}

        <button
          onClick={toggleSidebar}
          title={sidebarExpanded ? 'Collapse' : 'Expand'}
          className={`w-7 h-7 rounded-md flex items-center justify-center transition-all shrink-0
                      ${!sidebarExpanded ? 'mx-auto' : ''}`}
          style={{ color: 'var(--color-vault-text2)', background: 'var(--color-vault-surface2)' }}
          onMouseEnter={e => {
            e.currentTarget.style.background = 'var(--color-vault-surface3)';
            e.currentTarget.style.color      = 'var(--color-vault-text)';
          }}
          onMouseLeave={e => {
            e.currentTarget.style.background = 'var(--color-vault-surface2)';
            e.currentTarget.style.color      = 'var(--color-vault-text2)';
          }}
        >
          {sidebarExpanded ? <ChevronLeft size={16} /> : <ChevronRight size={16} />}
        </button>
      </div>

      {/* ── NAVEGACIÓN ──────────────────────────────────────────────────── */}
      <nav className="flex-1 px-3 py-6 space-y-1.5 overflow-hidden">
        {navItems.filter(item => !item.roles || item.roles.includes(user?.role)).map(({ to, icon: Icon, label, end }) => (
          <NavLink key={to} to={to} end={end} title={!sidebarExpanded ? label : undefined}>
            {({ isActive }) => (
              <div
                className={`relative flex items-center rounded-lg
                            transition-all duration-200 cursor-pointer select-none
                            ${sidebarExpanded ? 'gap-3 px-3 py-2.5' : 'justify-center py-3'}`}
                style={{
                  color:           isActive ? 'var(--color-vault-text)' : 'var(--color-vault-text2)',
                  backgroundColor: isActive ? 'var(--color-vault-surface2)' : 'transparent',
                  fontSize:        '0.85rem',
                  fontWeight:      isActive ? 600 : 500,
                }}
                onMouseEnter={e => {
                  if (!isActive) {
                    e.currentTarget.style.backgroundColor = 'var(--color-vault-surface2)';
                    e.currentTarget.style.color           = 'var(--color-vault-text)';
                  }
                }}
                onMouseLeave={e => {
                  if (!isActive) {
                    e.currentTarget.style.backgroundColor = 'transparent';
                    e.currentTarget.style.color           = 'var(--color-vault-text2)';
                  }
                }}
              >
                {/* Indicador lateral naranja solar */}
                {isActive && (
                  <div
                    className="absolute left-0 rounded-r-md"
                    style={{
                      top: '15%', bottom: '15%', width: '3px',
                      background: 'var(--color-vault-orange)',
                      boxShadow: '0 0 10px var(--color-vault-orange)'
                    }}
                  />
                )}

                <Icon size={18} className="shrink-0" style={{ color: isActive ? 'var(--color-vault-orange-soft)' : 'var(--color-vault-text3)' }} />
                {sidebarExpanded && <span className="truncate tracking-wide">{label}</span>}
              </div>
            )}
          </NavLink>
        ))}
      </nav>

      {/* ── USUARIO + LOGOUT ─────────────────────────────────────────────── */}
      <div className="px-3 py-4 shrink-0" style={{ borderTop: '1px solid var(--color-vault-border)' }}>
        {sidebarExpanded && (
          <div className="flex items-center gap-3 px-2 py-2 mb-2 bg-black/20 rounded-lg border border-white/5">
            <div
              className="w-8 h-8 rounded-md flex items-center justify-center font-bold shrink-0 font-mono"
              style={{ background: 'var(--color-vault-orange-dim)', color: 'var(--color-vault-orange-soft)', fontSize: '0.75rem' }}
            >
              {initials}
            </div>
            <div className="min-w-0 flex-1">
              <p className="truncate font-semibold text-white" style={{ fontSize: '0.8rem' }}>
                {user?.fullName || user?.username}
              </p>
              <p className="capitalize truncate" style={{ color: 'var(--color-vault-text2)', fontSize: '0.7rem' }}>
                {user?.role}
              </p>
            </div>
          </div>
        )}

        <button
          onClick={handleLogout}
          title={!sidebarExpanded ? 'Sign out' : undefined}
          className={`flex items-center w-full rounded-lg transition-all duration-200
                      ${sidebarExpanded ? 'gap-3 px-3 py-2.5' : 'justify-center py-3'}`}
          style={{ color: 'var(--color-vault-text2)', fontSize: '0.85rem', fontWeight: 500 }}
          onMouseEnter={e => {
            e.currentTarget.style.background = 'var(--color-vault-red-dim)';
            e.currentTarget.style.color      = 'var(--color-vault-red)';
          }}
          onMouseLeave={e => {
            e.currentTarget.style.background = 'transparent';
            e.currentTarget.style.color      = 'var(--color-vault-text2)';
          }}
        >
          <LogOut size={16} className="shrink-0" />
          {sidebarExpanded && <span>Sign out securely</span>}
        </button>
      </div>
    </aside>
  );
}