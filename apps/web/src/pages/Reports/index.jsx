// src/pages/Reports/index.jsx
import { useState, useMemo } from 'react';
import { useQuery }          from '@tanstack/react-query';
import apiClient             from '../../api/client.js';
import {
  FileSpreadsheet, Download, RefreshCw, ChevronDown,
  BarChart2, Banknote, BookOpen, CreditCard, ParkingSquare,
  LayoutDashboard, FileText, Filter, AlertCircle, Loader2,
} from 'lucide-react';

// ─── Paleta Unificada (Basada en Overview) ────────────────────────────────────
// ─── Paleta Titanium & Orange (Modo Datos/Reportes) ──────────────────────────
const T = {
  base:        'var(--color-report-base)',
  surface:     'var(--color-report-surface)',
  surface2:    'var(--color-report-row-alt)',
  surface3:    '#E2E8F0', // Slate-200 para hover
  border:      'var(--color-report-border)',
  border2:     '#CBD5E1', // Slate-300
  thBg:        'var(--color-report-header)',
  thText:      '#E2E8F0',
  textPrimary: 'var(--color-report-text)',
  textSecond:  'var(--color-report-text2)',
  textMuted:   '#94A3B8', // Slate-400
  orange:      'var(--color-vault-orange)',
  orangeText:  '#FFFFFF',
  orangeBorder:'var(--color-vault-orange)',
  orangeBg:    'var(--color-vault-orange-glow)',
  orangeLight: 'var(--color-vault-orange-soft)',
  green:       'var(--color-vault-green)',
  greenText:   '#FFFFFF',
  greenBorder: 'var(--color-vault-green)',
  greenBg:     'var(--color-vault-green-glow)',
  greenLight:  'var(--color-vault-green-soft)',
  amber:       'var(--color-vault-amber)',
  amberBg:     'var(--color-vault-amber-dim)',
  yellow:      'var(--color-vault-amber)',
  blue:        'var(--color-vault-blue)',
  blueBg:      'var(--color-vault-blue-dim)',
  red:         'var(--color-vault-red)',
  redBg:       'var(--color-vault-red-dim)',
};

// ─── Status Badges (Sincronizados con el resto del sistema) ────────────────────
const STATUS_BADGE = {
  PENDING:             { bg: 'rgba(239, 68, 68, 0.12)', color: '#991B1B', border: 'rgba(239, 68, 68, 0.3)'  },
  REVIEW:              { bg: 'rgba(245, 158, 11, 0.12)', color: '#92400E', border: 'rgba(245, 158, 11, 0.3)'  },
  MATCHED:             { bg: 'rgba(16, 185, 129, 0.12)', color: '#065F46', border: 'rgba(16, 185, 129, 0.3)'  },
  MATCHED_MANUAL:      { bg: 'rgba(16, 185, 129, 0.12)', color: '#065F46', border: 'rgba(16, 185, 129, 0.3)'  },
  CLOSED_IN_SOURCE_SAP:{ bg: 'rgba(107, 114, 128, 0.12)',color: 'var(--color-vault-text3)', border: 'var(--color-report-border)' },
  ENRICHED:            { bg: 'rgba(59, 130, 246, 0.12)', color: '#1E40AF', border: 'rgba(59, 130, 246, 0.3)'  },
  ASSIGNED:            { bg: 'rgba(99, 102, 241, 0.12)', color: '#3730A3', border: 'rgba(99, 102, 241, 0.3)'  },
  CLOSED:              { bg: 'rgba(107, 114, 128, 0.10)',color: 'var(--color-vault-text3)', border: 'var(--color-report-border)' },
};

// ─── Catálogo de reportes ─────────────────────────────────────────────────────
const REPORTS = [
  { id: 'overview',         label: 'Overview',                icon: LayoutDashboard, endpoint: '/reports/overview',          export: '/reports/export/overview',          filters: ['dates'],                         description: 'Transaction queue totals' },
  { id: 'bank',             label: 'Bank Reconciliation',     icon: Banknote,        endpoint: '/reports/bank',              export: '/reports/export/bank',              filters: ['dates','bankStatus'],            description: 'Open bank transactions' },
  { id: 'portfolio',        label: 'Portfolio',               icon: BookOpen,        endpoint: '/reports/portfolio',         export: '/reports/export/portfolio',         filters: ['dates','portfolioStatus'],       description: 'SAP invoice portfolio' },
  { id: 'card-details',     label: 'Card Vouchers',           icon: CreditCard,      endpoint: '/reports/card-details',      export: '/reports/export/card-details',      filters: ['dates','brand','cardStatus'],    description: 'Individual voucher details' },
  { id: 'card-settlements', label: 'Card Settlements',        icon: FileSpreadsheet, endpoint: '/reports/card-settlements',  export: '/reports/export/card-settlements',  filters: ['dates','brand','cardStatus'],    description: 'Card batches and brands' },
  { id: 'parking',          label: 'Parking Breakdown',       icon: ParkingSquare,   endpoint: '/reports/parking',           export: '/reports/export/parking',           filters: ['dates','brand'],                 description: 'Parking lot details' },
  { id: 'summary',          label: 'Reconciliation Summary',  icon: BarChart2,       endpoint: '/reports/summary',           export: '/reports/export/summary',           filters: ['dates'],          isSummary: true, description: 'Executive totals by status' },
];

const BANK_STATUSES      = ['PENDING','REVIEW','MATCHED','MATCHED_MANUAL','CLOSED_IN_SOURCE_SAP'];
const PORTFOLIO_STATUSES = ['PENDING','ENRICHED','REVIEW','MATCHED'];
const CARD_STATUSES      = ['PENDING','ASSIGNED','MATCHED'];
const BRANDS             = ['VISA','DINERS CLUB','AMEX','PACIFICARD','MASTERCARD'];

const fmt = (n) => n == null ? '—'
  : typeof n === 'number'
    ? n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    : String(n);

const today      = new Date().toISOString().split('T')[0];
const monthStart = new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().split('T')[0];

function StatusBadge({ status }) {
  const s = STATUS_BADGE[status] || { bg: T.surface3, color: T.textMuted, border: T.border };
  return (
    <span style={{
      fontSize: '0.7rem', fontWeight: 600, padding: '3px 8px', borderRadius: '4px',
      background: s.bg, color: s.color, border: `1px solid ${s.border}`, whiteSpace: 'nowrap',
    }}>{status || '—'}</span>
  );
}

// ─── Report Card (Sidebar Item) ───────────────────────────────────────────────
function ReportCard({ report, isActive, onClick }) {
  const Icon = report.icon;
  return (
    <button onClick={onClick} style={{
      width: '100%', textAlign: 'left', borderRadius: '8px', padding: '12px 14px',
      cursor: 'pointer', transition: 'all 0.15s',
      border: 'none',
      background: isActive ? T.surface : 'transparent',
      boxShadow: isActive ? '0 1px 3px rgba(28,32,20,0.06)' : 'none',
      display: 'flex', alignItems: 'flex-start', gap: '12px',
      position: 'relative',
    }}>
      {isActive && <div style={{ position: 'absolute', left: 0, top: '15%', bottom: '15%', width: '3px', background: T.green, borderRadius: '0 4px 4px 0' }} />}
      <div style={{
        flexShrink: 0, marginTop: '2px',
        width: '28px', height: '28px', borderRadius: '6px',
        background: isActive ? T.greenBg : T.surface3, 
        border: `1px solid ${isActive ? T.greenBorder : 'transparent'}`,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
      }}>
        <Icon size={14} style={{ color: isActive ? T.green : T.textMuted }} />
      </div>
      <div style={{ minWidth: 0 }}>
        <p style={{
          fontSize: '0.88rem', fontWeight: isActive ? 600 : 500, lineHeight: 1.2,
          color: isActive ? T.textPrimary : T.textSecond, margin: 0,
        }}>
          {report.label}
        </p>
        <p style={{ color: isActive ? T.textSecond : T.textMuted, fontSize: '0.75rem', marginTop: '4px', lineHeight: 1.3 }}>
          {report.description}
        </p>
      </div>
    </button>
  );
}

// ─── MultiSelect filter ───────────────────────────────────────────────────────
function MultiSelect({ label, options, value, onChange }) {
  const [open, setOpen] = useState(false);
  const toggle = (v) => onChange(value.includes(v) ? value.filter(x => x !== v) : [...value, v]);

  return (
    <div style={{ position: 'relative' }}>
      <button onClick={() => setOpen(o => !o)} style={{
        display: 'flex', alignItems: 'center', gap: '6px',
        padding: '7px 14px', background: T.surface,
        border: `1px solid ${open ? T.greenBorder : T.border}`, borderRadius: '6px',
        fontSize: '0.8rem', color: value.length > 0 ? T.textPrimary : T.textSecond, fontWeight: value.length > 0 ? 600 : 500,
        cursor: 'pointer', minWidth: '140px', transition: 'all 0.1s',
      }}>
        <Filter size={14} style={{ color: value.length > 0 ? T.green : T.textMuted, flexShrink: 0 }} />
        <span style={{ flex: 1, textAlign: 'left', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {value.length === 0 ? label : `${label}: ${value.length} sel.`}
        </span>
        <ChevronDown size={14} style={{ color: T.textMuted, transform: open ? 'rotate(180deg)' : '' }} />
      </button>
      {open && (
        <>
          <div style={{ position: 'fixed', inset: 0, zIndex: 10 }} onClick={() => setOpen(false)} />
          <div style={{
            position: 'absolute', top: 'calc(100% + 4px)', left: 0, zIndex: 20,
            background: T.surface, border: `1px solid ${T.border}`,
            borderRadius: '8px', boxShadow: '0 4px 16px rgba(28,32,20,0.12)',
            minWidth: '220px', padding: '6px 0',
          }}>
            {options.map(opt => (
              <button key={opt} onClick={() => toggle(opt)} style={{
                width: '100%', display: 'flex', alignItems: 'center', gap: '10px',
                padding: '10px 14px', fontSize: '0.8rem', color: T.textPrimary,
                background: 'none', border: 'none', cursor: 'pointer', textAlign: 'left',
              }}
              onMouseEnter={e => e.currentTarget.style.background = T.surface2}
              onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
                <div style={{
                  width: '16px', height: '16px', borderRadius: '4px', flexShrink: 0,
                  border: `1px solid ${value.includes(opt) ? T.green : T.border2}`,
                  background: value.includes(opt) ? T.green : 'transparent',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                }}>
                  {value.includes(opt) && <span style={{ color: T.greenText, fontSize: '0.7rem', lineHeight: 1 }}>✓</span>}
                </div>
                {opt}
              </button>
            ))}
            {value.length > 0 && (
              <button onClick={() => onChange([])} style={{
                width: '100%', padding: '10px 14px', fontSize: '0.78rem', color: T.red, fontWeight: 500,
                borderTop: `1px solid ${T.border}`, marginTop: '4px',
                background: 'none', border: 'none', cursor: 'pointer', textAlign: 'left',
              }}
              onMouseEnter={e => e.currentTarget.style.background = T.redBg}
              onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
                Clear all selections
              </button>
            )}
          </div>
        </>
      )}
    </div>
  );
}

// ─── Summary View ─────────────────────────────────────────────────────────────
function SummaryView({ data }) {
  if (!data) return null;
  const { bank, portfolio, cards } = data;

  const Section = ({ title, rows, amtKey = 'total_amount', accentColor }) => (
    <div style={{ background: T.surface, border: `1px solid ${T.border}`, borderRadius: '10px', overflow: 'hidden' }}>
      <div style={{ padding: '12px 20px', borderBottom: `1px solid ${T.border}`, background: T.surface2, display: 'flex', alignItems: 'center', gap: '10px' }}>
        <div style={{ width: '4px', height: '16px', borderRadius: '2px', background: accentColor, flexShrink: 0 }} />
        <span style={{ fontSize: '0.8rem', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.08em', color: T.textPrimary }}>{title}</span>
      </div>
      <div>
        {rows.map((r, i) => (
          <div key={i} style={{
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            padding: '14px 20px', borderBottom: i < rows.length - 1 ? `1px solid ${T.border}` : 'none',
            background: i % 2 === 0 ? T.surface : T.surface2,
          }}>
            <StatusBadge status={r.reconcile_status} />
            <div style={{ display: 'flex', alignItems: 'center', gap: '40px', fontSize: '0.9rem' }}>
              <span style={{ color: T.textSecond }}>
                <span style={{ color: T.textPrimary, fontFamily: 'var(--font-mono)', fontWeight: 600 }}>
                  {parseInt(r.count).toLocaleString()}
                </span> rows
              </span>
              <span style={{ color: T.textPrimary, fontFamily: 'var(--font-mono)', fontWeight: 600, minWidth: '130px', textAlign: 'right', fontSize: '1rem' }}>
                ${parseFloat(r[amtKey] || 0).toLocaleString('en-US', { minimumFractionDigits: 2 })}
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );

  const brandMap = {};
  cards.forEach(c => {
    if (!brandMap[c.brand]) brandMap[c.brand] = [];
    brandMap[c.brand].push(c);
  });

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px', padding: '32px', maxWidth: '1100px', margin: '0 auto', width: '100%' }}>
      <Section title="Bank Transactions"   rows={bank}      accentColor={T.greenLight} />
      <Section title="Portfolio (Cartera)" rows={portfolio} accentColor={T.blue} />
      {Object.entries(brandMap).map(([brand, rows]) => (
        <Section key={brand} title={brand} rows={rows} amtKey="total_gross" accentColor={T.amber} />
      ))}
    </div>
  );
}

// ─── Data Table (Edge to Edge) ────────────────────────────────────────────────
function DataTable({ columns, rows, meta }) {
  if (!rows || rows.length === 0) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', padding: '64px 0', flex: 1 }}>
        <FileText size={40} style={{ color: T.border2, marginBottom: '16px', opacity: 0.5 }} />
        <p style={{ color: T.textMuted, fontSize: '0.95rem' }}>No data found for the selected filters</p>
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0 }}>
      {meta && (
        <div style={{ padding: '12px 24px', background: T.surface2, borderBottom: `1px solid ${T.border}` }}>
          <span style={{ color: T.textSecond, fontSize: '0.85rem' }}>
            Showing <strong style={{ color: T.textPrimary }}>{meta.showing.toLocaleString()}</strong>
            {meta.total > meta.showing && (
              <> of <strong style={{ color: T.amber }}>{meta.total.toLocaleString()}</strong></>
            )} rows
            {meta.total > meta.showing && (
              <span style={{ color: T.amber, opacity: 0.8 }}> — export to see all</span>
            )}
          </span>
        </div>
      )}
      <div style={{ flex: 1, overflow: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem' }}>
          <thead style={{ position: 'sticky', top: 0, zIndex: 10, background: T.thBg }}>
            <tr>
              {columns.map((col, i) => (
                <th key={col} style={{
                  padding: '12px 16px', textAlign: 'left',
                  color: T.thText, fontWeight: 700, fontSize: '0.75rem',
                  letterSpacing: '0.06em', textTransform: 'uppercase', whiteSpace: 'nowrap',
                  borderBottom: `1px solid ${T.greenBorder}`,
                  paddingLeft: i === 0 ? '24px' : '16px',
                }}>
                  {col.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr key={i} style={{ background: i % 2 === 0 ? T.surface : T.surface2, transition: 'background 0.1s' }}
                  onMouseEnter={e => e.currentTarget.style.background = T.surface3}
                  onMouseLeave={e => e.currentTarget.style.background = i % 2 === 0 ? T.surface : T.surface2}>
                {columns.map((col, j) => {
                  const val = row[col];
                  const isStatus = col === 'reconcile_status';
                  const isNum = typeof val === 'number' ||
                    ['amount','gross','net','commission','tax','adjustment','outstanding','conciliable','financial'].some(k => col.includes(k));

                  return (
                    <td key={col} style={{
                      padding: '10px 16px', borderBottom: `1px solid ${T.border}`,
                      color: T.textPrimary, maxWidth: '240px',
                      overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                      paddingLeft: j === 0 ? '24px' : '16px',
                    }}>
                      {isStatus ? (
                        <StatusBadge status={val} />
                      ) : isNum && val != null ? (
                        <span style={{ fontFamily: 'var(--font-mono)', display: 'block', textAlign: 'right' }}>
                          {fmt(parseFloat(val))}
                        </span>
                      ) : val == null || val === '' ? (
                        <span style={{ color: T.textMuted }}>—</span>
                      ) : col.includes('date') && val ? (
                        new Date(val).toLocaleDateString('en-US')
                      ) : (
                        String(val)
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Página principal ─────────────────────────────────────────────────────────
export default function ReportsPage() {
  const [activeId,    setActiveId]   = useState('summary');
  const [startDate,   setStartDate]  = useState(monthStart);
  const [endDate,     setEndDate]    = useState(today);
  const [statusFilter, setStatus]     = useState([]);
  const [brandFilter,  setBrand]      = useState([]);
  const [queried,      setQueried]    = useState(false);
  const [exporting,    setExporting]  = useState(false);
  const [exportError,  setExportError] = useState(null);

  const activeReport = REPORTS.find(r => r.id === activeId);

  const handleSelectReport = (id) => {
    setActiveId(id); setStatus([]); setBrand([]); setQueried(false);
  };

  const queryParams = useMemo(() => {
    const p = new URLSearchParams({ startDate, endDate, preview: 'true' });
    statusFilter.forEach(s => p.append('status', s));
    brandFilter.forEach(b  => p.append('brand',  b));
    return p.toString();
  }, [startDate, endDate, statusFilter, brandFilter]);

  const { data, isLoading, isError, refetch, isFetching } = useQuery({
    queryKey: ['reports', activeId, queryParams],
    queryFn:  async () => (await apiClient.get(`${activeReport.endpoint}?${queryParams}`)).data,
    enabled:  queried,
    staleTime: 30 * 1000,
  });

  const handleExport = async () => {
    setExporting(true);
    setExportError(null);
    try {
      const p = new URLSearchParams({ startDate, endDate });
      statusFilter.forEach(s => p.append('status', s));
      brandFilter.forEach(b  => p.append('brand',  b));
      const res  = await apiClient.get(`${activeReport.export}?${p}`, { responseType: 'blob' });
      const blob = res.data;
      const url  = URL.createObjectURL(blob);
      const a    = document.createElement('a');
      a.href     = url;
      a.download = res.headers['content-disposition']?.match(/filename="(.+)"/)?.[1]
                   || `PACIOLI_${activeId}_${startDate}_${endDate}.xlsx`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      setExportError(err.response?.data?.message || 'Export failed. Please try again.');
    } finally {
      setExporting(false);
    }
  };

  const tableColumns = useMemo(() => {
    if (!data?.data || !Array.isArray(data.data) || data.data.length === 0) return [];
    return Object.keys(data.data[0]);
  }, [data]);

  const hasFilters = activeReport?.filters || [];

  return (
    <div style={{ display: 'flex', height: '100%', background: T.base, borderRadius: '12px', overflow: 'hidden', border: `1px solid ${T.border}` }}>
      
      {/* ── COLUMNA IZQUIERDA: SIDEBAR DE REPORTES ── */}
      <div style={{
        width: '300px', flexShrink: 0, display: 'flex', flexDirection: 'column',
        background: T.surface2, borderRight: `1px solid ${T.border}`,
      }}>
        {/* ENCABEZADO FIJO IZQUIERDO: 76px */}
        <div style={{ 
          height: '76px', padding: '0 24px', borderBottom: `1px solid ${T.border}`, 
          display: 'flex', flexDirection: 'column', justifyContent: 'center' 
        }}>
          <h2 style={{ margin: 0, fontSize: '1.2rem', fontWeight: 600, color: T.textPrimary }}>Report Center</h2>
          <p style={{ margin: '2px 0 0', fontSize: '0.8rem', color: T.textSecond }}>Select a data module</p>
        </div>
        
        <div style={{ flex: 1, overflowY: 'auto', padding: '16px', display: 'flex', flexDirection: 'column', gap: '6px' }}>
          {REPORTS.map(r => (
            <ReportCard key={r.id} report={r} isActive={r.id === activeId} onClick={() => handleSelectReport(r.id)} />
          ))}
        </div>
      </div>

      {/* ── COLUMNA DERECHA: WORKSPACE ── */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minWidth: 0, background: T.surface }}>
        
        {/* ENCABEZADO FIJO DERECHO (COMMAND RIBBON): 76px */}
        <div style={{ 
          height: '76px', padding: '0 24px', borderBottom: `1px solid ${T.border}`, background: T.surface, 
          display: 'flex', alignItems: 'center', gap: '20px' 
        }}>
          <div>
            <h3 style={{ margin: 0, fontSize: '1.05rem', fontWeight: 600, color: T.textPrimary }}>{activeReport.label}</h3>
          </div>
          
          <div style={{ width: '1px', height: '32px', background: T.border, margin: '0 4px' }} />

          {/* Filtros Dinámicos */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '16px', flex: 1 }}>
            {[['From', startDate, setStartDate], ['To', endDate, setEndDate]].map(([lbl, val, setter]) => (
              <div key={lbl} style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{ color: T.textSecond, fontSize: '0.8rem', fontWeight: 500 }}>{lbl}</span>
                <input type="date" value={val} onChange={e => { setter(e.target.value); setQueried(false); }}
                  style={{ background: T.surface, border: `1px solid ${T.border2}`, borderRadius: '6px', padding: '7px 10px', color: T.textPrimary, fontSize: '0.85rem', outline: 'none' }} />
              </div>
            ))}
            {hasFilters.includes('bankStatus') && <MultiSelect label="Status" options={BANK_STATUSES} value={statusFilter} onChange={v => { setStatus(v); setQueried(false); }} />}
            {hasFilters.includes('portfolioStatus') && <MultiSelect label="Status" options={PORTFOLIO_STATUSES} value={statusFilter} onChange={v => { setStatus(v); setQueried(false); }} />}
            {hasFilters.includes('cardStatus') && <MultiSelect label="Status" options={CARD_STATUSES} value={statusFilter} onChange={v => { setStatus(v); setQueried(false); }} />}
            {hasFilters.includes('brand') && <MultiSelect label="Brand" options={BRANDS} value={brandFilter} onChange={v => { setBrand(v); setQueried(false); }} />}
          </div>

          {/* Acciones */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            {exportError && (
              <span style={{ fontSize: '0.8rem', color: T.red || '#f87171' }}>
                {exportError}
              </span>
            )}
            <button onClick={handleExport} disabled={exporting} style={{
              display: 'flex', alignItems: 'center', gap: '8px', padding: '9px 18px', borderRadius: '8px', fontSize: '0.85rem', fontWeight: 600, cursor: 'pointer', transition: 'all 0.15s',
              background: T.surface2, border: `1px solid ${T.border2}`, color: T.textPrimary,
            }} onMouseEnter={e => e.currentTarget.style.background = T.surface3} onMouseLeave={e => e.currentTarget.style.background = T.surface2}>
              {exporting ? <><Loader2 size={16} style={{ animation: 'spin 1s linear infinite' }} /> Exporting...</> : <><Download size={16} /> Export</>}
            </button>
            <button onClick={() => { setQueried(true); refetch(); }} disabled={isLoading || isFetching} style={{
              display: 'flex', alignItems: 'center', gap: '8px', padding: '9px 24px', borderRadius: '8px', fontSize: '0.85rem', fontWeight: 600, cursor: 'pointer', transition: 'all 0.15s',
              background: T.green, border: `1px solid ${T.greenBorder}`, color: T.greenText,
            }}>
              {isFetching ? <><Loader2 size={16} style={{ animation: 'spin 1s linear infinite' }} /> Loading...</> : <><RefreshCw size={16} /> Run Preview</>}
            </button>
          </div>
        </div>

        {/* CONTENIDO (Edge to Edge) */}
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0, background: T.surface }}>
          
          {!queried && (
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', flex: 1, gap: '20px' }}>
              <div style={{ padding: '24px', borderRadius: '24px', background: T.surface2, border: `1px solid ${T.border}` }}>
                {activeReport && <activeReport.icon size={48} style={{ color: T.textMuted }} />}
              </div>
              <div style={{ textAlign: 'center' }}>
                <p style={{ color: T.textPrimary, fontSize: '1.25rem', fontWeight: 600, margin: '0 0 8px' }}>{activeReport?.label}</p>
                <p style={{ color: T.textSecond, fontSize: '0.95rem', margin: 0 }}>Configure parameters and click <strong style={{ color: T.green }}>Run Preview</strong></p>
              </div>
            </div>
          )}

          {queried && isFetching && (
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', flex: 1, gap: '16px' }}>
              <Loader2 size={40} style={{ color: T.green, animation: 'spin 1s linear infinite' }} />
              <p style={{ color: T.textSecond, fontSize: '0.95rem', fontWeight: 500 }}>Fetching data cluster...</p>
            </div>
          )}

          {queried && isError && !isFetching && (
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', flex: 1, gap: '16px' }}>
              <AlertCircle size={40} style={{ color: T.red }} />
              <p style={{ color: T.textSecond, fontSize: '0.95rem', fontWeight: 500 }}>Failed to fetch cluster. Please try again.</p>
            </div>
          )}

          {queried && !isFetching && !isError && data && activeReport?.isSummary && (
            <div style={{ flex: 1, overflowY: 'auto' }}>
              <SummaryView data={data.data} />
            </div>
          )}

          {queried && !isFetching && !isError && data && activeId === 'overview' && (
            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
              <div style={{ padding: '24px 24px 20px', display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '20px', flexShrink: 0 }}>
                {[
                  { label: 'Pending',  count: data.data?.kpis?.pending_count,  amount: data.data?.kpis?.pending_amount,  color: T.amber },
                  { label: 'Review',   count: data.data?.kpis?.review_count,   amount: data.data?.kpis?.review_amount,   color: T.yellow },
                  { label: 'Matched',  count: data.data?.kpis?.matched_count,  amount: data.data?.kpis?.matched_amount,  color: T.greenLight  },
                  { label: 'Total',    count: data.data?.kpis?.total_count,    amount: data.data?.kpis?.total_amount,    color: T.blue },
                ].map(k => (
                  <div key={k.label} style={{ background: T.surface, border: `1px solid ${T.border}`, borderTop: `4px solid ${k.color}`, borderRadius: '10px', padding: '16px 20px', boxShadow: '0 1px 4px rgba(28,32,20,0.05)' }}>
                    <p style={{ color: T.textSecond, fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.06em', margin: '0 0 10px', fontWeight: 600 }}>{k.label}</p>
                    <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between' }}>
                      <span style={{ fontSize: '1.8rem', fontWeight: 700, color: T.textPrimary, fontFamily: 'var(--font-mono)', lineHeight: 1 }}>
                        {parseInt(k.count || 0).toLocaleString()}
                      </span>
                      <span style={{ fontSize: '0.9rem', fontWeight: 500, color: T.textSecond, fontFamily: 'var(--font-mono)' }}>
                        ${parseFloat(k.amount || 0).toLocaleString('en-US', { minimumFractionDigits: 0 })}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
              <DataTable columns={data.data?.detail?.length > 0 ? Object.keys(data.data.detail[0]) : []} rows={data.data?.detail || []} meta={null} />
            </div>
          )}

          {queried && !isFetching && !isError && data && !activeReport?.isSummary && activeId !== 'overview' && (
            <DataTable columns={tableColumns} rows={data.data || []} meta={data.meta} />
          )}

        </div>
      </div>
    </div>
  );
}