// src/pages/Workspace/index.jsx  v11 — diseño refinado
import { useReducer, useEffect, useCallback, useRef, useState } from 'react';
import { useAuthStore }      from '../../stores/auth.store.js';
import { workspaceReducer, initialState } from './useWorkspaceReducer.js';
import {
  useMyQueue, usePanel, useCalculate,
  useApprove, useAcquireLock, useReleaseLock,
} from '../../hooks/useWorkspace.js';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import apiClient from '../../api/client.js';
import { IconMatched, IconPending, IconReview, IconProgress } from '../../components/icons/CustomIcons.jsx';
import {
  Search,
  ChevronRight, Lock, Unlock, RefreshCw,
  AlertTriangle, X, Eye, MessageSquare,
  ChevronDown, Loader2, Scissors,
  ArrowLeft, FileText, List, RotateCcw, User,
} from 'lucide-react';

// ─── Paleta Titanium & Orange (Modo Datos) ──────────────────────────────────
const T = {
  base:        'var(--color-report-base)',
  surface:     'var(--color-report-surface)',
  surface2:    'var(--color-report-row-alt)',
  surface3:    '#E2E8F0',   // panel bancario — más énfasis
  border:      'var(--color-report-border)',
  border2:     '#CBD5E1',   // Slate-300
  thBg:        'var(--color-report-header)',
  thText:      '#E2E8F0',
  textPrimary: 'var(--color-report-text)',
  textSecond:  'var(--color-report-text2)',
  textMuted:   '#94A3B8',
  orange:      'var(--color-vault-orange)',
  orangeText:  '#FFFFFF',
  orangeBorder:'var(--color-vault-orange)',
  orangeGlow:  'var(--color-vault-orange-glow)',
  green:       'var(--color-vault-green)',
  greenText:   '#FFFFFF',
  greenBorder: 'var(--color-vault-green)',
  greenGlow:   'var(--color-vault-green-glow)',
  emerald:     'var(--color-vault-green)',
  emeraldBg:   'var(--color-vault-green-glow)',
  blue:        'var(--color-vault-blue)',
  blueBg:      'var(--color-vault-blue-dim)',
  purple:      '#6366F1',   // Indigo moderno
  purpleBg:    'rgba(99, 102, 241, 0.12)',
  red:         'var(--color-vault-red)',
  redBg:       'var(--color-vault-red-dim)',
  amber:       'var(--color-vault-amber)',
  amberBg:     'var(--color-vault-amber-dim)',
  yellow:      'var(--color-vault-amber)',
  yellowBg:    'var(--color-vault-amber-dim)',
};

// Badges de estado — misma lógica unificada
const STATUS_BADGE = {
  PENDING:        { bg: 'rgba(239, 68, 68, 0.12)', color: '#991B1B', border: 'rgba(239, 68, 68, 0.3)'  },
  REVIEW:         { bg: 'rgba(245, 158, 11, 0.12)', color: '#92400E', border: 'rgba(245, 158, 11, 0.3)'  },
  MATCHED:        { bg: 'rgba(16, 185, 129, 0.12)', color: '#065F46', border: 'rgba(16, 185, 129, 0.3)'  },
  MATCHED_MANUAL: { bg: 'rgba(16, 185, 129, 0.12)', color: '#065F46', border: 'rgba(16, 185, 129, 0.3)'  },
};

const SCENARIO_LABELS = {
  REVIEW_TRANSFER_MATCHED:     'Transfer Match',
  REVIEW_CARD_PARKING:         'Card — Parking',
  REVIEW_CARD_VIP_ASSISTANCE:  'Card — VIP/Assistance',
  PENDING_WITH_SUGGESTIONS:    'Pending — Suggestions',
  PENDING_DEPOSIT_NORMAL:      'Deposit',
  PENDING_DEPOSIT_NO_INVOICES: 'Deposit — No Invoices',
  PENDING_UNKNOWN_CLIENT:      'Unknown Client',
};

const fmt = (n) => Number(n || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const fmtCompact = (n) => {
  const v = Number(n || 0);
  if (v >= 1_000_000) return `$${(v/1_000_000).toFixed(1)}M`;
  if (v >= 1_000)     return `$${(v/1_000).toFixed(1)}K`;
  return `$${fmt(v)}`;
};

// ─── parseDecimal — normaliza separadores decimal coma/punto ──────────────────
// El analista puede tipear "749,53" (formato ES/LA) o "749.53" (formato EN).
// parseFloat("749,53") → 749 (SILENCIOSO Y CATASTRÓFICO).
// Esta función detecta el separador real y convierte correctamente.
// Reglas:
//   "1.157,14" → 1157.14  (punto=miles, coma=decimal — formato europeo)
//   "749,53"   → 749.53   (coma=decimal, sin punto de miles)
//   "749.53"   → 749.53   (punto=decimal — formato estándar)
//   "749"      → 749.00
function parseDecimal(val) {
  if (val === null || val === undefined || val === '') return 0;
  const str = String(val).trim().replace(/\s/g, '');
  if (!str) return 0;
  // Formato europeo: "1.157,14" → tiene punto Y coma
  if (str.includes('.') && str.includes(',')) {
    // El último separador es el decimal
    const lastDot   = str.lastIndexOf('.');
    const lastComma = str.lastIndexOf(',');
    if (lastComma > lastDot) {
      // coma es decimal: "1.157,14"
      return parseFloat(str.replace(/\./g, '').replace(',', '.')) || 0;
    } else {
      // punto es decimal: "1,157.14" (formato EN con separador de miles)
      return parseFloat(str.replace(/,/g, '')) || 0;
    }
  }
  // Solo coma: "749,53" → coma es decimal
  if (str.includes(',') && !str.includes('.')) {
    return parseFloat(str.replace(',', '.')) || 0;
  }
  // Solo punto o solo dígitos: formato estándar
  return parseFloat(str) || 0;
}

// ─── KPI Card — mismo estilo que Overview ─────────────────────────────────────
function KpiCard({ label, count, amount, accentColor, accentBg, icon: Icon, extra }) {
  return (
    <div style={{
      background: T.surface, border: `1px solid ${T.border}`,
      borderTop: `3px solid ${accentColor}`, borderRadius: '10px',
      padding: '14px 18px', flex: 1,
      boxShadow: '0 1px 4px rgba(28,32,20,0.07)',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
        <div style={{ width: '26px', height: '26px', borderRadius: '6px', background: accentBg,
                      display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
          <Icon size={13} style={{ color: accentColor }} />
        </div>
        <span style={{ fontSize: '0.75rem', fontWeight: 500, letterSpacing: '0.04em',
                       textTransform: 'uppercase', color: T.textSecond }}>
          {label}
        </span>
      </div>
      <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between' }}>
        <span style={{ fontSize: '1.6rem', fontWeight: 700, color: T.textPrimary,
                       lineHeight: 1, fontVariantNumeric: 'tabular-nums' }}>
          {extra || count || '—'}
        </span>
        {amount != null && (
          <span style={{ fontSize: '0.88rem', color: T.textSecond,
                         fontVariantNumeric: 'tabular-nums', fontWeight: 500 }}>
            {fmtCompact(amount)}
          </span>
        )}
      </div>
    </div>
  );
}

// ─── Status Badge ─────────────────────────────────────────────────────────────
function StatusBadge({ status }) {
  const s = STATUS_BADGE[status] || { bg: T.surface2, color: T.textMuted, border: T.border };
  return (
    <span style={{
      fontSize: '0.65rem', fontWeight: 700, padding: '2px 7px', borderRadius: '4px',
      background: s.bg, color: s.color, border: `1px solid ${s.border}`,
      whiteSpace: 'nowrap', letterSpacing: '0.03em', textTransform: 'uppercase',
    }}>{status}</span>
  );
}

// ─── Filter Tab — estilo pill compacto ────────────────────────────────────────
function FilterTab({ label, active, onClick, activeColor, activeBg, activeBorder, fullWidth }) {
  const ac = activeColor  || T.green;
  const ab = activeBg    || T.greenGlow;
  const abr = activeBorder || T.greenBorder;
  return (
    <button onClick={onClick} style={{
      padding: '5px 14px', borderRadius: '6px',
      fontSize: '0.75rem', fontWeight: active ? 600 : 400,
      cursor: 'pointer', transition: 'all 0.15s',
      border: `1px solid ${active ? abr : T.border}`,
      background: active ? ac : T.surface,
      color: active ? (activeColor === T.green ? T.greenText : '#FFF') : T.textSecond,
      flex: fullWidth ? 1 : undefined, whiteSpace: 'nowrap',
    }}>{label}</button>
  );
}

// ─── Confidence Badge — más visible ──────────────────────────────────────────
function ConfidenceBadge({ score }) {
  if (!score) return null;
  const pct = Math.round(score);
  const color  = pct >= 90 ? '#1E4A10' : pct >= 60 ? '#6A4A04' : '#7A1A10';
  const bg     = pct >= 90 ? 'rgba(90,154,53,0.22)'  : pct >= 60 ? 'rgba(200,154,32,0.22)' : 'rgba(192,70,60,0.20)';
  const border = pct >= 90 ? 'rgba(90,154,53,0.5)'   : pct >= 60 ? 'rgba(200,154,32,0.5)'  : 'rgba(192,70,60,0.45)';
  return (
    <span style={{
      fontSize: '0.68rem', fontWeight: 700, fontVariantNumeric: 'tabular-nums',
      padding: '2px 6px', borderRadius: '4px',
      background: bg, color, border: `1px solid ${border}`,
    }}>{pct}%</span>
  );
}

// ─── Invoice Row — filas alternas con columna Action para Split ───────────────
function InvoiceRow({ item, isSelected, onToggle, highlight = false, isPinned = false, rowIndex = 0, onSplit = null, isSplitTarget = false }) {
  const isAlt  = rowIndex % 2 === 1;
  const baseBg = isSplitTarget ? 'rgba(200,120,64,0.10)'
               : isPinned      ? 'rgba(58,120,201,0.08)'
               : isSelected    ? 'rgba(58,82,48,0.08)'
               : highlight     ? 'rgba(122,80,201,0.05)'
               : isAlt         ? T.surface2
               : T.surface;

  return (
    <tr onClick={() => onToggle(item.id)}
      style={{ cursor: 'pointer', background: baseBg,
               borderBottom: `1px solid ${T.border}`, transition: 'background 0.1s' }}
      onMouseEnter={e => e.currentTarget.style.background = '#E4E0D4'}
      onMouseLeave={e => e.currentTarget.style.background = baseBg}
    >
      <td style={{ padding: '7px 12px', width: '32px' }}>
        <input type="checkbox" checked={isSelected} onChange={() => onToggle(item.id)}
          onClick={e => e.stopPropagation()} style={{ accentColor: T.green }} />
      </td>
      <td style={{ padding: '7px 12px' }}>
        <div style={{ color: T.textPrimary, fontSize: '0.78rem', fontWeight: 600 }}>{item.invoiceRef}</div>
        <div style={{ color: T.textMuted, fontSize: '0.7rem', marginTop: '1px' }}>{item.assignment}</div>
      </td>
      <td style={{ padding: '7px 12px', whiteSpace: 'nowrap' }}>
        <div style={{ color: T.textSecond, fontSize: '0.75rem' }}>
          {item.docDate ? new Date(item.docDate).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: '2-digit' }) : '—'}
        </div>
        <div style={{ color: T.textMuted, fontSize: '0.68rem', marginTop: '1px' }}>
          Due: {item.dueDate ? new Date(item.dueDate).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : '—'}
        </div>
      </td>
      <td style={{ padding: '7px 12px', maxWidth: '120px' }}>
        <div style={{ color: T.textSecond, fontSize: '0.75rem', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{item.customerName}</div>
        <div style={{ color: T.textMuted, fontSize: '0.68rem' }}>{item.customerCode}</div>
      </td>
      <td style={{ padding: '7px 12px', textAlign: 'right', whiteSpace: 'nowrap' }}>
        <div style={{ color: T.textPrimary, fontSize: '0.78rem', fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>
          ${fmt(item.conciliableAmount)}
        </div>
        {item.amountDiff !== null && item.amountDiff !== undefined && (
          <div style={{ color: item.amountDiff === 0 ? T.emerald : T.textMuted, fontSize: '0.68rem', fontVariantNumeric: 'tabular-nums' }}>
            Δ ${fmt(item.amountDiff)}
          </div>
        )}
      </td>
      <td style={{ padding: '7px 12px' }}><ConfidenceBadge score={item.matchConfidence} /></td>
      <td style={{ padding: '7px 12px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '4px', flexWrap: 'wrap' }}>
          <StatusBadge status={item.status} />
          {item.preSelected && (
            <span style={{
              fontSize: '0.62rem', fontWeight: 700, padding: '2px 5px', borderRadius: '4px',
              background: T.blueBg, color: T.blue, border: `1px solid rgba(58,120,201,0.4)`,
              letterSpacing: '0.03em',
            }}>AI</span>
          )}
          {isSplitTarget && (
            <span style={{
              fontSize: '0.62rem', fontWeight: 700, padding: '2px 5px', borderRadius: '4px',
              background: 'rgba(200,120,64,0.20)', color: T.orange,
              border: `1px solid rgba(200,120,64,0.5)`,
            }}>∂ PARTIAL</span>
          )}
        </div>
      </td>
      {/* Columna Action — botón naranja sólido visible cuando hay selección */}
      <td style={{ padding: '6px 8px' }} onClick={e => e.stopPropagation()}>
        {isSelected && onSplit && !isSplitTarget && (
          <button
            onClick={() => onSplit(item.id)}
            title="Apply partial payment to this invoice"
            style={{
              display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '4px',
              width: '100%', padding: '5px 6px',
              background: T.orange, border: `1px solid rgba(200,120,64,0.7)`,
              borderRadius: '6px', cursor: 'pointer',
              color: '#FFF', fontSize: '0.7rem', fontWeight: 600,
              whiteSpace: 'nowrap', transition: 'all 0.15s',
              boxShadow: '0 1px 3px rgba(200,120,64,0.3)',
            }}
            onMouseEnter={e => e.currentTarget.style.background = '#A05828'}
            onMouseLeave={e => e.currentTarget.style.background = T.orange}
          >
            <Scissors size={10} /> Partial
          </button>
        )}
        {isSplitTarget && (
          <span style={{
            display: 'flex', alignItems: 'center', gap: '3px',
            color: T.orange, fontSize: '0.68rem', fontWeight: 600,
          }}>
            <Scissors size={10} /> Active
          </span>
        )}
      </td>
    </tr>
  );
}

// ─── Portfolio Search Section ─────────────────────────────────────────────────
function PortfolioSearchSection({ stgId, bankAmount, selectedIds, allItems, onToggle }) {
  const [query, setQuery]           = useState('');
  const [debouncedQ, setDebouncedQ] = useState('');
  const timerRef                    = useRef(null);

  const handleChange = (val) => {
    setQuery(val);
    clearTimeout(timerRef.current);
    if (val.length >= 3) timerRef.current = setTimeout(() => setDebouncedQ(val), 350);
    else setDebouncedQ('');
  };

  const { data, isFetching } = useQuery({
    queryKey: ['portfolio', 'search', stgId, debouncedQ],
    queryFn:  async () => (await apiClient.get('/portfolio/search', {
      params: { q: debouncedQ, bank_amount: bankAmount, limit: 50 },
    })).data.rows || [],
    enabled: debouncedQ.length >= 3,
    staleTime: 30 * 1000,
  });

  const searchResults   = data || [];
  const isActive        = debouncedQ.length >= 3;
  const pinnedItems     = [
    ...allItems.filter(i => selectedIds.includes(i.id)),
    ...searchResults.filter(i => selectedIds.includes(i.id) && !allItems.some(a => a.id === i.id)),
  ];
  const filteredResults = searchResults.filter(i => !selectedIds.includes(i.id));

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '260px',
                  borderTop: `1px solid ${T.border}` }}>
      <div style={{ flexShrink: 0, padding: '10px 16px', background: T.surface2 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px', marginBottom: '8px' }}>
          <Search size={11} style={{ color: T.purple }} />
          <span style={{ color: T.purple, fontSize: '0.68rem', fontWeight: 600,
                         textTransform: 'uppercase', letterSpacing: '0.08em' }}>
            Search Entire Portfolio
          </span>
          <span style={{ color: T.textMuted, fontSize: '0.68rem' }}>— 3+ characters</span>
        </div>
        <div style={{ position: 'relative' }}>
          <Search size={11} style={{ position: 'absolute', left: '10px', top: '50%',
                                     transform: 'translateY(-50%)', color: T.textMuted, pointerEvents: 'none' }} />
          <input type="text" placeholder="Customer name, invoice ref, or assignment..."
            value={query} onChange={e => handleChange(e.target.value)}
            style={{
              width: '100%', background: T.surface,
              border: `1px solid ${T.border2}`, color: T.textPrimary,
              fontSize: '0.78rem', borderRadius: '6px', padding: '5px 30px', outline: 'none',
            }} />
          {isFetching && <Loader2 size={11} style={{ position: 'absolute', right: '10px', top: '50%',
            transform: 'translateY(-50%)', color: T.purple, animation: 'spin 1s linear infinite' }} />}
          {query && !isFetching && (
            <button onClick={() => { setQuery(''); setDebouncedQ(''); }}
              style={{ position: 'absolute', right: '10px', top: '50%', transform: 'translateY(-50%)',
                       background: 'none', border: 'none', cursor: 'pointer', color: T.textMuted }}>
              <X size={11} />
            </button>
          )}
        </div>
      </div>
      <div style={{ flex: 1, overflowY: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <tbody>
            {pinnedItems.length > 0 && (
              <>
                <tr style={{ background: 'rgba(58,120,201,0.06)', position: 'sticky', top: 0, zIndex: 10 }}>
                  <td colSpan={7} style={{ padding: '5px 12px', color: T.blue, fontSize: '0.68rem',
                                           fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.06em',
                                           borderBottom: `1px solid rgba(58,120,201,0.2)` }}>
                    ✓ Selected ({pinnedItems.length})
                  </td>
                </tr>
                {pinnedItems.map((item, i) => (
                  <InvoiceRow key={`pinned-${item.id}`} item={item} isSelected={true}
                    onToggle={onToggle} isPinned={true} rowIndex={i} />
                ))}
              </>
            )}
            {isActive && !isFetching && filteredResults.length === 0 && pinnedItems.length === 0 && (
              <tr><td colSpan={7} style={{ textAlign: 'center', color: T.textMuted, padding: '20px', fontSize: '0.75rem' }}>
                No invoices found for "{debouncedQ}"
              </td></tr>
            )}
            {isActive && filteredResults.length > 0 && (
              <>
                <tr style={{ background: T.surface2 }}>
                  <td colSpan={7} style={{ padding: '5px 12px', color: T.textMuted, fontSize: '0.68rem' }}>
                    {filteredResults.length} result(s) for "{debouncedQ}"
                  </td>
                </tr>
                {filteredResults.map((item, i) => (
                  <InvoiceRow key={`sr-${item.id}`} item={item} isSelected={false}
                    onToggle={onToggle} highlight={true} rowIndex={i} />
                ))}
              </>
            )}
            {!isActive && pinnedItems.length === 0 && (
              <tr><td colSpan={7} style={{ textAlign: 'center', color: T.textMuted, padding: '24px', fontSize: '0.75rem' }}>
                Type 3+ characters to search across the full portfolio
              </td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Balance Bar ─────────────────────────────────────────────────────────────
function BalanceBar({ calculation, onOpenAdjustments }) {
  if (!calculation) return null;
  const { bankAmount, invoicesTotal, unallocated, balanceStatus, canApprove, postingKeyHint } = calculation;
  const absUnalloc   = Math.abs(unallocated || 0);
  const isCents      = absUnalloc > 0 && absUnalloc <= 0.05;
  const postingLabel = postingKeyHint === '50' ? 'credit (50 / haber)' : 'debit (40 / debe)';
  const barBg  = canApprove ? 'rgba(90,154,53,0.12)'  : isCents ? 'rgba(200,154,32,0.12)' : 'rgba(192,80,74,0.12)';
  const barBdr = canApprove ? 'rgba(90,154,53,0.3)'   : isCents ? 'rgba(200,154,32,0.3)'  : 'rgba(192,80,74,0.3)';
  const barClr = canApprove ? T.emerald               : isCents ? T.yellow                : T.red;

  return (
    <div style={{ borderRadius: '8px', border: `1px solid ${barBdr}`, padding: '8px 14px', background: barBg }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                    fontSize: '0.75rem', flexWrap: 'wrap', gap: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
          {[
            { label: 'Bank',       val: `$${fmt(bankAmount)}`    },
            { label: 'Invoices',   val: `$${fmt(invoicesTotal)}` },
            { label: 'Unallocated',val: `$${fmt(absUnalloc)}` + (absUnalloc > 0 ? ` ${unallocated > 0 ? 'over' : 'under'}` : '') },
          ].map(f => (
            <span key={f.label} style={{ color: T.textSecond }}>
              {f.label}: <strong style={{ fontFamily: 'var(--font-sans)', fontVariantNumeric: 'tabular-nums',
                                          color: f.label === 'Unallocated' ? barClr : T.textPrimary }}>
                {f.val}
              </strong>
            </span>
          ))}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px', color: barClr, fontWeight: 600 }}>
          {canApprove ? <IconMatched size={13} /> : <IconReview size={13} />}
          <span>{canApprove ? 'Balanced' : balanceStatus}</span>
        </div>
      </div>
      {!canApprove && isCents && (
        <div style={{ marginTop: '6px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <span style={{ fontSize: '0.72rem', color: T.textSecond }}>
            Distribute <strong style={{ fontVariantNumeric: 'tabular-nums' }}>${fmt(absUnalloc)}</strong> as <strong>{postingLabel}</strong>
          </span>
          <button onClick={onOpenAdjustments} style={{
            fontSize: '0.72rem', color: T.blue, background: 'none', border: 'none',
            cursor: 'pointer', textDecoration: 'underline', marginLeft: '12px',
          }}>Open Adjustments →</button>
        </div>
      )}
      {!canApprove && !isCents && absUnalloc > 0 && (
        <div style={{ marginTop: '6px', fontSize: '0.72rem', color: T.textSecond }}>
          Distribute <strong style={{ fontVariantNumeric: 'tabular-nums' }}>${fmt(absUnalloc)}</strong> as <strong>{postingLabel}</strong> or adjust invoice selection
        </div>
      )}
    </div>
  );
}

// ─── Adjustment Drawer ────────────────────────────────────────────────────────
function AdjustmentDrawer({ state, dispatch, isTC }) {
  if (!state.drawerOpen) return null;
  const adj     = state.adjustments;
  const unalloc = state.calculation?.unallocated || 0;
  // postingKeyHint viene del backend — '40' = debe, '50' = haber
  const hint    = state.calculation?.postingKeyHint || null;

  // Badge de dirección para Exchange Diff — solo si hay cálculo activo
  const diffBadge = hint
    ? hint === '50'
      ? { label: '→ HABER (50)', bg: 'rgba(90,154,53,0.15)', color: '#1E4A10', border: 'rgba(90,154,53,0.35)' }
      : { label: '→ DEBE (40)',  bg: T.blueBg,                color: T.blue,    border: 'rgba(58,120,201,0.35)' }
    : { label: '→ auto',         bg: T.surface3,              color: T.textMuted, border: T.border };

  // Validación inline: rechaza negativos al cambiar el valor
  const handleAdjChange = (key, value) => {
    // Permitir string vacío (el usuario está borrando)
    if (value === '' || value === '-') {
      // Bloquear el signo negativo — solo positivos
      if (value === '-') return;
      dispatch({ type: 'SET_ADJUSTMENT', key, value: '' });
      return;
    }
    dispatch({ type: 'SET_ADJUSTMENT', key, value });
  };

  return (
    <div style={{ flexShrink: 0, borderTop: `1px solid ${T.border}`, background: T.surface2 }}>
      <div style={{ maxHeight: '220px', overflowY: 'auto', padding: '14px 16px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '10px' }}>
          <h4 style={{ color: T.textPrimary, fontSize: '0.82rem', fontWeight: 600, margin: 0 }}>Distribute Difference</h4>
          <button onClick={() => dispatch({ type: 'CLOSE_DRAWER' })}
            style={{ background: 'none', border: 'none', cursor: 'pointer', color: T.textMuted }}>
            <X size={14} />
          </button>
        </div>
        {Math.abs(unalloc) > 0.001 && (
          <div style={{ fontSize: '0.72rem', padding: '6px 10px', borderRadius: '6px', marginBottom: '10px',
                        background: unalloc > 0 ? T.blueBg : T.orangeBg,
                        color: unalloc > 0 ? T.blue : T.orange }}>
            {unalloc > 0
              ? `↑ Bank overpaid $${fmt(Math.abs(unalloc))} → posting key 50 (credit / haber)`
              : `↓ Bank underpaid $${fmt(Math.abs(unalloc))} → posting key 40 (debit / debe)`}
          </div>
        )}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px' }}>
          {[
            { key: 'commission', label: 'Commission',      account: '5540101004', isDiff: false },
            { key: 'taxIva',     label: 'VAT Withholding', account: '1140105023', isDiff: false },
            { key: 'taxIrf',     label: 'IRF Withholding', account: '140104019',  isDiff: false },
            { key: 'diffAmount', label: 'Exchange Diff',   account: '3710101001', isDiff: true  },
          ].map(({ key, label, account, isDiff }) => (
            <div key={key}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '3px' }}>
                <label style={{ color: T.textSecond, fontSize: '0.68rem' }}>
                  {label} <span style={{ color: T.textMuted, fontSize: '0.65rem' }}>/{account}</span>
                </label>
                {/* Badge de dirección — solo en Exchange Diff */}
                {isDiff && (
                  <span style={{
                    fontSize: '0.6rem', fontWeight: 700, padding: '1px 5px', borderRadius: '3px',
                    background: diffBadge.bg, color: diffBadge.color,
                    border: `1px solid ${diffBadge.border}`,
                  }}>
                    {diffBadge.label}
                  </span>
                )}
                {/* Badge fijo para comisiones/impuestos — siempre posting 40/50 según tipo */}
                {!isDiff && isTC && (
                  <span style={{
                    fontSize: '0.6rem', fontWeight: 700, padding: '1px 5px', borderRadius: '3px',
                    background: T.blueBg, color: T.blue, border: `1px solid rgba(58,120,201,0.35)`,
                  }}>→ DEBE (40)</span>
                )}
              </div>
              <input
                type="text"
                inputMode="decimal"
                placeholder="0.00"
                value={adj[key]}
                onChange={e => handleAdjChange(key, e.target.value)}
                onBlur={e => {
                  // Al salir del campo: normalizar y rechazar negativos
                  const parsed = parseDecimal(e.target.value);
                  const safe   = Math.max(0, parsed);
                  dispatch({ type: 'SET_ADJUSTMENT', key, value: safe > 0 ? String(safe) : '' });
                }}
                style={{
                  width: '100%', background: T.surface, border: `1px solid ${T.border2}`,
                  color: T.textPrimary, fontSize: '0.78rem', borderRadius: '6px',
                  padding: '5px 10px', outline: 'none', fontVariantNumeric: 'tabular-nums',
                  boxSizing: 'border-box',
                }}
              />
            </div>
          ))}
        </div>
        <p style={{ fontSize: '0.65rem', color: T.textMuted, margin: '8px 0 0', fontStyle: 'italic' }}>
          Accepts comma or period as decimal separator (e.g. 749,53 or 749.53). Positive values only.
        </p>
      </div>
    </div>
  );
}

// ─── Split Payment Panel ──────────────────────────────────────────────────────
function SplitPaymentPanel({ selectedItem, state, dispatch }) {
  const appliedAmount  = parseDecimal(state.splitAppliedAmount);
  const maxAmount      = parseFloat(selectedItem?.conciliableAmount) || 0;
  const residualAmount = Math.round((maxAmount - appliedAmount) * 100) / 100;
  const isValid        = appliedAmount > 0 && appliedAmount < maxAmount;

  const handleAmountChange = (val) => {
    // Bloquear negativos en tiempo real
    if (val.startsWith('-')) return;
    dispatch({ type: 'SET_SPLIT_AMOUNT', payload: val });
  };

  return (
    <div style={{ flexShrink: 0, borderTop: `1px solid rgba(200,120,64,0.3)`,
                  background: 'rgba(200,120,64,0.05)', padding: '10px 16px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
        <Scissors size={13} style={{ color: T.orange }} />
        <span style={{ color: T.orange, fontSize: '0.72rem', fontWeight: 600,
                       textTransform: 'uppercase', letterSpacing: '0.05em' }}>Partial Payment</span>
        <button onClick={() => dispatch({ type: 'DISABLE_SPLIT' })}
          style={{ marginLeft: 'auto', background: 'none', border: 'none', cursor: 'pointer', color: T.textMuted }}>
          <X size={13} />
        </button>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '10px' }}>
        <div>
          <label style={{ color: T.textSecond, fontSize: '0.68rem', display: 'block', marginBottom: '3px' }}>Invoice Total</label>
          <div style={{ background: T.surface2, borderRadius: '5px', padding: '5px 10px',
                        color: T.textSecond, fontSize: '0.75rem', fontVariantNumeric: 'tabular-nums' }}>
            ${fmt(maxAmount)}
          </div>
        </div>
        <div>
          <label style={{ color: T.textSecond, fontSize: '0.68rem', display: 'block', marginBottom: '3px' }}>Amount to Apply *</label>
          <input
            type="text"
            inputMode="decimal"
            placeholder="0.00"
            value={state.splitAppliedAmount}
            onChange={e => handleAmountChange(e.target.value)}
            onBlur={e => {
              // Normalizar al salir del campo: coma → punto, no negativos
              const parsed = parseDecimal(e.target.value);
              const safe   = Math.max(0, parsed);
              dispatch({ type: 'SET_SPLIT_AMOUNT', payload: safe > 0 ? String(safe) : '' });
            }}
            style={{
              width: '100%', background: T.surface,
              border: `1px solid ${isValid ? T.orange : T.border2}`,
              color: T.textPrimary, fontSize: '0.75rem', borderRadius: '5px',
              padding: '5px 10px', outline: 'none', fontVariantNumeric: 'tabular-nums',
              boxSizing: 'border-box',
            }}
          />
        </div>
        <div>
          <label style={{ color: T.textSecond, fontSize: '0.68rem', display: 'block', marginBottom: '3px' }}>Residual (stays open)</label>
          <div style={{ borderRadius: '5px', padding: '5px 10px', fontSize: '0.75rem', fontVariantNumeric: 'tabular-nums',
                        background: residualAmount > 0 ? 'rgba(200,120,64,0.10)' : T.surface2,
                        color: residualAmount > 0 ? T.orange : T.textMuted }}>
            ${fmt(residualAmount > 0 ? residualAmount : 0)}
          </div>
        </div>
      </div>
      {appliedAmount > maxAmount && (
        <p style={{ color: T.red, fontSize: '0.72rem', marginTop: '6px' }}>
          Amount to apply cannot exceed the invoice total (${fmt(maxAmount)})
        </p>
      )}
      <p style={{ fontSize: '0.65rem', color: T.textMuted, margin: '6px 0 0', fontStyle: 'italic' }}>
        Accepts comma or period as decimal separator. Positive values only.
      </p>
    </div>
  );
}

// ─── Bank Context Header — fondo más oscuro para diferenciarlo ────────────────
function BankContextHeader({ tx, panel, lockActive }) {
  if (!tx) return null;
  const uiConfig  = panel?.uiConfig || {};
  const scenario  = panel?.scenario || '';
  const isUnknown = scenario === 'PENDING_UNKNOWN_CLIENT' || !tx.customerCode;
  const isTC      = tx.transType === 'LIQUIDACION TC';
  const showBanner = uiConfig.showInfoBanner && uiConfig.infoBannerMessage && uiConfig.infoBannerMessage !== tx.enrichNotes;

  // Color del borde izquierdo según estado
  const s = STATUS_BADGE[tx.reconcileStatus] || {};
  const accentColor = s.color || T.textMuted;
  const accentBorder = s.border || T.border;

  return (
    <div style={{
      flexShrink: 0,
      background: T.surface3,   // más oscuro — diferenciado del portfolio
      border: `1px solid ${T.border2}`,
      borderLeft: `4px solid ${accentColor}`,
      borderRadius: '10px',
      padding: '14px 16px',
    }}>
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '16px' }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          {/* Fila de chips de estado */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px', flexWrap: 'wrap' }}>
            <StatusBadge status={tx.reconcileStatus} />
            {scenario && (
              <span style={{ color: T.textSecond, fontSize: '0.72rem', fontWeight: 400 }}>
                {SCENARIO_LABELS[scenario] || scenario}
              </span>
            )}
            {/* Locked by you — chip destacado verde */}
            {lockActive && (
              <span style={{
                display: 'inline-flex', alignItems: 'center', gap: '4px',
                fontSize: '0.68rem', fontWeight: 700,
                padding: '2px 8px', borderRadius: '4px',
                background: 'rgba(90,154,53,0.18)',
                color: '#1E4A10',
                border: '1px solid rgba(90,154,53,0.45)',
              }}>
                <Unlock size={10} /> Locked by you
              </span>
            )}
          </div>

          {/* Referencia bancaria */}
          <h3 style={{ color: T.textPrimary, fontWeight: 700, fontSize: '0.95rem',
                       margin: '0 0 10px', letterSpacing: '-0.01em' }}>
            {tx.bankRef1}
          </h3>

          {/* Grid de datos clave */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '4px 20px' }}>
            {[
              { label: 'Type',       value: tx.transType },
              tx.brand && tx.brand !== 'GENERICO' && tx.brand !== 'CASH' && {
                label: 'Brand', value: `${tx.brand}${tx.establishmentName ? ` · ${tx.establishmentName}` : ''}`
              },
              { label: 'Customer',   value: tx.customerName
                  ? `${tx.customerName} (${tx.customerCode})`
                  : 'Not identified' },
              isTC && tx.settlementId && { label: 'Settlement', value: tx.settlementId },
              tx.matchConfidence > 0 && { label: 'Confidence', value: null, badge: tx.matchConfidence },
            ].filter(Boolean).map((f, i) => (
              <div key={i} style={{ fontSize: '0.75rem', display: 'flex', alignItems: 'center', gap: '4px' }}>
                <span style={{ color: T.textMuted, fontWeight: 400 }}>{f.label}:</span>
                {f.badge
                  ? <ConfidenceBadge score={f.badge} />
                  : <span style={{ color: T.textPrimary, fontWeight: 500 }}>{f.value}</span>
                }
              </div>
            ))}
          </div>

          {/* Ref 2 para unknown */}
          {isUnknown && tx.bankRef2 && (
            <div style={{ marginTop: '8px', background: T.surface2, borderRadius: '5px',
                          padding: '4px 10px', fontSize: '0.72rem' }}>
              <span style={{ color: T.textMuted }}>Ref 2: </span>
              <span style={{ color: T.textPrimary, fontWeight: 500 }}>{tx.bankRef2}</span>
            </div>
          )}

          {/* Notes */}
          {tx.enrichNotes && (
            <div style={{ marginTop: '8px', background: T.blueBg,
                          border: `1px solid rgba(58,120,201,0.25)`,
                          borderRadius: '6px', padding: '5px 10px',
                          color: T.blue, fontSize: '0.72rem', fontStyle: 'italic' }}>
              <Eye size={10} style={{ display: 'inline', marginRight: '5px' }} />
              {tx.enrichNotes}
            </div>
          )}
          {tx.analystNote && (
            <div style={{ marginTop: '6px', background: '#FEF3C7',
                          borderLeft: '3px solid #F59E0B',
                          borderRadius: '6px', padding: '5px 10px',
                          color: '#92400E', fontSize: '0.72rem' }}>
              <MessageSquare size={10} style={{ display: 'inline', marginRight: '5px' }} />
              <strong>Admin Note:</strong> {tx.analystNote}
            </div>
          )}
          {showBanner && (
            <div style={{ marginTop: '6px', background: T.blueBg,
                          border: `1px solid rgba(58,120,201,0.2)`,
                          borderRadius: '6px', padding: '5px 10px',
                          color: T.blue, fontSize: '0.72rem' }}>
              {uiConfig.infoBannerMessage}
            </div>
          )}
        </div>

        {/* Monto */}
        <div style={{ textAlign: 'right', flexShrink: 0 }}>
          <p style={{ color: T.textPrimary, fontSize: '1.5rem', fontWeight: 700,
                      fontVariantNumeric: 'tabular-nums', margin: 0, lineHeight: 1 }}>
            ${fmt(tx.amountTotal)}
          </p>
          <p style={{ color: T.textMuted, fontSize: '0.72rem', marginTop: '4px' }}>
            {tx.currency} · {new Date(tx.bankDate).toLocaleDateString('en-US', {
              month: 'short', day: 'numeric', year: 'numeric'
            })}
          </p>
        </div>
      </div>
    </div>
  );
}

// ─── Override Section ─────────────────────────────────────────────────────────
function OverrideSection({ state, dispatch }) {
  if (!state.isOverride) return null;
  return (
    <div style={{ border: `1px solid rgba(200,154,32,0.35)`, background: 'rgba(200,154,32,0.06)',
                  borderRadius: '8px', padding: '10px 14px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
        <AlertTriangle size={13} style={{ color: T.yellow }} />
        <span style={{ color: T.yellow, fontSize: '0.72rem', fontWeight: 600 }}>Override — Justification Required</span>
      </div>
      <textarea value={state.overrideReason}
        onChange={e => dispatch({ type: 'SET_OVERRIDE_REASON', payload: e.target.value })}
        placeholder="Explain why this reconciliation requires an override (minimum 20 characters)..."
        rows={2}
        style={{ width: '100%', background: T.surface, border: `1px solid rgba(200,154,32,0.3)`,
                 color: T.textPrimary, fontSize: '0.75rem', borderRadius: '6px',
                 padding: '6px 10px', outline: 'none', resize: 'none', boxSizing: 'border-box' }} />
      <p style={{ color: T.textMuted, fontSize: '0.68rem', marginTop: '4px' }}>{state.overrideReason.length}/20 min</p>
    </div>
  );
}

// ─── Load More ────────────────────────────────────────────────────────────────
function LoadMoreSection({ hasNextPage, onLoadMore, isLoading }) {
  if (!hasNextPage) return null;
  return (
    <div style={{ padding: '8px 16px', borderTop: `1px solid ${T.border}`, background: T.surface2, textAlign: 'center' }}>
      <button onClick={onLoadMore} disabled={isLoading} style={{
        display: 'flex', alignItems: 'center', gap: '6px', margin: '0 auto',
        padding: '5px 14px', background: T.surface, border: `1px solid ${T.border}`,
        color: T.textSecond, borderRadius: '6px', fontSize: '0.75rem', cursor: 'pointer',
      }}>
        {isLoading ? <><Loader2 size={12} style={{ animation: 'spin 1s linear infinite' }} /> Loading...</>
                   : <><ChevronDown size={12} /> Load 50 more invoices</>}
      </button>
    </div>
  );
}

// ─── Helper: obtener stgId de un registro aprobado ────────────────────────────
// approved-today devuelve stg_id como campo del banco
function getStgId(tx) {
  return tx?.stgId ?? tx?.stg_id ?? tx?.id ?? null;
}

// ─── Panel de detalle completo de un match aprobado ───────────────────────────
// Consume GET /reconciliation/:stgId/approved-detail
// Muestra banco enriquecido + facturas con detección de cruce de cliente
// + botón de reversión con 3 estados claros
function ApprovedDetailPanel({ selectedApproved, onReversal, isRequestingReversal }) {
  const stgId = getStgId(selectedApproved);

  const { data, isLoading, isError } = useQuery({
    queryKey:  ['approved-detail', stgId],
    queryFn:   async () => (await apiClient.get(`/reconciliation/${stgId}/approved-detail`)).data.data,
    enabled:   !!stgId,
    staleTime: 30 * 1000,
  });

  const bank    = data?.bank     || null;
  const invoices = data?.invoices || [];
  const hasPendingReversal = data?.hasPendingReversal ?? selectedApproved?.hasPendingReversal ?? false;

  // ── Skeleton mientras carga ──────────────────────────────────────────────
  if (isLoading) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: '10px', minWidth: 0, minHeight: 0 }}>
        {/* Bank header skeleton */}
        <div style={{ flexShrink: 0, background: T.surface3, border: `1px solid ${T.border2}`,
                      borderLeft: `4px solid ${T.emerald}`, borderRadius: '10px', padding: '14px 16px' }}>
          {[120, 80, 160, 100].map((w, i) => (
            <div key={i} style={{ height: '12px', width: `${w}px`, borderRadius: '4px',
                                   background: T.border, marginBottom: '10px',
                                   animation: 'pulse 1.5s ease-in-out infinite' }} />
          ))}
        </div>
        {/* Invoice list skeleton */}
        <div style={{ flex: 1, background: T.surface, border: `1px solid ${T.border}`,
                      borderRadius: '10px', padding: '16px' }}>
          {[1,2,3].map(i => (
            <div key={i} style={{ height: '52px', borderRadius: '7px', background: T.border,
                                   marginBottom: '8px', animation: 'pulse 1.5s ease-in-out infinite' }} />
          ))}
        </div>
        <style>{`@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.5} }`}</style>
      </div>
    );
  }

  if (isError || !bank) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center',
                    background: T.surface, border: `1px solid ${T.border}`, borderRadius: '10px' }}>
        <div style={{ textAlign: 'center', padding: '32px' }}>
          <AlertTriangle size={24} style={{ color: T.red, margin: '0 auto 8px', display: 'block' }} />
          <p style={{ color: T.textSecond, fontSize: '0.82rem', margin: '0 0 4px' }}>Could not load detail</p>
          <p style={{ color: T.textMuted, fontSize: '0.73rem' }}>Check your connection and try again</p>
        </div>
      </div>
    );
  }

  // ── Detección de cruce de cliente ────────────────────────────────────────
  // Si alguna factura tiene customerCode distinto al banco → alerta visual
  const hasMismatch = bank.customerCode
    ? invoices.some(inv => inv.customerCode && inv.customerCode !== bank.customerCode)
    : false;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '10px', minWidth: 0, minHeight: 0 }}>

      {/* ── HEADER BANCARIO ────────────────────────────────────────────────── */}
      <div style={{
        flexShrink: 0, background: T.surface3,
        border: `1px solid ${T.border2}`, borderLeft: `4px solid ${T.emerald}`,
        borderRadius: '10px', padding: '14px 16px',
      }}>
        {/* Alerta de cruce de cliente — banner prominente */}
        {hasMismatch && (
          <div style={{
            display: 'flex', alignItems: 'center', gap: '8px',
            padding: '7px 12px', borderRadius: '7px', marginBottom: '12px',
            background: 'rgba(192,80,74,0.10)', border: `1px solid rgba(192,80,74,0.35)`,
          }}>
            <AlertTriangle size={13} style={{ color: T.red, flexShrink: 0 }} />
            <span style={{ color: T.red, fontSize: '0.73rem', fontWeight: 600 }}>
              Customer mismatch — one or more invoices belong to a different customer
            </span>
          </div>
        )}

        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '16px' }}>
          <div style={{ flex: 1, minWidth: 0 }}>
            {/* Badge + hora aprobación */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '10px', flexWrap: 'wrap' }}>
              <span style={{
                fontSize: '0.62rem', fontWeight: 700, padding: '2px 7px', borderRadius: '4px',
                background: 'rgba(90,154,53,0.22)', color: '#1E4A10',
                border: '1px solid rgba(90,154,53,0.5)', textTransform: 'uppercase',
              }}>✓ Matched</span>
              {bank.isOverride && (
                <span style={{
                  fontSize: '0.62rem', fontWeight: 700, padding: '2px 7px', borderRadius: '4px',
                  background: T.yellowBg, color: '#6A4A04',
                  border: `1px solid rgba(200,154,32,0.4)`, textTransform: 'uppercase',
                }}>⚠ Override</span>
              )}
              <span style={{ color: T.textMuted, fontSize: '0.72rem', marginLeft: 'auto' }}>
                Approved at {new Date(bank.approvedAt).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })}
                {bank.approvedBy && <span style={{ marginLeft: '4px' }}>by {bank.approvedBy}</span>}
              </span>
            </div>

            {/* Referencia bancaria */}
            <h3 style={{ color: T.textPrimary, fontWeight: 700, fontSize: '0.95rem',
                         margin: '0 0 10px', letterSpacing: '-0.01em' }}>
              {bank.bankRef1}
            </h3>

            {/* Grid de datos bancarios */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '5px 20px' }}>
              {[
                { label: 'Type',       value: bank.transType },
                { label: 'Date',       value: bank.bankDate
                    ? new Date(bank.bankDate).toLocaleDateString('en-US', { day: '2-digit', month: 'short', year: 'numeric' })
                    : null },
                bank.customerName && { label: 'Customer',   value: `${bank.customerName}${bank.customerCode ? ` (${bank.customerCode})` : ''}` },
                bank.brand && bank.brand !== 'GENERICO' && bank.brand !== 'CASH' && { label: 'Brand', value: bank.brand },
                bank.settlementId && { label: 'Settlement', value: bank.settlementId },
                bank.bankRef2      && { label: 'Ref 2',     value: bank.bankRef2 },
                bank.diffAmount    && { label: 'Diff',      value: `$${fmt(Math.abs(bank.diffAmount))} (${bank.diffAccountCode || '—'})` },
              ].filter(Boolean).map((f, i) => (
                <div key={i} style={{ fontSize: '0.75rem', display: 'flex', gap: '4px', alignItems: 'flex-start' }}>
                  <span style={{ color: T.textMuted, fontWeight: 400, whiteSpace: 'nowrap' }}>{f.label}:</span>
                  <span style={{ color: T.textPrimary, fontWeight: 500 }}>{f.value}</span>
                </div>
              ))}
            </div>

            {/* Enrich notes */}
            {bank.enrichNotes && (
              <div style={{ marginTop: '8px', background: T.blueBg, border: `1px solid rgba(58,120,201,0.25)`,
                            borderRadius: '6px', padding: '5px 10px', color: T.blue, fontSize: '0.72rem', fontStyle: 'italic' }}>
                <Eye size={10} style={{ display: 'inline', marginRight: '5px' }} />
                {bank.enrichNotes}
              </div>
            )}
          </div>

          {/* Monto */}
          <div style={{ textAlign: 'right', flexShrink: 0 }}>
            <p style={{ color: T.textPrimary, fontSize: '1.5rem', fontWeight: 700,
                        fontVariantNumeric: 'tabular-nums', margin: 0, lineHeight: 1 }}>
              ${fmt(bank.amountTotal)}
            </p>
            <p style={{ color: T.textMuted, fontSize: '0.72rem', marginTop: '4px' }}>
              {bank.currency}
            </p>
          </div>
        </div>
      </div>

      {/* ── PANEL DE FACTURAS ──────────────────────────────────────────────── */}
      <div style={{
        flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column',
        background: T.surface, border: `1px solid ${T.border}`,
        borderRadius: '10px', overflow: 'hidden',
      }}>
        {/* Header del panel */}
        <div style={{ flexShrink: 0, padding: '10px 16px', borderBottom: `1px solid ${T.border}`,
                       background: T.surface2, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <FileText size={12} style={{ color: T.textMuted }} />
            <span style={{ fontSize: '0.72rem', fontWeight: 600, color: T.textMuted,
                           textTransform: 'uppercase', letterSpacing: '0.06em' }}>
              Approved Invoices
            </span>
            <span style={{ fontSize: '0.68rem', color: T.textMuted, background: T.border,
                           padding: '1px 6px', borderRadius: '999px' }}>
              {invoices.length}
            </span>
          </div>
            {invoices.length > 0 && (
            <span style={{ fontSize: '0.72rem', color: T.textSecond, fontVariantNumeric: 'tabular-nums', fontWeight: 600 }}>
              Total: ${fmt(invoices.reduce((s, inv) => s + (inv.amount || 0), 0))}
            </span>
          )}
        </div>

        {/* Lista de facturas */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '12px 16px' }}>
          {invoices.length === 0 ? (
            <div style={{ textAlign: 'center', padding: '32px 0', color: T.textMuted, fontSize: '0.75rem' }}>
              <FileText size={24} style={{ margin: '0 auto 8px', display: 'block', opacity: 0.4 }} />
              No invoice details available
            </div>
          ) : invoices.map((inv, i) => {
            // Detectar cruce: factura de cliente diferente al banco
            const isMismatch = bank.customerCode && inv.customerCode && inv.customerCode !== bank.customerCode;
            return (
              <div key={inv.stgId ?? i} style={{
                padding: '10px 14px', borderRadius: '8px', marginBottom: '8px',
                background: isMismatch ? 'rgba(192,80,74,0.06)' : i % 2 === 0 ? T.surface : T.surface2,
                border: `1px solid ${isMismatch ? 'rgba(192,80,74,0.30)' : T.border}`,
                borderLeft: `3px solid ${isMismatch ? T.red : T.emerald}`,
              }}>
                <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '12px' }}>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    {/* Número de factura + badges */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: '6px', marginBottom: '4px', flexWrap: 'wrap' }}>
                      <span style={{ color: T.textPrimary, fontSize: '0.82rem', fontWeight: 700 }}>
                        {inv.invoiceRef || `#${inv.stgId}`}
                      </span>
                      {inv.isPartialPayment && (
                        <span style={{
                          fontSize: '0.6rem', fontWeight: 700, padding: '1px 5px', borderRadius: '3px',
                          background: 'rgba(200,120,64,0.15)', color: T.orange,
                          border: `1px solid rgba(200,120,64,0.40)`, textTransform: 'uppercase',
                        }}>∂ Partial</span>
                      )}
                      {isMismatch && (
                        <span style={{
                          fontSize: '0.6rem', fontWeight: 700, padding: '1px 5px', borderRadius: '3px',
                          background: 'rgba(192,80,74,0.15)', color: T.red,
                          border: `1px solid rgba(192,80,74,0.35)`, textTransform: 'uppercase',
                        }}>⚠ Diff. Customer</span>
                      )}
                    </div>
                    {/* Cliente de la factura */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: '4px', marginBottom: '3px' }}>
                      <User size={10} style={{ color: isMismatch ? T.red : T.textMuted, flexShrink: 0 }} />
                      <span style={{ fontSize: '0.73rem', color: isMismatch ? T.red : T.textSecond, fontWeight: isMismatch ? 600 : 400 }}>
                        {inv.customerName || inv.customerCode || '—'}
                        {inv.customerCode && inv.customerName && (
                          <span style={{ color: T.textMuted, fontWeight: 400 }}> ({inv.customerCode})</span>
                        )}
                      </span>
                    </div>
                    {/* Fechas */}
                    <div style={{ display: 'flex', gap: '12px' }}>
                      {inv.docDate && (
                        <span style={{ fontSize: '0.68rem', color: T.textMuted }}>
                          Doc: {new Date(inv.docDate).toLocaleDateString('en-US', { day: '2-digit', month: 'short', year: '2-digit' })}
                        </span>
                      )}
                      {inv.dueDate && (
                        <span style={{ fontSize: '0.68rem', color: T.textMuted }}>
                          Due: {new Date(inv.dueDate).toLocaleDateString('en-US', { day: '2-digit', month: 'short', year: '2-digit' })}
                        </span>
                      )}
                    </div>
                    {/* Assignment */}
                    {inv.assignment && (
                      <div style={{ fontSize: '0.68rem', color: T.textMuted, marginTop: '2px' }}>
                        {inv.assignment}
                      </div>
                    )}
                  </div>
                  {/* Monto — amount_outstanding (valor original al cierre) */}
                  <div style={{ textAlign: 'right', flexShrink: 0 }}>
                    <span style={{ color: T.textPrimary, fontSize: '0.88rem',
                                   fontVariantNumeric: 'tabular-nums', fontWeight: 700 }}>
                      ${fmt(inv.amount)}
                    </span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>

        {/* ── FOOTER — Botón de reversión ──────────────────────────────────── */}
        <div style={{ flexShrink: 0, padding: '12px 16px', borderTop: `1px solid ${T.border}`,
                       background: T.surface2 }}>
          {hasPendingReversal ? (
            /* Estado 2: reversión ya solicitada — banner bloqueante amarillo */
            <div style={{
              display: 'flex', alignItems: 'center', gap: '10px',
              padding: '10px 14px', borderRadius: '8px',
              background: T.yellowBg, border: `1px solid rgba(200,154,32,0.40)`,
            }}>
              <Loader2 size={14} style={{ color: T.yellow, flexShrink: 0 }} />
              <div>
                <div style={{ fontSize: '0.78rem', color: '#6A4A04', fontWeight: 600 }}>
                  Reversal request pending
                </div>
                <div style={{ fontSize: '0.68rem', color: T.yellow, marginTop: '1px' }}>
                  Waiting for admin approval — no further action needed
                </div>
              </div>
            </div>
          ) : (
            /* Estado 1: botón rojo sólido, visible, con peso de acción destructiva */
            <button
              onClick={() => onReversal({ id: getStgId(selectedApproved), bankRef1: bank.bankRef1 })}
              disabled={isRequestingReversal}
              style={{
                width: '100%', padding: '10px 16px', borderRadius: '8px',
                background: T.red, border: `1px solid rgba(192,80,74,0.6)`,
                color: '#FFF', fontSize: '0.82rem', fontWeight: 600, cursor: 'pointer',
                display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '7px',
                transition: 'all 0.15s',
                boxShadow: '0 2px 6px rgba(192,80,74,0.25)',
                opacity: isRequestingReversal ? 0.7 : 1,
              }}
              onMouseEnter={e => { e.currentTarget.style.background = '#A03A34'; e.currentTarget.style.boxShadow = '0 3px 10px rgba(192,80,74,0.35)'; }}
              onMouseLeave={e => { e.currentTarget.style.background = T.red;     e.currentTarget.style.boxShadow = '0 2px 6px rgba(192,80,74,0.25)'; }}
            >
              <RotateCcw size={14} />
              Reverse this Match
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

// ─── MAIN PAGE ────────────────────────────────────────────────────────────────
export default function WorkspacePage() {
  const [state, dispatch]           = useReducer(workspaceReducer, initialState);
  const [queueFilter, setQueueFilter] = useState('ALL');
  const [activeTab,   setActiveTab]   = useState('QUEUE');
  const [searchOpen,  setSearchOpen]  = useState(false);
  const [extraPages,  setExtraPages]  = useState([]);
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  const [reversalTarget,   setReversalTarget]   = useState(null);
  const [reversalReason,   setReversalReason]   = useState('');
  // Panel de detalle de transacción aprobada — null = lista, objeto = detalle
  const [selectedApproved, setSelectedApproved] = useState(null);
  const user        = useAuthStore(s => s.user);
  const queryClient = useQueryClient();
  const renewTimerRef     = useRef(null);
  const activeBankRef1Ref = useRef(null); // siempre contiene el bankRef1 activo — evita closures stale en el timer

  const { data: queue, isLoading: queueLoading, refetch: refetchQueue } = useMyQueue();
  const { data: panelData, isLoading: panelLoading } = usePanel(state.activeTx?.id);
  const { mutateAsync: calculate }   = useCalculate();
  const { mutateAsync: approve }     = useApprove();
  const { mutateAsync: acquireLock } = useAcquireLock();
  const { mutateAsync: releaseLock } = useReleaseLock();

  const { data: statsData } = useQuery({
    queryKey:        ['workspace', 'stats', user?.id],
    queryFn:         async () => (await apiClient.get('/workspace/my-stats')).data.data,
    staleTime:       30 * 1000,
    refetchInterval: 60 * 1000,
  });

  const { data: approvedToday = [], refetch: refetchApproved } = useQuery({
    queryKey:        ['notifications', 'approved-today'],
    queryFn:         async () => (await apiClient.get('/notifications/approved-today')).data.data,
    staleTime:       0,
    refetchInterval: 15 * 1000,
  });

  const { mutateAsync: requestReversal, isPending: isRequestingReversal } = useMutation({
    mutationFn: ({ stgId, reason }) => apiClient.post(`/notifications/reversals/${stgId}/request`, { reason }),
    onSuccess:  () => {
      setReversalTarget(null); setReversalReason('');
      refetchApproved();
      queryClient.invalidateQueries({ queryKey: ['notifications'] });
    },
  });

  const filteredQueue = (queue || []).filter(tx => {
    if (queueFilter === 'PENDING') return tx.reconcileStatus === 'PENDING';
    if (queueFilter === 'REVIEW')  return tx.reconcileStatus === 'REVIEW';
    return true;
  });

  useEffect(() => { setExtraPages([]); setSearchOpen(false); }, [state.activeTx?.id]);
  useEffect(() => { if (panelData && !panelLoading) dispatch({ type: 'PANEL_LOADED', payload: panelData }); }, [panelData, panelLoading]);

  useEffect(() => {
    if (!state.panel || !state.activeTx) return;
    const fa = state.activeTx.financialAmounts;
    if (state.activeTx.transType === 'LIQUIDACION TC' && fa) {
      dispatch({ type: 'SET_ADJUSTMENTS_BULK', payload: {
        commission: fa.commission > 0 ? String(fa.commission) : '',
        taxIva:     fa.taxIva     > 0 ? String(fa.taxIva)     : '',
        taxIrf:     fa.taxIrf     > 0 ? String(fa.taxIrf)     : '',
      }});
    }
  }, [state.panel]);

  useEffect(() => {
    const hasSelection = state.selectedIds.length > 0 || state.isSplitPayment;
    if (!state.activeTx || !hasSelection) return;
    const run = async () => {
      try {
        const result = await calculate({ stgId: state.activeTx.id, payload: {
          selected_portfolio_ids: state.selectedIds,
          portfolio_ids:          state.selectedIds,
          // Split payment flags — sin estos el service calcula con conciliableAmount=0
          // y el balance nunca cuadra cuando isSplitPayment=true
          is_split_payment:    state.isSplitPayment || false,
          split_applied_amount: state.isSplitPayment
            ? (parseDecimal(state.splitAppliedAmount) || null)
            : null,
          adjustments: {
            commission:      parseDecimal(state.adjustments.commission),
            taxIva:          parseDecimal(state.adjustments.taxIva),
            taxIrf:          parseDecimal(state.adjustments.taxIrf),
            diffAmount:      parseDecimal(state.adjustments.diffAmount),
            diffAccountCode: state.adjustments.diffAccountCode || null,
          },
        }});
        dispatch({ type: 'CALCULATION_DONE', payload: result.data });
      } catch {}
    };
    run();
  }, [state.selectedIds, state.adjustments, state.splitAppliedAmount, state.isSplitPayment]);

  const handleSelectTx = useCallback(async (tx) => {
    if (state.activeTx?.id === tx.id) return;
    if (state.activeTx && state.lockActive) { try { await releaseLock(state.activeTx.bankRef1); } catch {} }
    dispatch({ type: 'SELECT_TX', payload: tx });
    try {
      await acquireLock(tx.bankRef1);
      dispatch({ type: 'LOCK_ACQUIRED' });
      // Guardar el bankRef1 activo en un ref — el timer lo lee en cada tick
      // sin quedar atrapado en el closure con el valor viejo del ciclo anterior
      activeBankRef1Ref.current = tx.bankRef1;
      if (renewTimerRef.current) clearInterval(renewTimerRef.current);
      renewTimerRef.current = setInterval(async () => {
        const currentRef = activeBankRef1Ref.current;
        if (!currentRef) return;
        try { await apiClient.patch(`/locks/${encodeURIComponent(currentRef)}/renew`); } catch {}
      }, 4 * 60 * 1000);
    } catch {
      dispatch({ type: 'SET_ERROR', payload: 'Could not acquire lock. Transaction may be open by another analyst.' });
    }
  }, [state.activeTx, state.lockActive]);

  const handleLoadMore = async () => {
    if (!state.activeTx) return;
    setIsLoadingMore(true);
    try {
      const currentCount = (state.panel?.complementaryItems?.length || 0) + extraPages.length;
      const nextPage = Math.floor(currentCount / 50) + 1;
      const res = await apiClient.get(`/workspace/${state.activeTx.id}/panel`, { params: { page: nextPage + 1, limit: 50 }});
      setExtraPages(prev => [...prev, ...(res.data.suggestedItems||[]), ...(res.data.complementaryItems||[])]);
    } catch {}
    setIsLoadingMore(false);
  };

  const handleApprove = async () => {
    if (!state.activeTx || !state.calculation?.canApprove) return;
    if (state.isOverride && state.overrideReason.trim().length < 20) {
      dispatch({ type: 'SET_ERROR', payload: 'Override justification must be at least 20 characters.' }); return;
    }
    if (state.isSplitPayment) {
      const applied = parseDecimal(state.splitAppliedAmount);
      if (applied <= 0) { dispatch({ type: 'SET_ERROR', payload: 'Enter the amount to apply for partial payment.' }); return; }
      const allItems = [...(state.panel?.suggestedItems||[]), ...(state.panel?.complementaryItems||[]), ...extraPages];
      const sel = allItems.find(i => state.selectedIds.includes(i.id));
      if (sel && applied >= parseFloat(sel.conciliableAmount)) {
        dispatch({ type: 'SET_ERROR', payload: 'Partial amount must be less than the invoice total.' }); return;
      }
    }
    dispatch({ type: 'APPROVING' });
    try {
      const allItems = [...(state.panel?.suggestedItems||[]), ...(state.panel?.complementaryItems||[]), ...extraPages];
      const sel = state.isSplitPayment ? allItems.find(i => state.selectedIds.includes(i.id)) : null;
      await approve({ stgId: state.activeTx.id, payload: {
        selected_portfolio_ids: state.selectedIds,
        portfolio_ids:          state.selectedIds,
        adjustments: {
          commission:      parseDecimal(state.adjustments.commission),
          taxIva:          parseDecimal(state.adjustments.taxIva),
          taxIrf:          parseDecimal(state.adjustments.taxIrf),
          diffAmount:      parseDecimal(state.adjustments.diffAmount),
          diffAccountCode: state.adjustments.diffAccountCode || null,
        },
        is_override: state.isOverride, override_reason: state.overrideReason || null,
        is_split_payment: state.isSplitPayment,
        split_data: state.isSplitPayment && sel ? {
          parentStgId:    sel.id,
          appliedAmount:  parseDecimal(state.splitAppliedAmount),
          residualAmount: parseFloat(sel.conciliableAmount) - parseDecimal(state.splitAppliedAmount),
          settlementId:   state.activeTx?.settlementId,
          commission:     parseDecimal(state.adjustments.commission),
          taxIva:         parseDecimal(state.adjustments.taxIva),
          taxIrf:         parseDecimal(state.adjustments.taxIrf),
          financialGross: state.activeTx?.financialAmounts?.gross || 0,
          financialNet:   state.activeTx?.financialAmounts?.net   || 0,
        } : null,
      }});
      if (renewTimerRef.current) clearInterval(renewTimerRef.current);
      dispatch({ type: 'APPROVED' });
      refetchQueue(); refetchApproved();
    } catch (err) {
      dispatch({ type: 'APPROVE_ERROR', payload: err.response?.data?.message || 'Approval failed.' });
    }
  };

  const stats          = statsData || {};
  const scenario       = state.panel?.scenario || '';
  const isUnknown      = scenario === 'PENDING_UNKNOWN_CLIENT';
  const isTC           = state.activeTx?.transType === 'LIQUIDACION TC';
  const pagination     = state.panel?.pagination || {};
  const uiConfig       = state.panel?.uiConfig   || {};
  const canActivateSplit = state.selectedIds.length >= 1 && !state.isSplitPayment;
  const allPanelItems  = [...(state.panel?.suggestedItems||[]), ...(state.panel?.complementaryItems||[]), ...extraPages];
  const singleSelected  = state.selectedIds.length === 1 ? allPanelItems.find(i => i.id === state.selectedIds[0]) : null;
  // splitTargetItem: la factura objetivo del pago parcial
  // Busca en allPanelItems. Si vino del search global puede no estar ahí —
  // en ese caso usa singleSelected como fallback seguro.
  const splitTargetItem = state.isSplitPayment
    ? (state.splitParentStgId
        ? (allPanelItems.find(i => i.id === state.splitParentStgId) || singleSelected)
        : singleSelected)
    : null;
  const allSelectedItems = allPanelItems.filter(i => state.selectedIds.includes(i.id));
  const done  = stats.approved_count || 0;
  const total = stats.total_count    || 0;
  const pct   = total > 0 ? Math.round((done / total) * 100) : 0;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: '12px', background: T.base }}>

      {/* ── KPI CARDS ─────────────────────────────────────────────────────── */}
      <div style={{ display: 'flex', gap: '10px', flexShrink: 0 }}>
        <KpiCard label="Pending"  count={stats.pending_count}  amount={stats.pending_amount}
          accentColor='#C0504A' accentBg='rgba(192,80,74,0.12)' icon={IconPending} />
        <KpiCard label="Review"   count={stats.review_count}   amount={stats.review_amount}
          accentColor={T.yellow} accentBg={T.yellowBg} icon={IconReview} />
        <KpiCard label="Approved" count={stats.approved_count} amount={stats.approved_amount}
          accentColor={T.emerald} accentBg={T.emeraldBg} icon={IconMatched} />
        {/* Progress KPI */}
        <div style={{
          background: T.surface, border: `1px solid ${T.border}`,
          borderTop: `3px solid ${T.purple}`, borderRadius: '10px',
          padding: '14px 18px', flex: 1, boxShadow: '0 1px 4px rgba(28,32,20,0.07)',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
            <div style={{ width: '26px', height: '26px', borderRadius: '6px', background: T.purpleBg,
                          display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <IconProgress size={13} style={{ color: T.purple }} />
            </div>
            <span style={{ fontSize: '0.75rem', fontWeight: 500, letterSpacing: '0.04em',
                           textTransform: 'uppercase', color: T.textSecond }}>Progress</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <span style={{ fontSize: '1.6rem', fontWeight: 700, color: T.textPrimary,
                           lineHeight: 1, fontVariantNumeric: 'tabular-nums' }}>
              {done}/{total}
            </span>
            <div style={{ flex: 1 }}>
              <div style={{ background: T.border, borderRadius: '999px', height: '6px' }}>
                <div style={{ background: T.purple, height: '6px', borderRadius: '999px',
                               width: `${pct}%`, transition: 'width 0.3s' }} />
              </div>
              <span style={{ color: T.textMuted, fontSize: '0.7rem', marginTop: '2px', display: 'block' }}>{pct}%</span>
            </div>
          </div>
        </div>
      </div>

      {/* ── MAIN SPLIT ────────────────────────────────────────────────────── */}
      <div style={{
        flex: 1, minHeight: 0, display: 'grid', gap: '12px',
        gridTemplateColumns: (state.activeTx || selectedApproved) ? '268px 1fr' : '1fr',
      }}>

        {/* ── LEFT — Queue + Done ──────────────────────────────────────────── */}
        <div style={{
          display: 'flex', flexDirection: 'column',
          background: T.surface, border: `1px solid ${T.border}`,
          borderRadius: '10px', overflow: 'hidden',
        }}>
          {/* ── Header: tabs Queue / Done ─────────────────────────────── */}
          <div style={{ flexShrink: 0, padding: '10px 12px', borderBottom: `1px solid ${T.border}`,
                        background: T.surface2 }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
              {/* Tabs compactos — no full-width */}
              <div style={{ display: 'flex', gap: '4px' }}>
                <button onClick={() => { setActiveTab('QUEUE'); setSelectedApproved(null); }} style={{
                  padding: '4px 10px', borderRadius: '5px', cursor: 'pointer',
                  fontSize: '0.72rem', fontWeight: activeTab === 'QUEUE' ? 600 : 400,
                  border: `1px solid ${activeTab === 'QUEUE' ? T.greenBorder : T.border}`,
                  background: activeTab === 'QUEUE' ? T.green : 'none',
                  color: activeTab === 'QUEUE' ? T.greenText : T.textSecond,
                  transition: 'all 0.12s',
                }}>
                  Queue <span style={{ opacity: 0.8 }}>({filteredQueue.length})</span>
                </button>
                <button onClick={() => { setActiveTab('APPROVED'); setSelectedApproved(null); }} style={{
                  padding: '4px 10px', borderRadius: '5px', cursor: 'pointer',
                  fontSize: '0.72rem', fontWeight: activeTab === 'APPROVED' ? 600 : 400,
                  border: `1px solid ${activeTab === 'APPROVED' ? 'rgba(90,154,53,0.5)' : T.border}`,
                  background: activeTab === 'APPROVED' ? T.emerald : 'none',
                  color: activeTab === 'APPROVED' ? '#FFF' : T.textSecond,
                  transition: 'all 0.12s',
                }}>
                  Done <span style={{ opacity: 0.8 }}>({approvedToday.length})</span>
                </button>
              </div>
              <button
                onClick={() => activeTab === 'QUEUE' ? refetchQueue() : refetchApproved()}
                title="Refresh"
                style={{ background: 'none', border: 'none', cursor: 'pointer', color: T.textMuted,
                         display: 'flex', alignItems: 'center', padding: '4px' }}>
                <RefreshCw size={12} />
              </button>
            </div>

            {/* Sub-filtros de estado — solo en QUEUE, compactos */}
            {activeTab === 'QUEUE' && (
              <div style={{ display: 'flex', gap: '4px' }}>
                {[
                  { label: 'All',     val: 'ALL',     ac: T.green,   ab: T.greenGlow,           abr: T.greenBorder           },
                  { label: 'Pending', val: 'PENDING', ac: '#C0504A', ab: 'rgba(192,80,74,0.12)', abr: 'rgba(192,80,74,0.4)'  },
                  { label: 'Review',  val: 'REVIEW',  ac: T.yellow,  ab: T.yellowBg,             abr: 'rgba(200,154,32,0.4)' },
                ].map(f => (
                  <button key={f.val} onClick={() => setQueueFilter(f.val)} style={{
                    flex: 1, padding: '3px 0', borderRadius: '5px', cursor: 'pointer',
                    fontSize: '0.68rem', fontWeight: queueFilter === f.val ? 600 : 400,
                    border: `1px solid ${queueFilter === f.val ? f.abr : T.border}`,
                    background: queueFilter === f.val ? f.ac : 'none',
                    color: queueFilter === f.val ? (f.val === 'ALL' ? T.greenText : '#FFF') : T.textSecond,
                    transition: 'all 0.12s',
                  }}>{f.label}</button>
                ))}
              </div>
            )}

            {/* Breadcrumb cuando está en detalle de aprobada */}
            {activeTab === 'APPROVED' && selectedApproved && (
              <button onClick={() => setSelectedApproved(null)} style={{
                display: 'flex', alignItems: 'center', gap: '5px',
                background: 'none', border: 'none', cursor: 'pointer',
                color: T.textMuted, fontSize: '0.7rem', padding: '0',
              }}>
                <ArrowLeft size={11} /> Back to list
              </button>
            )}
          </div>

          {/* ── Lista y contenido scrollable ─────────────────────────── */}
          <div style={{ flex: 1, overflowY: 'auto' }}>

            {/* QUEUE — lista de transacciones pendientes */}
            {activeTab === 'QUEUE' && (
              queueLoading
                ? <div style={{ textAlign: 'center', color: T.textMuted, fontSize: '0.75rem', padding: '32px 0' }}>Loading...</div>
                : !filteredQueue.length
                ? <div style={{ textAlign: 'center', color: T.textMuted, fontSize: '0.75rem', padding: '32px 0' }}>No transactions</div>
                : filteredQueue.map((tx, i) => {
                    const s = STATUS_BADGE[tx.reconcileStatus] || { bg: T.surface2, color: T.textMuted, border: T.border };
                    const isActive = state.activeTx?.id === tx.id;
                    const isAlt    = i % 2 === 1;
                    return (
                      <button key={tx.id} onClick={() => handleSelectTx(tx)} style={{
                        width: '100%', textAlign: 'left', padding: '10px 14px',
                        borderBottom: `1px solid ${T.border}`, cursor: 'pointer',
                        background: isActive ? 'rgba(58,82,48,0.08)' : isAlt ? T.surface2 : T.surface,
                        borderLeft: isActive ? `3px solid ${T.green}` : '3px solid transparent',
                        transition: 'all 0.1s',
                      }}
                        onMouseEnter={e => { if (!isActive) e.currentTarget.style.background = '#E4E0D4'; }}
                        onMouseLeave={e => { if (!isActive) e.currentTarget.style.background = isAlt ? T.surface2 : T.surface; }}
                      >
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '3px' }}>
                          <span style={{ color: T.textSecond, fontSize: '0.72rem',
                                         overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '120px' }}>
                            {tx.bankRef1}
                          </span>
                          <span style={{ fontSize: '0.65rem', fontWeight: 700, padding: '1px 6px', borderRadius: '4px',
                                         background: s.bg, color: s.color, border: `1px solid ${s.border}`,
                                         textTransform: 'uppercase', letterSpacing: '0.03em' }}>
                            {tx.reconcileStatus}
                          </span>
                        </div>
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                          <span style={{ color: T.textPrimary, fontSize: '0.82rem', fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>
                            ${fmt(tx.amountTotal)}
                          </span>
                          <span style={{ color: T.textMuted, fontSize: '0.7rem' }}>
                            {new Date(tx.bankDate).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}
                          </span>
                        </div>
                        <div style={{ color: T.textMuted, fontSize: '0.7rem', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', marginTop: '2px' }}>
                          {tx.transType}{tx.brand && tx.brand !== 'GENERICO' && tx.brand !== 'CASH' ? ` · ${tx.brand}` : ''}
                        </div>
                        {tx.customerName && (
                          <div style={{ color: T.textSecond, fontSize: '0.7rem', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {tx.customerName}
                          </div>
                        )}
                        {/* enrichNotes como badge compacto, no texto truncado */}
                        {tx.enrichNotes && (
                          <div style={{ marginTop: '4px' }}>
                            <span style={{
                              display: 'inline-flex', alignItems: 'center', gap: '3px',
                              fontSize: '0.64rem', color: T.blue,
                              background: T.blueBg, padding: '1px 6px', borderRadius: '4px',
                              border: `1px solid rgba(58,120,201,0.2)`,
                              maxWidth: '100%', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                            }}>
                              <Eye size={9} style={{ flexShrink: 0 }} />
                              {tx.enrichNotes.length > 32 ? tx.enrichNotes.slice(0, 32) + '…' : tx.enrichNotes}
                            </span>
                          </div>
                        )}
                        {tx.isLocked && tx.lockedBy && (
                          <div style={{ display: 'flex', alignItems: 'center', gap: '4px', marginTop: '4px' }}>
                            <Lock size={10} style={{ color: T.orange }} />
                            <span style={{ color: T.orange, fontSize: '0.68rem' }}>{tx.lockedBy}</span>
                          </div>
                        )}
                      </button>
                    );
                  })
            )}

            {/* DONE — lista de aprobadas hoy */}
            {activeTab === 'APPROVED' && !selectedApproved && (
              !approvedToday.length
                ? (
                  <div style={{ textAlign: 'center', padding: '40px 20px' }}>
                    <IconMatched size={28} style={{ color: T.border2, margin: '0 auto 10px', display: 'block' }} />
                    <p style={{ color: T.textMuted, fontSize: '0.78rem', margin: '0 0 4px' }}>No approvals yet today</p>
                    <p style={{ color: T.textMuted, fontSize: '0.72rem', opacity: 0.7 }}>Approved transactions will appear here</p>
                  </div>
                )
                : (
                  <>
                    {/* Resumen del día — total y cantidad */}
                    <div style={{
                      padding: '10px 14px', borderBottom: `1px solid ${T.border}`,
                      background: 'rgba(90,154,53,0.06)',
                      display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                    }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                        <IconMatched size={12} style={{ color: T.emerald }} />
                        <span style={{ color: T.emerald, fontSize: '0.7rem', fontWeight: 600 }}>
                          {approvedToday.length} approved today
                        </span>
                      </div>
                      <span style={{ color: T.textSecond, fontSize: '0.72rem', fontWeight: 600,
                                     fontVariantNumeric: 'tabular-nums' }}>
                        ${fmt(approvedToday.reduce((s, t) => s + (parseFloat(t.amountTotal) || 0), 0))}
                      </span>
                    </div>
                    {/* Lista de aprobadas — click abre detalle */}
                    {approvedToday.map((tx, i) => (
                      <button key={tx.id} onClick={() => setSelectedApproved(tx)} style={{
                        width: '100%', textAlign: 'left', padding: '10px 14px',
                        borderBottom: `1px solid ${T.border}`, cursor: 'pointer',
                        background: i % 2 === 1 ? T.surface2 : T.surface,
                        borderLeft: '3px solid transparent',
                        transition: 'all 0.1s',
                      }}
                        onMouseEnter={e => {
                          e.currentTarget.style.background = '#E4E0D4';
                          e.currentTarget.style.borderLeftColor = T.emerald;
                        }}
                        onMouseLeave={e => {
                          e.currentTarget.style.background = i % 2 === 1 ? T.surface2 : T.surface;
                          e.currentTarget.style.borderLeftColor = 'transparent';
                        }}
                      >
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '3px' }}>
                          <span style={{ color: T.textSecond, fontSize: '0.72rem',
                                         overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '130px' }}>
                            {tx.bankRef1}
                          </span>
                          <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                            {tx.hasPendingReversal && (
                              <span style={{ fontSize: '0.6rem', color: T.yellow, background: T.yellowBg,
                                             padding: '1px 4px', borderRadius: '3px', border: `1px solid rgba(200,154,32,0.3)` }}>
                                ⏳
                              </span>
                            )}
                            <span style={{ color: T.emerald, fontSize: '0.65rem', fontWeight: 700 }}>✓</span>
                          </div>
                        </div>
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                          <span style={{ color: T.textPrimary, fontSize: '0.8rem', fontWeight: 600,
                                         fontVariantNumeric: 'tabular-nums' }}>
                            ${fmt(tx.amountTotal)}
                          </span>
                          <span style={{ color: T.textMuted, fontSize: '0.68rem' }}>
                            {new Date(tx.approvedAt).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })}
                          </span>
                        </div>
                        {tx.transType && (
                          <div style={{ color: T.textMuted, fontSize: '0.68rem', marginTop: '2px',
                                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {tx.transType}{tx.customerName ? ` · ${tx.customerName}` : ''}
                          </div>
                        )}
                      </button>
                    ))}
                  </>
                )
            )}

            {/* DONE — panel de detalle de una aprobada (panel izquierdo — resumen compacto) */}
            {activeTab === 'APPROVED' && selectedApproved && (
              <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>

                {/* Info bancaria compacta */}
                <div style={{
                  padding: '12px 14px', borderBottom: `1px solid ${T.border}`,
                  background: T.surface3, flexShrink: 0,
                }}>
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
                    <span style={{
                      fontSize: '0.62rem', fontWeight: 700, padding: '2px 7px', borderRadius: '4px',
                      background: 'rgba(90,154,53,0.22)', color: '#1E4A10',
                      border: '1px solid rgba(90,154,53,0.5)', textTransform: 'uppercase',
                    }}>✓ Matched</span>
                    <span style={{ color: T.textMuted, fontSize: '0.68rem' }}>
                      {new Date(selectedApproved.approvedAt).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })}
                    </span>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '8px', marginBottom: '6px' }}>
                    <div style={{ minWidth: 0 }}>
                      <div style={{ color: T.textPrimary, fontSize: '0.82rem', fontWeight: 700,
                                    letterSpacing: '-0.01em', marginBottom: '2px',
                                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {selectedApproved.bankRef1}
                      </div>
                      <div style={{ color: T.textSecond, fontSize: '0.7rem',
                                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {selectedApproved.transType}
                        {selectedApproved.customerName && (
                          <span style={{ marginLeft: '4px', color: T.textMuted }}>· {selectedApproved.customerName}</span>
                        )}
                      </div>
                    </div>
                    <div style={{ textAlign: 'right', flexShrink: 0 }}>
                      <div style={{ color: T.textPrimary, fontSize: '1.0rem', fontWeight: 700,
                                    fontVariantNumeric: 'tabular-nums', lineHeight: 1 }}>
                        ${fmt(selectedApproved.amountTotal)}
                      </div>
                      {selectedApproved.bankDate && (
                        <div style={{ color: T.textMuted, fontSize: '0.65rem', marginTop: '2px' }}>
                          {new Date(selectedApproved.bankDate).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}
                        </div>
                      )}
                    </div>
                  </div>
                  {/* Hint — el detalle completo y la acción están en el panel derecho */}
                  <div style={{ fontSize: '0.67rem', color: T.textMuted, fontStyle: 'italic', marginTop: '4px' }}>
                    Full detail and actions →
                  </div>
                </div>

              </div>
            )}

          </div>
        </div>

        {/* ── RIGHT — Panel de reconciliación ──────────────────────────────── */}
        {state.activeTx ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '10px', minWidth: 0, minHeight: 0 }}>
            <BankContextHeader tx={state.activeTx} panel={state.panel} lockActive={state.lockActive} />

            <div style={{
              flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column',
              background: T.surface, border: `1px solid ${T.border}`,
              borderRadius: '10px', overflow: 'hidden',
            }}>
              {/* Toolbar del panel */}
              <div style={{ flexShrink: 0, padding: '8px 14px', borderBottom: `1px solid ${T.border}`,
                             display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                             background: T.surface2 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                  <span style={{ color: T.textSecond, fontSize: '0.78rem', fontWeight: 500 }}>Portfolio</span>
                  {state.selectedIds.length > 0 && (
                    <span style={{ background: T.blueBg, color: T.blue, fontSize: '0.68rem',
                                   padding: '1px 8px', borderRadius: '999px',
                                   border: `1px solid rgba(58,120,201,0.3)` }}>
                      {state.selectedIds.length} selected
                    </span>
                  )}
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                  {/* Hint contextual — guía al botón Partial en la columna Action */}
                  {canActivateSplit && state.selectedIds.length > 0 && !state.isSplitPayment && (
                    <span style={{ color: T.textMuted, fontSize: '0.68rem', fontStyle: 'italic' }}>
                      Use <strong style={{ color: T.orange, fontStyle: 'normal' }}>Partial</strong> in the Action column
                    </span>
                  )}
                  {/* Indicador cuando el split ya está activo */}
                  {state.isSplitPayment && splitTargetItem && (
                    <span style={{
                      display: 'inline-flex', alignItems: 'center', gap: '4px',
                      fontSize: '0.68rem', fontWeight: 600, padding: '2px 8px', borderRadius: '4px',
                      background: 'rgba(200,120,64,0.12)', color: T.orange,
                      border: `1px solid rgba(200,120,64,0.35)`,
                    }}>
                      <Scissors size={10} /> Partial active: {splitTargetItem.invoiceRef}
                    </span>
                  )}
                  <button onClick={() => setSearchOpen(o => !o)} style={{
                    display: 'flex', alignItems: 'center', gap: '5px', padding: '4px 10px',
                    borderRadius: '6px', fontSize: '0.72rem', cursor: 'pointer',
                    background: searchOpen ? T.green : T.surface,
                    border: `1px solid ${searchOpen ? T.greenBorder : T.border}`,
                    color: searchOpen ? T.greenText : T.textSecond,
                  }}>
                    <Search size={11} /> Search Portfolio
                    {searchOpen && <X size={10} style={{ opacity: 0.7 }} />}
                  </button>
                  <button onClick={() => dispatch({ type: state.drawerOpen ? 'CLOSE_DRAWER' : 'OPEN_DRAWER' })}
                    style={{ padding: '4px 10px', borderRadius: '6px', fontSize: '0.72rem', cursor: 'pointer',
                             background: state.drawerOpen ? T.green : T.surface,
                             border: `1px solid ${state.drawerOpen ? T.greenBorder : T.border}`,
                             color: state.drawerOpen ? T.greenText : T.textSecond }}>
                    Adjustments
                  </button>
                  {uiConfig.requireOverrideNote && (
                    <button onClick={() => dispatch({ type: 'SET_OVERRIDE', payload: !state.isOverride })}
                      style={{ display: 'flex', alignItems: 'center', gap: '4px', padding: '4px 10px',
                               borderRadius: '6px', fontSize: '0.72rem', cursor: 'pointer',
                               background: state.isOverride ? T.yellowBg : T.surface,
                               border: `1px solid ${state.isOverride ? 'rgba(200,154,32,0.4)' : T.border}`,
                               color: state.isOverride ? T.yellow : T.textSecond }}>
                      <AlertTriangle size={11} /> Override
                    </button>
                  )}
                </div>
              </div>

              {/* Tabla de portfolio — thead verde + filas alternas */}
              <div style={{ flex: 1, overflowY: 'auto', minHeight: 0 }}>
                {panelLoading || state.isLoadingPanel ? (
                  <div style={{ textAlign: 'center', color: T.textMuted, fontSize: '0.75rem', padding: '48px 0' }}>
                    Loading portfolio...
                  </div>
                ) : (
                  <>
                    <table style={{ width: '100%', borderCollapse: 'collapse', tableLayout: 'fixed' }}>
                      <colgroup>
                        <col style={{ width: '32px' }} />
                        <col />
                        <col style={{ width: '80px' }} />
                        <col style={{ width: '118px' }} />
                        <col style={{ width: '102px' }} />
                        <col style={{ width: '56px' }} />
                        <col style={{ width: '96px' }} />
                        <col style={{ width: '86px' }} />
                      </colgroup>
                      <thead style={{ position: 'sticky', top: 0, zIndex: 5 }}>
                        <tr style={{ background: T.thBg }}>
                          {[
                            { label: '',                      align: 'left'  },
                            { label: 'Invoice / Assignment',  align: 'left'  },
                            { label: 'Date',                  align: 'left'  },
                            { label: 'Customer',              align: 'left'  },
                            { label: 'Amount',                align: 'right' },
                            { label: 'Conf.',                 align: 'left'  },
                            { label: 'Status',                align: 'left'  },
                            { label: 'Action',                align: 'left'  },
                          ].map(col => (
                            <th key={col.label} style={{
                              textAlign: col.align, padding: '8px 12px',
                              fontSize: '0.65rem', fontWeight: 700, letterSpacing: '0.06em',
                              textTransform: 'uppercase', color: T.thText, whiteSpace: 'nowrap', overflow: 'hidden',
                            }}>
                              {col.label}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {state.panel?.suggestedItems?.length > 0 && (
                          <>
                            <tr style={{ background: T.blueBg }}>
                              <td colSpan={8} style={{ padding: '5px 12px', color: T.blue, fontSize: '0.65rem',
                                                        fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em',
                                                        borderBottom: `1px solid rgba(58,120,201,0.2)` }}>
                                Algorithm Suggestions
                              </td>
                            </tr>
                            {state.panel.suggestedItems.map((item, i) => (
                              <InvoiceRow key={item.id} item={item} rowIndex={i}
                                isSelected={state.selectedIds.includes(item.id)}
                                onToggle={id => dispatch({ type: 'TOGGLE_INVOICE', payload: id })}
                                isSplitTarget={splitTargetItem?.id === item.id}
                                onSplit={canActivateSplit
                                  ? (id) => dispatch({ type: 'ENABLE_SPLIT', payload: { parentStgId: id } })
                                  : null} />
                            ))}
                          </>
                        )}
                        {state.panel?.complementaryItems?.length > 0 && (
                          <>
                            <tr style={{ background: T.surface2 }}>
                              <td colSpan={8} style={{ padding: '5px 12px', color: T.textMuted, fontSize: '0.65rem',
                                                        fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em',
                                                        borderBottom: `1px solid ${T.border}` }}>
                                {state.panel.suggestedItems?.length > 0 ? 'Other Open Invoices' : 'Open Invoices'}
                              </td>
                            </tr>
                            {state.panel.complementaryItems.map((item, i) => (
                              <InvoiceRow key={item.id} item={item} rowIndex={i}
                                isSelected={state.selectedIds.includes(item.id)}
                                onToggle={id => dispatch({ type: 'TOGGLE_INVOICE', payload: id })}
                                isSplitTarget={splitTargetItem?.id === item.id}
                                onSplit={canActivateSplit
                                  ? (id) => dispatch({ type: 'ENABLE_SPLIT', payload: { parentStgId: id } })
                                  : null} />
                            ))}
                          </>
                        )}
                        {extraPages.length > 0 && extraPages.map((item, i) => (
                          <InvoiceRow key={`extra-${item.id}`} item={item} rowIndex={i}
                            isSelected={state.selectedIds.includes(item.id)}
                            onToggle={id => dispatch({ type: 'TOGGLE_INVOICE', payload: id })}
                            isSplitTarget={splitTargetItem?.id === item.id}
                            onSplit={canActivateSplit
                              ? (id) => dispatch({ type: 'ENABLE_SPLIT', payload: { parentStgId: id } })
                              : null} />
                        ))}
                      </tbody>
                    </table>
                    <LoadMoreSection hasNextPage={isUnknown && pagination.hasNextPage}
                      onLoadMore={handleLoadMore} isLoading={isLoadingMore} />
                  </>
                )}
              </div>

              {/* Search global */}
              {state.activeTx && !panelLoading && searchOpen && (
                <PortfolioSearchSection stgId={state.activeTx.id} bankAmount={state.activeTx.amountTotal}
                  selectedIds={state.selectedIds} allItems={allPanelItems}
                  onToggle={id => dispatch({ type: 'TOGGLE_INVOICE', payload: id })} />
              )}

              {state.isSplitPayment && splitTargetItem && (
                <SplitPaymentPanel selectedItem={splitTargetItem} state={state} dispatch={dispatch} />
              )}

              <AdjustmentDrawer state={state} dispatch={dispatch} isTC={isTC} />

              {state.isOverride && (
                <div style={{ flexShrink: 0, padding: '12px 16px', borderTop: `1px solid ${T.border}`, background: T.surface2 }}>
                  <OverrideSection state={state} dispatch={dispatch} />
                </div>
              )}

              {/* Footer */}
              <div style={{ flexShrink: 0, padding: '12px 16px', borderTop: `1px solid ${T.border}`,
                             background: T.surface2, display: 'flex', flexDirection: 'column', gap: '8px' }}>
                <BalanceBar calculation={state.calculation} onOpenAdjustments={() => dispatch({ type: 'OPEN_DRAWER' })} />
                {state.error && (
                  <div style={{ background: T.redBg, border: `1px solid rgba(192,80,74,0.3)`,
                                 borderRadius: '7px', padding: '7px 12px', fontSize: '0.75rem',
                                 color: T.red, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <span>{state.error}</span>
                    <button onClick={() => dispatch({ type: 'CLEAR_ERROR' })}
                      style={{ background: 'none', border: 'none', cursor: 'pointer', color: T.red, marginLeft: '8px' }}>
                      <X size={12} />
                    </button>
                  </div>
                )}
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <button onClick={() => dispatch({ type: 'OPEN_DRAWER' })} style={{
                    padding: '8px 14px', background: T.surface,
                    border: `1px solid ${T.border}`, color: T.textSecond,
                    borderRadius: '8px', fontSize: '0.78rem', cursor: 'pointer' }}>
                    Distribute
                  </button>
                  <button onClick={handleApprove}
                    disabled={!state.calculation?.canApprove || state.isApproving || !state.lockActive ||
                      (state.isOverride && state.overrideReason.trim().length < 20) ||
                      (state.isSplitPayment && !parseDecimal(state.splitAppliedAmount))}
                    style={{
                      flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '8px',
                      padding: '10px', borderRadius: '8px', fontSize: '0.85rem', fontWeight: 600,
                      cursor: state.calculation?.canApprove && state.lockActive ? 'pointer' : 'not-allowed',
                      background: state.calculation?.canApprove && state.lockActive ? T.green : T.surface2,
                      border: `1px solid ${state.calculation?.canApprove && state.lockActive ? T.greenBorder : T.border}`,
                      color: state.calculation?.canApprove && state.lockActive ? T.greenText : T.textMuted,
                      opacity: state.isApproving ? 0.7 : 1,
                    }}>
                    <IconMatched size={15} />
                    {state.isApproving ? 'Approving...' : 'Approve Match'}
                  </button>
                </div>
              </div>
            </div>
          </div>
        ) : selectedApproved ? (
          /* ── Panel derecho: detalle enriquecido de transacción aprobada ──── */
          <ApprovedDetailPanel
            selectedApproved={selectedApproved}
            onReversal={(target) => setReversalTarget(target)}
            isRequestingReversal={isRequestingReversal}
          />
        ) : (
          /* ── Empty state — panel derecho sin selección ───────────────────── */
          <div style={{
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            background: T.surface, border: `1px solid ${T.border}`, borderRadius: '10px',
          }}>
            <div style={{ textAlign: 'center', maxWidth: '360px', padding: '24px' }}>
              {activeTab === 'APPROVED' && approvedToday.length > 0 ? (
                <>
                  <List size={28} style={{ color: T.border2, marginBottom: '10px', display: 'block', margin: '0 auto 10px' }} />
                  <p style={{ color: T.textSecond, fontSize: '0.85rem', margin: '0 0 4px', fontWeight: 500 }}>
                    Select an approval to review
                  </p>
                  <p style={{ color: T.textMuted, fontSize: '0.75rem', margin: '0 0 16px' }}>
                    {approvedToday.length} transactions approved today
                  </p>
                  {/* Resumen del día — compacto */}
                  <div style={{
                    padding: '12px 16px', borderRadius: '8px',
                    background: 'rgba(90,154,53,0.06)', border: '1px solid rgba(90,154,53,0.2)',
                    display: 'flex', flexDirection: 'column', gap: '6px',
                  }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                      <span style={{ color: T.textMuted, fontSize: '0.72rem' }}>Total approved</span>
                      <span style={{ color: T.emerald, fontSize: '0.78rem', fontWeight: 700, fontVariantNumeric: 'tabular-nums' }}>
                        ${fmt(approvedToday.reduce((s, t) => s + (parseFloat(t.amountTotal) || 0), 0))}
                      </span>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                      <span style={{ color: T.textMuted, fontSize: '0.72rem' }}>Transactions</span>
                      <span style={{ color: T.textPrimary, fontSize: '0.78rem', fontWeight: 600 }}>{approvedToday.length}</span>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                      <span style={{ color: T.textMuted, fontSize: '0.72rem' }}>Last approved</span>
                      <span style={{ color: T.textSecond, fontSize: '0.72rem' }}>
                        {new Date(approvedToday[0]?.approvedAt).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })}
                      </span>
                    </div>
                  </div>
                </>
              ) : (
                <>
                  <ChevronRight size={28} style={{ color: T.border2, display: 'block', margin: '0 auto 10px' }} />
                  <p style={{ color: T.textSecond, fontSize: '0.85rem', margin: '0 0 4px', fontWeight: 500 }}>
                    Select a transaction from the queue
                  </p>
                  <p style={{ color: T.textMuted, fontSize: '0.75rem', margin: 0 }}>
                    {queue?.length || 0} transactions waiting
                  </p>
                </>
              )}
            </div>
          </div>
        )}
      </div>
      {/* Reversal Modal */}
      {reversalTarget && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)',
                      display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 50, padding: '16px' }}>
          <div style={{
            background: T.surface, borderRadius: '12px', width: '100%', maxWidth: '400px',
            border: `1px solid rgba(192,80,74,0.35)`,
            boxShadow: '0 8px 32px rgba(192,80,74,0.18), 0 2px 8px rgba(0,0,0,0.15)',
          }}>
            {/* Header con acento rojo */}
            <div style={{
              padding: '16px 20px 12px', borderBottom: `1px solid ${T.border}`,
              borderRadius: '12px 12px 0 0', background: 'rgba(192,80,74,0.06)',
              display: 'flex', alignItems: 'center', gap: '10px',
            }}>
              <div style={{
                width: '32px', height: '32px', borderRadius: '8px', flexShrink: 0,
                background: 'rgba(192,80,74,0.15)', border: `1px solid rgba(192,80,74,0.30)`,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}>
                <RotateCcw size={15} style={{ color: T.red }} />
              </div>
              <div>
                <h3 style={{ color: T.textPrimary, fontWeight: 700, margin: 0, fontSize: '0.95rem' }}>
                  Reverse Match
                </h3>
                <p style={{ color: T.textMuted, fontSize: '0.72rem', margin: 0 }}>
                  {reversalTarget.bankRef1}
                </p>
              </div>
            </div>
            {/* Body */}
            <div style={{ padding: '16px 20px' }}>
              <div style={{
                padding: '8px 12px', borderRadius: '7px', marginBottom: '12px',
                background: 'rgba(192,80,74,0.08)', border: `1px solid rgba(192,80,74,0.20)`,
              }}>
                <p style={{ color: T.red, fontSize: '0.72rem', margin: 0, fontWeight: 500 }}>
                  ⚠ This action will undo the approved match and return the transaction to the queue.
                </p>
              </div>
              <label style={{ display: 'block', color: T.textSecond, fontSize: '0.72rem',
                              fontWeight: 600, marginBottom: '6px', textTransform: 'uppercase',
                              letterSpacing: '0.04em' }}>
                Reason for reversal *
              </label>
              <textarea
                value={reversalReason}
                onChange={e => setReversalReason(e.target.value)}
                placeholder="Explain why this match needs to be reversed (minimum 10 characters)..."
                rows={3}
                style={{
                  width: '100%', background: T.surface2,
                  border: `1px solid ${reversalReason.trim().length >= 10 ? T.border2 : T.border}`,
                  color: T.textPrimary, fontSize: '0.78rem', borderRadius: '7px',
                  padding: '8px 12px', outline: 'none', resize: 'none',
                  marginBottom: '4px', boxSizing: 'border-box', lineHeight: 1.5,
                }}
              />
              <p style={{ color: T.textMuted, fontSize: '0.65rem', margin: '0 0 14px', textAlign: 'right' }}>
                {reversalReason.trim().length}/10 min
              </p>
              <div style={{ display: 'flex', gap: '8px' }}>
                <button
                  onClick={() => { setReversalTarget(null); setReversalReason(''); }}
                  style={{
                    flex: 1, padding: '9px', background: T.surface2,
                    border: `1px solid ${T.border}`, color: T.textSecond,
                    borderRadius: '8px', fontSize: '0.78rem', cursor: 'pointer',
                    fontWeight: 500, transition: 'all 0.15s',
                  }}
                  onMouseEnter={e => e.currentTarget.style.background = T.surface3}
                  onMouseLeave={e => e.currentTarget.style.background = T.surface2}
                >
                  Cancel
                </button>
                <button
                  onClick={() => requestReversal({ stgId: reversalTarget.id, reason: reversalReason })}
                  disabled={reversalReason.trim().length < 10 || isRequestingReversal}
                  style={{
                    flex: 1, padding: '9px',
                    background: reversalReason.trim().length < 10 ? T.surface2 : T.red,
                    border: `1px solid ${reversalReason.trim().length < 10 ? T.border : 'rgba(192,80,74,0.6)'}`,
                    color: reversalReason.trim().length < 10 ? T.textMuted : '#FFF',
                    borderRadius: '8px', fontSize: '0.78rem', fontWeight: 600,
                    cursor: reversalReason.trim().length < 10 ? 'not-allowed' : 'pointer',
                    display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '6px',
                    transition: 'all 0.15s',
                    boxShadow: reversalReason.trim().length >= 10 ? '0 2px 6px rgba(192,80,74,0.25)' : 'none',
                  }}
                  onMouseEnter={e => { if (reversalReason.trim().length >= 10 && !isRequestingReversal) e.currentTarget.style.background = '#A03A34'; }}
                  onMouseLeave={e => { if (reversalReason.trim().length >= 10 && !isRequestingReversal) e.currentTarget.style.background = T.red; }}
                >
                  {isRequestingReversal
                    ? <><Loader2 size={13} style={{ animation: 'spin 1s linear infinite' }} /> Processing...</>
                    : <><RotateCcw size={13} /> Confirm Reversal</>
                  }
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}