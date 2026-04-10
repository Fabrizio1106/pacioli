// src/pages/Overview/index.jsx  v4 — date range + analyst filter + notes
import React, { useState, useRef, useCallback } from 'react';
import { useAuthStore }      from '../../stores/auth.store.js';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  useOverviewData,
  useAnalysts,
  useReassignTransaction,
  useOverviewFilters,
}                            from '../../hooks/useOverview.js';
import { updateAnalystNoteRequest, syncMatchesRequest } from '../../api/endpoints/overview.api.js';
import { applyAssignmentRulesRequest } from '../../api/endpoints/assignments.api.js';
import { IconMatched, IconPending, IconReview, IconTotal } from '../../components/icons/CustomIcons.jsx';
import {
  RefreshCw, Search,
  Zap, X, MessageSquare, Plus, Save, Users,
}                            from 'lucide-react';

// ─── Paleta Titanium & Emerald (Modo Datos) ──────────────────────────────────
const T = {
  base:        'var(--color-report-base)',
  surface:     'var(--color-report-surface)',
  surface2:    'var(--color-report-row-alt)',
  border:      'var(--color-report-border)',
  border2:     '#CBD5E1', // Slate-300 para bordes de inputs
  thBg:        'var(--color-report-header)',
  thText:      '#E2E8F0',
  dateBg:      '#F1F5F9',
  dateText:    'var(--color-report-text2)',
  subtotalBg:  '#F8FAFC',
  totalBg:     '#F1F5F9',
  textPrimary: 'var(--color-report-text)',
  textSecond:  'var(--color-report-text2)',
  textMuted:   '#94A3B8', // Slate-400
  orange:      'var(--color-vault-orange)',
  orangeText:  '#FFFFFF',
  orangeHover: 'var(--color-vault-orange-soft)',
  orangeBorder:'var(--color-vault-orange)',
  orangeGlow:  'var(--color-vault-orange-glow)',
  green:       'var(--color-vault-green)',
  greenText:   '#FFFFFF',
  greenHover:  'var(--color-vault-green-soft)',
  greenBorder: 'var(--color-vault-green)',
  greenGlow:   'var(--color-vault-green-glow)',
  amber:       'var(--color-vault-amber)',
  amberBg:     'var(--color-vault-amber-dim)',
  amberBorder: 'rgba(245, 158, 11, 0.25)',
  yellow:      'var(--color-vault-amber)',
  yellowBg:    'var(--color-vault-amber-dim)',
  yellowBorder:'rgba(245, 158, 11, 0.25)',
  emerald:     'var(--color-vault-green)',
  emeraldBg:   'var(--color-vault-green-glow)',
  emeraldBorder:'rgba(16, 185, 129, 0.25)',
  blue:        'var(--color-vault-blue)',
  blueBg:      'var(--color-vault-blue-dim)',
  blueBorder:  'rgba(59, 130, 246, 0.25)',
  red:         'var(--color-vault-red)',
  noteBg:      'rgba(245, 158, 11, 0.08)',
  noteBorder:  'rgba(245, 158, 11, 0.25)',
  noteColor:   '#92400E',
  noteAccent:  'var(--color-vault-amber)',
};

const STATUS_BADGE = {
  PENDING:        { bg: 'rgba(239, 68, 68, 0.12)', color: '#991B1B', border: 'rgba(239, 68, 68, 0.3)'  },
  REVIEW:         { bg: 'rgba(245, 158, 11, 0.12)', color: '#92400E', border: 'rgba(245, 158, 11, 0.3)'  },
  MATCHED:        { bg: 'rgba(16, 185, 129, 0.12)', color: '#065F46', border: 'rgba(16, 185, 129, 0.3)'  },
  MATCHED_MANUAL: { bg: 'rgba(16, 185, 129, 0.12)', color: '#065F46', border: 'rgba(16, 185, 129, 0.3)'  },
  REVERSED:       { bg: 'rgba(239, 68, 68, 0.12)', color: '#991B1B', border: 'rgba(239, 68, 68, 0.3)'  },
};

const fmt = (n) => Number(n || 0).toLocaleString('en-US', {
  minimumFractionDigits: 2, maximumFractionDigits: 2,
});
const fmtCompact = (n) => {
  const v = Number(n || 0);
  if (v >= 1_000_000) return `$${(v/1_000_000).toFixed(1)}M`;
  if (v >= 1_000)     return `$${(v/1_000).toFixed(1)}K`;
  return `$${fmt(v)}`;
};

// ─── KPI Card ─────────────────────────────────────────────────────────────────
function KpiCard({ label, count, amount, accentColor, accentBg, icon: Icon }) {
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
          {count ?? '—'}
        </span>
        <span style={{ fontSize: '0.88rem', color: T.textSecond,
                       fontVariantNumeric: 'tabular-nums', fontWeight: 500 }}>
          {fmtCompact(amount)}
        </span>
      </div>
    </div>
  );
}

function StatusBadge({ status }) {
  const s = STATUS_BADGE[status] || { bg: '#F0EDE6', color: T.textMuted, border: T.border };
  return (
    <span style={{
      fontSize: '0.65rem', fontWeight: 700, padding: '2px 7px', borderRadius: '4px',
      background: s.bg, color: s.color, border: `1px solid ${s.border}`,
      whiteSpace: 'nowrap', letterSpacing: '0.03em', textTransform: 'uppercase',
    }}>{status}</span>
  );
}

function FilterTab({ label, active, onClick }) {
  return (
    <button onClick={onClick} style={{
      padding: '5px 12px', borderRadius: '6px',
      fontSize: '0.75rem', fontWeight: active ? 600 : 400,
      cursor: 'pointer', transition: 'all 0.15s',
      border: `1px solid ${active ? T.greenBorder : T.border}`,
      background: active ? T.green : T.surface,
      color: active ? T.greenText : T.textSecond,
    }}
      onMouseEnter={e => { if (!active) { e.currentTarget.style.borderColor = T.greenBorder; e.currentTarget.style.color = T.green; }}}
      onMouseLeave={e => { if (!active) { e.currentTarget.style.borderColor = T.border; e.currentTarget.style.color = T.textSecond; }}}
    >{label}</button>
  );
}

// ─── Date Range — estilo BlackLine: dos inputs integrados ─────────────────────
function DateRangePicker({ dateFrom, dateTo, onChange }) {
  const inputStyle = {
    background: 'none', border: 'none', outline: 'none',
    color: T.textPrimary, fontSize: '0.75rem', cursor: 'pointer', width: '98px',
  };
  const labelStyle = {
    color: T.textMuted, fontSize: '0.68rem', fontWeight: 600,
    textTransform: 'uppercase', letterSpacing: '0.06em', whiteSpace: 'nowrap',
  };
  const wrapStyle = {
    display: 'flex', alignItems: 'center', gap: '5px',
    background: T.surface, border: `1px solid ${T.border2}`,
    borderRadius: '7px', padding: '4px 10px',
  };
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
      <div style={wrapStyle}>
        <span style={labelStyle}>From</span>
        <input type="date" value={dateFrom || ''}
          onChange={e => onChange('dateFrom', e.target.value || null)} style={inputStyle} />
      </div>
      <span style={{ color: T.textMuted, fontSize: '0.75rem' }}>→</span>
      <div style={wrapStyle}>
        <span style={labelStyle}>To</span>
        <input type="date" value={dateTo || ''}
          onChange={e => onChange('dateTo', e.target.value || null)} style={inputStyle} />
      </div>
      {(dateFrom || dateTo) && (
        <button onClick={() => { onChange('dateFrom', null); onChange('dateTo', null); }}
          title="Clear dates"
          style={{ background: 'none', border: 'none', cursor: 'pointer', color: T.textMuted,
                   display: 'flex', alignItems: 'center' }}>
          <X size={12} />
        </button>
      )}
    </div>
  );
}

// ─── Analyst Filter ───────────────────────────────────────────────────────────
function AnalystFilter({ analysts, value, onChange }) {
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: '5px',
      background: T.surface, border: `1px solid ${T.border2}`,
      borderRadius: '7px', padding: '4px 10px',
    }}>
      <span style={{ color: T.textMuted, fontSize: '0.68rem', fontWeight: 600,
                     textTransform: 'uppercase', letterSpacing: '0.06em', whiteSpace: 'nowrap' }}>
        Analyst
      </span>
      <select value={value || ''}
        onChange={e => onChange(e.target.value || null)}
        style={{
          background: 'none', border: 'none', outline: 'none',
          color: value ? T.textPrimary : T.textMuted,
          fontSize: '0.75rem', cursor: 'pointer',
          minWidth: '110px', maxWidth: '160px',
        }}
      >
        <option value="">All analysts</option>
        {analysts.map(a => (
          <option key={a.id} value={a.id}>{a.full_name || a.username}</option>
        ))}
      </select>
      {value && (
        <button onClick={() => onChange(null)}
          style={{ background: 'none', border: 'none', cursor: 'pointer', color: T.textMuted,
                   display: 'flex', alignItems: 'center' }}>
          <X size={11} />
        </button>
      )}
    </div>
  );
}

// ─── Assigned Selector ────────────────────────────────────────────────────────
function AssignedSelector({ row, analysts, isAdmin }) {
  const { mutateAsync: reassign } = useReassignTransaction();

  if (row.detectedScenario === 'AUTO_MATCHED' && !row.assignedUserId) {
    return (
      <span style={{
        fontSize: '0.65rem', fontWeight: 700, padding: '2px 7px', borderRadius: '4px',
        background: T.blueBg, color: T.blue, border: `1px solid ${T.blueBorder}`,
        whiteSpace: 'nowrap', letterSpacing: '0.03em', textTransform: 'uppercase',
      }}>PACIOLI</span>
    );
  }

  if (!isAdmin) {
    return <span style={{ color: T.textSecond, fontSize: '0.78rem' }}>{row.assignedUserName || '—'}</span>;
  }
  return (
    <select value={row.assignedUserId || ''}
      onChange={async (e) => {
        const newId = e.target.value ? parseInt(e.target.value, 10) : null;
        try { await reassign({ bankRef1: row.bankRef1, toUserId: newId }); } catch {}
      }}
      onClick={e => e.stopPropagation()}
      style={{
        background: T.surface2, border: `1px solid ${T.border}`,
        color: T.textPrimary, fontSize: '0.75rem',
        borderRadius: '5px', padding: '3px 6px',
        width: '100%', cursor: 'pointer', outline: 'none',
      }}
    >
      <option value="">— Unassigned —</option>
      {analysts.map(a => (
        <option key={a.id} value={a.id}>{a.full_name || a.username}</option>
      ))}
    </select>
  );
}

// ─── Note Cell — tooltip + inline editor ─────────────────────────────────────
function NoteCell({ row, isAdmin, onNoteSaved }) {
  const [editing,  setEditing]  = useState(false);
  const [draft,    setDraft]    = useState(row.analystNote || '');
  const [saving,   setSaving]   = useState(false);
  const [showTip,  setShowTip]  = useState(false);
  const [error,    setError]    = useState(null);

  const handleSave = async () => {
    setSaving(true);
    setError(null);
    try {
      await updateAnalystNoteRequest(row.bankRef1, draft.trim() || null);
      onNoteSaved(row.bankRef1, draft.trim() || null);
      setEditing(false);
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to save note');
    } finally {
      setSaving(false);
    }
  };

  const handleCancel = () => {
    setDraft(row.analystNote || '');
    setEditing(false);
  };

  // Con nota existente — chip con tooltip
  if (row.analystNote && !editing) {
    return (
      <div style={{ position: 'relative', display: 'inline-flex', alignItems: 'center' }}>
        <button
          onClick={e => { e.stopPropagation(); if (isAdmin) { setDraft(row.analystNote); setEditing(true); }}}
          onMouseEnter={() => setShowTip(true)}
          onMouseLeave={() => setShowTip(false)}
          style={{
            display: 'flex', alignItems: 'center', gap: '4px',
            background: T.noteBg, border: `1px solid ${T.noteBorder}`,
            borderRadius: '5px', padding: '2px 7px',
            cursor: isAdmin ? 'pointer' : 'default',
          }}
        >
          <MessageSquare size={11} style={{ color: T.noteAccent, flexShrink: 0 }} />
          <span style={{
            color: T.noteColor, fontSize: '0.68rem', fontWeight: 500,
            maxWidth: '96px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
          }}>
            {row.analystNote}
          </span>
        </button>
        {showTip && (
          <div style={{
            position: 'absolute', bottom: 'calc(100% + 6px)', left: 0, zIndex: 50,
            background: '#2A2A1A', border: `1px solid ${T.noteBorder}`,
            borderRadius: '7px', padding: '8px 12px',
            color: '#F0E8C0', fontSize: '0.72rem', lineHeight: 1.5,
            maxWidth: '240px', whiteSpace: 'normal',
            boxShadow: '0 4px 20px rgba(0,0,0,0.35)', pointerEvents: 'none',
          }}>
            <div style={{
              display: 'flex', alignItems: 'center', gap: '5px', marginBottom: '5px',
              color: T.noteAccent, fontSize: '0.62rem', fontWeight: 700,
              textTransform: 'uppercase', letterSpacing: '0.08em',
            }}>
              <MessageSquare size={10} /> Meeting Note
            </div>
            {row.analystNote}
            {isAdmin && (
              <div style={{ marginTop: '5px', color: T.textMuted, fontSize: '0.62rem', opacity: 0.7 }}>
                Click to edit
              </div>
            )}
          </div>
        )}
      </div>
    );
  }

  // Inline editor
  if (editing) {
    return (
      <div onClick={e => e.stopPropagation()}
        style={{ display: 'flex', flexDirection: 'column', gap: '4px', minWidth: '180px' }}>
        <textarea
          autoFocus
          value={draft}
          onChange={e => setDraft(e.target.value)}
          maxLength={500}
          rows={2}
          placeholder="Add meeting note or hint..."
          onKeyDown={e => {
            if (e.key === 'Enter' && e.ctrlKey) handleSave();
            if (e.key === 'Escape') handleCancel();
          }}
          style={{
            width: '100%', background: T.surface, resize: 'vertical', outline: 'none',
            border: `1px solid ${T.noteAccent}`, borderRadius: '5px',
            padding: '4px 8px', fontSize: '0.72rem', color: T.textPrimary,
            boxSizing: 'border-box', minHeight: '48px',
          }}
        />
        {error && (
          <span style={{ fontSize: '0.65rem', color: T.red || '#f87171' }}>
            {error}
          </span>
        )}
        <div style={{ display: 'flex', gap: '4px', justifyContent: 'flex-end' }}>
          {draft.trim() && draft.trim() !== (row.analystNote || '') && (
            <span style={{ fontSize: '0.6rem', color: T.textMuted, alignSelf: 'center', marginRight: '2px' }}>
              Ctrl+Enter to save
            </span>
          )}
          <button onClick={handleCancel} style={{
            padding: '2px 8px', borderRadius: '4px', fontSize: '0.68rem',
            background: T.surface2, border: `1px solid ${T.border}`,
            color: T.textSecond, cursor: 'pointer',
          }}>Cancel</button>
          <button onClick={handleSave} disabled={saving} style={{
            padding: '2px 8px', borderRadius: '4px', fontSize: '0.68rem',
            background: T.green, border: `1px solid ${T.greenBorder}`,
            color: T.greenText, cursor: 'pointer', opacity: saving ? 0.7 : 1,
            display: 'flex', alignItems: 'center', gap: '3px',
          }}>
            <Save size={10} /> {saving ? 'Saving...' : 'Save'}
          </button>
        </div>
      </div>
    );
  }

  // Sin nota — admin: botón +; analista: vacío
  if (isAdmin) {
    return (
      <button
        onClick={e => { e.stopPropagation(); setDraft(''); setEditing(true); }}
        title="Add meeting note"
        style={{
          display: 'flex', alignItems: 'center', gap: '3px',
          background: 'none', border: `1px dashed ${T.border2}`,
          borderRadius: '5px', padding: '2px 8px', cursor: 'pointer',
          color: T.textMuted, fontSize: '0.68rem', transition: 'all 0.15s',
        }}
        onMouseEnter={e => {
          e.currentTarget.style.borderColor = T.noteAccent;
          e.currentTarget.style.color = T.noteColor;
          e.currentTarget.style.background = T.noteBg;
        }}
        onMouseLeave={e => {
          e.currentTarget.style.borderColor = T.border2;
          e.currentTarget.style.color = T.textMuted;
          e.currentTarget.style.background = 'none';
        }}
      >
        <Plus size={10} /> Note
      </button>
    );
  }

  return <span style={{ color: T.textMuted, fontSize: '0.7rem' }}>—</span>;
}

// ─── MAIN PAGE ────────────────────────────────────────────────────────────────
export default function OverviewPage() {
  const user        = useAuthStore(state => state.user);
  const isAdmin     = user?.role === 'admin';
  const canManage   = ['admin', 'senior_analyst'].includes(user?.role);
  const queryClient = useQueryClient();

  const { filters, debouncedFilters, updateFilter } = useOverviewFilters();
  const [search,          setSearch]          = useState('');
  const [debouncedSearch, setDebouncedSearch] = useState('');
  const [syncResult,      setSyncResult]      = useState(null);
  const [rulesResult,     setRulesResult]     = useState(null);
  const [noteOverrides,   setNoteOverrides]   = useState({});

  const handleSearch = (value) => {
    setSearch(value);
    clearTimeout(window._searchTimer);
    window._searchTimer = setTimeout(() => setDebouncedSearch(value), 300);
  };

  const { data, isLoading, refetch } = useOverviewData({
    status:           debouncedFilters.status !== 'ALL' ? debouncedFilters.status : undefined,
    customer:         debouncedSearch || undefined,
    date_from:        debouncedFilters.dateFrom      || undefined,
    date_to:          debouncedFilters.dateTo        || undefined,
    assigned_user_id: debouncedFilters.assignedUserId || undefined,
  });

  const { data: analysts = [] } = useAnalysts();

  const { mutateAsync: syncMatches, isPending: isSyncing } = useMutation({
    mutationFn: () => syncMatchesRequest(),
    onSuccess: (res) => {
      setSyncResult(res.data.data);
      refetch();
      queryClient.invalidateQueries({ queryKey: ['gold-export', 'preview'] });
    },
  });

  // Apply assignment rules — admin only, necesario al reiniciar el sistema desde cero.
  // Paso 1: sincroniza workitems con el snapshot actual de stg_bank_transactions
  // Paso 2: aplica las reglas de biq_auth.assignment_rules a los workitems sin asignar
  const { mutateAsync: applyRules, isPending: isApplyingRules } = useMutation({
    mutationFn: () => applyAssignmentRulesRequest(),
    onSuccess: (res) => {
      setRulesResult(res.data.data);
      refetch();
      queryClient.invalidateQueries({ queryKey: ['workspace'] });
    },
    onError: (err) => {
      setRulesResult({ error: true, message: err.response?.data?.message || 'Failed to apply rules' });
    },
  });

  const handleNoteSaved = useCallback((bankRef1, note) => {
    setNoteOverrides(prev => ({ ...prev, [bankRef1]: note }));
  }, []);

  const clearAllFilters = useCallback(() => {
    updateFilter('dateFrom', null);
    updateFilter('dateTo', null);
    updateFilter('assignedUserId', null);
    setSearch(''); setDebouncedSearch('');
  }, [updateFilter]);

  const summary    = data?.summary    || {};
  const groups     = data?.groups     || [];
  const grandTotal = data?.grandTotal || 0;
  const totalCount = data?.totalCount || 0;

  const hasDateFilter    = debouncedFilters.dateFrom || debouncedFilters.dateTo;
  const hasAnalystFilter = debouncedFilters.assignedUserId;
  const hasAnyFilter     = hasDateFilter || hasAnalystFilter || debouncedSearch;

  let rowIndex = 0;

  const btnAction = {
    display: 'flex', alignItems: 'center', gap: '6px',
    padding: '6px 14px', borderRadius: '7px',
    fontSize: '0.75rem', fontWeight: 500, cursor: 'pointer',
    border: `1px solid ${T.greenBorder}`,
    background: T.green, color: T.greenText,
    whiteSpace: 'nowrap', transition: 'all 0.15s',
  };

  // Auto-dismiss de banners a los 5 segundos
  React.useEffect(() => {
    if (!rulesResult) return;
    const t = setTimeout(() => setRulesResult(null), 5000);
    return () => clearTimeout(t);
  }, [rulesResult]);

  React.useEffect(() => {
    if (!syncResult) return;
    const t = setTimeout(() => setSyncResult(null), 5000);
    return () => clearTimeout(t);
  }, [syncResult]);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: '0',
                  background: T.base }}>

      {/* ── FILA 1: KPI CARDS ─────────────────────────────────────────────── */}
      <div style={{ display: 'flex', gap: '10px', flexShrink: 0, padding: '0 0 10px 0' }}>
        <KpiCard label="Pending"
          count={summary.pending?.count}  amount={summary.pending?.amount}
          accentColor='#C0504A'           accentBg='rgba(192,80,74,0.12)' icon={IconPending} />
        <KpiCard label="Needs Review"
          count={summary.review?.count}   amount={summary.review?.amount}
          accentColor={T.yellow}          accentBg={T.yellowBg}           icon={IconReview} />
        <KpiCard label="Matched"
          count={summary.matched?.count}  amount={summary.matched?.amount}
          accentColor={T.emerald}         accentBg={T.emeraldBg}          icon={IconMatched} />
        <KpiCard label="Total"
          count={summary.total?.count}    amount={summary.total?.amount}
          accentColor={T.blue}            accentBg={T.blueBg}             icon={IconTotal} />
      </div>

      {/* ── CONTENEDOR PRINCIPAL — tabla + toolbar integrada ──────────────── */}
      <div style={{
        flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column',
        background: T.surface, border: `1px solid ${T.border}`,
        borderRadius: '10px', overflow: 'hidden',
        boxShadow: '0 1px 4px rgba(28,32,20,0.06)',
      }}>

        {/* ── TOOLBAR ADMIN — separada visualmente, visible para admin y senior_analyst ─ */}
        {canManage && (
          <div style={{
            flexShrink: 0,
            padding: '8px 16px',
            background: T.surface2,
            borderBottom: `1px solid ${T.border}`,
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          }}>
            {/* Izquierda: label de sección */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{
                fontSize: '0.68rem', fontWeight: 600, letterSpacing: '0.06em',
                textTransform: 'uppercase', color: T.textMuted,
              }}>
                Admin
              </span>
              <div style={{ width: '1px', height: '12px', background: T.border2 }} />
              <span style={{ fontSize: '0.72rem', color: T.textMuted }}>
                Manage assignments and sync automatic matches
              </span>
            </div>

            {/* Derecha: botones de acción */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <button
                onClick={() => applyRules()}
                disabled={isApplyingRules}
                title="Sync workitems from snapshot and apply assignment rules to unassigned transactions"
                style={{
                  display: 'flex', alignItems: 'center', gap: '6px',
                  padding: '5px 12px', borderRadius: '6px',
                  fontSize: '0.73rem', fontWeight: 500, cursor: 'pointer',
                  border: `1px solid ${T.greenBorder}`,
                  background: T.surface, color: T.green,
                  whiteSpace: 'nowrap', transition: 'all 0.15s',
                  opacity: isApplyingRules ? 0.7 : 1,
                }}
                onMouseEnter={e => { e.currentTarget.style.background = T.greenGlow; }}
                onMouseLeave={e => { e.currentTarget.style.background = T.surface; }}
              >
                {isApplyingRules
                  ? <><RefreshCw size={12} style={{ animation: 'spin 1s linear infinite' }} /> Applying...</>
                  : <><Users size={12} /> Apply Rules</>}
              </button>

              <button
                onClick={() => syncMatches()}
                disabled={isSyncing}
                title="Sync automatic matches to Submit for Posting"
                style={{
                  display: 'flex', alignItems: 'center', gap: '6px',
                  padding: '5px 12px', borderRadius: '6px',
                  fontSize: '0.73rem', fontWeight: 500, cursor: 'pointer',
                  border: `1px solid ${T.greenBorder}`,
                  background: T.green, color: T.greenText,
                  whiteSpace: 'nowrap', transition: 'all 0.15s',
                  opacity: isSyncing ? 0.7 : 1,
                }}
              >
                {isSyncing
                  ? <><RefreshCw size={12} style={{ animation: 'spin 1s linear infinite' }} /> Syncing...</>
                  : <><Zap size={12} /> Sync Matches</>}
              </button>

              <div style={{ width: '1px', height: '18px', background: T.border }} />

              <button
                onClick={() => refetch()}
                title="Refresh data"
                style={{
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  width: '30px', height: '30px', borderRadius: '6px',
                  border: `1px solid ${T.border}`,
                  background: T.surface, color: T.textMuted,
                  cursor: 'pointer', transition: 'all 0.15s',
                }}
                onMouseEnter={e => { e.currentTarget.style.borderColor = T.border2; e.currentTarget.style.color = T.textSecond; }}
                onMouseLeave={e => { e.currentTarget.style.borderColor = T.border; e.currentTarget.style.color = T.textMuted; }}
              >
                <RefreshCw size={13} />
              </button>
            </div>
          </div>
        )}

        {/* Banners de resultado — auto-dismiss 5s, apilados compactamente */}
        {(rulesResult || syncResult) && (
          <div style={{ flexShrink: 0 }}>
            {rulesResult && (
              <div style={{
                padding: '7px 16px',
                background: rulesResult.error ? 'rgba(192,80,74,0.06)' : 'rgba(90,154,53,0.06)',
                borderBottom: `1px solid ${rulesResult.error ? 'rgba(192,80,74,0.2)' : 'rgba(90,154,53,0.2)'}`,
                borderLeft: `3px solid ${rulesResult.error ? T.red : T.emerald}`,
                display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                fontSize: '0.75rem',
              }}>
                <span style={{ display: 'flex', alignItems: 'center', gap: '8px', color: T.textSecond }}>
                  <Users size={12} style={{ color: rulesResult.error ? T.red : T.emerald, flexShrink: 0 }} />
                  {rulesResult.error
                    ? <span style={{ color: T.red }}>{rulesResult.message}</span>
                    : <>
                        <strong style={{ color: T.green }}>Rules applied</strong>
                        <span>—</span>
                        <strong style={{ color: T.textPrimary, fontVariantNumeric: 'tabular-nums' }}>
                          {rulesResult.assigned || 0} transactions assigned
                        </strong>
                        {rulesResult.sync?.new_workitems > 0 && (
                          <span style={{ color: T.textMuted }}>· {rulesResult.sync.new_workitems} new workitems</span>
                        )}
                      </>
                  }
                </span>
                <button onClick={() => setRulesResult(null)}
                  style={{ background: 'none', border: 'none', cursor: 'pointer', color: T.textMuted,
                           display: 'flex', alignItems: 'center', padding: '0 0 0 8px' }}>
                  <X size={11} />
                </button>
              </div>
            )}
            {syncResult && (
              <div style={{
                padding: '7px 16px',
                background: 'rgba(58,120,201,0.06)',
                borderBottom: `1px solid rgba(58,120,201,0.2)`,
                borderLeft: `3px solid ${T.blue}`,
                display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                fontSize: '0.75rem',
              }}>
                <span style={{ display: 'flex', alignItems: 'center', gap: '8px', color: T.textSecond }}>
                  <Zap size={12} style={{ color: T.blue, flexShrink: 0 }} />
                  <strong style={{ color: T.blue }}>Sync complete</strong>
                  <span>— {syncResult.message}</span>
                  {syncResult.synced > 0 && (
                    <span style={{ color: T.textMuted }}>· {syncResult.synced} ready in Submit for Posting</span>
                  )}
                </span>
                <button onClick={() => setSyncResult(null)}
                  style={{ background: 'none', border: 'none', cursor: 'pointer', color: T.textMuted,
                           display: 'flex', alignItems: 'center', padding: '0 0 0 8px' }}>
                  <X size={11} />
                </button>
              </div>
            )}
          </div>
        )}

        {/* ── FILTROS — toolbar compacta integrada en el contenedor ───────── */}
        <div style={{
          flexShrink: 0, padding: '9px 16px',
          borderBottom: `1px solid ${T.border}`,
          background: T.surface,
          display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap',
        }}>
          {/* Status dropdown */}
          <div style={{
            display: 'flex', alignItems: 'center', gap: '5px',
            background: T.surface2, border: `1px solid ${T.border}`,
            borderRadius: '6px', padding: '4px 10px',
          }}>
            <span style={{ color: T.textMuted, fontSize: '0.68rem', fontWeight: 600,
                           textTransform: 'uppercase', letterSpacing: '0.06em', whiteSpace: 'nowrap' }}>
              Status
            </span>
            <select
              value={filters.status || 'ALL'}
              onChange={e => updateFilter('status', e.target.value)}
              style={{
                background: 'none', border: 'none', outline: 'none',
                color: (filters.status && filters.status !== 'ALL') ? T.textPrimary : T.textSecond,
                fontSize: '0.75rem', cursor: 'pointer',
                fontWeight: (filters.status && filters.status !== 'ALL') ? 600 : 400,
              }}
            >
              <option value="ALL">All</option>
              <option value="PENDING">Pending</option>
              <option value="REVIEW">Review</option>
              <option value="MATCHED">Matched</option>
            </select>
          </div>

          <div style={{ width: '1px', height: '20px', background: T.border }} />

          {/* Date Range */}
          <DateRangePicker
            dateFrom={filters.dateFrom}
            dateTo={filters.dateTo}
            onChange={updateFilter}
          />

          <div style={{ width: '1px', height: '20px', background: T.border }} />

          {/* Analyst Filter */}
          <AnalystFilter
            analysts={analysts}
            value={filters.assignedUserId}
            onChange={v => updateFilter('assignedUserId', v)}
          />

          <div style={{ width: '1px', height: '20px', background: T.border }} />

          {/* Search */}
          <div style={{ position: 'relative', flex: 1, minWidth: '180px' }}>
            <Search size={13} style={{
              position: 'absolute', left: '10px', top: '50%',
              transform: 'translateY(-50%)', color: T.textMuted, pointerEvents: 'none',
            }} />
            <input type="text"
              placeholder="Customer, reference, or description..."
              value={search} onChange={e => handleSearch(e.target.value)}
              style={{
                width: '100%', background: T.surface2,
                border: `1px solid ${T.border}`, color: T.textPrimary,
                fontSize: '0.78rem', borderRadius: '6px',
                padding: '5px 28px 5px 30px', outline: 'none', boxSizing: 'border-box',
              }}
            />
            {search && (
              <button onClick={() => handleSearch('')}
                style={{ position: 'absolute', right: '8px', top: '50%', transform: 'translateY(-50%)',
                         background: 'none', border: 'none', cursor: 'pointer', color: T.textMuted }}>
                <X size={11} />
              </button>
            )}
          </div>

          {/* Clear all — solo cuando hay filtros activos, al final de la línea */}
          {hasAnyFilter && (
            <>
              <div style={{ width: '1px', height: '20px', background: T.border }} />
              <button onClick={clearAllFilters} style={{
                display: 'flex', alignItems: 'center', gap: '4px',
                background: 'none', border: `1px solid ${T.border}`,
                borderRadius: '6px', padding: '4px 10px',
                fontSize: '0.72rem', color: T.textMuted, cursor: 'pointer',
                whiteSpace: 'nowrap', transition: 'all 0.15s',
              }}
                onMouseEnter={e => { e.currentTarget.style.borderColor = T.border2; e.currentTarget.style.color = T.textSecond; }}
                onMouseLeave={e => { e.currentTarget.style.borderColor = T.border; e.currentTarget.style.color = T.textMuted; }}
              >
                <X size={10} /> Clear
              </button>
            </>
          )}

          {/* Refresh para no-admin — solo ícono, al extremo derecho */}
          {!isAdmin && (
            <button
              onClick={() => refetch()}
              title="Refresh data"
              style={{
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                width: '28px', height: '28px', borderRadius: '6px',
                border: `1px solid ${T.border}`,
                background: 'none', color: T.textMuted,
                cursor: 'pointer', marginLeft: 'auto',
              }}
            >
              <RefreshCw size={13} />
            </button>
          )}
        </div>

        {/* ── TABLA ───────────────────────────────────────────────────────── */}
        <div style={{ flex: 1, overflowY: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem', tableLayout: 'fixed' }}>
            <colgroup>
              <col style={{ width: '34px' }} />
              <col style={{ width: '40px' }} />
              <col style={{ width: '66px' }} />
              <col />
              <col style={{ width: '108px' }} />
              <col style={{ width: '148px' }} />
              <col style={{ width: '88px' }} />
              <col style={{ width: '128px' }} />
              <col style={{ width: '106px' }} />
              <col style={{ width: '138px' }} />
              <col style={{ width: '148px' }} />
            </colgroup>
            <thead style={{ position: 'sticky', top: 0, zIndex: 5 }}>
              <tr style={{ background: T.thBg }}>
                {[
                  { label: 'N',           align: 'left'  },
                  { label: 'Doc',         align: 'left'  },
                  { label: 'Date',        align: 'left'  },
                  { label: 'Text',        align: 'left'  },
                  { label: 'Amount',      align: 'right' },
                  { label: 'Type',        align: 'left'  },
                  { label: 'Cod Cliente', align: 'left'  },
                  { label: 'Customer',    align: 'left'  },
                  { label: 'Status',      align: 'left'  },
                  { label: 'Assigned',    align: 'left'  },
                  { label: 'Notes',       align: 'left'  },
                ].map(col => (
                  <th key={col.label} style={{
                    textAlign: col.align, padding: '9px 12px',
                    fontSize: '0.66rem', fontWeight: 700, letterSpacing: '0.06em',
                    textTransform: 'uppercase', color: T.thText,
                    whiteSpace: 'nowrap', overflow: 'hidden',
                  }}>
                    {col.label === 'Notes'
                      ? <span style={{ display: 'inline-flex', alignItems: 'center', gap: '4px' }}>
                          <MessageSquare size={10} style={{ opacity: 0.8 }} /> {col.label}
                        </span>
                      : col.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {isLoading ? (
                <tr><td colSpan={11} style={{ textAlign: 'center', color: T.textMuted, padding: '64px 0' }}>
                  Loading transactions...
                </td></tr>
              ) : groups.length === 0 ? (
                <tr><td colSpan={11} style={{ textAlign: 'center', color: T.textMuted, padding: '64px 0' }}>
                  No transactions found
                  {hasAnyFilter && (
                    <span style={{ display: 'block', fontSize: '0.72rem', marginTop: '6px', opacity: 0.7 }}>
                      Try clearing the filters above
                    </span>
                  )}
                </td></tr>
              ) : groups.map(group => (
                <React.Fragment key={`group-${group.date}`}>
                  <tr style={{ background: T.dateBg }}>
                    <td colSpan={11} style={{
                      padding: '6px 12px', color: T.dateText,
                      fontSize: '0.72rem', fontWeight: 700,
                      letterSpacing: '0.05em', textTransform: 'uppercase',
                      borderBottom: `1px solid ${T.border}`, borderTop: `1px solid ${T.border}`,
                    }}>
                      {new Date(group.date + 'T12:00:00').toLocaleDateString('en-US', {
                        weekday: 'long', year: 'numeric', month: 'long', day: 'numeric',
                      })}
                    </td>
                  </tr>

                  {group.rows.map((tx, i) => {
                    rowIndex++;
                    const isAlt = i % 2 === 1;
                    const effectiveNote = noteOverrides.hasOwnProperty(tx.bankRef1)
                      ? noteOverrides[tx.bankRef1]
                      : tx.analystNote;
                    const txWithNote = { ...tx, analystNote: effectiveNote };

                    return (
                      <tr key={tx.id} style={{
                        background: isAlt ? T.surface2 : T.surface,
                        borderBottom: `1px solid ${T.border}`,
                        transition: 'background 0.1s',
                      }}
                        onMouseEnter={e => e.currentTarget.style.background = '#E4E0D4'}
                        onMouseLeave={e => e.currentTarget.style.background = isAlt ? T.surface2 : T.surface}
                      >
                        <td style={{ padding: '8px 12px', color: T.textMuted, fontSize: '0.72rem' }}>{rowIndex}</td>
                        <td style={{ padding: '8px 12px', color: T.textSecond }}>{tx.docType}</td>
                        <td style={{ padding: '8px 12px', color: T.textSecond, whiteSpace: 'nowrap' }}>
                          {new Date(tx.bankDate + 'T12:00:00').toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}
                        </td>
                        <td style={{ padding: '8px 12px', color: T.textPrimary, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                          {tx.sapDescription}
                        </td>
                        <td style={{ padding: '8px 12px', color: T.textPrimary, fontVariantNumeric: 'tabular-nums', fontWeight: 600, whiteSpace: 'nowrap', textAlign: 'right' }}>
                          ${fmt(tx.amountTotal)}
                        </td>
                        <td style={{ padding: '8px 12px', color: T.textSecond, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                          {tx.transType}
                        </td>
                        <td style={{ padding: '8px 12px', color: T.textSecond, fontVariantNumeric: 'tabular-nums' }}>
                          {tx.enrichCustomerId || '—'}
                        </td>
                        <td style={{ padding: '8px 12px', color: T.textPrimary, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                          {tx.enrichCustomerName || '—'}
                        </td>
                        <td style={{ padding: '8px 12px' }}><StatusBadge status={tx.reconcileStatus} /></td>
                        <td style={{ padding: '8px 12px' }}>
                          <AssignedSelector row={tx} analysts={analysts} isAdmin={canManage} />
                        </td>
                        {/* Notes — columna completa con chip, tooltip e inline editor */}
                        <td style={{ padding: '6px 10px' }}>
                          <NoteCell
                            row={txWithNote}
                            isAdmin={canManage}
                            onNoteSaved={handleNoteSaved}
                          />
                        </td>
                      </tr>
                    );
                  })}

                  {/* Subtotal del día */}
                  <tr style={{ background: T.subtotalBg, borderBottom: `1px solid ${T.border}` }}>
                    <td colSpan={3} style={{ padding: '5px 12px', color: T.textMuted, fontSize: '0.72rem', fontStyle: 'italic' }}>
                      Subtotal · {group.count} tx
                    </td>
                    <td />
                    <td style={{ padding: '5px 12px', color: '#2D4A20', fontVariantNumeric: 'tabular-nums', fontWeight: 700, textAlign: 'right' }}>
                      ${fmt(group.subtotal)}
                    </td>
                    <td colSpan={6} />
                  </tr>
                </React.Fragment>
              ))}

              {/* Grand Total */}
              {groups.length > 0 && (
                <tr style={{ background: T.totalBg, borderTop: `2px solid ${T.border2}` }}>
                  <td colSpan={3} style={{ padding: '10px 12px', color: T.textPrimary, fontWeight: 700, fontSize: '0.75rem', letterSpacing: '0.05em', textTransform: 'uppercase' }}>
                    Total
                  </td>
                  <td />
                  <td style={{ padding: '10px 12px', color: T.textPrimary, fontVariantNumeric: 'tabular-nums', fontWeight: 700, fontSize: '0.9rem', textAlign: 'right' }}>
                    ${fmt(grandTotal)}
                  </td>
                  <td colSpan={5} style={{ padding: '10px 12px', color: T.textSecond, fontSize: '0.75rem' }}>
                    {totalCount} transactions
                  </td>
                  <td />
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}