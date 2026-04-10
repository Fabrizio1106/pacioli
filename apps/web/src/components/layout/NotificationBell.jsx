// src/components/layout/NotificationBell.jsx
import { useState, useRef, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import apiClient from '../../api/client.js';
import { Bell as IconBell, CheckCircle, XCircle, Clock, X } from 'lucide-react';

import { useLocation } from 'react-router-dom';

// ─── Paleta Titanium & Emerald ────────────────────────────────────────────────
const P = {
  panelBg:       '#1E211E', // vault-surface
  panelBg2:      '#262B26', // vault-surface2
  panelBorder:   '#3F473F', // vault-border2
  panelDivider:  'rgba(255,255,255,0.06)',

  textPrimary:   '#F3F4F6', // vault-text
  textSecond:    '#9CA3AF', // vault-text2
  textMuted:     '#6B7280', // vault-text3

  pendingBg:     'rgba(245, 158, 11, 0.12)',
  pendingColor:  '#FBBF24',
  pendingBorder: 'rgba(245, 158, 11, 0.25)',

  approvedBg:    'rgba(16, 185, 129, 0.12)',
  approvedColor: '#34D399',
  approvedBorder:'rgba(16, 185, 129, 0.25)',

  rejectedBg:    'rgba(239, 68, 68, 0.12)',
  rejectedColor: '#F87171',
  rejectedBorder:'rgba(239, 68, 68, 0.25)',

  approveBtnBg:      'rgba(234, 106, 26, 0.15)',
  approveBtnBgHover: 'rgba(234, 106, 26, 0.25)',
  approveBtnBorder:  'rgba(234, 106, 26, 0.35)',
  approveBtnText:    '#FB923C',

  rejectBtnBg:       'rgba(239, 68, 68, 0.15)',
  rejectBtnBgHover:  'rgba(239, 68, 68, 0.25)',
  rejectBtnBorder:   'rgba(239, 68, 68, 0.35)',
  rejectBtnText:     '#F87171',

  inputBg:     '#121412',
  inputBorder: 'rgba(234, 106, 26, 0.20)',

  warnBg:      'rgba(245, 158, 11, 0.15)',
  warnColor:   '#FBBF24',
  warnBorder:  'rgba(245, 158, 11, 0.35)',
};

const fmt = (n) => Number(n || 0).toLocaleString('en-US', {
  minimumFractionDigits: 2, maximumFractionDigits: 2,
});

// ─── Badge de estado ──────────────────────────────────────────────────────────
function StatusPill({ status }) {
  const map = {
    PENDING_APPROVAL: { label: 'PENDING',  bg: P.pendingBg,  color: P.pendingColor,  border: P.pendingBorder  },
    APPROVED:         { label: 'APPROVED', bg: P.approvedBg, color: P.approvedColor, border: P.approvedBorder },
    REJECTED:         { label: 'REJECTED', bg: P.rejectedBg, color: P.rejectedColor, border: P.rejectedBorder },
  };
  const s = map[status] || { label: status, bg: P.panelBg2, color: P.textSecond, border: P.panelDivider };
  return (
    <span style={{
      fontSize: '0.6rem', fontWeight: 700, padding: '2px 6px', borderRadius: '4px',
      background: s.bg, color: s.color, border: `1px solid ${s.border}`,
      textTransform: 'uppercase', letterSpacing: '0.04em',
    }}>
      {s.label}
    </span>
  );
}

// ─── Tarjeta de solicitud de reversal ────────────────────────────────────────
function ReversalCard({ item, canApprove, onApprove, onReject, isLast }) {
  const [rejectReason, setRejectReason] = useState('');
  const [showReject,   setShowReject]   = useState(false);

  return (
    <div style={{
      padding: '14px 16px',
      borderBottom: isLast ? 'none' : `1px solid ${P.panelDivider}`,
    }}>
      {/* Badges de estado */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '6px', marginBottom: '6px', flexWrap: 'wrap' }}>
        <StatusPill status={item.status} />
        {item.alreadyPosted && (
          <span style={{
            fontSize: '0.6rem', fontWeight: 700, padding: '2px 6px', borderRadius: '4px',
            background: P.warnBg, color: P.warnColor, border: `1px solid ${P.warnBorder}`,
            textTransform: 'uppercase', letterSpacing: '0.04em',
          }}>⚠ Posted to SAP</span>
        )}
      </div>

      {/* Referencia bancaria */}
      <div style={{
        color: P.textPrimary, fontSize: '0.82rem', fontWeight: 700,
        fontFamily: 'monospace', letterSpacing: '0.02em', marginBottom: '3px',
        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
      }}>
        {item.bankRef1}
      </div>

      {/* Tipo y monto */}
      <div style={{ color: P.textSecond, fontSize: '0.72rem', marginBottom: '5px' }}>
        {item.transType}
        <span style={{
          color: P.textPrimary, fontWeight: 600,
          fontVariantNumeric: 'tabular-nums', marginLeft: '6px',
        }}>
          ${fmt(item.amountTotal)}
        </span>
      </div>

      {/* Razón del reverso */}
      {item.requestReason && (
        <div style={{
          color: P.textSecond, fontSize: '0.7rem', fontStyle: 'italic',
          padding: '4px 8px', borderRadius: '5px', marginBottom: '5px',
          background: 'rgba(255,255,255,0.04)', border: `1px solid ${P.panelDivider}`,
        }}>
          "{item.requestReason}"
        </div>
      )}

      {/* Solicitado por */}
      <div style={{ color: P.textMuted, fontSize: '0.68rem', marginBottom: '10px' }}>
        by <span style={{ color: P.textSecond }}>{item.requestedByName}</span>
        {' · '}
        {new Date(item.requestedAt).toLocaleString('en-US', {
          month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
        })}
      </div>

      {/* Advertencia SAP */}
      {canApprove && item.status === 'PENDING_APPROVAL' && item.alreadyPosted && (
        <div style={{
          fontSize: '0.68rem', color: P.warnColor, padding: '6px 10px', borderRadius: '6px',
          background: P.warnBg, border: `1px solid ${P.warnBorder}`, marginBottom: '8px',
          lineHeight: 1.4,
        }}>
          ⚠ Posted to SAP — approving will cancel in PACIOLI but requires a manual SAP contra-entry.
        </div>
      )}

      {/* Botones de acción */}
      {canApprove && item.status === 'PENDING_APPROVAL' && (
        <div>
          {!showReject ? (
            <div style={{ display: 'flex', gap: '8px' }}>
              <button
                onClick={() => onApprove(item.id)}
                style={{
                  flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center',
                  gap: '5px', padding: '8px 0', borderRadius: '7px', cursor: 'pointer',
                  background: P.approveBtnBg, border: `1px solid ${P.approveBtnBorder}`,
                  color: P.approveBtnText, fontSize: '0.75rem', fontWeight: 600,
                  transition: 'all 0.15s',
                }}
                onMouseEnter={e => e.currentTarget.style.background = P.approveBtnBgHover}
                onMouseLeave={e => e.currentTarget.style.background = P.approveBtnBg}
              >
                <CheckCircle size={12} /> Approve
              </button>
              <button
                onClick={() => setShowReject(true)}
                style={{
                  flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center',
                  gap: '5px', padding: '8px 0', borderRadius: '7px', cursor: 'pointer',
                  background: P.rejectBtnBg, border: `1px solid ${P.rejectBtnBorder}`,
                  color: P.rejectBtnText, fontSize: '0.75rem', fontWeight: 600,
                  transition: 'all 0.15s',
                }}
                onMouseEnter={e => e.currentTarget.style.background = P.rejectBtnBgHover}
                onMouseLeave={e => e.currentTarget.style.background = P.rejectBtnBg}
              >
                <XCircle size={12} /> Reject
              </button>
            </div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
              <input
                type="text"
                placeholder="Rejection reason (min 5 characters)..."
                value={rejectReason}
                onChange={e => setRejectReason(e.target.value)}
                style={{
                  width: '100%', background: P.inputBg,
                  border: `1px solid ${P.inputBorder}`,
                  color: P.textPrimary, fontSize: '0.75rem', borderRadius: '6px',
                  padding: '7px 10px', outline: 'none', boxSizing: 'border-box',
                }}
              />
              <div style={{ display: 'flex', gap: '8px' }}>
                <button
                  onClick={() => { onReject(item.id, rejectReason); setShowReject(false); }}
                  disabled={rejectReason.trim().length < 5}
                  style={{
                    flex: 1, padding: '7px 0', borderRadius: '7px', cursor: 'pointer',
                    background: P.rejectBtnBg, border: `1px solid ${P.rejectBtnBorder}`,
                    color: P.rejectBtnText, fontSize: '0.75rem', fontWeight: 600,
                    opacity: rejectReason.trim().length < 5 ? 0.45 : 1,
                    transition: 'all 0.15s',
                  }}
                >
                  Confirm Reject
                </button>
                <button
                  onClick={() => { setShowReject(false); setRejectReason(''); }}
                  style={{
                    padding: '7px 14px', borderRadius: '7px', cursor: 'pointer',
                    background: 'rgba(255,255,255,0.06)',
                    border: `1px solid ${P.panelDivider}`,
                    color: P.textSecond, fontSize: '0.75rem',
                    transition: 'all 0.15s',
                  }}
                >
                  Cancel
                </button>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Resultado para solicitudes ya procesadas */}
      {item.status !== 'PENDING_APPROVAL' && (
        <div style={{
          marginTop: '6px', fontSize: '0.7rem', padding: '5px 10px', borderRadius: '6px',
          background: item.status === 'APPROVED' ? P.approvedBg : P.rejectedBg,
          color:      item.status === 'APPROVED' ? P.approvedColor : P.rejectedColor,
          border: `1px solid ${item.status === 'APPROVED' ? P.approvedBorder : P.rejectedBorder}`,
        }}>
          {item.status === 'APPROVED' ? '✓ Approved' : '✗ Rejected'}
          {item.reviewedByName && (
            <span style={{ color: P.textMuted }}> by {item.reviewedByName}</span>
          )}
          {item.reviewReason && (
            <span style={{ color: P.textMuted }}> — {item.reviewReason}</span>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Componente principal ─────────────────────────────────────────────────────
export default function NotificationBell({ user }) {
  const [open, setOpen]             = useState(false);
  const [actionError, setActionError] = useState(null);
  const panelRef                    = useRef(null);
  const queryClient       = useQueryClient();
  const location          = useLocation();
  const canApprove        = user?.role === 'admin' || user?.permissions?.includes('approve_reversals');

  const isDataMode = ['/', '/workspace', '/submit-posting', '/reports', '/ingestion'].includes(location.pathname);

  const { data: countData } = useQuery({
    queryKey:        ['notifications', 'count'],
    queryFn:         async () => (await apiClient.get('/notifications/count')).data.data,
    staleTime:       0,
    refetchInterval: 30 * 1000,
  });

  const { data: reversals = [], isLoading } = useQuery({
    queryKey:  ['notifications', 'reversals'],
    queryFn:   async () => (await apiClient.get('/notifications/reversals')).data.data,
    enabled:   open,
    staleTime: 0,
  });

  const { mutateAsync: approveReversal } = useMutation({
    mutationFn: ({ id, reason }) =>
      apiClient.post(`/notifications/reversals/${id}/approve`, { reason }),
    onSuccess: () => {
      setActionError(null);
      queryClient.invalidateQueries({ queryKey: ['notifications'] });
      queryClient.invalidateQueries({ queryKey: ['workspace'] });
      queryClient.invalidateQueries({ queryKey: ['overview'] });
    },
    onError: (err) => {
      setActionError(err.response?.data?.message || 'Failed to approve reversal. Please try again.');
    },
  });

  const { mutateAsync: rejectReversal } = useMutation({
    mutationFn: ({ id, reason }) =>
      apiClient.post(`/notifications/reversals/${id}/reject`, { reason }),
    onSuccess: () => {
      setActionError(null);
      queryClient.invalidateQueries({ queryKey: ['notifications'] });
    },
    onError: (err) => {
      setActionError(err.response?.data?.message || 'Failed to reject reversal. Please try again.');
    },
  });

  useEffect(() => {
    function handleClick(e) {
      if (panelRef.current && !panelRef.current.contains(e.target)) setOpen(false);
    }
    if (open) document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [open]);

  const count = countData?.count || 0;

  return (
    <div style={{ position: 'relative' }} ref={panelRef}>

      {/* ── Botón campanita ── compatible con header dinámico ────────────── */}
      <button
        onClick={() => { setActionError(null); setOpen(!open); }}
        style={{
          position: 'relative', width: '32px', height: '32px', borderRadius: '8px',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          background: open 
            ? (isDataMode ? 'var(--color-report-border)' : 'var(--color-vault-surface3)') 
            : 'transparent',
          border: `1px solid ${open ? (isDataMode ? 'rgba(0,0,0,0.1)' : 'var(--color-vault-border2)') : 'transparent'}`,
          color: isDataMode ? 'var(--color-report-text2)' : 'var(--color-vault-text2)',
          cursor: 'pointer', transition: 'all 0.15s',
        }}
        onMouseEnter={e => {
          e.currentTarget.style.background  = isDataMode ? 'var(--color-report-row-alt)' : 'var(--color-vault-surface2)';
          e.currentTarget.style.color       = isDataMode ? 'var(--color-report-text)' : 'var(--color-vault-text)';
        }}
        onMouseLeave={e => {
          if (!open) {
            e.currentTarget.style.background  = 'transparent';
            e.currentTarget.style.color       = isDataMode ? 'var(--color-report-text2)' : 'var(--color-vault-text2)';
          }
        }}
      >
        <IconBell size={16} />
        {count > 0 && (
          <span style={{
            position: 'absolute', top: '-4px', right: '-4px',
            minWidth: '16px', height: '16px', padding: '0 3px',
            background: 'var(--color-vault-red)', color: '#FFFFFF',
            fontSize: '0.6rem', fontWeight: 700, borderRadius: '999px',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            lineHeight: 1, boxShadow: '0 1px 4px rgba(239, 68, 68, 0.45)',
            border: '1.5px solid ' + (isDataMode ? 'var(--color-report-surface)' : 'var(--color-vault-surface)'),
          }}>
            {count > 9 ? '9+' : count}
          </span>
        )}
      </button>

      {/* ── Panel flotante ── tema titanio oscuro ────── */}
      {open && (
        <div style={{
          position: 'absolute', right: 0, top: '42px', width: '384px',
          background: P.panelBg,
          border: `1px solid ${P.panelBorder}`,
          borderRadius: '12px',
          boxShadow: '0 8px 32px rgba(0,0,0,0.45), 0 2px 8px rgba(0,0,0,0.25)',
          zIndex: 50, overflow: 'hidden',
        }}>

          {/* Header del panel */}
          <div style={{
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            padding: '12px 16px', borderBottom: `1px solid ${P.panelDivider}`,
            background: P.panelBg2,
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <IconBell size={13} style={{ color: P.textSecond }} />
              <span style={{ color: T.textPrimary, fontSize: '0.85rem', fontWeight: 600 }}>
                {canApprove ? 'Reversal Requests' : 'My Requests'}
              </span>
              {count > 0 && (
                <span style={{
                  background: 'var(--color-vault-red)', color: '#FFFFFF',
                  fontSize: '0.62rem', fontWeight: 700,
                  padding: '1px 6px', borderRadius: '999px',
                  border: '1px solid rgba(239, 68, 68, 0.5)',
                }}>
                  {count}
                </span>
              )}
            </div>
            <button
              onClick={() => setOpen(false)}
              style={{
                background: 'none', border: 'none', cursor: 'pointer',
                color: P.textMuted, display: 'flex', alignItems: 'center',
                padding: '2px', borderRadius: '4px', transition: 'color 0.15s',
              }}
              onMouseEnter={e => e.currentTarget.style.color = P.textSecond}
              onMouseLeave={e => e.currentTarget.style.color = P.textMuted}
            >
              <X size={14} />
            </button>
          </div>

          {/* Cuerpo */}
          {actionError && (
            <div style={{
              padding: '8px 16px',
              background: 'rgba(239,68,68,0.12)',
              borderBottom: `1px solid rgba(239,68,68,0.25)`,
              color: P.rejectedColor,
              fontSize: '0.75rem',
              fontWeight: 500,
            }}>
              ❌ {actionError}
            </div>
          )}
          <div style={{ maxHeight: '400px', overflowY: 'auto' }}>
            {isLoading ? (
              <div style={{ textAlign: 'center', color: P.textMuted, fontSize: '0.75rem', padding: '32px 0' }}>
                Loading...
              </div>
            ) : reversals.length === 0 ? (
              <div style={{ textAlign: 'center', padding: '36px 20px' }}>
                <IconPending size={24} style={{ color: P.textMuted, margin: '0 auto 10px', display: 'block' }} />
                <p style={{ color: P.textSecond, fontSize: '0.78rem', margin: '0 0 4px', fontWeight: 500 }}>
                  {canApprove ? 'No pending reversal requests' : 'No requests in the last 7 days'}
                </p>
                <p style={{ color: P.textMuted, fontSize: '0.7rem', margin: 0 }}>
                  {canApprove ? 'All reversals are up to date' : 'Your requests will appear here'}
                </p>
              </div>
            ) : (
              reversals.map((item, i) => (
                <ReversalCard
                  key={item.id}
                  item={item}
                  canApprove={canApprove}
                  isLast={i === reversals.length - 1}
                  onApprove={(id) => approveReversal({ id, reason: '' })}
                  onReject={(id, reason) => rejectReversal({ id, reason })}
                />
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
}