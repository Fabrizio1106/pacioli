// src/pages/SubmitPosting/index.jsx
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import apiClient from '../../api/client.js';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, Legend,
} from 'recharts';
import { IconMatched, IconPending, IconReview, IconReadyToPost } from '../../components/icons/CustomIcons.jsx';
import {
  Loader2, ChevronDown, ChevronUp,
} from 'lucide-react';

// ─── Paleta Titanium & Orange (Modo Datos) ──────────────────────────────────
const T = {
  base:        'var(--color-report-base)',
  surface:     'var(--color-report-surface)',
  surface2:    'var(--color-report-row-alt)',
  border:      'var(--color-report-border)',
  border2:     '#CBD5E1',
  thBg:        'var(--color-report-header)',
  thText:      '#E2E8F0',
  textPrimary: 'var(--color-report-text)',
  textSecond:  'var(--color-report-text2)',
  textMuted:   '#94A3B8',
  orange:      'var(--color-vault-orange)',
  orangeText:  '#FFFFFF',
  orangeBorder:'var(--color-vault-orange)',
  orangeLight: 'var(--color-vault-orange-soft)',
  orangeBg:    'var(--color-vault-orange-glow)',
  green:       'var(--color-vault-green)',
  greenText:   '#FFFFFF',
  greenBorder: 'var(--color-vault-green)',
  greenLight:  'var(--color-vault-green-soft)',
  greenBg:     'var(--color-vault-green-glow)',
  amber:       'var(--color-vault-amber)',
  amberBg:     'var(--color-vault-amber-dim)',
  yellow:      'var(--color-vault-amber)',
  yellowBg:    'var(--color-vault-amber-dim)',
  blue:        'var(--color-vault-blue)',
  blueBg:      'var(--color-vault-blue-dim)',
  purple:      '#6366F1',
  purpleBg:    'rgba(99, 102, 241, 0.12)',
  red:         'var(--color-vault-red)',
  redBg:       'var(--color-vault-red-dim)',
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

const CHART_COLORS = {
  manual: '#1E4A10',
  auto:   'var(--color-vault-green)',
};

const typeColor = (type) => {
  if (type?.includes('TRANSFER')) return 'var(--color-vault-blue)';
  if (type?.includes('DEPOSITO')) return 'var(--color-vault-green)';
  if (type?.includes('LIQUIDACION')) return 'var(--color-vault-orange)';
  return 'var(--color-vault-amber)';
};

// ─── KPI Card ─────────────────────────────────────────────────────────────────
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

// ─── Confirm Modal ───────────────────────────────────────────────────────────
function ConfirmModal({ preview, onConfirm, onCancel, isSubmitting }) {
  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)',
                  display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 100 }}>
      <div style={{ background: T.surface, border: `1px solid ${T.border}`,
                    borderRadius: '14px', padding: '24px', maxWidth: '420px', width: '100%',
                    boxShadow: '0 20px 60px rgba(0,0,0,0.3)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '20px' }}>
          <div style={{ width: '40px', height: '40px', borderRadius: '10px', background: T.greenBg,
                        display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <IconReadyToPost size={20} style={{ color: T.greenLight }} />
          </div>
          <div>
            <h3 style={{ color: T.textPrimary, fontWeight: 600, fontSize: '0.95rem', margin: 0 }}>
              Confirm Submit for Posting
            </h3>
            <p style={{ color: T.textMuted, fontSize: '0.72rem', marginTop: '2px' }}>
              This action writes data to the Gold Layer
            </p>
          </div>
        </div>

        <div style={{ background: T.surface2, border: `1px solid ${T.border}`,
                      borderRadius: '8px', padding: '16px', marginBottom: '16px' }}>
          {[
            { label: 'Transactions to post', value: preview?.totalCount || 0 },
            { label: 'Total amount',         value: `$${fmt(preview?.totalAmount)}` },
            { label: 'Manual approvals',     value: preview?.manualCount || 0 },
            { label: 'Batch ID',             value: preview?.nextBatchId, mono: true },
          ].map((row, i) => (
            <div key={i} style={{
              display: 'flex', justifyContent: 'space-between', alignItems: 'center',
              padding: '5px 0',
              borderBottom: i < 3 ? `1px solid ${T.border}` : 'none',
            }}>
              <span style={{ color: T.textSecond, fontSize: '0.82rem' }}>{row.label}:</span>
              <span style={{
                color: T.textPrimary, fontWeight: 600, fontSize: '0.82rem',
                fontFamily: row.mono ? 'var(--font-mono)' : undefined,
              }}>{row.value}</span>
            </div>
          ))}
        </div>

        <p style={{ color: T.textMuted, fontSize: '0.72rem', marginBottom: '20px', lineHeight: 1.5 }}>
          Transactions will be written to Gold Layer with status{' '}
          <strong style={{ color: T.amber }}>PENDING_RPA</strong>.
          The RPA will process them in SAP. This cannot be undone without a reversal request.
        </p>

        <div style={{ display: 'flex', gap: '10px' }}>
          <button onClick={onCancel} disabled={isSubmitting} style={{
            flex: 1, padding: '10px', background: T.surface2, border: `1px solid ${T.border}`,
            color: T.textSecond, borderRadius: '8px', fontSize: '0.82rem', cursor: 'pointer',
          }}>Cancel</button>
          <button onClick={onConfirm} disabled={isSubmitting} style={{
            flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '8px',
            padding: '10px', background: T.green, border: `1px solid ${T.greenBorder}`,
            color: T.greenText, borderRadius: '8px', fontSize: '0.82rem', fontWeight: 600,
            cursor: 'pointer', opacity: isSubmitting ? 0.6 : 1,
          }}>
            {isSubmitting
              ? <><Loader2 size={14} style={{ animation: 'spin 1s linear infinite' }} /> Posting...</>
              : <><IconReadyToPost size={14} /> Confirm & Post</>}
          </button>
        </div>
      </div>
    </div>
  );
}

// ─── Result Log ───────────────────────────────────────────────────────────────
function ResultLog({ result }) {
  const [expanded, setExpanded] = useState(false);
  if (!result) return null;
  const hasErrors = result.errors?.length > 0;
  const bg  = hasErrors ? T.redBg  : T.greenBg;
  const bdr = hasErrors ? 'rgba(192,80,74,0.3)' : 'rgba(90,154,53,0.3)';
  const clr = hasErrors ? T.red    : T.greenLight;

  return (
    <div style={{ borderRadius: '10px', border: `1px solid ${bdr}`, padding: '16px', background: bg }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '12px' }}>
        {hasErrors
          ? <IconReview size={18} style={{ color: T.red }} />
          : <IconMatched size={18} style={{ color: T.greenLight }} />}
        <div>
          <p style={{ fontWeight: 600, fontSize: '0.85rem', color: clr, margin: 0 }}>
            {hasErrors ? 'Completed with errors' : 'Successfully posted to Gold Layer'}
          </p>
          <p style={{ color: T.textMuted, fontSize: '0.72rem', marginTop: '2px' }}>
            Batch: <span style={{ fontFamily: 'var(--font-mono)', color: T.textPrimary }}>{result.batchId}</span>
            {' · '}{new Date().toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })}
          </p>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '10px', marginBottom: '12px' }}>
        {[
          { label: 'Exported', value: result.exported, color: T.greenLight },
          { label: 'Total Amount', value: fmtCompact(result.totalAmount), color: T.textPrimary },
          { label: 'Errors', value: result.errors?.length || 0, color: hasErrors ? T.red : T.textMuted },
        ].map((s, i) => (
          <div key={i} style={{ background: T.surface, border: `1px solid ${T.border}`,
                                 borderRadius: '8px', padding: '10px 14px', textAlign: 'center' }}>
            <p style={{ fontSize: '1.4rem', fontWeight: 700, color: s.color,
                         fontFamily: 'var(--font-mono)', margin: 0 }}>{s.value}</p>
            <p style={{ color: T.textMuted, fontSize: '0.68rem', marginTop: '3px' }}>{s.label}</p>
          </div>
        ))}
      </div>

      <button onClick={() => setExpanded(!expanded)} style={{
        display: 'flex', alignItems: 'center', gap: '6px',
        color: T.textSecond, background: 'none', border: 'none',
        cursor: 'pointer', fontSize: '0.75rem',
      }}>
        {expanded ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
        {expanded ? 'Hide' : 'Show'} transaction log
      </button>

      {expanded && result.items?.length > 0 && (
        <div style={{ marginTop: '10px', border: `1px solid ${T.border}`,
                      borderRadius: '8px', overflow: 'hidden', maxHeight: '256px', overflowY: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.75rem' }}>
            <thead style={{ background: T.thBg, position: 'sticky', top: 0 }}>
              <tr>
                {['Reference','Type','Amount','Status'].map((h, i) => (
                  <th key={h} style={{ padding: '7px 12px', textAlign: i === 2 ? 'right' : i === 3 ? 'center' : 'left',
                                        color: T.thText, fontWeight: 600, fontSize: '0.65rem',
                                        letterSpacing: '0.06em', textTransform: 'uppercase' }}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.items.map((item, i) => (
                <tr key={i} style={{ background: i % 2 === 0 ? T.surface : T.surface2,
                                     borderBottom: `1px solid ${T.border}` }}>
                  <td style={{ padding: '6px 12px', color: T.textPrimary, fontFamily: 'var(--font-mono)', fontSize: '0.72rem' }}>{item.bankRef1}</td>
                  <td style={{ padding: '6px 12px', color: T.textSecond, maxWidth: '120px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{item.transType}</td>
                  <td style={{ padding: '6px 12px', textAlign: 'right', color: T.textPrimary, fontFamily: 'var(--font-mono)', fontWeight: 600 }}>${fmt(item.amount)}</td>
                  <td style={{ padding: '6px 12px', textAlign: 'center' }}>
                    <span style={{
                      fontSize: '0.65rem', fontWeight: 600, padding: '1px 6px', borderRadius: '4px',
                      background: item.error ? T.redBg : T.greenBg,
                      color: item.error ? T.red : T.greenLight,
                    }}>
                      {item.error ? 'ERROR' : 'OK'}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {hasErrors && (
        <div style={{ marginTop: '10px', display: 'flex', flexDirection: 'column', gap: '4px' }}>
          {result.errors.map((e, i) => (
            <div key={i} style={{ background: T.redBg, borderRadius: '6px', padding: '6px 12px',
                                   color: T.red, fontSize: '0.72rem' }}>
              <span style={{ fontFamily: 'var(--font-mono)' }}>{e.bankRef1}</span>: {e.reason}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ─── Main Page ────────────────────────────────────────────────────────────────
export default function SubmitPostingPage() {
  const [showConfirm,  setShowConfirm]  = useState(false);
  const [submitResult, setSubmitResult] = useState(null);
  const queryClient = useQueryClient();

  const { data: previewRaw, isLoading: previewLoading, refetch: refetchPreview } = useQuery({
    queryKey:        ['gold-export', 'preview'],
    queryFn:         async () => (await apiClient.get('/gold-export/preview')).data,
    staleTime:       0,
    refetchInterval: 60 * 1000,
  });

  const { data: batchesRaw, isLoading: batchesLoading, refetch: refetchBatches } = useQuery({
    queryKey:  ['gold-export', 'batches'],
    queryFn:   async () => (await apiClient.get('/gold-export/batches')).data,
    staleTime: 30 * 1000,
  });

  const { mutateAsync: submitPosting, isPending: isSubmitting } = useMutation({
    mutationFn: () => apiClient.post('/gold-export/submit'),
    onSuccess: (res) => {
      setSubmitResult(res.data?.data || res.data);
      setShowConfirm(false);
      queryClient.invalidateQueries({ queryKey: ['gold-export'] });
      queryClient.invalidateQueries({ queryKey: ['overview'] });
      queryClient.invalidateQueries({ queryKey: ['workspace'] });
    },
    onError: (err) => {
      setShowConfirm(false);
      setSubmitResult({ error: true, message: err.response?.data?.message || 'Submission failed' });
    },
  });

  const preview    = previewRaw?.data || previewRaw || {};
  const batches    = batchesRaw?.data || [];
  const totalCount = preview.totalCount || preview.totalTransactions || 0;
  const totalAmt   = preview.totalAmount || 0;
  const isReady    = totalCount > 0;

  const matchMethodData = [
    { name: 'Manual Match', value: preview.manualCount || preview.manualApprovals || 0, color: CHART_COLORS.manual },
    { name: 'Auto Match',   value: preview.autoCount   || preview.autoMatched     || 0, color: CHART_COLORS.auto   },
  ].filter(d => d.value > 0);

  const byTypeData = (preview.byTransType || []).map(d => ({
    name:  (d.transType || 'OTHER')
             .replace('TRANSFERENCIA DIRECTA', 'DIR')
             .replace('TRANSFERENCIA SPI', 'SPI')
             .replace('LIQUIDACION TC', 'TC')
             .replace('DEPOSITO EFECTIVO', 'DEP'),
    count:  d.count,
    amount: parseFloat(d.amount) || 0,
    color:  typeColor(d.transType),
  }));

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: '14px',
                  background: T.base }}>

      {/* ── KPI CARDS ─────────────────────────────────────────────────────── */}
      <div style={{ display: 'flex', gap: '10px', flexShrink: 0 }}>
        <KpiCard label="Ready to Post"
          count={previewLoading ? '…' : totalCount} amount={totalAmt}
          accentColor={isReady ? T.greenLight : T.textMuted}
          accentBg={isReady ? T.greenBg : T.surface2} icon={IconReadyToPost} />
        <KpiCard label="Manual Approvals"
          count={preview.manualCount || preview.manualApprovals || 0}
          extra={`${preview.manualCount || preview.manualApprovals || 0} / ${totalCount}`}
          accentColor={T.blue} accentBg={T.blueBg} icon={IconMatched} />
        <KpiCard label="Total Batches"
          count={batches.length} accentColor={T.purple} accentBg={T.purpleBg} icon={IconPending} />

        {/* Submit action card */}
        <div style={{
          background: T.surface, border: `1px solid ${T.border}`,
          borderTop: `3px solid ${isReady ? T.greenLight : T.textMuted}`,
          borderRadius: '10px', padding: '14px 18px', width: '200px', flexShrink: 0,
          display: 'flex', flexDirection: 'column', alignItems: 'center',
          boxShadow: '0 1px 4px rgba(28,32,20,0.07)',
        }}>
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center',
                         flex: 1, justifyContent: 'center', textAlign: 'center' }}>
            <span style={{ fontSize: '2.2rem', fontWeight: 700, fontFamily: 'var(--font-mono)',
                           color: isReady ? T.greenLight : T.textMuted, lineHeight: 1 }}>
              {previewLoading ? '…' : totalCount}
            </span>
            <span style={{ color: T.textMuted, fontSize: '0.68rem', marginTop: '3px' }}>transactions</span>
            <span style={{ color: T.textPrimary, fontFamily: 'var(--font-mono)', fontWeight: 600,
                           fontSize: '0.9rem', marginTop: '4px' }}>${fmt(totalAmt)}</span>
            {preview.nextBatchId && (
              <span style={{ color: T.textMuted, fontFamily: 'var(--font-mono)', fontSize: '0.65rem', marginTop: '3px' }}>
                {preview.nextBatchId}
              </span>
            )}
          </div>
          <button onClick={() => setShowConfirm(true)} disabled={!isReady || isSubmitting}
            style={{
              width: '100%', marginTop: '12px', padding: '9px',
              display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '7px',
              borderRadius: '8px', fontSize: '0.78rem', fontWeight: 600,
              cursor: isReady && !isSubmitting ? 'pointer' : 'not-allowed',
              background: isReady ? T.green : T.surface2,
              border: `1px solid ${isReady ? T.greenBorder : T.border}`,
              color: isReady ? T.greenText : T.textMuted,
              opacity: isSubmitting ? 0.6 : 1,
            }}>
            <IconReadyToPost size={14} />
            {isReady ? 'Submit for Posting' : 'Nothing to Post'}
          </button>
        </div>
      </div>

      <div style={{ flex: 1, minHeight: 0, display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '14px' }}>
        {/* Left Col: Result Log & Batch List */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '14px', minHeight: 0 }}>
          {submitResult && <ResultLog result={submitResult} />}

          <div style={{ flex: 1, background: T.surface, border: `1px solid ${T.border}`,
                        borderRadius: '10px', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
            <div style={{ padding: '12px 16px', borderBottom: `1px solid ${T.border}`, background: T.surface2 }}>
              <h4 style={{ color: T.textPrimary, fontSize: '0.85rem', fontWeight: 600, margin: 0 }}>Batch History (Last 10)</h4>
            </div>
            <div style={{ flex: 1, overflowY: 'auto' }}>
              {batchesLoading ? (
                <div style={{ padding: '32px', textAlign: 'center', color: T.textMuted }}>Loading batches...</div>
              ) : batches.length === 0 ? (
                <div style={{ padding: '32px', textAlign: 'center', color: T.textMuted }}>No batches yet</div>
              ) : (
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }}>
                  <thead style={{ background: T.thBg, position: 'sticky', top: 0 }}>
                    <tr>
                      {['Batch ID','Count','Amount','Date'].map(h => (
                        <th key={h} style={{ padding: '8px 14px', textAlign: 'left', color: T.thText, fontWeight: 600, fontSize: '0.65rem', textTransform: 'uppercase' }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {batches.slice(0, 10).map((b, i) => (
                      <tr key={i} style={{ borderBottom: `1px solid ${T.border}` }}>
                        <td style={{ padding: '9px 14px', color: T.textPrimary, fontFamily: 'var(--font-mono)' }}>{b.batchId}</td>
                        <td style={{ padding: '9px 14px', color: T.textSecond }}>{b.count} tx</td>
                        <td style={{ padding: '9px 14px', color: T.textPrimary, fontWeight: 600 }}>${fmt(b.totalAmount)}</td>
                        <td style={{ padding: '9px 14px', color: T.textSecond, fontSize: '0.75rem' }}>
                          {b.submittedAt ? new Date(b.submittedAt).toLocaleString('en-US', {
                            month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'
                          }) : '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        </div>

        {/* Right Col: Charts */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '14px', minHeight: 0 }}>
          {/* Chart by TransType */}
          <div style={{ flex: 1, background: T.surface, border: `1px solid ${T.border}`,
                        borderRadius: '10px', padding: '16px', display: 'flex', flexDirection: 'column' }}>
            <h4 style={{ color: T.textPrimary, fontSize: '0.82rem', fontWeight: 600, marginBottom: '16px' }}>Volume by Transaction Type</h4>
            <div style={{ flex: 1, minHeight: 0 }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={byTypeData} layout="vertical">
                  <XAxis type="number" hide />
                  <YAxis dataKey="name" type="category" width={40} fontSize={10} axisLine={false} tickLine={false} />
                  <Tooltip
                    contentStyle={{ background: T.surface, border: `1px solid ${T.border}`, borderRadius: '8px', fontSize: '0.75rem' }}
                    formatter={(val) => [`$${fmt(val)}`, 'Amount']}
                  />
                  <Bar dataKey="amount" radius={[0, 4, 4, 0]}>
                    {byTypeData.map((entry, index) => (
                      <Cell key={index} fill={entry.color} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Chart by Match Method */}
          <div style={{ flex: 1, background: T.surface, border: `1px solid ${T.border}`,
                        borderRadius: '10px', padding: '16px', display: 'flex', flexDirection: 'column' }}>
            <h4 style={{ color: T.textPrimary, fontSize: '0.82rem', fontWeight: 600, marginBottom: '16px' }}>Approval Method</h4>
            <div style={{ flex: 1, minHeight: 0 }}>
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie data={matchMethodData} cx="50%" cy="50%" innerRadius={45} outerRadius={70} paddingAngle={5} dataKey="value">
                    {matchMethodData.map((entry, index) => (
                      <Cell key={index} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{ background: T.surface, border: `1px solid ${T.border}`, borderRadius: '8px', fontSize: '0.75rem' }}
                  />
                  <Legend verticalAlign="bottom" height={36} formatter={(val) => <span style={{ fontSize: '0.72rem', color: T.textSecond }}>{val}</span>} />
                </PieChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>
      </div>

      {showConfirm && (
        <ConfirmModal
          preview={{ ...preview, totalCount, manualCount: preview.manualCount || preview.manualApprovals }}
          onConfirm={() => submitPosting()}
          onCancel={() => setShowConfirm(false)}
          isSubmitting={isSubmitting}
        />
      )}
    </div>
  );
}
