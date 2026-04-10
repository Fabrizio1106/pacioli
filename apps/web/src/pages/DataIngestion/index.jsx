// src/pages/DataIngestion/index.jsx
import { useState, useRef, useCallback } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import apiClient from '../../api/client.js';
import {
  Upload, FolderSearch, Play, RefreshCw, ChevronDown,
  CheckCircle, XCircle, AlertTriangle, Clock,
  Zap, FileSpreadsheet, Mail, Loader2, X, Activity, Terminal
} from 'lucide-react';

// ─── Paleta Titanium & Orange (Modo Datos) ──────────────────────────────────
const T = {
  base:        'var(--color-report-base)',
  surface:     'var(--color-report-surface)',
  surface2:    'var(--color-report-row-alt)',
  surface3:    '#E2E8F0',
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
  orangeBg:    'var(--color-vault-orange-glow)',
  green:       'var(--color-vault-green)',
  greenText:   '#FFFFFF',
  greenBorder: 'var(--color-vault-green)',
  greenBg:     'var(--color-vault-green-glow)',
  emerald:     'var(--color-vault-green)',
  emeraldBg:   'var(--color-vault-green-glow)',
  blue:        'var(--color-vault-blue)',
  blueBg:      'var(--color-vault-blue-dim)',
  red:         'var(--color-vault-red)',
  redBg:       'var(--color-vault-red-dim)',
  amber:       'var(--color-vault-amber)',
  amberBg:     'var(--color-vault-amber-dim)',
  yellow:      'var(--color-vault-amber)',
  yellowBg:    'var(--color-vault-amber-dim)',
};

// ─── Helpers ─────────────────────────────────────────────────────────────────
const fmtCompact = (n) => {
  if (n == null) return '—';
  const v = Number(n);
  if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000)     return `$${(v / 1_000).toFixed(1)}K`;
  return `$${v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
};

const fmtSeconds = (s) => {
  if (!s) return '—';
  if (s < 60) return `${Math.round(s)}s`;
  return `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`;
};

const STATUS_BADGE = {
  ok:       { bg: 'rgba(16, 185, 129, 0.12)', color: '#065F46', border: 'rgba(16, 185, 129, 0.3)', text: 'UP TO DATE' },
  warning:  { bg: 'rgba(245, 158, 11, 0.12)',  color: '#92400E', border: 'rgba(245, 158, 11, 0.3)', text: 'BEHIND' },
  outdated: { bg: 'rgba(239, 68, 68, 0.12)',   color: '#991B1B', border: 'rgba(239, 68, 68, 0.3)',  text: 'OUTDATED' },
  empty:    { bg: 'var(--color-report-row-alt)', color: 'var(--color-report-text2)', border: 'var(--color-report-border)', text: 'NO DATA' },
  error:    { bg: 'rgba(239, 68, 68, 0.12)',   color: '#991B1B', border: 'rgba(239, 68, 68, 0.3)',  text: 'ERROR' },
};

const LOADER_OPTIONS = [
  { id: 'banco_239',       label: 'Banco 239' },
  { id: 'sap_239',         label: 'SAP Cta 239' },
  { id: 'fbl5n',           label: 'FBL5N — SAP Portfolio' },
  { id: 'diners_club',     label: 'Diners Club' },
  { id: 'guayaquil',       label: 'Guayaquil (AMEX)' },
  { id: 'pacificard',      label: 'Pacificard' },
  { id: 'databalance',     label: 'Databalance' },
  { id: 'webpos',          label: 'WebPos' },
  { id: 'retenciones',     label: 'SRI Withholdings' },
  { id: 'manual_requests', label: 'Manual Requests' },
];

const GROUP_ICON = {
  completed: <CheckCircle  size={16} style={{ color: T.emerald }} />,
  failed:    <XCircle      size={16} style={{ color: T.red }} />,
  running:   <Loader2      size={16} style={{ color: T.blue, animation: 'spin 1s linear infinite' }} />,
  partial:   <AlertTriangle size={16} style={{ color: T.amber }} />,
  pending:   <Clock        size={16} style={{ color: T.textMuted }} />,
};

// ─── Components ─────────────────────────────────────────────────────────────

function LoaderCard({ loader }) {
  const badge = STATUS_BADGE[loader.status] || STATUS_BADGE.empty;
  const isOutdated = loader.status === 'outdated' || loader.status === 'warning' || loader.status === 'error';

  return (
    <div style={{
      background: isOutdated ? 'rgba(200,154,32,0.04)' : T.surface,
      border: `1px solid ${isOutdated ? 'rgba(200,154,32,0.3)' : T.border}`,
      borderLeft: `4px solid ${isOutdated ? T.yellow : 'transparent'}`,
      borderRadius: '8px', padding: '12px 16px', transition: 'all 0.15s',
    }}
      onMouseEnter={e => e.currentTarget.style.background = T.surface2}
      onMouseLeave={e => e.currentTarget.style.background = isOutdated ? 'rgba(200,154,32,0.04)' : T.surface}
    >
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px' }}>
        <span style={{ color: T.textPrimary, fontSize: '0.9rem', fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>
          {loader.label}
        </span>
        <span style={{
          fontSize: '0.65rem', fontWeight: 700, padding: '3px 8px', borderRadius: '4px',
          background: badge.bg, color: badge.color, border: `1px solid ${badge.border}`, letterSpacing: '0.04em', flexShrink: 0
        }}>{badge.text}</span>
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ color: T.textMuted, fontSize: '0.75rem' }}>Last Update</span>
          <span style={{ color: T.textPrimary, fontSize: '0.8rem', fontWeight: 500 }}>
            {loader.lastDate ? new Date(loader.lastDate + 'T12:00:00').toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) : '—'}
          </span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ color: T.textMuted, fontSize: '0.75rem' }}>Total Amount</span>
          <span style={{ color: T.textPrimary, fontSize: '0.85rem', fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>
            {fmtCompact(loader.lastSum)}
          </span>
        </div>
      </div>
    </div>
  );
}

function DropZone({ onFiles, isPipelineRunning }) {
  const [dragging, setDragging] = useState(false);
  const inputRef = useRef(null);

  const handleDrop = (e) => {
    e.preventDefault(); setDragging(false);
    if (isPipelineRunning) return;
    const files = Array.from(e.dataTransfer.files);
    if (files.length) onFiles(files);
  };
  
  return (
    <div
      onDragOver={(e) => { e.preventDefault(); if (!isPipelineRunning) setDragging(true); }}
      onDragLeave={() => setDragging(false)}
      onDrop={handleDrop}
      onClick={() => !isPipelineRunning && inputRef.current?.click()}
      style={{
        display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
        minHeight: '220px', borderRadius: '12px',
        border: `2px dashed ${dragging ? T.greenBorder : T.border2}`,
        background: isPipelineRunning ? T.surface3 : (dragging ? T.greenBg : T.surface2),
        cursor: isPipelineRunning ? 'not-allowed' : 'pointer',
        transition: 'all 0.2s', transform: dragging ? 'scale(1.01)' : 'scale(1)',
      }}
    >
      <input ref={inputRef} type="file" multiple style={{ display: 'none' }}
             onChange={e => { if (e.target.files.length) onFiles(Array.from(e.target.files)); e.target.value = ''; }}
             accept=".xlsx,.xls,.msg,.csv,.txt" disabled={isPipelineRunning} />
      
      <Upload size={36} style={{ marginBottom: '16px', color: dragging ? T.green : T.textMuted }} />
      <h3 style={{ color: T.textPrimary, fontSize: '1.1rem', fontWeight: 600, margin: '0 0 6px' }}>
        {isPipelineRunning ? 'Upload locked during execution' : (dragging ? 'Drop to upload' : 'Drag & Drop files here')}
      </h3>
      <p style={{ color: T.textSecond, fontSize: '0.85rem', margin: 0 }}>
        or click to browse your computer
      </p>
      <div style={{ display: 'flex', gap: '8px', marginTop: '20px' }}>
        {['.xlsx', '.csv', '.msg', '.txt'].map(ext => (
          <span key={ext} style={{ background: T.surface, border: `1px solid ${T.border}`, 
                                   color: T.textMuted, fontSize: '0.72rem', padding: '3px 8px', borderRadius: '6px' }}>
            {ext}
          </span>
        ))}
      </div>
    </div>
  );
}

function ClassificationTable({ classifications = [], onConfirm, onCancel, isUploading }) {
  const [assignments, setAssignments] = useState(() => (classifications || []).map(c => ({
    originalName: c.originalName || 'Unknown File', 
    loaderId: c.detectedLoader || '', 
    confidence: c.confidence, 
    status: c.status
  })));
  
  const updateLoader = (idx, loaderId) => setAssignments(prev => prev.map((a, i) => i === idx ? { ...a, loaderId } : a));
  const allAssigned = assignments.every(a => a.loaderId);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', background: T.surface }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', paddingBottom: '20px', borderBottom: `1px solid ${T.border}`, marginBottom: '20px' }}>
        <div>
          <h2 style={{ color: T.textPrimary, fontSize: '1.2rem', fontWeight: 600, margin: '0 0 6px' }}>Verify Assignments</h2>
          <p style={{ color: T.textSecond, fontSize: '0.85rem', margin: 0 }}>Review the automatically detected loaders before uploading {(classifications || []).length} file(s).</p>
        </div>
        <div style={{ display: 'flex', gap: '10px' }}>
          <button onClick={onCancel} disabled={isUploading} style={{
            padding: '10px 18px', borderRadius: '8px', fontSize: '0.85rem', fontWeight: 500, cursor: 'pointer',
            background: T.surface2, border: `1px solid ${T.border}`, color: T.textSecond,
          }}>Cancel</button>
          <button onClick={() => onConfirm(assignments)} disabled={!allAssigned || isUploading}
            style={{ display: 'flex', alignItems: 'center', gap: '8px', padding: '10px 20px', borderRadius: '8px', fontSize: '0.85rem', fontWeight: 600,
                     cursor: allAssigned && !isUploading ? 'pointer' : 'not-allowed',
                     background: allAssigned && !isUploading ? T.green : T.surface3, border: `1px solid ${allAssigned && !isUploading ? T.greenBorder : T.border}`, color: allAssigned && !isUploading ? T.greenText : T.textMuted,
            }}>
            {isUploading ? <><Loader2 size={16} style={{ animation: 'spin 1s linear infinite' }} /> Uploading...</> : <><CheckCircle size={16} /> Confirm Upload</>}
          </button>
        </div>
      </div>

      <div style={{ flex: 1, overflowY: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead style={{ position: 'sticky', top: 0, zIndex: 5 }}>
            <tr style={{ background: T.surface2 }}>
              <th style={{ textAlign: 'left', padding: '12px 16px', fontSize: '0.75rem', fontWeight: 600, textTransform: 'uppercase', color: T.textSecond, borderBottom: `1px solid ${T.border}` }}>File Name</th>
              <th style={{ textAlign: 'left', padding: '12px 16px', fontSize: '0.75rem', fontWeight: 600, textTransform: 'uppercase', color: T.textSecond, borderBottom: `1px solid ${T.border}` }}>Detection</th>
              <th style={{ textAlign: 'left', padding: '12px 16px', fontSize: '0.75rem', fontWeight: 600, textTransform: 'uppercase', color: T.textSecond, borderBottom: `1px solid ${T.border}` }}>Target Loader</th>
            </tr>
          </thead>
          <tbody>
            {assignments.map((item, idx) => (
              <tr key={idx} style={{ background: T.surface, borderBottom: `1px solid ${T.border}`, transition: 'background 0.15s' }} onMouseEnter={e => e.currentTarget.style.background = T.surface2} onMouseLeave={e => e.currentTarget.style.background = T.surface}>
                <td style={{ padding: '14px 16px', color: T.textPrimary, fontSize: '0.85rem', fontWeight: 500, display: 'flex', alignItems: 'center', gap: '10px' }}>
                  {item.originalName?.endsWith('.msg') ? <Mail size={16} style={{ color: T.blue }} /> : <FileSpreadsheet size={16} style={{ color: T.green }} />}
                  {item.originalName}
                </td>
                <td style={{ padding: '14px 16px' }}>
                  {item.status === 'detected' && (
                    <span style={{ fontSize: '0.7rem', fontWeight: 600, padding: '3px 8px', borderRadius: '4px',
                                   background: item.confidence === 'name' ? T.emeraldBg : T.blueBg, color: item.confidence === 'name' ? T.emerald : T.blue }}>
                      {item.confidence === 'name' ? 'By Filename' : 'By Structure'}
                    </span>
                  )}
                </td>
                <td style={{ padding: '14px 16px' }}>
                  <div style={{ position: 'relative', width: '240px' }}>
                    <select value={item.loaderId} onChange={(e) => updateLoader(idx, e.target.value)}
                      style={{ width: '100%', background: T.surface, border: `1px solid ${item.loaderId ? T.border2 : T.amber}`,
                               color: item.loaderId ? T.textPrimary : T.amber, fontSize: '0.85rem', borderRadius: '6px', padding: '8px 12px',
                               cursor: 'pointer', outline: 'none', appearance: 'none', fontWeight: 500 }}>
                      <option value="">⚠ Select Loader...</option>
                      {LOADER_OPTIONS.map(o => <option key={o.id} value={o.id}>{o.label}</option>)}
                    </select>
                    <ChevronDown size={14} style={{ position: 'absolute', right: '12px', top: '50%', transform: 'translateY(-50%)', color: T.textMuted, pointerEvents: 'none' }} />
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function PipelineMonitor({ status, onBack }) {
  const { pipeline, groups = [], progressPct = 0 } = status || {};
  const isRunning = pipeline?.running;
  const isDone = !isRunning && pipeline?.status === 'completed';
  const isFailed = !isRunning && pipeline?.status === 'failed';
  
  const barColor = isFailed ? T.red : isDone ? T.emerald : T.blue;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', paddingBottom: '20px', borderBottom: `1px solid ${T.border}`, marginBottom: '20px' }}>
        <div>
          <h2 style={{ color: T.textPrimary, fontSize: '1.2rem', fontWeight: 600, margin: '0 0 6px', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Activity size={20} style={{ color: barColor }} />
            Pipeline Execution
          </h2>
          <p style={{ color: T.textSecond, fontSize: '0.85rem', margin: 0 }}>
            {isRunning ? 'Jobs are currently running...' : isDone ? 'All jobs completed successfully.' : isFailed ? 'Pipeline failed during execution.' : 'Waiting for execution.'}
          </p>
        </div>
        {!isRunning && (
          <button onClick={onBack} style={{
            padding: '8px 16px', borderRadius: '8px', fontSize: '0.85rem', fontWeight: 500, cursor: 'pointer',
            background: T.surface2, border: `1px solid ${T.border}`, color: T.textPrimary,
          }}>Exit Monitor</button>
        )}
      </div>

      <div style={{ display: 'flex', gap: '24px', flex: 1, minHeight: 0 }}>
        <div style={{ width: '300px', display: 'flex', flexDirection: 'column', gap: '16px', overflowY: 'auto', paddingRight: '8px' }}>
          <div style={{ marginBottom: '8px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
              <span style={{ color: T.textSecond, fontSize: '0.8rem', fontWeight: 500 }}>Overall Progress</span>
              <span style={{ color: T.textPrimary, fontSize: '0.85rem', fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>{progressPct}%</span>
            </div>
            <div style={{ background: T.surface3, borderRadius: '999px', height: '8px', overflow: 'hidden' }}>
              <div style={{ height: '100%', width: `${progressPct}%`, background: barColor, transition: 'width 0.5s ease' }} />
            </div>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
            {groups.map((group) => {
              const isActive = group.status === 'running';
              const isError = group.status === 'failed';
              return (
                <div key={group.key} style={{
                  display: 'flex', alignItems: 'flex-start', gap: '12px', padding: '12px', borderRadius: '8px',
                  background: isActive ? T.blueBg : isError ? T.redBg : T.surface,
                  border: `1px solid ${isActive ? 'rgba(58,120,201,0.3)' : isError ? 'rgba(192,80,74,0.3)' : T.border}`,
                }}>
                  <div style={{ marginTop: '2px' }}>{GROUP_ICON[group.status] || GROUP_ICON.pending}</div>
                  <div style={{ flex: 1 }}>
                    <p style={{ color: isError ? T.red : isActive ? T.blue : T.textPrimary, fontSize: '0.85rem', fontWeight: 600, margin: '0 0 4px' }}>
                      {group.label}
                    </p>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                      {group.totalRecords > 0 && (
                        <span style={{ color: T.textSecond, fontSize: '0.75rem', fontVariantNumeric: 'tabular-nums' }}>
                          {group.totalRecords.toLocaleString()} rec.
                        </span>
                      )}
                      {group.totalSeconds > 0 && (
                        <span style={{ color: T.textMuted, fontSize: '0.75rem', fontVariantNumeric: 'tabular-nums' }}>
                          {fmtSeconds(group.totalSeconds)}
                        </span>
                      )}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        <div style={{
          flex: 1, display: 'flex', flexDirection: 'column',
          background: T.surface2, border: `1px solid ${T.border}`, borderRadius: '10px', overflow: 'hidden'
        }}>
          <div style={{ padding: '10px 16px', background: T.surface3, borderBottom: `1px solid ${T.border}`, display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Terminal size={14} style={{ color: T.textSecond }} />
            <span style={{ color: T.textPrimary, fontSize: '0.75rem', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.04em' }}>System Logs</span>
          </div>
          <div style={{ flex: 1, padding: '16px', overflowY: 'auto', fontFamily: 'var(--font-mono, monospace)', fontSize: '0.8rem', lineHeight: 1.6 }}>
            {(!pipeline?.log || pipeline.log.length === 0) ? (
              <span style={{ color: T.textMuted }}>Waiting for logs...</span>
            ) : (
              pipeline.log.map((line, i) => {
                const isErr = line.includes('[ERR]') || line.includes('ERROR');
                const isWarn = line.includes('WARN');
                const isSuccess = line.includes('SUCCESS');
                return (
                  <div key={i} style={{ color: isErr ? T.red : isSuccess ? T.emerald : isWarn ? T.amber : T.textSecond, marginBottom: '2px', wordBreak: 'break-all' }}>
                    {line}
                  </div>
                );
              })
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Main Page Component ──────────────────────────────────────────────────────
export default function DataIngestionPage() {
  const [view, setView] = useState('main'); // 'main', 'classify', 'pipeline'
  const [pendingFiles, setPendingFiles] = useState([]);
  const [uploadResult, setUploadResult] = useState(null);
  const [scanResults, setScanResults] = useState([]);
  const [pipelineError, setPipelineError] = useState(null);
  const queryClient = useQueryClient();

  const { data: loaderStatus = [], isLoading: loadersLoading, refetch: refetchLoaders } = useQuery({
    queryKey: ['ingestion', 'loader-status'],
    queryFn: async () => (await apiClient.get('/ingestion/loader-status')).data.data,
    staleTime: 30 * 1000,
  });

  const { data: pipelineStatus, refetch: refetchStatus } = useQuery({
    queryKey: ['ingestion', 'pipeline-status'],
    queryFn: async () => (await apiClient.get('/ingestion/pipeline-status', { params: { _t: Date.now() } })).data.data,
    staleTime: 0, gcTime: 0,
    refetchInterval: (query) => query.state.data?.pipeline?.running ? 3000 : false,
  });

  const classifyMutation = useMutation({
    mutationFn: async (files) => {
      const formData = new FormData(); files.forEach(f => formData.append('files', f));
      return (await apiClient.post('/ingestion/classify', formData)).data.data;
    },
    onSuccess: (data) => { setPendingFiles(data || []); setView('classify'); },
  });

  const uploadMutation = useMutation({
    mutationFn: async ({ files, assignments }) => {
      const formData = new FormData(); files.forEach(f => formData.append('files', f));
      formData.append('assignments', JSON.stringify(assignments));
      return (await apiClient.post('/ingestion/upload', formData)).data;
    },
    onSuccess: (data) => {
      setUploadResult(data); setView('main'); refetchLoaders();
      queryClient.invalidateQueries({ queryKey: ['ingestion'] });
    },
    onError: (err) => {
      setUploadResult({ error: true, message: err.response?.data?.message || 'Upload failed. Please try again.' });
      setView('main');
    },
  });

  const runPipelineMutation = useMutation({
    mutationFn: async () => (await apiClient.post('/ingestion/run-pipeline')).data,
    onSuccess: () => { setPipelineError(null); setView('pipeline'); refetchStatus(); },
    onError: (err) => {
      setPipelineError(err.response?.data?.message || 'Failed to start pipeline. Is the Pipeline Server running?');
    },
  });

  const handleFiles = useCallback((files) => {
    setUploadResult(null); 
    setView('classify'); 
    classifyMutation.mutate(files); 
    window._pendingFileObjects = files;
  }, [classifyMutation]);

  // RESTORED FUNCTION
  const handleConfirmUpload = (assignments) => {
    uploadMutation.mutate({ files: window._pendingFileObjects || [], assignments });
  };

  const handleScanFolders = async () => {
    const res = await apiClient.get('/ingestion/scan-folders');
    setScanResults(res.data.data || []);
  };

  const isPipelineRunning = pipelineStatus?.pipeline?.running;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', background: T.surface, borderRadius: '12px', border: `1px solid ${T.border}`, overflow: 'hidden' }}>
      
      {/* ── Main Workspace Split Area ────────────────────────────────────── */}
      <div style={{ flex: 1, minHeight: 0, display: 'flex' }}>
        
        {/* Left Column: The Active Checklist */}
        <div style={{ width: '340px', display: 'flex', flexDirection: 'column', borderRight: `1px solid ${T.border}`, background: T.surface }}>
          
          {/* Header de Sidebar con Status Integrado */}
          <div style={{ 
            padding: '16px 20px', borderBottom: `1px solid ${T.border}`, 
            display: 'flex', alignItems: 'center', justifyContent: 'space-between', 
            background: T.surface2, height: '64px'
          }}>
            <div style={{ display: 'flex', flexDirection: 'column' }}>
              <h3 style={{ color: T.textPrimary, fontSize: '0.75rem', fontWeight: 700, margin: 0, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                Source Check
              </h3>
              {/* Status Badge Minimalista */}
              <div style={{ display: 'flex', alignItems: 'center', gap: '5px', marginTop: '2px' }}>
                {isPipelineRunning ? (
                  <><Loader2 size={10} style={{ color: T.blue, animation: 'spin 1s linear infinite' }} /> <span style={{ color: T.blue, fontSize: '0.65rem', fontWeight: 700, textTransform: 'uppercase' }}>Running</span></>
                ) : (
                  <><div style={{ width: '6px', height: '6px', borderRadius: '50%', background: T.textMuted }} /> <span style={{ color: T.textMuted, fontSize: '0.65rem', fontWeight: 700, textTransform: 'uppercase' }}>Idle</span></>
                )}
              </div>
            </div>

            <button onClick={() => refetchLoaders()} style={{ 
              background: 'var(--color-report-surface)', border: `1px solid ${T.border}`, 
              borderRadius: '6px', padding: '5px 10px',
              cursor: 'pointer', color: T.textSecond, display: 'flex', alignItems: 'center', gap: '6px',
              transition: 'all 0.15s'
            }} onMouseEnter={e => e.currentTarget.style.background = '#FFF'} onMouseLeave={e => e.currentTarget.style.background = 'var(--color-report-surface)'}>
              <span style={{ fontSize: '0.7rem', fontWeight: 600 }}>Refresh</span>
              <RefreshCw size={12} />
            </button>
          </div>

          <div style={{ flex: 1, overflowY: 'auto', padding: '16px', display: 'flex', flexDirection: 'column', gap: '12px' }}>
            {loadersLoading && <div style={{ color: T.textMuted, fontSize: '0.85rem', textAlign: 'center', marginTop: '20px' }}>Loading sources...</div>}
            {loaderStatus.map(loader => <LoaderCard key={loader.id} loader={loader} />)}
          </div>
        </div>

        {/* Right Column: Action Center */}
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', background: T.surface }}>
          <div style={{ flex: 1, overflowY: 'auto', padding: '32px' }}>
            
            {view === 'classify' && (
              classifyMutation.isPending ? (
                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100%', gap: '16px' }}>
                  <Loader2 size={36} style={{ color: T.green, animation: 'spin 1s linear infinite' }} />
                  <h3 style={{ color: T.textPrimary, fontSize: '1.1rem', fontWeight: 600, margin: 0 }}>Analyzing Files...</h3>
                  <p style={{ color: T.textSecond, fontSize: '0.85rem', margin: 0 }}>Detecting structures and matching loaders.</p>
                </div>
              ) : classifyMutation.isError ? (
                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100%', gap: '16px' }}>
                  <XCircle size={36} style={{ color: T.red }} />
                  <h3 style={{ color: T.red, fontSize: '1.1rem', fontWeight: 600, margin: 0 }}>Error Analyzing Files</h3>
                  <p style={{ color: T.textSecond, fontSize: '0.85rem', margin: 0 }}>The server rejected the files. Please check the format and try again.</p>
                  <button onClick={() => setView('main')} style={{ marginTop: '10px', padding: '8px 16px', background: T.surface2, border: `1px solid ${T.border}`, borderRadius: '6px', cursor: 'pointer' }}>Go Back</button>
                </div>
              ) : (
                <ClassificationTable classifications={pendingFiles} onConfirm={handleConfirmUpload} onCancel={() => setView('main')} isUploading={uploadMutation.isPending} />
              )
            )}

            {view === 'pipeline' && pipelineStatus && (
              <PipelineMonitor status={pipelineStatus} onBack={() => setView('main')} />
            )}

            {view === 'main' && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '32px', maxWidth: '800px', margin: '0 auto', height: '100%' }}>
                
                {classifyMutation.isError && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px', borderRadius: '8px', padding: '12px 20px', fontSize: '0.85rem', fontWeight: 500, background: T.redBg, border: `1px solid rgba(192,80,74,0.3)`, color: T.red }}>
                    <XCircle size={16} /> Error during file analysis. Please try again.
                  </div>
                )}

                {uploadResult && (
                  uploadResult.error ? (
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderRadius: '8px', padding: '12px 20px', fontSize: '0.85rem', fontWeight: 500, background: T.redBg, border: `1px solid rgba(192,80,74,0.3)`, color: T.red }}>
                      <span>❌ {uploadResult.message}</span>
                      <button onClick={() => setUploadResult(null)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'inherit' }}><X size={16} /></button>
                    </div>
                  ) : (
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderRadius: '8px', padding: '12px 20px', fontSize: '0.85rem', fontWeight: 500, background: uploadResult.summary?.failed > 0 ? T.amberBg : T.emeraldBg, border: `1px solid ${uploadResult.summary?.failed > 0 ? 'rgba(224,122,32,0.3)' : 'rgba(90,154,53,0.3)'}`, color: uploadResult.summary?.failed > 0 ? '#8A6A00' : T.emerald }}>
                      <span>{uploadResult.summary?.failed > 0 ? `⚠️ ${uploadResult.summary.success} file(s) uploaded, ${uploadResult.summary.failed} failed` : `✅ ${uploadResult.summary?.success} file(s) successfully deposited`}</span>
                      <button onClick={() => setUploadResult(null)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'inherit' }}><X size={16} /></button>
                    </div>
                  )
                )}

                <DropZone onFiles={handleFiles} isPipelineRunning={isPipelineRunning} />
                
                <div style={{ background: T.surface2, border: `1px solid ${T.border}`, borderRadius: '12px', padding: '20px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                  <div>
                    <h4 style={{ color: T.textPrimary, fontSize: '0.95rem', fontWeight: 600, margin: '0 0 6px', display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <FolderSearch size={18} style={{ color: T.textSecond }} /> Network Folders
                    </h4>
                    <p style={{ color: T.textSecond, fontSize: '0.85rem', margin: 0 }}>Scan server directories for automatically deposited extracts.</p>
                  </div>
                  <button onClick={handleScanFolders} style={{ padding: '10px 20px', background: T.surface, border: `1px solid ${T.border}`, color: T.textPrimary, fontSize: '0.85rem', fontWeight: 600, borderRadius: '8px', cursor: 'pointer', transition: 'all 0.1s' }} onMouseEnter={e => e.currentTarget.style.background = '#FFF'} onMouseLeave={e => e.currentTarget.style.background = T.surface}>
                    Scan Now
                  </button>
                </div>

                {scanResults.length > 0 && (
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px' }}>
                    {scanResults.map(r => (
                      <div key={r.loaderId} style={{ display: 'flex', alignItems: 'center', gap: '10px', background: T.blueBg, border: `1px solid rgba(58,120,201,0.2)`, borderRadius: '8px', padding: '12px 16px' }}>
                        <Zap size={16} style={{ color: T.blue }} />
                        <span style={{ color: T.blue, fontWeight: 600, fontSize: '0.85rem', flex: 1 }}>{r.loaderLabel}</span>
                        <span style={{ color: T.blue, fontSize: '0.75rem', fontWeight: 600, background: T.surface, padding: '3px 10px', borderRadius: '4px' }}>{r.count} pending</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Fixed Footer */}
          {view === 'main' && (
            <div style={{ padding: '20px 32px', borderTop: `1px solid ${T.border}`, background: T.surface2, display: 'flex', justifyContent: 'flex-end' }}>
              <div style={{ flex: 1, display: 'flex', alignItems: 'center' }}>
                {pipelineError && (
                  <span style={{ fontSize: '0.8rem', color: T.red }}>{pipelineError}</span>
                )}
              </div>
              <button onClick={() => { if (isPipelineRunning) setView('pipeline'); else runPipelineMutation.mutate(); }} disabled={runPipelineMutation.isPending}
                style={{ display: 'flex', alignItems: 'center', gap: '10px', padding: '12px 32px', borderRadius: '8px', fontSize: '0.95rem', fontWeight: 600, cursor: 'pointer',
                         background: isPipelineRunning ? T.blueBg : T.green, border: `1px solid ${isPipelineRunning ? 'rgba(58,120,201,0.3)' : T.greenBorder}`, color: isPipelineRunning ? T.blue : T.greenText, transition: 'all 0.15s' }}>
                {runPipelineMutation.isPending ? <><Loader2 size={18} style={{ animation: 'spin 1s linear infinite' }} /> Initializing...</> : isPipelineRunning ? <><Activity size={18} /> View Pipeline Progress</> : <><Play size={18} fill="currentColor" /> Run Data Pipeline</>}
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}