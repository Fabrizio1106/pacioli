// src/modules/ingestion/ingestion.service.js
import { pool }   from '../../config/database.js';
import * as repo  from './ingestion.repository.js';
import path       from 'path';
import fsp        from 'fs/promises';
import XLSX       from 'xlsx';

// ─────────────────────────────────────────────────────────────────────────────
// CONFIGURACIÓN DEL PIPELINE SERVER (FastAPI)
// ─────────────────────────────────────────────────────────────────────────────
const PIPELINE_SERVER_URL = process.env.PIPELINE_SERVER_URL || 'http://localhost:8000';
const PIPELINE_API_KEY    = process.env.PIPELINE_API_KEY    || '';

// ─────────────────────────────────────────────────────────────────────────────
// CONFIGURACIÓN DE LOADERS
// Define la identidad de cada loader: su nombre visible, la tabla raw donde
// vive su data, las columnas de fecha y monto para las métricas, y las carpetas
// donde espera sus archivos de entrada.
// ─────────────────────────────────────────────────────────────────────────────
const LOADER_CONFIG = [
  {
    id:           'banco_239',
    label:        'Banco 239',
    table:        'biq_raw.raw_banco_239',
    dateCol:      'fecha_transaccion',   // ✓ confirmado
    amountCol:    'valor',               // ✓ confirmado
    inputFolder:  '00. Banco_Cta_239/Principal_Bco_239',
    filePatterns: ['banco', 'bco239', 'bco_239', 'principal_bco'],
    headerSignature: ['FECHA', 'REFERENCIA', 'VALOR', 'SALDO CONTABLE'],
  },
  {
    id:           'sap_239',
    label:        'SAP Cta 239',
    table:        'biq_raw.raw_sap_cta_239',
    dateCol:      'fecha_documento',     // ✓ confirmado
    amountCol:    'importe_ml',          // ✓ confirmado
    inputFolder:  '00. Banco_Cta_239/Cta_Transitoria_239',
    filePatterns: ['sap_239', 'cta_239', 'transitoria', 't239'],
    headerSignature: ['Nº documento', 'Fecha de documento', 'Importe en moneda local'],
  },
  {
    id:           'fbl5n',
    label:        'FBL5N — Cartera SAP',
    table:        'biq_raw.raw_customer_portfolio',
    dateCol:      'fecha_documento',     // ✓ confirmado
    amountCol:    'importe',             // ✓ confirmado
    inputFolder:  '02. FBL5N',
    filePatterns: ['fbl5n', 'fbl5', 'cartera', 'portfolio'],
    headerSignature: ['Cuenta', 'Nombre 1', 'Referencia a factura', 'Importe en moneda local'],
  },
  {
    id:           'diners_club',
    label:        'Diners Club',
    table:        'biq_raw.raw_diners_club',
    dateCol:      'fecha_del_pago',       // ✓ confirmado
    amountCol:    'valor_bruto_cuota',    // ✓ corregido (era valor_total_pago)
    inputFolder:  '01. Establecimientos/Diners_Club',
    filePatterns: ['diners', 'dc_', '_dc_', 'dinersclub'],
    headerSignature: ['Fecha del pago', 'Marca', 'Número Recap o Lote', 'Valor Bruto Cuota'],
  },
  {
    id:           'guayaquil',
    label:        'Guayaquil (AMEX)',
    table:        'biq_raw.raw_guayaquil',
    dateCol:      'fecha_liquida',        // ✓ corregido (era fecha_transaccion)
    amountCol:    'total',                // ✓ corregido (era a_pagar)
    inputFolder:  '01. Establecimientos/Guayaquil',
    filePatterns: ['guayaquil', 'amex', 'gye'],
    headerSignature: ['Recap', 'Referencia', 'Comercio Descripcion', 'Total'],
  },
  {
    id:           'pacificard',
    label:        'Pacificard',
    table:        'biq_raw.raw_pacificard',
    dateCol:      'fecha_pago',           // ✓ confirmado
    amountCol:    'valor_transaccion',    // ✓ corregido (era valor_de_pago)
    inputFolder:  '01. Establecimientos/Pacificard',
    filePatterns: ['pacificard', 'pcf', 'vip', 'parking', 'sala'],
    headerSignature: ['Fecha de Pago', 'Numero de Recap', 'Valor Transaccion'],
    acceptsMsg:   true,
  },
  {
    id:           'databalance',
    label:        'Databalance',
    table:        'biq_raw.raw_databalance',
    dateCol:      'fecha_voucher',        // ✓ confirmado
    amountCol:    'valor_total',          // ✓ corregido (era valor_pagado)
    inputFolder:  '01. Establecimientos/Databalance',
    filePatterns: ['databalance', 'data_balance', 'dbalance'],
    headerSignature: ['TDTLF', 'FECHA TRX', 'VALOR TOTAL'],
  },
  {
    id:           'webpos',
    label:        'WebPos',
    table:        'biq_raw.raw_webpos',
    dateCol:      'fecha',                // ✓ confirmado
    amountCol:    'total',                // ✓ confirmado
    inputFolder:  '03. WebPos',
    filePatterns: ['webpos', 'web_pos', 'wp_'],
    headerSignature: ['TIPO_PAGO', 'FACTURA', 'LOTE', 'TOTAL'],
  },
  {
    id:           'retenciones',
    label:        'Retenciones SRI',
    table:        'biq_raw.raw_retenciones_sri',
    dateCol:      'fecha_autorizacion_ret', // ✓ corregido (era fecha_emision_ret)
    amountCol:    'valor_ret_iva',          // ✓ corregido (era valor_ret_renta)
    inputFolder:  '05. retenciones',
    filePatterns: ['retencion', 'ret_', 'sri'],
    headerSignature: ['RUC / CI Emisor', 'Valor Ret IVA', 'Fecha de autorización Retención'],
  },
  {
    id:           'manual_requests',
    label:        'Solicitudes Manuales',
    table:        'biq_raw.raw_manual_requests',
    dateCol:      'fecha',
    amountCol:    'valor',
    inputFolder:  '06. manual_requests',
    filePatterns: ['manual', 'solicitud', 'request'],
    headerSignature: ['FECHA', 'COD CLIENTE', 'CLIENTE', 'VALOR'],
  },
];

// Ruta raíz del proyecto Python — solo se usa para RAW_DATA_ROOT (upload de archivos)
const PYTHON_PROJECT_ROOT = process.env.PYTHON_PROJECT_ROOT
  || path.resolve(process.cwd(), '../../');

const RAW_DATA_ROOT = process.env.RAW_DATA_ROOT
  || path.join(PYTHON_PROJECT_ROOT, 'data_raw');

// ─────────────────────────────────────────────────────────────────────────────
// 1. LOADER STATUS — Estado de salud de cada fuente de datos
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Returns the current ingestion metrics for all configured data loaders.
 *
 * Queries each loader's raw table in parallel to retrieve the last date with
 * data, total row count, and amount totals. Covers all configured sources:
 * Banco 239, SAP Cta 239, Pacificard, and any additional loaders in LOADER_CONFIG.
 *
 * @returns {Promise<object[]>} Array of loader metric objects, one per configured loader.
 */
export async function getLoaderStatus() {
  const results = await Promise.all(
    LOADER_CONFIG.map(loader => _getLoaderMetrics(loader))
  );
  return results;
}

async function _getLoaderMetrics(loader) {
  try {
    // Última fecha con datos en la tabla raw
    const dateResult = await pool.query(`
      SELECT
        MAX(${loader.dateCol}::date)  AS last_date,
        MIN(${loader.dateCol}::date)  AS first_date
      FROM ${loader.table}
    `);

    const lastDate  = dateResult.rows[0]?.last_date  || null;
    const firstDate = dateResult.rows[0]?.first_date || null;

    if (!lastDate) {
      return {
        id:        loader.id,
        label:     loader.label,
        status:    'empty',
        lastDate:  null,
        firstDate: null,
        lastCount: 0,
        lastSum:   null,
        daysAgo:   null,
        message:   'Sin datos cargados',
      };
    }

    // Registros y suma de la última fecha
    const metricsResult = await pool.query(`
      SELECT
        COUNT(*)::integer                            AS count,
        ROUND(SUM(ABS(${loader.amountCol}))::numeric, 2) AS total
      FROM ${loader.table}
      WHERE ${loader.dateCol}::date = $1
    `, [lastDate]);

    const lastCount = metricsResult.rows[0]?.count || 0;
    const lastSum   = metricsResult.rows[0]?.total != null
      ? parseFloat(metricsResult.rows[0].total)
      : null;

    // Días desde la última fecha de datos
    const daysAgoResult = await pool.query(`
      SELECT (CURRENT_DATE - $1::date)::integer AS days_ago
    `, [lastDate]);
    const daysAgo = daysAgoResult.rows[0]?.days_ago ?? null;

    // Semáforo de estado
    let status = 'ok';
    if (daysAgo > 3)  status = 'warning';
    if (daysAgo > 7)  status = 'outdated';

    // Normalizar fecha a string YYYY-MM-DD (PostgreSQL puede devolver Date object)
    const toDateStr = (d) => {
      if (!d) return null;
      if (d instanceof Date) return d.toISOString().split('T')[0];
      return String(d).split('T')[0];
    };

    return {
      id:        loader.id,
      label:     loader.label,
      status,
      lastDate:  toDateStr(lastDate),
      firstDate: toDateStr(firstDate),
      lastCount,
      lastSum,
      daysAgo,
      message:   daysAgo === 0 ? 'Al día'
               : daysAgo === 1 ? 'Actualizado ayer'
               : `${daysAgo} días sin actualizar`,
    };

  } catch (err) {
    // La tabla puede no existir aún en un ambiente limpio
    return {
      id:        loader.id,
      label:     loader.label,
      status:    'error',
      lastDate:  null,
      firstDate: null,
      lastCount: 0,
      lastSum:   null,
      daysAgo:   null,
      message:   'Tabla no encontrada o sin permisos',
    };
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. CLASSIFY FILES — Detecta a qué loader va cada archivo
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Classifies uploaded files by detecting which data loader each file belongs to.
 *
 * Detection uses two strategies in order: filename pattern matching, then Excel
 * header signature inspection. Files with a .msg extension are routed directly
 * to the Pacificard loader. Each result includes a confidence level indicating
 * how the match was determined (name, header, or unknown).
 *
 * @param {object[]} files                - Multer file objects from the upload middleware.
 * @param {string}   files[].originalname - Original filename as uploaded by the client.
 * @param {Buffer}   files[].buffer       - File contents in memory.
 * @returns {Promise<object[]>} Classification results with detectedLoader, confidence, and status per file.
 */
export async function classifyFiles(files) {
  const results = await Promise.all(
    files.map(file => _classifyOneFile(file))
  );
  return results;
}

async function _classifyOneFile(file) {
  const nameLower = file.originalname.toLowerCase();
  const ext       = path.extname(nameLower);

  // Archivos .msg solo van a Pacificard
  if (ext === '.msg') {
    const loader = LOADER_CONFIG.find(l => l.acceptsMsg);
    return {
      originalName: file.originalname,
      detectedLoader: loader?.id || null,
      confidence:     loader ? 'name'   : 'unknown',
      status:         loader ? 'detected' : 'unrecognized',
    };
  }

  // Paso 1: detección por nombre de archivo
  for (const loader of LOADER_CONFIG) {
    const match = loader.filePatterns.some(p => nameLower.includes(p));
    if (match) {
      return {
        originalName:   file.originalname,
        detectedLoader: loader.id,
        confidence:     'name',
        status:         'detected',
      };
    }
  }

  // Paso 2: detección por estructura interna del Excel
  if (['.xlsx', '.xls'].includes(ext) && file.buffer) {
    try {
      const wb      = XLSX.read(file.buffer, { type: 'buffer', sheetRows: 15 });
      const sheet   = wb.Sheets[wb.SheetNames[0]];
      const rows    = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });

      // Buscar la fila de encabezados (primeras 10 filas)
      let headerRow = [];
      for (let i = 0; i < Math.min(10, rows.length); i++) {
        const row = rows[i].map(c => String(c).trim()).filter(Boolean);
        if (row.length >= 3) { headerRow = row; break; }
      }

      if (headerRow.length > 0) {
        for (const loader of LOADER_CONFIG) {
          const sig     = loader.headerSignature || [];
          const matches = sig.filter(col =>
            headerRow.some(h => h.toLowerCase().includes(col.toLowerCase()))
          );
          // Si al menos el 60% de la firma coincide → detectado
          if (sig.length > 0 && matches.length / sig.length >= 0.6) {
            return {
              originalName:   file.originalname,
              detectedLoader: loader.id,
              confidence:     'structure',
              status:         'detected',
            };
          }
        }
      }
    } catch {
      // Si no se puede leer el Excel, continúa sin detección por estructura
    }
  }

  return {
    originalName:   file.originalname,
    detectedLoader: null,
    confidence:     'unknown',
    status:         'unrecognized',
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. UPLOAD FILES — Mueve archivos confirmados a sus carpetas raw
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Writes classified files to their loader's configured input folder on disk.
 *
 * Creates the destination directory if it does not exist. Per-file errors are
 * captured and returned in the results array without aborting the batch —
 * a failure on one file does not prevent the others from being written.
 *
 * @param {object[]} assignments                - File assignment list from classifyFiles.
 * @param {string}   assignments[].originalName - Filename to write to disk.
 * @param {string}   assignments[].loaderId     - ID of the target loader from LOADER_CONFIG.
 * @param {Buffer}   assignments[].buffer       - File contents to write.
 * @returns {Promise<object[]>} Per-file results with success flag, destination path, and message.
 */
export async function uploadFiles(assignments) {
  // assignments: [{ originalName, loaderId, buffer }]
  const results = [];

  for (const item of assignments) {
    const loader = LOADER_CONFIG.find(l => l.id === item.loaderId);
    if (!loader) {
      results.push({
        originalName: item.originalName,
        success:      false,
        message:      `Loader desconocido: ${item.loaderId}`,
      });
      continue;
    }

    const destDir  = path.join(RAW_DATA_ROOT, loader.inputFolder);
    const destPath = path.join(destDir, item.originalName);

    try {
      await fsp.mkdir(destDir, { recursive: true });
      await fsp.writeFile(destPath, item.buffer);
      results.push({
        originalName: item.originalName,
        loaderId:     item.loaderId,
        loaderLabel:  loader.label,
        destPath,
        success:      true,
        message:      `Archivo depositado en ${loader.inputFolder}`,
      });
    } catch (err) {
      results.push({
        originalName: item.originalName,
        loaderId:     item.loaderId,
        success:      false,
        message:      `Error al escribir archivo: ${err.message}`,
      });
    }
  }

  return results;
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. SCAN FOLDERS — Detecta archivos ya colocados manualmente en carpetas raw
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Scans each loader's input folder for files placed there manually.
 *
 * Only loaders with at least one pending file appear in the result.
 * Accepts .xlsx, .xls, .msg, .csv, and .txt files; skips temporary
 * Excel lock files (~$...). Missing folders are silently ignored —
 * they are normal in a clean environment.
 *
 * @returns {Promise<object[]>} Array of { loaderId, loaderLabel, folder, files, count }
 *   for each loader that has pending files.
 */
export async function scanFolders() {
  const found = [];
  const allowedExt = ['.xlsx', '.xls', '.msg', '.csv', '.txt'];

  for (const loader of LOADER_CONFIG) {
    const folderPath = path.join(RAW_DATA_ROOT, loader.inputFolder);
    try {
      const files = await fsp.readdir(folderPath);
      const pending = files.filter(f => {
        const ext = path.extname(f).toLowerCase();
        return allowedExt.includes(ext) && !f.startsWith('~$');
      });

      if (pending.length > 0) {
        found.push({
          loaderId:    loader.id,
          loaderLabel: loader.label,
          folder:      loader.inputFolder,
          files:       pending,
          count:       pending.length,
        });
      }
    } catch {
      // Carpeta no existe aún — normal en ambiente limpio
    }
  }

  return found;
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. RUN PIPELINE — Delega al Pipeline Server (FastAPI)
// El servidor FastAPI mantiene Python siempre activo con librerías precargadas,
// eliminando el overhead de arranque de ~2 min que tenía el spawn directo.
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Triggers a pipeline run via the FastAPI Pipeline Server.
 *
 * Delegates execution to the always-running Pipeline Server process, which
 * keeps Python libraries pre-loaded to avoid the ~2-minute cold-start overhead
 * of spawning a new process. Returns started: false (without throwing) if the
 * pipeline is already running (HTTP 409). Throws if the server is unreachable.
 *
 * @returns {Promise<{ started: boolean, jobId: string|null, message: string }>}
 *   Start confirmation, or a no-op result if already running.
 * @throws {Error} If the Pipeline Server is not running or cannot be reached.
 */
export async function runPipeline() {
  try {
    const res  = await fetch(`${PIPELINE_SERVER_URL}/run`, {
      method:  'POST',
      headers: PIPELINE_API_KEY ? { 'X-Pipeline-Key': PIPELINE_API_KEY } : {},
    });
    const data = await res.json();

    if (res.status === 409) {
      return {
        started: false,
        jobId:   data.job_id,
        message: data.message,
      };
    }

    return {
      started: data.started,
      jobId:   data.job_id,
      message: data.message,
    };
  } catch (err) {
    console.error('[ingestion] runPipeline — Pipeline Server no disponible:', err.message);
    throw new Error(
      'Pipeline Server no está corriendo. ' +
      'Inicia pipeline_server.py antes de ejecutar el pipeline.'
    );
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. PIPELINE STATUS — Estado del FastAPI server + progreso desde etl_process_windows
// El estado del proceso (running/completed/failed) viene del Pipeline Server.
// El progreso por grupo viene directamente de biq_config.etl_process_windows.
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Returns the combined pipeline status from the server and the ETL progress table.
 *
 * Merges two data sources:
 *   - Pipeline Server (/status): job state, running flag, log lines, and error.
 *   - biq_config.etl_process_windows: per-group progress rows for the current run.
 *
 * If the Pipeline Server is unreachable, serverAvailable is set to false and
 * server fields default to idle state — the ETL progress data is still returned.
 *
 * @returns {Promise<object>} Combined status with server state, ETL group progress,
 *   and serverAvailable flag.
 */
export async function getPipelineStatus() {
  // Consultar el estado del Pipeline Server
  let serverState = { running: false, status: 'idle', job_id: null,
                      started_at: null, finished_at: null, log: [], error: null };
  let serverAvailable = true;
  try {
    const res  = await fetch(`${PIPELINE_SERVER_URL}/status`);
    serverState = await res.json();
  } catch {
    serverAvailable = false;
    // Si el servidor no está disponible, mostrar estado neutro
  }

  // Normalizar el estado del pipeline para el frontend
  const pipelineInfo = {
    running:     serverState.running    ?? false,
    status:      serverState.status     ?? 'idle',
    jobId:       serverState.job_id     ?? null,
    startedAt:   serverState.started_at ?? null,
    finishedAt:  serverState.finished_at?? null,
    log:         serverState.log        ?? [],
    error:       serverState.error      ?? null,
    serverAvailable,
  };


  // Grupos del orquestador — nombres exactos de biq_config.etl_process_windows
  // RAW Layer no registra en etl_process_windows — se infiere del estado del pipeline
  const GROUPS = [
    {
      key:       'RAW',
      label:     'RAW Layer',
      processes: [],   // No registra en etl_process_windows
      isRaw:     true, // Tratamiento especial: se marca completed cuando GRUPO1 arranca
    },
    {
      key:       'GRUPO1',
      label:     'Grupo 1 — SAP + Cartera',
      processes: ['SAP_TRANSACTIONS', 'CUSTOMER_PORTFOLIO_PHASE1'],
    },
    {
      key:       'GRUPO2',
      label:     'Grupo 2 — Tarjetas',
      processes: ['DINERS_CARDS', 'GUAYAQUIL_CARDS', 'PACIFICARD_CARDS'],
    },
    {
      key:       'GRUPO3',
      label:     'Grupo 3 — Derivados',
      processes: ['PARKING_BREAKDOWN'],
    },
    {
      key:       'GRUPO4',
      label:     'Grupo 4 — Retenciones',
      processes: ['WITHHOLDINGS_PROCESS', 'WITHHOLDINGS_MATCH', 'WITHHOLDINGS_APPLY'],
    },
    {
      key:       'GRUPO5',
      label:     'Grupo 5 — Cartera Avanzada',
      processes: ['MANUAL_REQUESTS', 'BANK_ENRICHMENT', 'CUSTOMER_PORTFOLIO_PHASE2', 'CUSTOMER_PORTFOLIO_PHASE3'],
    },
    {
      key:       'GRUPO6',
      label:     'Grupo 6 — Conciliación',
      // VALIDATE_PORTFOLIO_MATCHES y RestoreApprovedTransactions no registran en la tabla
      // BANK_RECONCILIATION y BANK_VALIDATION_METRICS sí registran
      processes: ['BANK_RECONCILIATION', 'BANK_VALIDATION_METRICS'],
    },
  ];

  // Leer el estado de los procesos de HOY desde etl_process_windows
  let processRows = [];
  try {
    const result = await pool.query(`
      SELECT
        process_name,
        status,
        records_processed,
        execution_time_seconds,
        completed_at,
        error_message
      FROM biq_config.etl_process_windows
      WHERE DATE(created_at) = CURRENT_DATE
      ORDER BY completed_at ASC NULLS LAST
    `);
    processRows = result.rows;
  } catch {
    // La tabla puede no existir en primer arranque
  }

  // Construir el estado de cada grupo
  const processMap = {};
  for (const row of processRows) {
    processMap[row.process_name] = row;
  }

  // Determinar si el RAW Layer ya completó:
  // Si al menos un proceso de GRUPO1 existe en la BD, RAW ya terminó
  const rawCompleted = !!(processMap['SAP_TRANSACTIONS']);

  const groups = GROUPS.map(group => {

    // RAW Layer — no registra en etl_process_windows, se infiere
    if (group.isRaw) {
      const groupStatus = rawCompleted ? 'completed'
                        : pipelineInfo.running ? 'running'
                        : 'pending';
      return {
        key:          group.key,
        label:        group.label,
        status:       groupStatus,
        totalRecords: 0,
        totalSeconds: 0,
        processes:    [],
      };
    }

    // Grupos normales — leer desde etl_process_windows
    const groupProcesses = group.processes.map(name => {
      const row = processMap[name];
      return {
        name,
        status:           row?.status                                    || 'pending',
        recordsProcessed: row?.records_processed                         || 0,
        executionSeconds: row?.execution_time_seconds
          ? parseFloat(row.execution_time_seconds)
          : null,
        completedAt:      row?.completed_at                              || null,
        errorMessage:     row?.error_message                             || null,
      };
    });

    // Estado del grupo = el peor estado de sus procesos
    const statuses = groupProcesses.map(p => p.status);
    let groupStatus = 'pending';
    if (statuses.length > 0 && statuses.every(s => s === 'COMPLETED')) groupStatus = 'completed';
    else if (statuses.some(s => s === 'FAILED'))                        groupStatus = 'failed';
    else if (statuses.some(s => s === 'RUNNING'))                       groupStatus = 'running';
    else if (statuses.some(s => s === 'COMPLETED'))                     groupStatus = 'partial';

    const totalRecords = groupProcesses.reduce((s, p) => s + (p.recordsProcessed || 0), 0);
    const totalSeconds = groupProcesses.reduce((s, p) => s + (p.executionSeconds || 0), 0);

    return {
      key:          group.key,
      label:        group.label,
      status:       groupStatus,
      totalRecords,
      totalSeconds: Math.round(totalSeconds),
      processes:    groupProcesses,
    };
  });

  // Progreso global — solo contar grupos con procesos reales
  const completedGroups = groups.filter(g => g.status === 'completed').length;
  const progressPct     = Math.round((completedGroups / groups.length) * 100);

  return {
    pipeline:       pipelineInfo,
    groups,
    progressPct,
    completedGroups,
    totalGroups:    groups.length,
    serverAvailable,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. PIPELINE HISTORY — Historial de ejecuciones recientes
// ─────────────────────────────────────────────────────────────────────────────
/**
 * Returns a summary of pipeline runs for the last 7 days grouped by date.
 *
 * Queries biq_config.etl_process_windows and aggregates completed, failed, and
 * total process counts alongside total records and execution time per day.
 * Returns up to 14 entries. Returns an empty array on any database error rather
 * than throwing — this endpoint is non-critical and must not break the UI.
 *
 * @returns {Promise<object[]>} Daily run summaries ordered by date descending,
 *   each with runDate, completed, failed, totalRecords, totalSeconds, and status
 *   ('partial' if any process failed, 'completed' otherwise).
 */
export async function getPipelineHistory() {
  try {
    const rows = await repo.findPipelineHistory();
    return rows.map(row => ({
      runDate:        row.run_date,
      startedAt:      row.started_at,
      finishedAt:     row.finished_at,
      totalProcesses: row.total_processes,
      completed:      row.completed,
      failed:         row.failed,
      totalRecords:   row.total_records || 0,
      totalSeconds:   parseFloat(row.total_seconds) || 0,
      status:         row.failed > 0 ? 'partial' : 'completed',
    }));
  } catch {
    return [];
  }
}