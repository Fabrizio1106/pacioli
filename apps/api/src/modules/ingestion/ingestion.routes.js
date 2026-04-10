// src/modules/ingestion/ingestion.routes.js
import { Router }      from 'express';
import multer          from 'multer';
import { requireAuth, requireRole } from '../../shared/middleware/auth.middleware.js';
import * as controller from './ingestion.controller.js';

const router = Router();

// Multer en memoria — los archivos se procesan en RAM antes de clasificarlos
// y moverlos a disco. Límite de 50MB por archivo, 20 archivos por request.
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 50 * 1024 * 1024, // 50 MB por archivo
    files:    20,               // máximo 20 archivos a la vez
  },
  fileFilter: (req, file, cb) => {
    const allowed = [
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', // .xlsx
      'application/vnd.ms-excel',                                           // .xls
      'application/vnd.ms-outlook',                                         // .msg
      'text/csv',                                                            // .csv
      'text/plain',                                                          // .txt
      'application/octet-stream',                                            // fallback para .msg
    ];
    // También permitir por extensión cuando el mime type no es reconocido
    const ext = file.originalname.split('.').pop().toLowerCase();
    if (allowed.includes(file.mimetype) || ['xlsx','xls','msg','csv','txt'].includes(ext)) {
      cb(null, true);
    } else {
      cb(new Error(`Tipo de archivo no permitido: ${file.originalname}`));
    }
  },
});

const analystOrAdmin = [requireAuth, requireRole('admin', 'analyst', 'senior_analyst')];

// ── Métricas de estado ────────────────────────────────────────────────────────

// GET  /api/v1/ingestion/loader-status
router.get('/loader-status',   requireAuth, controller.getLoaderStatus);

// ── Flujo de carga de archivos ────────────────────────────────────────────────

// POST /api/v1/ingestion/classify — analyst or admin only
router.post('/classify',       ...analystOrAdmin, upload.array('files'), controller.classifyFiles);

// POST /api/v1/ingestion/upload — analyst or admin only
router.post('/upload',         ...analystOrAdmin, upload.array('files'), controller.uploadFiles);

// GET  /api/v1/ingestion/scan-folders — analyst or admin only
router.get('/scan-folders',    ...analystOrAdmin, controller.scanFolders);

// ── Control del pipeline ──────────────────────────────────────────────────────

// POST /api/v1/ingestion/run-pipeline — analyst or admin only
router.post('/run-pipeline',   ...analystOrAdmin, controller.runPipeline);

// GET  /api/v1/ingestion/pipeline-status
router.get('/pipeline-status', requireAuth, controller.getPipelineStatus);

// GET  /api/v1/ingestion/history
router.get('/history',         requireAuth, controller.getPipelineHistory);

export default router;