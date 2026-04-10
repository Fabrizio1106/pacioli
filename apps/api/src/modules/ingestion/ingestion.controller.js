// src/modules/ingestion/ingestion.controller.js
import * as service from './ingestion.service.js';

// GET /api/v1/ingestion/loader-status
// Estado de salud de cada fuente de datos (última fecha, registros, suma)
export async function getLoaderStatus(req, res) {
  try {
    const data = await service.getLoaderStatus();
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    console.error('[ingestion] getLoaderStatus:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// POST /api/v1/ingestion/classify
// Recibe archivos en memoria y devuelve la clasificación detectada
// sin moverlos todavía. El frontend muestra la tabla de confirmación.
export async function classifyFiles(req, res) {
  try {
    if (!req.files || req.files.length === 0) {
      return res.status(400).json({
        status: 'error', message: 'No se recibieron archivos',
      });
    }

    const data = await service.classifyFiles(req.files);
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    console.error('[ingestion] classifyFiles:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// POST /api/v1/ingestion/upload
// Recibe archivos + asignaciones confirmadas por el usuario
// y los mueve a sus carpetas raw correspondientes.
// Body: multipart/form-data
//   files: los archivos binarios
//   assignments: JSON string con [{ originalName, loaderId }]
export async function uploadFiles(req, res) {
  try {
    if (!req.files || req.files.length === 0) {
      return res.status(400).json({
        status: 'error', message: 'No se recibieron archivos',
      });
    }

    // Parsear las asignaciones confirmadas por el usuario
    let assignments = [];
    try {
      assignments = JSON.parse(req.body.assignments || '[]');
    } catch {
      return res.status(400).json({
        status: 'error', message: 'El campo assignments debe ser JSON válido',
      });
    }

    if (assignments.length === 0) {
      return res.status(400).json({
        status: 'error', message: 'No se recibieron asignaciones de loaders',
      });
    }

    // Mapear buffer de cada archivo con su asignación
    const fileMap = {};
    for (const file of req.files) {
      fileMap[file.originalname] = file.buffer;
    }

    const enrichedAssignments = assignments
      .filter(a => a.loaderId) // Ignorar los que el usuario dejó sin asignar
      .map(a => ({
        originalName: a.originalName,
        loaderId:     a.loaderId,
        buffer:       fileMap[a.originalName],
      }))
      .filter(a => a.buffer); // Solo los que tienen buffer

    if (enrichedAssignments.length === 0) {
      return res.status(400).json({
        status:  'error',
        message: 'Ningún archivo tiene asignación válida con loader',
      });
    }

    const data = await service.uploadFiles(enrichedAssignments);
    const success = data.filter(r => r.success).length;
    const failed  = data.filter(r => !r.success).length;

    return res.status(200).json({
      status: 'success',
      data,
      summary: { success, failed, total: data.length },
    });
  } catch (err) {
    console.error('[ingestion] uploadFiles:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// GET /api/v1/ingestion/scan-folders
// Detecta archivos ya colocados manualmente en carpetas raw
export async function scanFolders(req, res) {
  try {
    const data = await service.scanFolders();
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    console.error('[ingestion] scanFolders:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// POST /api/v1/ingestion/run-pipeline
// Dispara el orquestador Python. Solo una ejecución a la vez.
export async function runPipeline(req, res) {
  try {
    const result = await service.runPipeline();
    const statusCode = result.started ? 202 : 409; // 409 Conflict si ya corre
    return res.status(statusCode).json({ status: 'success', data: result });
  } catch (err) {
    console.error('[ingestion] runPipeline:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// GET /api/v1/ingestion/pipeline-status
// Estado actual del pipeline: proceso en curso + progreso por grupo
export async function getPipelineStatus(req, res) {
  try {
    const data = await service.getPipelineStatus();
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    console.error('[ingestion] getPipelineStatus:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// GET /api/v1/ingestion/history
// Historial de ejecuciones de los últimos 7 días
export async function getPipelineHistory(req, res) {
  try {
    const data = await service.getPipelineHistory();
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    console.error('[ingestion] getPipelineHistory:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}