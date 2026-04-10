// src/modules/gold-export/gold-export.controller.js
import * as service from './gold-export.service.js';

// GET /api/v1/gold-export/preview
export async function preview(req, res) {
  try {
    const data = await service.getExportPreview();
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    console.error('[gold-export.controller] preview:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// POST /api/v1/gold-export/submit
export async function submit(req, res) {
  try {
    const result = await service.submitForPosting({
      exportedBy: req.user.username,
    });
    return res.status(200).json({ status: 'success', data: result });
  } catch (err) {
    if (err.status === 400) {
      return res.status(400).json({ status: 'error', message: err.message });
    }
    console.error('[gold-export.controller] submit:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}

// GET /api/v1/gold-export/batches
export async function getBatches(req, res) {
  try {
    const data = await service.getBatchHistory();
    return res.status(200).json({ status: 'success', data });
  } catch (err) {
    console.error('[gold-export.controller] getBatches:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}