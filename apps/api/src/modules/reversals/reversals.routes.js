// src/modules/reversals/reversals.routes.js
import { Router }      from 'express';
import { requireAuth, requireRole } from '../../shared/middleware/auth.middleware.js';
import * as controller from './reversals.controller.js';

const router = Router();

// GET /api/v1/reversals/daily
// "Processed Today" tab — any authenticated user can see their own processed items
router.get('/daily', requireAuth, controller.getDaily);

// POST /api/v1/reversals/:stgId
// Execute a reversal — analyst or admin only (service further validates ownership)
router.post('/:stgId', requireAuth, requireRole('admin', 'analyst'), controller.reverse);

export default router;