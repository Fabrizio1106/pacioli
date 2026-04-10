// src/modules/reconciliation/reconciliation.routes.js
import { Router }      from 'express';
import { requireAuth, requireRole } from '../../shared/middleware/auth.middleware.js';
import * as controller from './reconciliation.controller.js';

const router = Router();

const analystOrAdmin = [requireAuth, requireRole('admin', 'analyst')];

// GET /api/v1/reconciliation/:stgId/approved-detail
// Read-only detail for DONE tab — viewer can also see approved detail
router.get('/:stgId/approved-detail', requireAuth, controller.getApprovedDetail);

// POST /api/v1/reconciliation/:stgId/calculate
// Preview balance — analyst or admin only
router.post('/:stgId/calculate', ...analystOrAdmin, controller.calculate);

// POST /api/v1/reconciliation/:stgId/approve
// Commit the approval — analyst or admin only
router.post('/:stgId/approve', ...analystOrAdmin, controller.approve);

export default router;