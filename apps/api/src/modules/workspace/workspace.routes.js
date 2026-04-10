// src/modules/workspace/workspace.routes.js
import { Router }      from 'express';
import { requireAuth, requireRole } from '../../shared/middleware/auth.middleware.js';
import * as controller from './workspace.controller.js';

const router = Router();

const analystOrAdmin = [requireAuth, requireRole('admin', 'analyst', 'senior_analyst')];

// GET  /api/v1/workspace/my-queue
router.get('/my-queue',          ...analystOrAdmin, controller.getMyQueue);

// GET  /api/v1/workspace/my-stats
router.get('/my-stats',          ...analystOrAdmin, controller.getMyStats);

// GET  /api/v1/workspace/:stgId/panel
router.get('/:stgId/panel',      ...analystOrAdmin, controller.getPanel);

// POST /api/v1/workspace/:stgId/calculate
router.post('/:stgId/calculate', ...analystOrAdmin, controller.calculate);

// POST /api/v1/workspace/:stgId/approve
router.post('/:stgId/approve',   ...analystOrAdmin, controller.approve);

export default router;