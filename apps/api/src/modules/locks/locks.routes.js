// src/modules/locks/locks.routes.js
import { Router }      from 'express';
import { requireAuth, requireRole } from '../../shared/middleware/auth.middleware.js';
import * as controller from './locks.controller.js';

const router = Router();

const analystOrAdmin = [requireAuth, requireRole('admin', 'analyst', 'senior_analyst')];

// POST   /api/v1/locks/:bankRef1/acquire  → open a transaction
router.post('/:bankRef1/acquire',   ...analystOrAdmin, controller.acquire);

// PATCH  /api/v1/locks/:bankRef1/renew    → heartbeat every 4 min
router.patch('/:bankRef1/renew',    ...analystOrAdmin, controller.renew);

// DELETE /api/v1/locks/:bankRef1/release  → close or approve
router.delete('/:bankRef1/release', ...analystOrAdmin, controller.release);

// GET    /api/v1/locks/:bankRef1/status   → check if locked
router.get('/:bankRef1/status',     ...analystOrAdmin, controller.status);

export default router;