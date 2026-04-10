// src/modules/overview/overview.routes.js
import { Router }      from 'express';
import { requireAuth, requireRole } from '../../shared/middleware/auth.middleware.js';
import * as controller from './overview.controller.js';

const router = Router();
router.get('/',                requireAuth,                        controller.getOverview);
router.post('/sync-matches',   requireAuth, requireRole('admin', 'senior_analyst'), controller.syncMatches);
router.patch('/:bankRef1/note', requireAuth, requireRole('admin', 'senior_analyst'), controller.updateNote);
export default router;