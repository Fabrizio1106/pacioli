// src/modules/notifications/notifications.routes.js
import { Router }      from 'express';
import { requireAuth, requireRole } from '../../shared/middleware/auth.middleware.js';
import * as controller from './notifications.controller.js';

const router = Router();

// Notification count — for badge polling (every 30s)
router.get('/count',            requireAuth, controller.getCount);

// Pending reversals — for the panel
router.get('/reversals',        requireAuth, controller.getPendingReversals);

// Approved today — for analyst's tab
router.get('/approved-today',   requireAuth, controller.getApprovedToday);

// Request a reversal — analyst or admin only
router.post('/reversals/:stgId/request',     requireAuth, requireRole('admin', 'analyst'), controller.requestReversal);

// Approve or reject a reversal — admin only
router.post('/reversals/:requestId/approve', requireAuth, requireRole('admin'), controller.approveReversal);
router.post('/reversals/:requestId/reject',  requireAuth, requireRole('admin'), controller.rejectReversal);

export default router;