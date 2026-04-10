// src/modules/gold-export/gold-export.routes.js
import { Router }      from 'express';
import { requireAuth, requireRole } from '../../shared/middleware/auth.middleware.js';
import * as controller from './gold-export.controller.js';

const router = Router();

router.get('/preview', requireAuth, requireRole('admin', 'senior_analyst'), controller.preview);
router.post('/submit', requireAuth, requireRole('admin', 'senior_analyst'), controller.submit);
router.get('/batches', requireAuth, requireRole('admin', 'senior_analyst'), controller.getBatches);

export default router;