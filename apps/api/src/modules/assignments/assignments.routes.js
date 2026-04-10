// src/modules/assignments/assignments.routes.js
import { Router }      from 'express';
import { requireAuth } from '../../shared/middleware/auth.middleware.js';
import { requireRole } from '../../shared/middleware/auth.middleware.js';
import * as controller from './assignments.controller.js';

const router = Router();

// All assignment endpoints require admin role
// requireAuth verifies JWT, requireRole('admin') verifies role

// GET  /api/v1/admin/assignments/rules
router.get('/rules', requireAuth, requireRole('admin'), controller.getRules);

// POST /api/v1/admin/assignments/apply-rules
// Triggers sync + applies all assignment rules
router.post('/apply-rules', requireAuth, requireRole('admin', 'senior_analyst'), controller.applyRules);

// PATCH /api/v1/admin/assignments/:bankRef1/reassign
// Manual reassignment of a single transaction
router.patch('/:bankRef1/reassign', requireAuth, requireRole('admin', 'senior_analyst'), controller.reassign);

export default router;