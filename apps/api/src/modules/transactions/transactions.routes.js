// src/modules/transactions/transactions.routes.js
import { Router }      from 'express';
import { requireAuth } from '../../shared/middleware/auth.middleware.js';
import * as controller from './transactions.controller.js';

const router = Router();

// Todas las rutas de transacciones requieren autenticación
// requireAuth verifica el JWT antes de llegar al controller

// GET /api/v1/transactions/summary  ← debe ir ANTES que /:id
// Si va después, Express interpretaría "summary" como un ID
router.get('/summary', requireAuth, controller.summary);

// GET /api/v1/transactions?status=REVIEW&page=1&limit=20
router.get('/', requireAuth, controller.list);

// GET /api/v1/transactions/123
router.get('/:id', requireAuth, controller.detail);

export default router;