// src/modules/portfolio/portfolio.routes.js
import { Router }      from 'express';
import { requireAuth, requireRole } from '../../shared/middleware/auth.middleware.js';
import * as controller from './portfolio.controller.js';

const router = Router();

const analystOrAdmin = [requireAuth, requireRole('admin', 'analyst')];

// GET /api/v1/portfolio/for-transaction/:stgId
router.get('/for-transaction/:stgId', ...analystOrAdmin, controller.getForTransaction);

// GET /api/v1/portfolio/search?q=texto&bank_amount=105.66
router.get('/search',                 ...analystOrAdmin, controller.searchPortfolio);

// POST /api/v1/portfolio/validate-selection
router.post('/validate-selection',    ...analystOrAdmin, controller.validateSelection);

export default router;