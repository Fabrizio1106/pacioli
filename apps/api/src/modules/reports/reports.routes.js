// src/modules/reports/reports.routes.js
import { Router }      from 'express';
import { requireAuth } from '../../shared/middleware/auth.middleware.js';
import * as ctrl       from './reports.controller.js';

const router = Router();

// ── Preview (JSON — para mostrar en pantalla) ─────────────────────────────
// Todos retornan máximo 200 filas cuando preview=true
router.get('/overview',          requireAuth, ctrl.getOverview);
router.get('/bank',              requireAuth, ctrl.getBank);
router.get('/portfolio',         requireAuth, ctrl.getPortfolio);
router.get('/card-details',      requireAuth, ctrl.getCardDetails);
router.get('/card-settlements',  requireAuth, ctrl.getCardSettlements);
router.get('/parking',           requireAuth, ctrl.getParking);
router.get('/summary',           requireAuth, ctrl.getSummary);

// ── Export (Excel — descarga directa) ────────────────────────────────────
// Sin límite de filas — genera el Excel completo
router.get('/export/overview',         requireAuth, ctrl.exportOverview);
router.get('/export/bank',             requireAuth, ctrl.exportBank);
router.get('/export/portfolio',        requireAuth, ctrl.exportPortfolio);
router.get('/export/card-details',     requireAuth, ctrl.exportCardDetails);
router.get('/export/card-settlements', requireAuth, ctrl.exportCardSettlements);
router.get('/export/parking',          requireAuth, ctrl.exportParking);
router.get('/export/summary',          requireAuth, ctrl.exportSummary);

export default router;