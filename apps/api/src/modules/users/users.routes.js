// src/modules/users/users.routes.js
import { Router }      from 'express';
import { requireAuth } from '../../shared/middleware/auth.middleware.js';
import * as controller from './users.controller.js';

const router = Router();
router.get('/analysts', requireAuth, controller.getAnalysts);
export default router;