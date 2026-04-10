// src/modules/auth/auth.routes.js
import { Router }     from 'express';
import { rateLimit }  from 'express-rate-limit';
import * as controller from './auth.controller.js';
import { requireAuth } from '../../shared/middleware/auth.middleware.js';

const router = Router();

// Rate limiter ESPECÍFICO para login — más estricto que el global.
// Máximo 10 intentos por IP en 15 minutos.
// Después del intento 10, la IP queda bloqueada 15 minutos.
// Esto hace la fuerza bruta prácticamente inviable.
const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutos
  max:      10,
  message: {
    status:  'error',
    message: 'Demasiados intentos de login. Intenta en 15 minutos.',
  },
  standardHeaders: true,
  legacyHeaders:   false,
});

// POST /api/v1/auth/login
// loginLimiter se aplica SOLO a esta ruta, no a toda la app
router.post('/login', loginLimiter, controller.login);

// GET /api/v1/auth/me
// requireAuth es el middleware que verifica el JWT
// Si el token no es válido, devuelve 401 antes de llegar al controller
router.get('/me', requireAuth, controller.getMe);

// POST /api/v1/auth/logout
router.post('/logout', requireAuth, controller.logout);

export default router;