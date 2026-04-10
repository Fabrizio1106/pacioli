// src/shared/middleware/auth.middleware.js
import { verifyAccessToken } from '../../modules/auth/auth.service.js';
import { findUserById }      from '../../modules/auth/auth.repository.js';

// ─────────────────────────────────────────────
// requireAuth
// Middleware que protege rutas — úsalo así:
//   router.get('/transactions', requireAuth, controller.list)
//
// Flujo:
// 1. Leer el header Authorization: Bearer <token>
// 2. Verificar que el JWT es válido y no expiró
// 3. Verificar que el usuario aún existe y está activo en DB
// 4. Inyectar req.user para que el controller lo use
// 5. Llamar next() para continuar al controller
// ─────────────────────────────────────────────
export async function requireAuth(req, res, next) {
  try {
    // PASO 1: Extraer el token del header
    // El formato estándar es: Authorization: Bearer eyJhbGc...
    const authHeader = req.headers['authorization'];

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({
        status:  'error',
        message: 'Token de autenticación requerido',
      });
    }

    const token = authHeader.split(' ')[1]; // extrae solo el token

    // PASO 2: Verificar la firma y expiración del JWT
    const payload = verifyAccessToken(token);

    if (!payload) {
      return res.status(401).json({
        status:  'error',
        message: 'Token inválido o expirado',
      });
    }

    // PASO 3: Verificar que el usuario sigue activo en DB
    // Esto captura el caso donde un admin desactiva un usuario
    // pero ese usuario todavía tiene un JWT válido de 8 horas.
    const user = await findUserById(payload.sub);

    if (!user) {
      return res.status(401).json({
        status:  'error',
        message: 'Usuario no encontrado o inactivo',
      });
    }

    // PASO 4: Inyectar el usuario en req para que los controllers lo usen
    req.user = {
      id:       user.id,
      username: user.username,
      fullName: user.full_name,
      role:     user.role,
      email:    user.email,
    };

    // PASO 5: Continuar al siguiente middleware o controller
    next();

  } catch (err) {
    console.error('[auth.middleware] Error inesperado:', err);
    return res.status(500).json({
      status:  'error',
      message: 'Error interno de autenticación',
    });
  }
}

// ─────────────────────────────────────────────
// requireRole
// Middleware de autorización — verifica que el usuario
// tiene el rol necesario para acceder a un recurso.
// Úsalo DESPUÉS de requireAuth:
//   router.delete('/users/:id', requireAuth, requireRole('admin'), controller.delete)
// ─────────────────────────────────────────────
export function requireRole(...roles) {
  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({
        status:  'error',
        message: 'No autenticado',
      });
    }

    if (!roles.includes(req.user.role)) {
      return res.status(403).json({
        status:  'error',
        // 403 Forbidden: sabemos quién eres, pero no puedes hacer esto
        message: `Acceso denegado. Se requiere rol: ${roles.join(' o ')}`,
      });
    }

    next();
  };
}