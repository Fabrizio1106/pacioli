// src/modules/auth/auth.controller.js
import * as authService from './auth.service.js';

// ─────────────────────────────────────────────
// POST /api/v1/auth/login
// ─────────────────────────────────────────────
export async function login(req, res) {
  try {
    const { username, password } = req.body;

    // Validación básica de entrada — antes de llamar al service
    if (!username || !password) {
      return res.status(400).json({
        status:  'error',
        message: 'username y password son requeridos',
      });
    }

    // Extraer metadata de la petición para auditoría
    const ipAddress = req.ip || req.connection.remoteAddress;
    const userAgent = req.headers['user-agent'] || 'unknown';

    // Llamar al service — aquí vive la lógica real
    const result = await authService.login({
      username: username.toLowerCase().trim(),
      password,
      ipAddress,
      userAgent,
    });

    // Login exitoso — responder con tokens y datos del usuario
    return res.status(200).json({
      status: 'success',
      data:   {
        accessToken:  result.accessToken,
        refreshToken: result.refreshToken,
        user:         result.user,
      },
    });

  } catch (err) {
    // 'Credenciales inválidas' viene del service — es un error esperado
    if (err.message === 'Credenciales inválidas') {
      return res.status(401).json({
        status:  'error',
        message: 'Credenciales inválidas',
      });
    }
    // Cualquier otro error es inesperado — log interno, mensaje genérico al cliente
    console.error('[auth.controller] Error en login:', err);
    return res.status(500).json({
      status:  'error',
      message: 'Error interno del servidor',
    });
  }
}

// ─────────────────────────────────────────────
// GET /api/v1/auth/me
// Retorna el perfil del usuario autenticado.
// Este endpoint está protegido — requiere JWT válido.
// El middleware de auth inyecta req.user antes de llegar aquí.
// ─────────────────────────────────────────────
export async function getMe(req, res) {
  // req.user viene del auth.middleware — ya está validado
  return res.status(200).json({
    status: 'success',
    data:   { user: req.user },
  });
}

// POST /api/v1/auth/logout
export async function logout(req, res) {
  try {
    // Release all locks held by this user
    const { releaseAllLocksForUser } = await import('../locks/locks.repository.js');
    const released = await releaseAllLocksForUser(req.user.id);

    await authService.writeAuditLog?.({
      userId:   req.user.id,
      username: req.user.username,
      action:   'LOGOUT',
      detail:   { locks_released: released },
      ipAddress: req.ip,
    });

    return res.status(200).json({
      status: 'success',
      data:   { message: 'Logged out successfully', locks_released: released },
    });
  } catch (err) {
    console.error('[auth.controller] logout:', err);
    return res.status(500).json({ status: 'error', message: err.message });
  }
}