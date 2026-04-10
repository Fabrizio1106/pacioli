// src/modules/auth/auth.service.js
import bcrypt     from 'bcrypt';
import jwt        from 'jsonwebtoken';
import crypto     from 'crypto'; // módulo nativo de Node — no necesita npm install
import { env }    from '../../config/env.js';
import * as repo  from './auth.repository.js';

/**
 * Authenticates a user and issues access and refresh tokens.
 *
 * Validates credentials against the database using bcrypt comparison.
 * On success, issues a signed JWT access token (8h) and a cryptographically
 * random refresh token stored as a SHA-256 hash in the database. Both
 * successful and failed attempts are recorded in the audit log. Error
 * messages are intentionally generic to prevent user enumeration attacks.
 *
 * @param {object} params
 * @param {string} params.username   - The user's login username.
 * @param {string} params.password   - Plaintext password to verify against the stored hash.
 * @param {string} params.ipAddress  - Caller IP address for the audit log and refresh token record.
 * @param {string} params.userAgent  - Caller user agent stored alongside the refresh token.
 * @returns {Promise<object>} Authentication result with accessToken, refreshToken,
 *   and user profile ({ id, username, fullName, role, email }).
 * @throws {Error} With a generic "Credenciales inválidas" message if the user
 *   does not exist or the password is incorrect.
 */
export async function login({ username, password, ipAddress, userAgent }) {

  // PASO 1: ¿Existe el usuario?
  const user = await repo.findUserByUsername(username);
  
  if (!user) {
    // Registramos el intento fallido ANTES de lanzar el error
    await repo.writeAuditLog({
      username,
      action:    'LOGIN_FAILED',
      resource:  'auth',
      detail:    { reason: 'user_not_found' },
      ipAddress,
    });
    // Mensaje genérico intencional — no le decimos al atacante
    // si el usuario existe o no. Eso sería información valiosa.
    throw new Error('Credenciales inválidas');
  }

  // PASO 2: ¿El password es correcto?
  // bcrypt.compare tarda ~100ms intencionalmente (ver lección en env.js)
  const passwordValid = await bcrypt.compare(password, user.password_hash);
  
  if (!passwordValid) {
    await repo.writeAuditLog({
      userId:    user.id,
      username:  user.username,
      action:    'LOGIN_FAILED',
      resource:  'auth',
      detail:    { reason: 'wrong_password' },
      ipAddress,
    });
    throw new Error('Credenciales inválidas'); // mismo mensaje genérico
  }

  // PASO 3: Generar Access Token (JWT)
  // Este token viaja en cada petición HTTP en el header Authorization.
  // Contiene: id, username, role — lo suficiente para autorizar sin ir a DB.
  const accessToken = jwt.sign(
    { 
      sub:      user.id,       // 'sub' = subject, estándar JWT
      username: user.username,
      role:     user.role,
    },
    env.jwt.secret,
    { expiresIn: env.jwt.expiresIn } // '8h'
  );

  // PASO 4: Generar Refresh Token
  // Un string aleatorio de 64 bytes — imposible de adivinar.
  // Lo guardamos hasheado en DB, enviamos el original al cliente.
  const refreshToken     = crypto.randomBytes(64).toString('hex');
  const refreshTokenHash = crypto
    .createHash('sha256')
    .update(refreshToken)
    .digest('hex');

  const expiresAt = new Date();
  expiresAt.setDate(expiresAt.getDate() + 7); // expira en 7 días

  await repo.saveRefreshToken({
    userId:    user.id,
    tokenHash: refreshTokenHash,
    expiresAt,
    ipAddress,
    userAgent,
  });

  // PASO 5: Registrar login exitoso en audit log
  await repo.updateLastLogin(user.id);
  await repo.writeAuditLog({
    userId:   user.id,
    username: user.username,
    action:   'LOGIN_SUCCESS',
    resource: 'auth',
    detail:   { role: user.role },
    ipAddress,
  });

  // PASO 6: Retornar lo que el controller necesita
  return {
    accessToken,
    refreshToken,
    user: {
      id:       user.id,
      username: user.username,
      fullName: user.full_name,
      role:     user.role,
      email:    user.email,
    },
  };
}

/**
 * Verifies a JWT access token and returns its decoded payload.
 *
 * Returns null on any failure (expired, malformed, wrong signature) rather
 * than throwing, so the auth middleware can handle the 401 response cleanly
 * without try/catch at the call site.
 *
 * @param {string} token - Raw JWT string from the Authorization header.
 * @returns {{ sub: number, username: string, role: string, iat: number, exp: number } | null}
 *   Decoded payload if the token is valid, or null if invalid or expired.
 */
export function verifyAccessToken(token) {
  try {
    // jwt.verify lanza excepción si el token es inválido o expiró
    return jwt.verify(token, env.jwt.secret);
  } catch (err) {
    return null; // token inválido — el middleware manejará el 401
  }
}
