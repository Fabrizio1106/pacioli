// src/modules/auth/auth.repository.js
import { pool } from '../../config/database.js';

// ─────────────────────────────────────────────
// Busca un usuario por username para el proceso de login.
// Retorna el registro completo incluyendo password_hash.
// IMPORTANTE: Este método SOLO se usa en el login.
// En cualquier otro contexto, nunca expongas password_hash.
// ─────────────────────────────────────────────
export async function findUserByUsername(username) {
  const query = `
    SELECT 
      id,
      username,
      email,
      password_hash,
      full_name,
      role,
      is_active,
      last_login_at
    FROM biq_auth.users
    WHERE username = $1
      AND is_active = true
  `;
  // $1 es un parámetro posicional — pg lo escapa automáticamente.
  // NUNCA construyas queries con template literals: `WHERE username = '${username}'`
  // Eso abre la puerta a SQL Injection, el ataque más común del mundo.
  const result = await pool.query(query, [username]);
  
  // result.rows es un array. Si no encontró nada, rows[0] es undefined.
  return result.rows[0] || null;
}

// ─────────────────────────────────────────────
// Busca un usuario por ID — usado por el middleware de auth
// para verificar que el usuario del JWT sigue activo en DB.
// No retorna password_hash — no lo necesitamos aquí.
// ─────────────────────────────────────────────
export async function findUserById(id) {
  const query = `
    SELECT 
      id,
      username,
      email,
      full_name,
      role,
      is_active
    FROM biq_auth.users
    WHERE id = $1
      AND is_active = true
  `;
  const result = await pool.query(query, [id]);
  return result.rows[0] || null;
}

// ─────────────────────────────────────────────
// Actualiza el timestamp de último login.
// Se llama DESPUÉS de un login exitoso para auditoría.
// ─────────────────────────────────────────────
export async function updateLastLogin(userId) {
  const query = `
    UPDATE biq_auth.users
    SET last_login_at = NOW()
    WHERE id = $1
  `;
  await pool.query(query, [userId]);
}

// ─────────────────────────────────────────────
// Guarda un refresh token hasheado en la base de datos.
// Nunca guardamos el token original — solo su hash.
// Mismo principio que los passwords.
// ─────────────────────────────────────────────
export async function saveRefreshToken({ userId, tokenHash, expiresAt, ipAddress, userAgent }) {
  const query = `
    INSERT INTO biq_auth.refresh_tokens 
      (user_id, token_hash, expires_at, ip_address, user_agent)
    VALUES 
      ($1, $2, $3, $4, $5)
    RETURNING id
  `;
  const result = await pool.query(query, [userId, tokenHash, expiresAt, ipAddress, userAgent]);
  return result.rows[0].id;
}

// ─────────────────────────────────────────────
// Registra una acción en el audit log.
// Se llama en login exitoso, login fallido, logout.
// ─────────────────────────────────────────────
export async function writeAuditLog({ userId, username, action, resource, detail, ipAddress }) {
  const query = `
    INSERT INTO biq_auth.audit_log
      (user_id, username, action, resource, detail, ip_address)
    VALUES
      ($1, $2, $3, $4, $5, $6)
  `;
  await pool.query(query, [
    userId   || null,
    username || null,
    action,
    resource || null,
    detail   ? JSON.stringify(detail) : null,
    ipAddress || null,
  ]);
}