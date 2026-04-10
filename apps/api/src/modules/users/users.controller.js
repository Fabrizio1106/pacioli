// src/modules/users/users.controller.js
//
// USERS MODULE — Intentional architectural exception.
//
// This module exposes a single shared lookup: the list of active
// analysts and admins used to populate assignment dropdowns across
// the application. It is self-contained in this controller with no
// service or repository layer — a deliberate trade-off for a query
// this simple and this stable. Adding a full stack (controller →
// service → repository) for one parameterless SELECT would be
// over-engineering. If this module grows beyond lookup endpoints,
// the standard layered structure should be introduced at that point.
//
import { pool } from '../../config/database.js';

async function getAnalystsFromDb() {
  const result = await pool.query(`
    SELECT id, username, full_name, role
    FROM biq_auth.users
    WHERE is_active = TRUE
      AND role IN ('admin', 'analyst', 'senior_analyst')
    ORDER BY full_name ASC
  `);
  return result.rows;
}

export async function getAnalysts(req, res) {
  try {
    const rows = await getAnalystsFromDb();
    return res.status(200).json({ status: 'success', data: rows });
  } catch (err) {
    return res.status(500).json({ status: 'error', message: err.message });
  }
}