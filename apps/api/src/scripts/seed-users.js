// src/scripts/seed-users.js
// Ejecutar UNA SOLA VEZ: node src/scripts/seed-users.js
import bcrypt   from 'bcrypt';
import { pool } from '../config/database.js';
import { env }  from '../config/env.js';

const users = [
  { username: 'admin',     email: 'admin@empresa.com',     fullName: 'Administrador PACIOLI', role: 'admin',    password: 'Admin2026$' },
  { username: 'analista1', email: 'analista1@empresa.com', fullName: 'Analista Tesorería 1',  role: 'analyst',  password: 'Analyst1$2026' },
  { username: 'analista2', email: 'analista2@empresa.com', fullName: 'Analista Tesorería 2',  role: 'analyst',  password: 'Analyst2$2026' },
  { username: 'analista3', email: 'analista3@empresa.com', fullName: 'Analista Tesorería 3',  role: 'analyst',  password: 'Analyst3$2026' },
  { username: 'analista4', email: 'analista4@empresa.com', fullName: 'Analista Tesorería 4',  role: 'analyst',  password: 'Analyst4$2026' },
  { username: 'viewer',    email: 'viewer@empresa.com',    fullName: 'Gerencia / Solo Vista', role: 'viewer',   password: 'Viewer$2026' },
];

async function seed() {
  console.log('Iniciando seed de usuarios PACIOLI...\n');

  for (const user of users) {
    const hash = await bcrypt.hash(user.password, env.bcryptRounds);

    await pool.query(`
      INSERT INTO biq_auth.users (username, email, password_hash, full_name, role)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (username) DO NOTHING
    `, [user.username, user.email, hash, user.fullName, user.role]);

    console.log(`✅ Usuario creado: ${user.username} (${user.role})`);
  }

  console.log('\n🏁 Seed completado. Cambia los passwords en producción.');
  await pool.end();
}

seed().catch(err => {
  console.error('Error en seed:', err.message);
  process.exit(1);
});