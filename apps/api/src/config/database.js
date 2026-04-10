// src/config/database.js
// EL POOL DE CONEXIONES A POSTGRESQL
// Este módulo crea y exporta UN SOLO pool que toda la aplicación comparte.
// Ese patrón se llama "Singleton" — una sola instancia compartida.

import pg from 'pg';
import { env } from './env.js';

// Extraemos Pool del módulo pg
// pg también exporta Client (conexión única) — nosotros usamos Pool
const { Pool } = pg;

// Crear el pool con la configuración validada por env.js
// Nota: usamos env.db — si llegamos aquí, sabemos que todos los valores existen
const pool = new Pool({
  host:     env.db.host,
  port:     env.db.port,
  database: env.db.name,
  user:     env.db.user,
  password: env.db.password,
  min:      env.db.poolMin,   // Conexiones mínimas siempre abiertas
  max:      env.db.poolMax,   // Máximo de conexiones simultáneas
  
  // Si una conexión lleva más de 30s idle, se cierra automáticamente
  idleTimeoutMillis: 30000,
  
  // Si no se puede obtener una conexión en 10s, lanza error
  connectionTimeoutMillis: 10000,
});

// Escuchar eventos del pool para logging
// 'connect' se dispara cada vez que se abre una nueva conexión física
pool.on('connect', () => {
  if (!env.isProd) {
    console.log('🔗 Nueva conexión PostgreSQL establecida en el pool');
  }
});

// 'error' se dispara si una conexión del pool falla inesperadamente
pool.on('error', (err) => {
  console.error('Error inesperado en el pool de PostgreSQL:', err.message);
});

// Función para verificar que la conexión funciona al arrancar
// La llamaremos desde app.js antes de levantar el servidor
export async function connectDatabase() {
  try {
    // Tomamos una conexión del pool y ejecutamos una query mínima
    const client = await pool.connect();
    const result = await client.query('SELECT NOW() as time, current_database() as db');
    client.release(); // MUY IMPORTANTE: devolver la conexión al pool
    
    console.log(`PostgreSQL conectado — DB: ${result.rows[0].db} — ${result.rows[0].time}`);
    return true;
  } catch (err) {
    console.error('No se pudo conectar a PostgreSQL:', err.message);
    throw err; // Propagamos el error hacia app.js
  }
}

// Exportamos el pool para que los repositorios puedan ejecutar queries
export { pool };
