// src/app.js
import { env } from './config/env.js';
import { connectDatabase } from './config/database.js';

import express   from 'express';
import cors      from 'cors';
import morgan    from 'morgan';
import helmet    from 'helmet';
import { rateLimit } from 'express-rate-limit';
import authRoutes from './modules/auth/auth.routes.js';
import transactionRoutes from './modules/transactions/transactions.routes.js';
import portfolioRoutes from './modules/portfolio/portfolio.routes.js';
import assignmentRoutes from './modules/assignments/assignments.routes.js';
import lockRoutes from './modules/locks/locks.routes.js';
import reconciliationRoutes from './modules/reconciliation/reconciliation.routes.js';
import reversalRoutes from './modules/reversals/reversals.routes.js';
import goldExportRoutes from './modules/gold-export/gold-export.routes.js';
import overviewRoutes from './modules/overview/overview.routes.js';
import usersRoutes    from './modules/users/users.routes.js';
import workspaceRoutes from './modules/workspace/workspace.routes.js';
import notificationsRoutes from './modules/notifications/notifications.routes.js';
import ingestionRoutes from './modules/ingestion/ingestion.routes.js';
import reportsRoutes from './modules/reports/reports.routes.js';


const app = express();

// ─────────────────────────────────────────────
// CAPA 1 — SEGURIDAD: helmet
// Agrega ~15 headers HTTP de seguridad automáticamente.
// Va PRIMERO — antes de que cualquier otra cosa procese la petición.
// ─────────────────────────────────────────────
app.use(helmet());

// ─────────────────────────────────────────────
// CAPA 2 — CORS
// ─────────────────────────────────────────────
app.use(cors({
  origin: (origin, callback) => {
    // En desarrollo acepta cualquier localhost
    // En producción solo el dominio real
    if (env.isProd) {
      callback(null, 'https://tu-dominio.com');
    } else if (!origin || origin.startsWith('http://localhost')) {
      callback(null, true);
    } else {
      callback(new Error('Not allowed by CORS'));
    }
  },
  credentials: true,
}));

// ─────────────────────────────────────────────
// CAPA 3 — PARSERS
// ─────────────────────────────────────────────
app.use(express.json());

// ─────────────────────────────────────────────
// CAPA 4 — LOGGING
// ─────────────────────────────────────────────
app.use(morgan(env.isProd ? 'combined' : 'dev'));

// ─────────────────────────────────────────────
// CAPA 5 — RATE LIMITER GLOBAL
// Protección general: máximo 200 peticiones por IP cada 15 minutos.
// El rate limiter específico del login será más estricto (lo definimos
// en auth.routes.js directamente sobre ese endpoint).
//
// ¿Por qué 200 y no 100? Porque el frontend hace múltiples llamadas
// simultáneas al cargar una pantalla (transacciones, KPIs, usuario).
// ─────────────────────────────────────────────
const globalLimiter = rateLimit({
  windowMs:         15 * 60 * 1000, // 15 minutos en milisegundos
  max:              200,
  standardHeaders:  true,  // Incluye RateLimit-* headers en la respuesta
  legacyHeaders:    false,
  message: {
    status:  'error',
    message: 'Demasiadas peticiones. Intenta en 15 minutos.',
  },
});

app.use(globalLimiter);

// ─────────────────────────────────────────────
// RUTAS
// ─────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({
    status:      'ok',
    service:     'pacioli-api',
    environment: env.nodeEnv,
    timestamp:   new Date().toISOString(),
  });
});

// Aquí irán los módulos. Los agregaremos uno a uno:
app.use('/api/v1/auth', authRoutes);       //  ← Sprint 2
app.use('/api/v1/transactions', transactionRoutes); // Sprint 3
app.use('/api/v1/portfolio', portfolioRoutes); // Sprint 4
app.use('/api/v1/admin/assignments', assignmentRoutes); // Sprint 5
app.use('/api/v1/locks', lockRoutes); // Sprint 6
app.use('/api/v1/reconciliation', reconciliationRoutes); // Sprint 8
app.use('/api/v1/reversals', reversalRoutes); // Sprint 9
app.use('/api/v1/gold-export', goldExportRoutes); // Sprint 10
app.use('/api/v1/overview', overviewRoutes);
app.use('/api/v1/users',    usersRoutes);
app.use('/api/v1/workspace', workspaceRoutes);
app.use('/api/v1/notifications', notificationsRoutes);
app.use('/api/v1/ingestion', ingestionRoutes);
app.use('/api/v1/reports', reportsRoutes);

// ─────────────────────────────────────────────
// RUTA NO ENCONTRADA — siempre al final
// ─────────────────────────────────────────────
app.use((req, res) => {
  res.status(404).json({
    status:  'error',
    message: `Ruta ${req.method} ${req.path} no encontrada`,
  });
});

// ─────────────────────────────────────────────
// ARRANQUE
// ─────────────────────────────────────────────
async function startServer() {
  try {
    await connectDatabase();
    app.listen(env.port, () => {
      console.log(`\n🚀 PACIOLI API corriendo`);
      console.log(`   • Entorno:  ${env.nodeEnv}`);
      console.log(`   • Puerto:   http://localhost:${env.port}`);
      console.log(`   • Health:   http://localhost:${env.port}/health\n`);
    });
  } catch (err) {
    console.error('💥 Error fatal al arrancar:', err.message);
    process.exit(1);
  }
}

startServer();