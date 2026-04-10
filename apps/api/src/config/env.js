// src/config/env.js
import { config } from 'dotenv';
import { z } from 'zod';

config();

const envSchema = z.object({

  // Servidor
  PORT:     z.string().default('3000'),
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),

  // Base de Datos PostgreSQL
  DB_HOST:     z.string().min(1, 'DB_HOST es requerido'),
  DB_PORT:     z.string().default('5432'),
  DB_NAME:     z.string().min(1, 'DB_NAME es requerido'),
  DB_USER:     z.string().min(1, 'DB_USER es requerido'),
  DB_PASSWORD: z.string().min(1, 'DB_PASSWORD es requerido'),
  DB_POOL_MIN: z.string().default('2'),
  DB_POOL_MAX: z.string().default('10'),

  // JWT
  JWT_SECRET:     z.string().min(32, 'JWT_SECRET debe tener al menos 32 caracteres'),
  JWT_EXPIRES_IN: z.string().default('8h'),

  // Seguridad — NUEVO
  // coerce.number() convierte el string "12" al número 12 automáticamente
  BCRYPT_ROUNDS: z.coerce.number().min(10).max(14).default(12),

});

const result = envSchema.safeParse(process.env);

if (!result.success) {
  console.error('\n❌ CONFIGURACIÓN INVÁLIDA — PACIOLI no puede arrancar\n');
  result.error.issues.forEach(issue => {
    console.error(`   • ${issue.path.join('.')}: ${issue.message}`);
  });
  console.error('\n💡 Revisa tu archivo apps/api/.env\n');
  process.exit(1);
}

export const env = {
  port:    parseInt(result.data.PORT, 10),
  nodeEnv: result.data.NODE_ENV,
  isProd:  result.data.NODE_ENV === 'production',

  db: {
    host:     result.data.DB_HOST,
    port:     parseInt(result.data.DB_PORT, 10),
    name:     result.data.DB_NAME,
    user:     result.data.DB_USER,
    password: result.data.DB_PASSWORD,
    poolMin:  parseInt(result.data.DB_POOL_MIN, 10),
    poolMax:  parseInt(result.data.DB_POOL_MAX, 10),
  },

  jwt: {
    secret:    result.data.JWT_SECRET,
    expiresIn: result.data.JWT_EXPIRES_IN,
  },

  // NUEVO
  bcryptRounds: result.data.BCRYPT_ROUNDS,
};