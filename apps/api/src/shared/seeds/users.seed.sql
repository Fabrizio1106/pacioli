-- ============================================================
-- PACIOLI / BalanceIQ — Seed usuarios y assignment rules
-- Idempotente: puede ejecutarse múltiples veces sin duplicar
-- ============================================================

-- ─── 1. USUARIOS ────────────────────────────────────────────

INSERT INTO biq_auth.users (username, email, password_hash, full_name, role, is_active)
VALUES
  (
    'admin',
    'admin@example.com',
    '$2b$12$cLntJPCv8x26dt2jgQmO0eJquV3ZBN5DzDHC0nCd4dMKUtOzhbeSm',
    'Admin User',
    'admin',
    true
  ),
  (
    'fpilatasig',
    'analyst1@example.com',
    '$2b$12$KPyrYOwXjEFKKvf8vqSPb.YHPpfBn5ruBuooiWnKGBRzW7uoS3nii',
    'Analyst One',
    'admin',
    true
  ),
  (
    'snavas',
    'analyst2@example.com',
    '$2b$12$pQgpuIxZGaSCKdVrCgw3Ceuc74cm/TVhksDbPEw8WU0aEQu08YeDK',
    'Analyst Two',
    'senior_analyst',
    true
  ),
  (
    'rpillajo',
    'analyst3@example.com',
    '$2b$12$vU.RguuKad.DXGDsn/MQGOgXs8PGSucUlDDGQjj4ygQsMaE8nJ3sW',
    'Analyst Three',
    'analyst',
    true
  ),
  (
    'wcolcha',
    'analyst4@example.com',
    '$2b$12$HJzNEic3As2GInU9nHxZ8uPT0MliIEfWx3Z3cJmDuOxvZbr4C5UWW',
    'Analyst Four',
    'analyst',
    true
  ),
  (
    'aleon',
    'analyst5@example.com',
    '$2b$12$ExlvQ32glKiwGkulYF6YDuihNIS.qPM5EAZyzdGpsz/Yal3k2XZ/i',
    'Analyst Five',
    'analyst',
    true
  ),
  (
    'jquezada',
    'viewer1@example.com',
    '$2b$12$m18LhqgAcQoDYM61Cs3PReze37pKgAgkroJqdlWkqHZszMC0ZKPVW',
    'Viewer One',
    'viewer',
    true
  )
ON CONFLICT (username) DO UPDATE SET
  email         = EXCLUDED.email,
  full_name     = EXCLUDED.full_name,
  role          = EXCLUDED.role,
  is_active     = EXCLUDED.is_active,
  password_hash = EXCLUDED.password_hash;

-- ─── 2. ASSIGNMENT RULES ────────────────────────────────────

-- TC Visa, Diners Club, Amex → analyst1
UPDATE biq_auth.assignment_rules
SET assign_to_user_id = (SELECT id FROM biq_auth.users WHERE username = 'fpilatasig')
WHERE trans_type = 'LIQUIDACION TC'
  AND brand IN ('VISA', 'DINERS CLUB', 'AMEX');

-- TC Pacificard → analyst5
UPDATE biq_auth.assignment_rules
SET assign_to_user_id = (SELECT id FROM biq_auth.users WHERE username = 'aleon')
WHERE trans_type = 'LIQUIDACION TC'
  AND brand = 'PACIFICARD';

-- DEPOSITO EFECTIVO: Urbaparking (400419) y Pending (999998) → analyst1
UPDATE biq_auth.assignment_rules
SET assign_to_user_id = (SELECT id FROM biq_auth.users WHERE username = 'fpilatasig')
WHERE trans_type = 'DEPOSITO EFECTIVO'
  AND enrich_customer_id IN ('400419', '999998');

-- DEPOSITO EFECTIVO: Salas VIP (999999) → analyst5
UPDATE biq_auth.assignment_rules
SET assign_to_user_id = (SELECT id FROM biq_auth.users WHERE username = 'aleon')
WHERE trans_type = 'DEPOSITO EFECTIVO'
  AND enrich_customer_id = '999999';

-- Transferencias y otros → analyst3
UPDATE biq_auth.assignment_rules
SET assign_to_user_id = (SELECT id FROM biq_auth.users WHERE username = 'rpillajo')
WHERE trans_type IN (
  'TRANSFERENCIA SPI',
  'TRANSFERENCIA DIRECTA',
  'TRANSFERENCIA EXTERIOR',
  'OTROS',
  'DEPOSITO CHEQUE'
);

-- DEFAULT → analyst3
UPDATE biq_auth.assignment_rules
SET assign_to_user_id = (SELECT id FROM biq_auth.users WHERE username = 'rpillajo')
WHERE is_default = TRUE;

-- ─── 3. VERIFICACIÓN ────────────────────────────────────────

SELECT id, username, email, full_name, role, is_active
FROM biq_auth.users
ORDER BY role, username;

SELECT r.id, r.rule_name, r.trans_type, r.brand, r.enrich_customer_id,
       u.username AS asignado_a
FROM biq_auth.assignment_rules r
LEFT JOIN biq_auth.users u ON u.id = r.assign_to_user_id
ORDER BY r.priority DESC, r.id;