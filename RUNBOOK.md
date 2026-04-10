# RUNBOOK — Guía de Operaciones PACIOLI

Este documento es para operadores y administradores del sistema. Responde la pregunta "¿cómo hago X?" para operaciones diarias, recuperación de fallas y tareas administrativas.

---

## Tabla de Contenidos

1. [Operaciones Diarias](#operaciones-diarias)
2. [Fallas Comunes](#fallas-comunes)
3. [Tareas Administrativas](#tareas-administrativas)
4. [Consultas de Monitoreo](#consultas-de-monitoreo)
5. [Procedimientos de Emergencia](#procedimientos-de-emergencia)

---

## Operaciones Diarias

### Iniciar todos los componentes en el orden correcto

Iniciar en este orden. Cada componente depende de que el anterior esté listo.

**1. Verificar que PostgreSQL está corriendo**
```bash
psql -U postgres -c "SELECT version();"
```
Si este comando falla, iniciar PostgreSQL antes de continuar.

**2. Iniciar el Pipeline Server**
```bash
cd data-pipeline
PIPELINE_API_KEY=<secreto> python pipeline_server.py
```
En Windows:
```
cd data-pipeline
set PIPELINE_API_KEY=<secreto>
python pipeline_server.py
```
Confirmar que está activo:
```bash
curl http://localhost:8000/health
```
Respuesta esperada: `{"status":"ok","service":"pacioli-pipeline-server",...}`

**3. Iniciar la API de Node.js**
```bash
cd apps/api
npm start          # producción
# o
npm run dev        # desarrollo (nodemon, reinicio automático)
```
La API corre en el puerto 3000. Confirmar:
```bash
curl http://localhost:3000/api/health
```

**4. Iniciar el frontend React** (si no se sirve estáticamente)
```bash
cd apps/web
npm run dev        # desarrollo, puerto 5173
# o servir el build de producción desde tu servidor web
```

---

### Disparar una ejecución del pipeline

**Desde la interfaz:**
1. Iniciar sesión con un usuario de rol `admin` o `senior_analyst`.
2. Navegar a **Data Ingestion**.
3. Subir los archivos fuente (SAP FBL5N, estado de cuenta bancario, archivos de tarjetas, etc.).
4. Hacer clic en **Run Pipeline**.
5. Los logs en tiempo real aparecen en la página mientras el pipeline se ejecuta (~44 segundos).

**Manualmente (sin subir archivos):**
```bash
cd data-pipeline
python main_silver_orchestrator.py
```
Esto asume que los archivos fuente ya están en `data-pipeline/data_raw/`.

**Vía HTTP (sin pasar por la interfaz):**
```bash
curl -X POST http://localhost:8000/run \
  -H "X-Pipeline-Key: <secreto>"
```

---

### Verificar el estado del pipeline

**Desde la interfaz:** La página de Data Ingestion muestra los logs en tiempo real y un indicador de estado.

**Vía HTTP:**
```bash
curl http://localhost:8000/status
```
Campos clave en la respuesta:
- `status`: `idle` | `running` | `completed` | `failed`
- `exit_code`: `0` = éxito, distinto de cero = falla
- `started_at` / `finished_at`: marcas de tiempo ISO
- `log`: últimas 1000 líneas del output del pipeline

**Desde la base de datos:**
```sql
SELECT process_name, status, started_at, finished_at,
       records_processed, notes
FROM biq_config.etl_process_windows
ORDER BY started_at DESC
LIMIT 20;
```

---

### Verificar una ejecución exitosa del pipeline

Una ejecución exitosa cumple todas las siguientes condiciones:

**1. El Pipeline Server reporta `completed` con exit code 0:**
```bash
curl http://localhost:8000/status | python -m json.tool
```

**2. Ningún proceso falló en biq_config:**
```sql
SELECT process_name, status, notes, started_at
FROM biq_config.etl_process_windows
WHERE status = 'FAILED'
ORDER BY started_at DESC;
```
Resultado esperado: 0 filas.

**3. Las transacciones bancarias fueron cargadas y la conciliación se ejecutó:**
```sql
SELECT reconcile_status, COUNT(*), SUM(amount_total)
FROM biq_stg.stg_bank_transactions
WHERE is_compensated_sap = FALSE
  AND is_compensated_intraday = FALSE
GROUP BY reconcile_status
ORDER BY reconcile_status;
```
Una ejecución exitosa mostrará filas con estados `MATCHED`, `REVIEW` y/o `PENDING`. Si todo está en `PENDING`, la conciliación no se ejecutó.

**4. Los workitems auto-conciliados fueron sincronizados:**
```sql
SELECT work_status, COUNT(*)
FROM biq_auth.transaction_workitems
GROUP BY work_status;
```
Las filas con `APPROVED` indican transacciones auto-conciliadas listas para exportar al Gold Layer.

---

## Fallas Comunes

### El Pipeline Server no responde

**Síntoma:** `curl http://localhost:8000/health` se agota o devuelve un error de conexión. La interfaz muestra "Pipeline server unavailable" o el botón Run Pipeline falla silenciosamente.

**Recuperación:**
1. Verificar si el proceso está corriendo:
   ```bash
   # Windows
   netstat -ano | findstr :8000
   # Linux/macOS
   lsof -i :8000
   ```
2. Si ningún proceso está en el puerto 8000, reiniciar el servidor:
   ```bash
   cd data-pipeline
   PIPELINE_API_KEY=<secreto> python pipeline_server.py
   ```
3. Si el puerto está ocupado por otro proceso, terminarlo primero:
   ```bash
   # Windows — obtener el PID con netstat, luego:
   taskkill /PID <pid> /F
   # Linux
   kill -9 <pid>
   ```
4. Confirmar la recuperación: `curl http://localhost:8000/health`

---

### La ejecución del pipeline falló a mitad de camino

**Síntoma:** `GET /status` devuelve `"status": "failed"` con un `exit_code` distinto de cero. La interfaz muestra un indicador de error rojo. Los logs contienen entradas `ERROR`.

**Recuperación:**
1. Leer el log de la falla:
   ```bash
   curl http://localhost:8000/status | python -m json.tool
   ```
   Buscar líneas `[ERR]` al final del arreglo `log`.

2. Verificar qué proceso falló:
   ```sql
   SELECT process_name, status, notes, started_at
   FROM biq_config.etl_process_windows
   WHERE status = 'FAILED'
     AND started_at > NOW() - INTERVAL '24 hours'
   ORDER BY started_at DESC;
   ```

3. Causas comunes:
   - **Archivo fuente faltante** — un loader no encontró su archivo en `data_raw/`. Subir el archivo faltante desde Data Ingestion y volver a ejecutar.
   - **Conexión a la base de datos perdida** — verificar que PostgreSQL sigue corriendo. Reiniciar si es necesario y luego volver a ejecutar.
   - **Incompatibilidad de esquema en el archivo fuente** — una columna fue renombrada o el formato cambió. Revisar el YAML del loader en `data-pipeline/config/schemas/` contra el archivo real.

4. Volver a ejecutar una vez resuelto el problema raíz:
   ```bash
   curl -X POST http://localhost:8000/run -H "X-Pipeline-Key: <secreto>"
   ```
   El pipeline es idempotente — volver a ejecutarlo sobreescribe los datos de staging del período actual.

---

### Una transacción está atascada en IN_PROGRESS (lock no liberado)

**Síntoma:** Un analista reporta que no puede abrir una transacción. La transacción aparece como "bloqueada por [analista]" aunque ese analista no está trabajando activamente. El `work_status` del workitem es `IN_PROGRESS`.

**Identificar el lock atascado:**
```sql
SELECT tl.bank_ref_1, tl.locked_by_name, tl.expires_at,
       tw.work_status
FROM biq_auth.transaction_locks tl
JOIN biq_auth.transaction_workitems tw
  ON tw.bank_ref_1 = tl.bank_ref_1
WHERE tl.expires_at > NOW()
ORDER BY tl.expires_at ASC;
```

**Si el lock ya expiró** (`expires_at < NOW()`), será reclamado automáticamente la próxima vez que cualquier analista abra una transacción. No se requiere ninguna acción.

**Si el lock sigue vigente** pero se confirma que el analista no está trabajando (por ejemplo, cerró el navegador sin navegar fuera), liberarlo a la fuerza:
```sql
-- Paso 1: Eliminar el lock
DELETE FROM biq_auth.transaction_locks
WHERE bank_ref_1 = '<valor_bank_ref_1>';

-- Paso 2: Resetear el workitem a ASSIGNED
UPDATE biq_auth.transaction_workitems
SET work_status = 'ASSIGNED', updated_at = NOW()
WHERE bank_ref_1 = '<valor_bank_ref_1>'
  AND work_status = 'IN_PROGRESS';
```

Confirmar:
```sql
SELECT work_status FROM biq_auth.transaction_workitems
WHERE bank_ref_1 = '<valor_bank_ref_1>';
```
Resultado esperado: `ASSIGNED`.

---

### La exportación al Gold Layer falla

**Síntoma:** Un analista senior hace clic en Submit for Posting y recibe un error, o la exportación se completa pero los registros no aparecen en el Gold Layer.

**Verificar que los registros son elegibles para exportar:**
```sql
SELECT t.stg_id, t.bank_ref_1, t.amount_total, w.work_status,
       t.doc_type, t.is_compensated_sap, t.is_compensated_intraday
FROM biq_stg.stg_bank_transactions t
JOIN biq_auth.transaction_workitems w
  ON w.bank_ref_1 = COALESCE(NULLIF(TRIM(t.bank_ref_1),''), t.sap_description)
WHERE w.work_status = 'APPROVED'
  AND t.doc_type IN ('ZR','SA')
  AND t.is_compensated_sap = FALSE
  AND t.is_compensated_intraday = FALSE
  AND NOT EXISTS (
    SELECT 1 FROM biq_gold.payment_header gh
    WHERE gh.bank_ref_1 = COALESCE(NULLIF(TRIM(t.bank_ref_1),''), t.sap_description)
      AND gh.rpa_status != 'FAILED'
  );
```
Si esto devuelve 0 filas, los registros ya fueron exportados o son inelegibles (tipo de documento incorrecto, compensado, etc.).

**Verificar hash de idempotencia duplicado (registro omitido):**
```sql
SELECT bank_ref_1, rpa_status, exported_at
FROM biq_gold.payment_header
WHERE bank_ref_1 = '<valor_bank_ref_1>'
ORDER BY exported_at DESC;
```
Si existe una fila con `rpa_status != 'FAILED'`, la transacción ya fue exportada y está siendo omitida (protección de idempotencia). Este es el comportamiento correcto — no es una falla.

**Revisar el log de la API de Node.js** para la solicitud POST `/gold-export/submit`. Un error 500 con una violación de restricción de base de datos indica un problema de integridad de datos que requiere investigación.

---

### Un analista no puede iniciar sesión

**Síntoma:** Un analista reporta que sus credenciales son rechazadas. La pantalla de login muestra "Invalid credentials."

**Verificar que la cuenta existe y está activa:**
```sql
SELECT id, username, full_name, role, is_active, last_login_at
FROM biq_auth.users
WHERE username = '<username>';
```

**Si `is_active = false`:** Reactivar la cuenta:
```sql
UPDATE biq_auth.users
SET is_active = true, updated_at = NOW()
WHERE username = '<username>';
```

**Si la cuenta no existe:** Ver [Agregar un nuevo usuario](#agregar-un-nuevo-usuario) más abajo.

**Si la cuenta está activa pero la contraseña es incorrecta:** Restablecer la contraseña generando un nuevo hash bcrypt (factor de costo 12) y actualizando:
```sql
UPDATE biq_auth.users
SET password_hash = '<nuevo_hash_bcrypt>', updated_at = NOW()
WHERE username = '<username>';
```
Para generar un hash bcrypt desde la línea de comandos:
```bash
node -e "const bcrypt = require('bcrypt'); bcrypt.hash('nuevaContrasena', 12).then(h => console.log(h));"
```
Ejecutar esto desde `apps/api/` donde `bcrypt` está instalado.

**Si la cuenta y contraseña son correctas pero el login sigue fallando:** Verificar que la API de Node.js está corriendo y conectada a la base de datos:
```bash
curl http://localhost:3000/api/health
```

---

### El robot RPA no puede leer los registros del Gold Layer

**Síntoma:** El robot RPA reporta que no hay registros para procesar, o no puede conectarse a la base de datos.

**Verificar que hay registros en el Gold Layer con estado PENDING_RPA:**
```sql
SELECT COUNT(*), MIN(exported_at), MAX(exported_at)
FROM biq_gold.payment_header
WHERE rpa_status = 'PENDING_RPA';
```
Si devuelve 0, no se han exportado registros aún, o todos ya fueron procesados. Ejecutar una exportación Gold desde la página Submit for Posting.

**Verificar que el usuario de base de datos del RPA tiene acceso SELECT:**
```sql
SELECT grantee, privilege_type
FROM information_schema.role_table_grants
WHERE table_schema = 'biq_gold'
  AND table_name = 'payment_header';
```
La cuenta de servicio del RPA debe aparecer con el privilegio `SELECT`.

**Verificar registros atascados en PENDING_RPA por más tiempo del esperado:**
```sql
SELECT bank_ref_1, amount, customer_code, exported_at,
       NOW() - exported_at AS antiguedad
FROM biq_gold.payment_header
WHERE rpa_status = 'PENDING_RPA'
ORDER BY exported_at ASC;
```
Registros con más de un día hábil sin actualización de estado indican que el robot RPA no está procesando. Escalar al equipo de RPA.

---

## Tareas Administrativas

### Agregar un nuevo usuario

**Paso 1.** Generar un hash bcrypt de contraseña (costo 12):
```bash
cd apps/api
node -e "const bcrypt = require('bcrypt'); bcrypt.hash('contrasenaInicial123', 12).then(h => console.log(h));"
```

**Paso 2.** Insertar el usuario:
```sql
INSERT INTO biq_auth.users (username, email, password_hash, full_name, role, is_active)
VALUES (
  'nuevousuario',
  'nuevo.usuario@quiport.com',
  '<hash_bcrypt_del_paso_1>',
  'Nombre Completo',
  'analyst',   -- roles: admin | senior_analyst | analyst | viewer
  true
);
```

**Paso 3.** Asignar el usuario a las reglas de asignación correspondientes:
```sql
UPDATE biq_auth.assignment_rules
SET assign_to_user_id = (SELECT id FROM biq_auth.users WHERE username = 'nuevousuario')
WHERE trans_type = 'TRANSFERENCIA SPI';  -- ajustar según corresponda
```

**Paso 4.** Verificar:
```sql
SELECT id, username, full_name, role, is_active
FROM biq_auth.users
WHERE username = 'nuevousuario';
```

Comunicar la contraseña inicial al usuario y pedirle que la cambie en el primer inicio de sesión.

---

### Cambiar las reglas de asignación de un analista

Las reglas de asignación determinan qué analista recibe las nuevas transacciones de un tipo dado después de cada ejecución del pipeline.

**Ver las reglas actuales:**
```sql
SELECT r.id, r.rule_name, r.trans_type, r.brand, r.enrich_customer_id,
       r.priority, u.username AS asignado_a
FROM biq_auth.assignment_rules r
LEFT JOIN biq_auth.users u ON u.id = r.assign_to_user_id
ORDER BY r.priority DESC, r.id;
```

**Reasignar una regla a un analista diferente:**
```sql
UPDATE biq_auth.assignment_rules
SET assign_to_user_id = (SELECT id FROM biq_auth.users WHERE username = '<nuevo_analista>')
WHERE rule_name = '<nombre_de_regla>';
```

**Reasignar todas las reglas de un analista a otro** (por ejemplo, analista de vacaciones):
```sql
UPDATE biq_auth.assignment_rules
SET assign_to_user_id = (SELECT id FROM biq_auth.users WHERE username = '<reemplazo>')
WHERE assign_to_user_id = (SELECT id FROM biq_auth.users WHERE username = '<analista_ausente>');
```

Las reglas de asignación toman efecto en la **próxima ejecución del pipeline** — los workitems ya asignados no se mueven automáticamente. Usar la función de Reasignación en la página Overview para mover transacciones individuales de inmediato.

---

### Abrir un nuevo período contable

El pipeline usa la configuración de ventana ETL en `biq_config` para determinar qué período contable está activo.

**Verificar la ventana activa actual:**
```sql
SELECT window_name, period_start, period_end, is_active, created_at
FROM biq_config.etl_process_windows
WHERE is_active = TRUE
ORDER BY period_start DESC;
```

**Cerrar el período actual y abrir uno nuevo:**
```sql
-- Paso 1: Desactivar el período actual
UPDATE biq_config.etl_process_windows
SET is_active = FALSE, updated_at = NOW()
WHERE is_active = TRUE;

-- Paso 2: Insertar el nuevo período
INSERT INTO biq_config.etl_process_windows
  (window_name, period_start, period_end, is_active)
VALUES
  ('2026-04', '2026-04-01', '2026-04-30', TRUE);
```

Ajustar `window_name`, `period_start` y `period_end` al período contable real que se está abriendo.

**Verificar:**
```sql
SELECT window_name, period_start, period_end, is_active
FROM biq_config.etl_process_windows
ORDER BY period_start DESC
LIMIT 3;
```

Después de abrir el nuevo período, disparar una ejecución del pipeline para cargar los datos de la nueva ventana.

---

### Ejecutar una reversión

Las reversiones deshacen una conciliación aprobada. Solo puede ejecutarlas el analista que originalmente aprobó la transacción, o un administrador.

**Desde la interfaz (recomendado):**
1. Un analista navega al **Workspace** y solicita una reversión sobre una transacción aprobada.
2. Un administrador o analista senior ve la solicitud en el panel de **Notificaciones** (campana).
3. El administrador revisa y hace clic en **Aprobar Reversión**.
4. El sistema restaura atómicamente la transacción bancaria a `PENDING`, los ítems de cartera a `PENDING`, el workitem a `REVERSED`, y cancela la entrada Gold si el robot RPA aún no la ha procesado.

**Condiciones previas a verificar antes de aprobar una reversión:**
```sql
-- Verificar el estado del workitem
SELECT work_status, approved_by, approved_at, bank_ref_1
FROM biq_auth.transaction_workitems
WHERE stg_id = <stg_id>;

-- Verificar si el Gold ya fue procesado por el RPA (no se puede revertir si está POSTED)
SELECT bank_ref_1, rpa_status, exported_at
FROM biq_gold.payment_header
WHERE bank_ref_1 = '<valor_bank_ref_1>';
```
- Si `rpa_status = 'POSTED'`: la reversión puede ejecutarse en el sistema, pero la contabilización SAP ya fue realizada. Coordinar con el equipo SAP para una reversión manual en SAP primero.
- Si `rpa_status = 'PENDING_RPA'`: la reversión cancelará el registro Gold antes de que el RPA lo procese. Seguro para continuar.

---

## Consultas de Monitoreo

### Estado del pipeline

**Últimas 10 ejecuciones de procesos del pipeline:**
```sql
SELECT process_name, status, started_at, finished_at,
       EXTRACT(EPOCH FROM (finished_at - started_at))::int AS duracion_segundos,
       records_processed, notes
FROM biq_config.etl_process_windows
ORDER BY started_at DESC
LIMIT 10;
```

**Procesos que fallaron en las últimas 24 horas:**
```sql
SELECT process_name, notes, started_at
FROM biq_config.etl_process_windows
WHERE status = 'FAILED'
  AND started_at > NOW() - INTERVAL '24 hours'
ORDER BY started_at DESC;
```

---

### Transacciones no conciliadas

**Conteo y total por estado de conciliación (período actual, excluyendo compensadas):**
```sql
SELECT reconcile_status,
       COUNT(*)               AS cantidad,
       SUM(amount_total)      AS monto_total
FROM biq_stg.stg_bank_transactions
WHERE is_compensated_sap      = FALSE
  AND is_compensated_intraday = FALSE
GROUP BY reconcile_status
ORDER BY reconcile_status;
```

**Transacciones en REVIEW (requieren atención del analista):**
```sql
SELECT stg_id, doc_date, bank_ref_1, amount_total,
       enrich_customer_name, reconcile_reason, enrich_notes
FROM biq_stg.stg_bank_transactions
WHERE reconcile_status = 'REVIEW'
  AND is_compensated_sap      = FALSE
  AND is_compensated_intraday = FALSE
ORDER BY doc_date ASC;
```

**Transacciones PENDING sin cliente identificado:**
```sql
SELECT stg_id, doc_date, bank_ref_1, amount_total,
       trans_type, sap_description
FROM biq_stg.stg_bank_transactions
WHERE reconcile_status = 'PENDING'
  AND enrich_customer_id IS NULL
  AND is_compensated_sap      = FALSE
  AND is_compensated_intraday = FALSE
ORDER BY doc_date ASC;
```

**Locks activos (transacciones siendo editadas en este momento):**
```sql
SELECT tl.bank_ref_1, tl.locked_by_name,
       tl.expires_at,
       NOW() > tl.expires_at AS expirado
FROM biq_auth.transaction_locks tl
ORDER BY tl.expires_at ASC;
```

---

### Cola de exportación del Gold Layer

**Cola completa — registros pendientes de procesamiento por el RPA:**
```sql
SELECT bank_ref_1, amount, customer_code, customer_name,
       rpa_status, exported_at,
       NOW() - exported_at AS antiguedad
FROM biq_gold.payment_header
WHERE rpa_status = 'PENDING_RPA'
ORDER BY exported_at ASC;
```

**Resumen de exportaciones por estado RPA:**
```sql
SELECT rpa_status,
       COUNT(*)           AS cantidad,
       SUM(amount)        AS monto_total,
       MIN(exported_at)   AS mas_antiguo
FROM biq_gold.payment_header
GROUP BY rpa_status
ORDER BY rpa_status;
```

**Líneas de detalle para un encabezado Gold específico:**
```sql
SELECT pd.invoice_ref, pd.customer_code, pd.amount_gross,
       pd.financial_amount_net, pd.gl_account, pd.is_partial_payment
FROM biq_gold.payment_detail pd
JOIN biq_gold.payment_header ph ON ph.id = pd.header_id
WHERE ph.bank_ref_1 = '<valor_bank_ref_1>';
```

---

## Procedimientos de Emergencia

### Forzar la liberación de un lock atascado

Usar esto únicamente cuando se confirma que el analista ya no está trabajando en la transacción (navegador cerrado, sesión expirada, analista confirmado inactivo).

```sql
-- Paso 1: Confirmar que el lock existe
SELECT bank_ref_1, locked_by_name, expires_at
FROM biq_auth.transaction_locks
WHERE bank_ref_1 = '<valor_bank_ref_1>';

-- Paso 2: Eliminar el lock
DELETE FROM biq_auth.transaction_locks
WHERE bank_ref_1 = '<valor_bank_ref_1>';

-- Paso 3: Resetear el workitem
UPDATE biq_auth.transaction_workitems
SET work_status = 'ASSIGNED', updated_at = NOW()
WHERE bank_ref_1 = '<valor_bank_ref_1>'
  AND work_status = 'IN_PROGRESS';

-- Paso 4: Verificar
SELECT work_status, updated_at
FROM biq_auth.transaction_workitems
WHERE bank_ref_1 = '<valor_bank_ref_1>';
```

Para liberar todos los locks de un usuario específico a la vez:
```sql
DELETE FROM biq_auth.transaction_locks
WHERE locked_by_id = (SELECT id FROM biq_auth.users WHERE username = '<username>');

UPDATE biq_auth.transaction_workitems
SET work_status = 'ASSIGNED', updated_at = NOW()
WHERE work_status = 'IN_PROGRESS'
  AND bank_ref_1 NOT IN (
    SELECT bank_ref_1 FROM biq_auth.transaction_locks WHERE expires_at > NOW()
  );
```

---

### Marcar manualmente un registro Gold como FAILED

Usar esto cuando un registro Gold debe anularse antes de que el robot RPA lo procese. Si el RPA ya contabilizó el registro (`rpa_status = 'POSTED'`), coordinar con el equipo SAP para una reversión manual en SAP — este SQL por sí solo no es suficiente.

```sql
-- Paso 1: Confirmar el estado actual
SELECT bank_ref_1, rpa_status, amount, exported_at
FROM biq_gold.payment_header
WHERE bank_ref_1 = '<valor_bank_ref_1>';

-- Paso 2: Marcar como FAILED solo si está en PENDING_RPA
UPDATE biq_gold.payment_header
SET rpa_status = 'FAILED',
    rpa_error_message = 'Cancelado manualmente por operador'
WHERE bank_ref_1 = '<valor_bank_ref_1>'
  AND rpa_status = 'PENDING_RPA';

-- Paso 3: Confirmar
SELECT bank_ref_1, rpa_status, rpa_error_message FROM biq_gold.payment_header
WHERE bank_ref_1 = '<valor_bank_ref_1>';
```

Los registros en estado `FAILED` se conservan como pista de auditoría y **no serán reprocesados por el RPA**. El workitem de la transacción permanece en `APPROVED` y la transacción bancaria permanece en `MATCHED_MANUAL`. Si se requiere una nueva exportación, debe coordinarse manualmente con el equipo técnico.

---

### Volver a ejecutar el pipeline después de una falla

El pipeline es idempotente: volver a ejecutarlo para el mismo período sobreescribe los datos de staging y repite los 18 procesos desde el principio.

**Antes de volver a ejecutar, verificar que la causa raíz fue corregida** (archivo faltante subido, conectividad de base de datos restaurada, incompatibilidad de esquema corregida).

**Desde la interfaz:** Navegar a Data Ingestion y hacer clic en Run Pipeline. No se requiere subir archivos si ya están en `data_raw/`.

**Vía HTTP:**
```bash
curl -X POST http://localhost:8000/run \
  -H "X-Pipeline-Key: <secreto>"
```

**Manualmente:**
```bash
cd data-pipeline
python main_silver_orchestrator.py
```

**Si el Pipeline Server colapsó**, reiniciarlo primero:
```bash
cd data-pipeline
PIPELINE_API_KEY=<secreto> python pipeline_server.py
```

Luego disparar la ejecución con cualquiera de los métodos anteriores.

**Monitorear el progreso:**
```bash
# Linux/macOS — verificar el estado cada 10 segundos
watch -n 10 'curl -s http://localhost:8000/status | python -m json.tool | grep -E "status|exit_code"'
# Windows — verificar manualmente:
curl http://localhost:8000/status
```
