# ══════════════════════════════════════════════════════════════════════════════
# SCRIPT DE CORRECCIÓN: Limpiar y Reconstruir Hash Cache
# UBICACIÓN: scripts/fix_hash_cache.py
# PROPÓSITO: Reconstruir cache con filtro clase_documento correcto
# ══════════════════════════════════════════════════════════════════════════════

"""
PROBLEMA DETECTADO:
══════════════════════════════════════════════════════════════════════════════

El hash_counter_cache estaba contando transacciones ZR + DZ:
- ZR = Transacción original del banco (CORRECTA)
- DZ = Compensación SAP (FALSA DUPLICADA)

Ejemplo:
   AMEX 1041.63 → last_counter = 2 ❌ (contó ZR + DZ)
   Debería ser:   last_counter = 1 ✅ (solo ZR)


SOLUCIÓN:
══════════════════════════════════════════════════════════════════════════════

1. Truncar tabla hash_counter_cache (limpiar datos corruptos)
2. Reconstruir con filtro clase_documento = 'ZR'
3. Verificar integridad


EJECUCIÓN:
══════════════════════════════════════════════════════════════════════════════

python scripts/fix_hash_cache.py


IMPACTO:
══════════════════════════════════════════════════════════════════════════════

ANTES:
   AMEX 1041.63 → last_counter: 2 → próximo hash: "_3" ❌

DESPUÉS:
   AMEX 1041.63 → last_counter: 1 → próximo hash: "_2" ✅
"""

from sqlalchemy import create_engine, text
from logic.domain.services.hash_counter_cache_manager import HashCounterCacheManager
from utils.db_config import get_db_engine
from utils.logger import get_logger


def main():
    logger = get_logger("FIX_HASH_CACHE")
    
    logger("═" * 80, "INFO")
    logger("🔧 CORRECCIÓN DE HASH CACHE", "INFO")
    logger("═" * 80, "INFO")
    
    # Conectar a base de datos
    engine_stg = get_db_engine('stg')
    
    with engine_stg.connect() as conn:
        session = conn
        
        # Paso 1: Mostrar estado ANTES
        logger("\n📊 ESTADO ANTES DE CORRECCIÓN:", "INFO")
        
        query_before = text("""
            SELECT 
                brand,
                amount_total,
                last_counter,
                total_occurrences
            FROM hash_counter_cache
            WHERE (brand = 'AMEX' AND amount_total = 1041.63)
               OR (brand = 'AMEX' AND amount_total = 68.67)
               OR (brand = 'AMEX' AND amount_total = 201.29)
            ORDER BY amount_total
        """)
        
        results_before = session.execute(query_before).fetchall()
        
        logger("─" * 80, "INFO")
        logger("Ejemplos ANTES:", "INFO")
        for row in results_before:
            logger(
                f"   {row.brand} {row.amount_total}: "
                f"counter={row.last_counter}, occurrences={row.total_occurrences}",
                "INFO"
            )
        
        # Paso 2: Verificar datos en RAW
        logger("\n🔍 VERIFICANDO DATOS EN v2_stg_bank_transactions:", "INFO")
        
        query_verify = text("""
            SELECT 
                brand,
                amount_total,
                clase_documento,
                COUNT(*) as count
            FROM v2_stg_bank_transactions
            WHERE brand = 'AMEX' 
              AND ABS(amount_total) = 1041.63
            GROUP BY brand, amount_total, clase_documento
            ORDER BY clase_documento
        """)
        
        verify_results = session.execute(query_verify).fetchall()
        
        logger("─" * 80, "INFO")
        logger("Datos en v2_stg_bank_transactions:", "INFO")
        for row in verify_results:
            logger(
                f"   {row.brand} {row.amount_total} {row.clase_documento}: "
                f"{row.count} filas",
                "INFO"
            )
        
        # Paso 3: Confirmar reconstrucción
        logger("\n⚠️ ¿Reconstruir cache? (s/n): ", "WARN")
        confirm = input().strip().lower()
        
        if confirm != 's':
            logger("❌ Cancelado por usuario", "WARN")
            return
        
        # Paso 4: Reconstruir cache
        logger("\n🔄 Reconstruyendo cache...", "INFO")
        
        cache_manager = HashCounterCacheManager(session)
        stats = cache_manager.rebuild_cache_from_scratch()
        
        session.commit()
        
        logger("\n✅ Cache reconstruido exitosamente", "SUCCESS")
        logger(f"   Total grupos: {stats['total_groups']}", "SUCCESS")
        logger(f"   Total transacciones: {stats['total_transactions']}", "SUCCESS")
        
        # Paso 5: Mostrar estado DESPUÉS
        logger("\n📊 ESTADO DESPUÉS DE CORRECCIÓN:", "INFO")
        
        results_after = session.execute(query_before).fetchall()
        
        logger("─" * 80, "INFO")
        logger("Ejemplos DESPUÉS:", "INFO")
        for row in results_after:
            logger(
                f"   {row.brand} {row.amount_total}: "
                f"counter={row.last_counter}, occurrences={row.total_occurrences}",
                "SUCCESS"
            )
        
        # Paso 6: Validar integridad
        logger("\n🔍 Validando integridad...", "INFO")
        
        validation = cache_manager.validate_cache_integrity()
        
        if validation['is_valid']:
            logger("✅ Cache válido", "SUCCESS")
        else:
            logger(f"⚠️ Problemas: {validation['issues']}", "WARN")
        
        logger("\n═" * 80, "SUCCESS")
        logger("🏁 CORRECCIÓN COMPLETADA", "SUCCESS")
        logger("═" * 80, "SUCCESS")


if __name__ == "__main__":
    main()


# ══════════════════════════════════════════════════════════════════════════════
# QUERIES DE VERIFICACIÓN MANUAL
# ══════════════════════════════════════════════════════════════════════════════

"""
VERIFICACIÓN 1: Ver contadores actuales
───────────────────────────────────────────────────────────────────────────────

SELECT 
    brand,
    amount_total,
    last_counter,
    total_occurrences
FROM hash_counter_cache
WHERE brand = 'AMEX'
  AND amount_total IN (1041.63, 68.67, 201.29, 205.98, 250.86, 492.11)
ORDER BY amount_total;


VERIFICACIÓN 2: Comparar con v2_stg_bank_transactions
───────────────────────────────────────────────────────────────────────────────

SELECT 
    brand,
    ABS(amount_total) as amount,
    clase_documento,
    COUNT(*) as count,
    MAX(
        CAST(
            SUBSTRING_INDEX(match_hash_key, '_', -1) 
            AS UNSIGNED
        )
    ) as max_counter
FROM v2_stg_bank_transactions
WHERE brand = 'AMEX'
  AND ABS(amount_total) = 1041.63
GROUP BY brand, ABS(amount_total), clase_documento
ORDER BY clase_documento;

-- Esperado:
-- ZR → count: 1, max_counter: 1
-- DZ → count: 1, max_counter: 1
-- Cache debe mostrar: last_counter: 1 (solo cuenta ZR)


VERIFICACIÓN 3: Ver todas las transacciones AMEX 1041.63
───────────────────────────────────────────────────────────────────────────────

SELECT 
    doc_date,
    clase_documento,
    amount_total,
    match_hash_key,
    doc_number,
    bank_ref_1
FROM v2_stg_bank_transactions
WHERE brand = 'AMEX'
  AND ABS(amount_total) = 1041.63
ORDER BY doc_date, clase_documento;


LIMPIEZA MANUAL (si es necesario):
───────────────────────────────────────────────────────────────────────────────

TRUNCATE TABLE hash_counter_cache;

INSERT INTO hash_counter_cache (
    brand, amount_total, last_counter, last_updated_date, total_occurrences
)
SELECT 
    brand,
    amount_total,
    MAX(CAST(SUBSTRING_INDEX(match_hash_key, '_', -1) AS UNSIGNED)) as last_counter,
    MAX(doc_date) as last_updated_date,
    COUNT(*) as total_occurrences
FROM v2_stg_bank_transactions
WHERE match_hash_key REGEXP '_[0-9]+$'
  AND brand IS NOT NULL
  AND brand != 'NA'
  AND clase_documento = 'ZR'  ← CRÍTICO
GROUP BY brand, amount_total;
"""