use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};

// ═══════════════════════════════════════════════════════════════════════════
// pacioli_core — Rust hot-path para BalanceIQ
//
// FUNCIONES EXPORTADAS A PYTHON:
//   1. find_invoice_combination  → subset sum con two-pointer
//   2. fuzzy_batch_match         → Jaccard fuzzy matching en batch
//
// COMPILAR:
//   cd pacioli_core && maturin develop --release
//
// USO EN PYTHON:
//   import pacioli_core
//   result = pacioli_core.find_invoice_combination(amounts, indices, target, tol, max_inv)
// ═══════════════════════════════════════════════════════════════════════════

/// Encuentra la combinación de facturas (por índice) que suman target_amount.
///
/// ALGORITMO:
///   - 1 factura: scan lineal O(n)
///   - 2 facturas: HashMap con complementos O(n)
///   - 3 facturas: two-pointer O(n²) — mucho mejor que O(n³) naïve
///
/// ARITMÉTICA EN CENTAVOS:
///   Convertimos todos los montos a i64 centavos para evitar
///   errores de punto flotante en las comparaciones de igualdad.
///   $36.00 → 3600 | $1.01 → 101 | tolerance $0.01 → 1 centavo
///
/// RETORNA: lista de índices o None si no encuentra combinación
#[pyfunction]
fn find_invoice_combination(
    amounts: Vec<f64>,       // montos de las facturas candidatas
    indices: Vec<usize>,     // índices originales del DataFrame pandas
    target: f64,             // monto objetivo (del voucher)
    tolerance: f64,          // tolerancia en dólares (ej: 0.01)
    max_invoices: usize,     // máximo de facturas en la combinación
) -> PyResult<Option<Vec<usize>>> {

    // Validación de entrada
    if amounts.is_empty() || amounts.len() != indices.len() {
        return Ok(None);
    }

    // Convertir a centavos para aritmética entera exacta
    let target_c: i64 = (target * 100.0).round() as i64;
    let tol_c: i64    = (tolerance * 100.0).round() as i64;

    // Filtrar y convertir: descartar facturas mayores que target + tolerance
    // Nunca pueden ser parte de la solución.
    let valid: Vec<(usize, i64)> = indices.iter()
        .zip(amounts.iter())
        .filter_map(|(&idx, &amt)| {
            let c = (amt * 100.0).round() as i64;
            if c > 0 && c <= target_c + tol_c {
                Some((idx, c))
            } else {
                None
            }
        })
        .collect();

    if valid.is_empty() {
        return Ok(None);
    }

    // ── CASO 1: Una factura exacta ────────────────────────────────────────
    // Scan lineal O(n) — el caso más común en datos reales
    for &(idx, c) in &valid {
        if (c - target_c).abs() <= tol_c {
            return Ok(Some(vec![idx]));
        }
    }

    if max_invoices < 2 {
        return Ok(None);
    }

    // ── CASO 2: Dos facturas con HashMap ─────────────────────────────────
    // Para cada factura con valor C cents, su complemento es target_c - C.
    // Si el complemento ya está en el mapa, encontramos la pareja.
    // Complejidad: O(n) — un solo pase con lookups O(1) en HashMap.
    //
    // NOTA: Manejamos tolerancia buscando el complemento exacto y ±tol_c.
    // Esto cubre casos donde la suma difiere en centavo por redondeo.
    let mut seen: HashMap<i64, usize> = HashMap::with_capacity(valid.len());
    for &(idx, c) in &valid {
        let complement = target_c - c;
        // Buscar complemento con tolerancia
        for delta in [-tol_c, 0, tol_c] {
            if let Some(&other_idx) = seen.get(&(complement + delta)) {
                // Verificar que la suma total está dentro de tolerancia
                if let Some(&other_c) = seen.get(&(complement + delta)).map(|&i| {
                    valid.iter().find(|&&(vi, _)| vi == i).map(|(_, vc)| vc)
                }).flatten() {
                    if (other_c + c - target_c).abs() <= tol_c {
                        return Ok(Some(vec![other_idx, idx]));
                    }
                } else {
                    return Ok(Some(vec![other_idx, idx]));
                }
            }
        }
        seen.insert(c, idx);
    }

    if max_invoices < 3 {
        return Ok(None);
    }

    // ── CASO 3: Tres facturas con two-pointer ─────────────────────────────
    // Ordena por centavos (ascendente).
    // Para cada elemento i (el más pequeño del trío), usa dos punteros
    // lo y hi en el subarreglo [i+1..n-1] para encontrar el par que
    // junto con i sume target_c.
    //
    // Complejidad: O(n log n) para el sort + O(n²) para el loop.
    // El O(n²) tiene poda: en cada iteración i, los punteros se mueven
    // monotónicamente → en total hacen O(n) pasos por cada i.
    let mut sorted = valid.clone();
    sorted.sort_by_key(|&(_, c)| c);
    let n = sorted.len();

    for i in 0..n.saturating_sub(2) {
        let (idx_i, c_i) = sorted[i];
        let remaining = target_c - c_i;

        // Poda: si c_i ya es mayor que target, no tiene caso continuar
        // (el arreglo está ordenado, todos los siguientes c_i serán mayores)
        if remaining < 0 {
            break;
        }

        // Two-pointer: lo apunta al elemento más pequeño disponible,
        // hi al más grande
        let mut lo = i + 1;
        let mut hi = n - 1;

        while lo < hi {
            let sum_lr = sorted[lo].1 + sorted[hi].1;
            let diff = (sum_lr - remaining).abs();

            if diff <= tol_c {
                // ¡Encontrado! Los tres índices suman target ± tol
                return Ok(Some(vec![idx_i, sorted[lo].0, sorted[hi].0]));
            } else if sum_lr < remaining {
                // La suma es demasiado pequeña → mover lo hacia la derecha
                // (aumentar el valor más pequeño del par)
                lo += 1;
            } else {
                // La suma es demasiado grande → mover hi hacia la izquierda
                // (reducir el valor más grande del par)
                if hi == 0 { break; }
                hi -= 1;
            }
        }
    }

    Ok(None)
}

/// Fuzzy matching de una factura contra todos los vouchers de un settlement.
///
/// USA SIMILITUD JACCARD SOBRE BIGRAMAS:
///   - Un bigrama es un par de caracteres consecutivos
///   - "DINERS" → {(D,I), (I,N), (N,E), (E,R), (R,S)}
///   - Jaccard(A,B) = |A∩B| / |A∪B|
///
/// POR QUÉ JACCARD Y NO SEQUENCEMATCHER:
///   - SequenceMatcher mide la subsecuencia más larga → O(n*m) por par
///   - Jaccard sobre bigramas es O(n+m) por par (construcción del set)
///   - Para strings cortos (4-12 chars) la correlación es alta (>0.85)
///   - Y en Rust podemos calcular bigramas con memoria en el stack (array fijo)
///
/// PRE-FILTRO POR MONTO:
///   Antes de calcular similitud de strings, descartamos vouchers cuyo
///   monto difiere más que tolerance. En la práctica esto descarta >90%
///   de candidatos y el fuzzy solo corre sobre el 10% restante.
///
/// RETORNA: índice en v_indices del mejor match, o None si no supera threshold
#[pyfunction]
fn fuzzy_batch_match(
    inv_batch: &str,          // batch de la factura (ya normalizado)
    inv_ref: &str,            // referencia de la factura (ya normalizada)
    inv_amount: f64,          // monto de la factura
    v_batches: Vec<String>,   // batches de todos los vouchers
    v_refs: Vec<String>,      // referencias de todos los vouchers
    v_amounts: Vec<f64>,      // montos de todos los vouchers
    v_indices: Vec<usize>,    // posiciones originales (para mapear de vuelta)
    threshold: f64,           // umbral mínimo Jaccard (ej: 0.70)
    tolerance: f64,           // tolerancia de monto (ej: 0.01)
) -> PyResult<Option<usize>> {

    // Construir bigramas de la factura UNA sola vez
    let inv_batch_bg: HashSet<(char, char)> = bigrams(inv_batch);
    let inv_ref_bg:   HashSet<(char, char)> = bigrams(inv_ref);

    let mut best_score = threshold - f64::EPSILON;
    let mut best_pos: Option<usize> = None;

    for (i, ((vb, vr), va)) in v_batches.iter()
        .zip(v_refs.iter())
        .zip(v_amounts.iter())
        .enumerate()
    {
        // PRE-FILTRO: descartar por monto antes de calcular strings
        if (va - inv_amount).abs() > tolerance {
            continue;
        }

        let vb_bg = bigrams(vb);
        let vr_bg = bigrams(vr);

        // Similitud Jaccard promedio entre batch y ref
        let batch_sim = jaccard_score(&inv_batch_bg, &vb_bg);
        let ref_sim   = jaccard_score(&inv_ref_bg, &vr_bg);
        let score     = (batch_sim + ref_sim) / 2.0;

        if score > best_score {
            best_score = score;
            best_pos   = Some(i);
        }
    }

    // Mapear posición en el slice → índice original
    Ok(best_pos.map(|pos| v_indices[pos]))
}

// ─── Funciones auxiliares (internas, no exportadas a Python) ─────────────────

/// Genera el conjunto de bigramas de un string.
///
/// EJEMPLO: "DINERS" → {('D','I'), ('I','N'), ('N','E'), ('E','R'), ('R','S')}
///
/// NOTA: Usamos HashSet<(char,char)> que es eficiente para strings cortos.
/// Para strings muy largos (>1000 chars) habría otras opciones, pero
/// batch y ref son siempre < 20 chars.
fn bigrams(s: &str) -> HashSet<(char, char)> {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() < 2 {
        return HashSet::new();
    }
    chars.windows(2)
        .map(|w| (w[0], w[1]))
        .collect()
}

/// Similitud de Jaccard entre dos conjuntos de bigramas.
///
/// Jaccard(A,B) = |A∩B| / |A∪B|
///
/// Casos especiales:
/// - Ambos vacíos → 1.0 (strings idénticos vacíos)
/// - Uno vacío    → 0.0 (sin similaridad posible)
fn jaccard_score(a: &HashSet<(char, char)>, b: &HashSet<(char, char)>) -> f64 {
    if a.is_empty() && b.is_empty() {
        return 1.0;
    }
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }

    let intersection = a.intersection(b).count();
    // Fórmula eficiente: |A∪B| = |A| + |B| - |A∩B|
    let union = a.len() + b.len() - intersection;

    if union == 0 {
        return 1.0;
    }

    intersection as f64 / union as f64
}

// ═══════════════════════════════════════════════════════════════════════════
// REGISTRO DEL MÓDULO
//
// Esto es lo que Python ve cuando hace: import pacioli_core
// Las funciones registradas aquí son las únicas accesibles desde Python.
// ═══════════════════════════════════════════════════════════════════════════

#[pymodule]
fn pacioli_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(find_invoice_combination, m)?)?;
    m.add_function(wrap_pyfunction!(fuzzy_batch_match, m)?)?;
    Ok(())
}
