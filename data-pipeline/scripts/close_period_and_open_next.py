# ══════════════════════════════════════════════════════════════════════════════
# SCRIPT: Cierre de Periodo + Apertura Nuevo Periodo
# UBICACIÓN: scripts/close_period_and_open_next.py
# USO: python scripts/close_period_and_open_next.py --current 2026-02 --next 2026-03
# ══════════════════════════════════════════════════════════════════════════════

import argparse
from datetime import datetime, timedelta
from sqlalchemy import text
from utils.db_config import get_db_engine
from utils.logger import get_logger


class PeriodManager:
    """Gestor de periodos para procesos TRANSACTIONAL."""
    
    def __init__(self):
        self.engine = get_db_engine('config')
        self.logger = get_logger("PERIOD_MANAGER")
    
    def validate_current_period(self, periodo: str) -> bool:
        """Valida que el periodo actual esté completo."""
        
        self.logger(f"🔍 Validando periodo {periodo}...", "INFO")
        
        query = text("""
            SELECT 
                process_name,
                status,
                records_processed,
                execution_time_seconds
            FROM etl_process_windows
            WHERE periodo_mes = :periodo
              AND process_type = 'TRANSACTIONAL'
        """)
        
        try:
            with self.engine.connect() as conn:
                results = conn.execute(query, {'periodo': periodo}).fetchall()
                
                if not results:
                    self.logger(f"❌ No se encontraron procesos para el periodo {periodo}", "ERROR")
                    return False
                
                incomplete = []
                for row in results:
                    if row.status != 'COMPLETED':
                        incomplete.append({'process': row.process_name, 'status': row.status})
                
                if incomplete:
                    self.logger(f"❌ El periodo {periodo} tiene procesos incompletos:", "ERROR")
                    for item in incomplete:
                        self.logger(f"   - {item['process']}: {item['status']}", "ERROR")
                    return False
                
                self.logger(f"✅ Periodo {periodo} validado: {len(results)} procesos completados", "SUCCESS")
                
                total_records = sum(r.records_processed for r in results)
                total_time = sum(r.execution_time_seconds for r in results)
                
                self.logger(
                    f"   📊 Total procesado: {total_records:,} registros en {total_time:.2f}s",
                    "INFO"
                )
                
                return True
        
        except Exception as e:
            self.logger(f"❌ Error validando periodo: {e}", "ERROR")
            return False
    
    def close_period(self, periodo: str) -> bool:
        """Cierra un periodo (marca ventanas como COMPLETED)."""
        
        self.logger(f"🔒 Cerrando periodo {periodo}...", "INFO")
        
        if not self.validate_current_period(periodo):
            self.logger(f"❌ No se puede cerrar {periodo}: tiene procesos incompletos", "ERROR")
            return False
        
        query = text("""
            UPDATE etl_process_windows
            SET 
                status = 'COMPLETED',
                completed_at = CASE 
                    WHEN completed_at IS NULL THEN NOW() 
                    ELSE completed_at 
                END
            WHERE periodo_mes = :periodo
              AND process_type = 'TRANSACTIONAL'
              AND status IN ('PENDING', 'RUNNING')
        """)
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {'periodo': periodo})
                conn.commit()
                
                rows_affected = result.rowcount
                
                if rows_affected > 0:
                    self.logger(f"✅ Periodo {periodo} cerrado: {rows_affected} ventanas actualizadas", "SUCCESS")
                else:
                    self.logger(f"ℹ️ Periodo {periodo}: Ya estaba cerrado", "INFO")
                
                return True
        
        except Exception as e:
            self.logger(f"❌ Error cerrando periodo: {e}", "ERROR")
            return False
    
    def open_new_period(self, periodo: str, start_date: str, end_date: str) -> bool:
        """Abre un nuevo periodo (crea ventanas PENDING)."""
        
        self.logger(f"📂 Abriendo periodo {periodo} ({start_date} → {end_date})...", "INFO")
        
        check_query = text("""
            SELECT COUNT(*) as count
            FROM etl_process_windows
            WHERE periodo_mes = :periodo
              AND process_type = 'TRANSACTIONAL'
        """)
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(check_query, {'periodo': periodo}).fetchone()
                
                if result.count > 0:
                    self.logger(f"⚠️ El periodo {periodo} ya existe ({result.count} ventanas encontradas)", "WARN")
                    
                    response = input("¿Desea re-crear las ventanas? (yes/no): ")
                    
                    if response.lower() != 'yes':
                        self.logger("❌ Operación cancelada por el usuario", "WARN")
                        return False
                    
                    delete_query = text("""
                        DELETE FROM etl_process_windows
                        WHERE periodo_mes = :periodo
                          AND process_type = 'TRANSACTIONAL'
                    """)
                    conn.execute(delete_query, {'periodo': periodo})
                    conn.commit()
                    
                    self.logger("🗑️ Ventanas existentes eliminadas", "INFO")
        
        except Exception as e:
            self.logger(f"❌ Error verificando periodo: {e}", "ERROR")
            return False
        
        insert_query = text("""
            INSERT INTO etl_process_windows (
                process_name, process_type, window_start, window_end, periodo_mes, status, notes
            ) VALUES
            ('SAP_TRANSACTIONS', 'TRANSACTIONAL', :start, :end, :periodo, 'PENDING', CONCAT('Periodo ', :periodo)),
            ('DINERS_CARDS', 'TRANSACTIONAL', :start, :end, :periodo, 'PENDING', CONCAT('Periodo ', :periodo)),
            ('GUAYAQUIL_CARDS', 'TRANSACTIONAL', :start, :end, :periodo, 'PENDING', CONCAT('Periodo ', :periodo)),
            ('PACIFICARD_CARDS', 'TRANSACTIONAL', :start, :end, :periodo, 'PENDING', CONCAT('Periodo ', :periodo)),
            ('PARKING_BREAKDOWN', 'TRANSACTIONAL', :start, :end, :periodo, 'PENDING', CONCAT('Periodo ', :periodo))
        """)
        
        try:
            with self.engine.connect() as conn:
                conn.execute(insert_query, {'periodo': periodo, 'start': start_date, 'end': end_date})
                conn.commit()
                
                self.logger(f"✅ Periodo {periodo} abierto: 5 ventanas creadas", "SUCCESS")
                
                self._show_period_summary(periodo)
                
                return True
        
        except Exception as e:
            self.logger(f"❌ Error abriendo periodo: {e}", "ERROR")
            return False
    
    def _show_period_summary(self, periodo: str):
        """Muestra resumen de un periodo."""
        
        query = text("""
            SELECT 
                process_name, window_start, window_end, status
            FROM etl_process_windows
            WHERE periodo_mes = :periodo
              AND process_type = 'TRANSACTIONAL'
            ORDER BY process_name
        """)
        
        try:
            with self.engine.connect() as conn:
                results = conn.execute(query, {'periodo': periodo}).fetchall()
                
                if results:
                    self.logger(f"\n📋 Resumen del periodo {periodo}:", "INFO")
                    self.logger("─" * 80, "INFO")
                    
                    for row in results:
                        self.logger(
                            f"   {row.process_name}: {row.window_start} → {row.window_end} ({row.status})",
                            "INFO"
                        )
        
        except Exception as e:
            self.logger(f"⚠️ Error mostrando resumen: {e}", "WARN")
    
    def close_and_open(self, current_periodo: str, next_periodo: str, next_start: str, next_end: str) -> bool:
        """Flujo completo: Cierra periodo actual y abre siguiente."""
        
        self.logger("═" * 80, "INFO")
        self.logger("🔄 INICIO DE CIERRE Y APERTURA DE PERIODO", "INFO")
        self.logger("═" * 80, "INFO")
        
        if not self.close_period(current_periodo):
            self.logger("❌ No se pudo cerrar el periodo actual", "ERROR")
            return False
        
        if not self.open_new_period(next_periodo, next_start, next_end):
            self.logger("❌ No se pudo abrir el periodo siguiente", "ERROR")
            return False
        
        self.logger("\n" + "═" * 80, "SUCCESS")
        self.logger("✅ CIERRE Y APERTURA COMPLETADOS", "SUCCESS")
        self.logger("═" * 80, "SUCCESS")
        
        self.logger(f"\n📊 Periodo {current_periodo}: CERRADO ✅", "SUCCESS")
        self.logger(f"📂 Periodo {next_periodo}: ABIERTO ✅", "SUCCESS")
        
        return True


def calculate_period_dates(periodo: str) -> tuple:
    """Calcula fechas de inicio y fin para un periodo."""
    year, month = periodo.split('-')
    year = int(year)
    month = int(month)
    
    start_date = datetime(year, month, 1)
    
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    
    return (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))


def get_next_period(periodo: str) -> str:
    """Calcula el periodo siguiente."""
    year, month = periodo.split('-')
    year = int(year)
    month = int(month)
    
    if month == 12:
        return f"{year + 1}-01"
    else:
        return f"{year}-{month + 1:02d}"


def main():
    """Interfaz de línea de comandos."""
    
    parser = argparse.ArgumentParser(description='Gestor de periodos para procesos ETL')
    
    parser.add_argument('--current', help='Periodo actual a cerrar (formato: YYYY-MM)')
    parser.add_argument('--next', help='Periodo siguiente a abrir (formato: YYYY-MM)')
    parser.add_argument('--action', choices=['close', 'open', 'both'], default='both', help='Acción a realizar')
    
    args = parser.parse_args()
    
    manager = PeriodManager()
    
    if args.current and args.next and args.action == 'both':
        next_start, next_end = calculate_period_dates(args.next)
        success = manager.close_and_open(args.current, args.next, next_start, next_end)
        return 0 if success else 1
    
    if args.current and args.action == 'close':
        success = manager.close_period(args.current)
        return 0 if success else 1
    
    if args.next and args.action == 'open':
        next_start, next_end = calculate_period_dates(args.next)
        success = manager.open_new_period(args.next, next_start, next_end)
        return 0 if success else 1
    
    # Modo interactivo
    print("\n" + "═" * 80)
    print("🔄 GESTOR DE PERIODOS - MODO INTERACTIVO")
    print("═" * 80 + "\n")
    
    current = input("Periodo actual a cerrar (YYYY-MM): ").strip()
    next_period = input("Periodo siguiente a abrir (YYYY-MM, Enter para auto): ").strip()
    
    if not next_period:
        next_period = get_next_period(current)
        print(f"   → Calculado automáticamente: {next_period}")
    
    next_start, next_end = calculate_period_dates(next_period)
    
    print(f"\n📋 Resumen:")
    print(f"   Cerrar: {current}")
    print(f"   Abrir: {next_period} ({next_start} → {next_end})")
    
    confirm = input("\n¿Continuar? (yes/no): ").strip().lower()
    
    if confirm != 'yes':
        print("❌ Operación cancelada")
        return 1
    
    success = manager.close_and_open(current, next_period, next_start, next_end)
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())