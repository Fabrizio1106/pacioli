# ONE-TIME SCRIPT: Used for initial historical data migration.
# Do not run again without reviewing the full context.
# Moved to scripts/ for archiving purposes.
# UBICACIÓN: run_historical_ingestion.py (En la raíz del proyecto)
from logic.loaders.historical_collections_data_processor import HistoricalDataProcessor
from config.settings import PROJECT_ROOT
import os

# Usamos raw string (r) para evitar problemas con backslashes en Windows
HISTORICAL_PATH = PROJECT_ROOT / "data_raw" / "04. historical_collection"

def main():
    print("==========================================")
    print("PACIOLI - INGESTIÓN HISTÓRICA ML")
    print("==========================================")
    
    # Verificación manual de la ruta para debug
    print(f"📂 Ruta objetivo: {HISTORICAL_PATH}")

    if not os.path.exists(HISTORICAL_PATH):
        print(f"ERROR: No existe la carpeta: {HISTORICAL_PATH}")
        print("Por favor crea la carpeta y coloca los archivos Excel ahí.")
        return

    try:
        processor = HistoricalDataProcessor()
        processor.run(str(HISTORICAL_PATH))
    except Exception as e:
        print(f"Error fatal: {e}")

if __name__ == "__main__":
    main()