import pandas as pd
import sys
import time
from pathlib import Path
from tqdm import tqdm
import sys

# ==============================================================================
# 1. LOGICA DI IMPORT (Per trovare 'constants.py')
# ==============================================================================
try:
    SRC_ROOT = Path(__file__).parent.parent.parent.resolve()
    if str(SRC_ROOT) not in sys.path:
        sys.path.append(str(SRC_ROOT))
    
    # Importa il percorso dal tuo file di costanti
    from Hybrid_RAG.constants import RAW_DATA_DIR_NGRAMS
except ImportError:
    print("❌ ERRORE CRITICO: Impossibile importare 'constants.py'.")
    sys.exit(1)
# ==============================================================================

# --- CONFIGURAZIONE ---
EMBEDDING_DATA_DIR = RAW_DATA_DIR_NGRAMS / "embedding_output"

# --- MODIFICA 1: Cambia l'estensione del file di output ---
OUTPUT_FILE = RAW_DATA_DIR_NGRAMS / "elasticsearch_data.parquet" 
# ----------------------

def extract_data_for_elasticsearch():
    """
    Legge i dati di embedding, estrae l'ID e il testo del chunk,
    e li salva in un file Parquet pronto per Elasticsearch.
    """
    print(f"Caricamento dati Parquet da: {EMBEDDING_DATA_DIR}")
    try:
        df = pd.read_parquet(EMBEDDING_DATA_DIR)
        print(f"✅ Dati caricati: {len(df)} chunk totali.")
    except Exception as e:
        print(f"❌ FALLIMENTO: Impossibile caricare i file Parquet: {e}")
        sys.exit(1)

    print("Estrazione di 'id' e 'chunk_text' in corso...")
    
    tqdm.pandas(desc="Estrazione Payload")
    df['chunk_text'] = df['payload'].progress_apply(lambda p: p.get('chunk_text'))
    
    df_es = df[['id', 'chunk_text']].copy()
    df_es.rename(columns={'id': 'id_chunk'}, inplace=True)

    print(f"Salvataggio dati in formato Parquet su: {OUTPUT_FILE}")
    try:
        # --- MODIFICA 2: Salva in formato Parquet ---
        df_es.to_parquet(OUTPUT_FILE, engine='pyarrow', index=False)
        
        print("\n--- ESTRAZIONE COMPLETATA ---")
        print(f"✅ File {OUTPUT_FILE} creato con successo.")
        
    except Exception as e:
        print(f"❌ FALLIMENTO: Impossibile salvare il file Parquet: {e}")

# --- Esecuzione Principale ---
if __name__ == "__main__":
    start_time = time.time()
    extract_data_for_elasticsearch()
    end_time = time.time()
    print(f"Tempo totale: {end_time - start_time:.2f} secondi.")