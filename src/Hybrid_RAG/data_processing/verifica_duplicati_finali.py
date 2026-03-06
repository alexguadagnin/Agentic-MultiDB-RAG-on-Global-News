import pandas as pd
from pathlib import Path
import sys
import os

# --- 1. CONFIGURAZIONE ---

try:
    # Importa i path dal tuo file constants.py
    from Hybrid_RAG.constants import RAW_DATA_DIR_NGRAMS, PROJECT_ROOT
    print(f"Path Cartella Genitore Ngrams (da constants): {RAW_DATA_DIR_NGRAMS}")
except ImportError:
    print("ATTENZIONE: Impossibile importare da Hybrid_RAG.constants.")
    PROJECT_ROOT_FALLBACK = Path("D:/progetto-rag-gdelt")
    DATA_DIR_FALLBACK = PROJECT_ROOT_FALLBACK / "data"
    RAW_DATA_DIR_NGRAMS = DATA_DIR_FALLBACK / "gdelt_ngrams"
    PROJECT_ROOT = PROJECT_ROOT_FALLBACK # Assumiamo la root del progetto
    print(f"Uso path di fallback: RAW_DATA_DIR_NGRAMS={RAW_DATA_DIR_NGRAMS}")

# Path al dataset finale e pulito (IL NOSTRO INPUT)
PATH_DATASET_PULITO = RAW_DATA_DIR_NGRAMS / "parquet_dati_puliti_unici"

# File di output dove salvare la lista dei duplicati (se trovati)
FILE_OUTPUT_DUPLICATI = PROJECT_ROOT / "report_duplicati_finali.txt"

# --- 2. ESECUZIONE ---

def controlla_duplicati_finali():
    print(f"\n--- AVVIO CONTROLLO DUPLICATI ---")
    print(f"Sorgente: {PATH_DATASET_PULITO}")

    try:
        if not PATH_DATASET_PULITO.is_dir():
            print(f"ERRORE CRITICO: Cartella sorgente non trovata: {PATH_DATASET_PULITO}")
            return
            
        print(f" -> Caricamento di {PATH_DATASET_PULITO} in memoria (solo colonna 'url')...")
        # Carichiamo solo la colonna 'url' per essere super veloci
        df = pd.read_parquet(PATH_DATASET_PULITO, columns=['url'], engine='pyarrow')
        print(f" -> Caricamento completato.")

        total_rows = len(df)
        
        # Trova gli URL duplicati
        # 'duplicated(keep=False)' segna *tutte* le occorrenze di un duplicato
        duplicati_df = df[df['url'].duplicated(keep=False)]
        
        num_righe_duplicate = len(duplicati_df)
        num_url_unici_duplicati = duplicati_df['url'].nunique()

        print("\n--- ✅ CONTROLLO COMPLETATO ---")
        print(f" Righe totali nel dataset: {total_rows:,}")
        
        if num_righe_duplicate == 0:
            print("\nRISULTATO: 🎉 CONGRATULAZIONI! Non ci sono URL duplicati.")
            print("Il tuo dataset è pulito e ogni URL è unico.")
        else:
            print(f"\nRISULTATO: ⚠️ ATTENZIONE! Trovati duplicati.")
            print(f" Ci sono {num_url_unici_duplicati:,} URL unici che appaiono più volte.")
            print(f" In totale, ci sono {num_righe_duplicate:,} righe che sono duplicati.")
            
            # Salva la lista dei duplicati per l'analisi
            try:
                print(f" -> Salvataggio della lista di URL duplicati in: {FILE_OUTPUT_DUPLICATI}...")
                duplicati_ordinati = duplicati_df.sort_values(by='url')
                # Salviamo come .csv per facilità di lettura
                duplicati_ordinati.to_csv(FILE_OUTPUT_DUPLICATI, index=False)
                print(f" -> File salvato: {FILE_OUTPUT_DUPLICATI}")
            except Exception as e:
                print(f"ERRORE: Impossibile salvare il file dei duplicati: {e}")

    except Exception as e:
        print(f"\nERRORE INASPETTATO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    controlla_duplicati_finali()