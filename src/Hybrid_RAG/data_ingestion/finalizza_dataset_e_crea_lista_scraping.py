import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from pathlib import Path
import pandas as pd
import sys
import os

# --- 1. CONFIGURAZIONE ---

# Tentativo di importare i path da constants
try:
    from Hybrid_RAG.constants import RAW_DATA_DIR_NGRAMS
    print(f"Path Cartella Genitore Ngrams (da constants): {RAW_DATA_DIR_NGRAMS}")
except ImportError:
    print("ATTENZIONE: Impossibile importare da Hybrid_RAG.constants.")
    PROJECT_ROOT_FALLBACK = Path("D:/progetto-rag-gdelt")
    DATA_DIR_FALLBACK = PROJECT_ROOT_FALLBACK / "data"
    RAW_DATA_DIR_NGRAMS = DATA_DIR_FALLBACK / "gdelt_ngrams"
    print(f"Uso path di fallback: RAW_DATA_DIR_NGRAMS={RAW_DATA_DIR_NGRAMS}")

# --- Path Sorgente ---
PATH_SORGENTE = RAW_DATA_DIR_NGRAMS / "parquet_autorevoli_filtrati"

# --- Path Destinazioni ---
# 1. La nuova cartella Parquet che conterrà SOLO i dati con testo
PATH_OUTPUT_VALIDI = RAW_DATA_DIR_NGRAMS / "parquet_final_con_testo"

# 2. Il file .txt che conterrà gli URL falliti
#    (salvato nella cartella principale del progetto per trovarlo facilmente)
try:
    from Hybrid_RAG.constants import PROJECT_ROOT
    FILE_URL_FALLITI = PROJECT_ROOT / "url_scraping_necessario.txt"
except ImportError:
    FILE_URL_FALLITI = RAW_DATA_DIR_NGRAMS.parent.parent / "url_scraping_necessario.txt"

print(f"\nLeggerò da: {PATH_SORGENTE}")
print(f"Salverò i dati validi in: {PATH_OUTPUT_VALIDI}")
print(f"Salverò gli URL falliti in: {FILE_URL_FALLITI}")


# --- 2. ESECUZIONE ---

def main():
    try:
        if not PATH_SORGENTE.is_dir():
            print(f"ERRORE CRITICO: La cartella sorgente {PATH_SORGENTE} non esiste.")
            return

        # Carica il dataset filtrato
        ddf = dd.read_parquet(
            PATH_SORGENTE,
            engine='pyarrow'
        )
        print(f"Caricate le colonne: {list(ddf.columns)}")

        # --- Creazione Filtri ---
        
        # Filtro per i dati VALIDI (con testo)
        filtro_validi = (ddf['text'].notnull() & (ddf['text'] != ''))
        
        # Filtro per i dati FALLITI (senza testo)
        filtro_falliti = (ddf['text'].isnull() | (ddf['text'] == ''))

        # Separa il DataFrame in due
        ddf_validi = ddf[filtro_validi]
        ddf_falliti = ddf[filtro_falliti]

        # --- Definizione Task ---
        
        # Task 1: Salvare il DataFrame VALIDO in una nuova cartella Parquet
        # Usiamo compute=False per preparare il task senza eseguirlo
        task_salva_parquet = ddf_validi.to_parquet(
            PATH_OUTPUT_VALIDI,
            write_index=False,
            engine='pyarrow',
            compute=False # IMPORTANTE: non eseguire ora
        )
        
        # Task 2: Ottenere la Series Pandas degli URL FALLITI
        # Questo è solo un task, non esegue nulla
        task_ottieni_url_falliti = ddf_falliti['url'].compute()

        
        print("\nAvvio elaborazione in parallelo (Task 1: Salva Parquet validi, Task 2: Estrai URL falliti)...")
        
        # Esegui entrambi i task contemporaneamente
        with ProgressBar():
            # Dask esegue i task e restituisce i risultati
            # risultati_parquet conterrà i metadati del salvataggio
            # urls_falliti_series conterrà la Series Pandas con gli URL
            (risultati_parquet, urls_falliti_series) = dd.compute(
                task_salva_parquet, 
                task_ottieni_url_falliti
            )

        print(" -> Dati Parquet validi salvati con successo.")

        # --- Salvataggio File TXT ---
        print(f" -> Trovati {len(urls_falliti_series)} URL falliti. Salvataggio in corso...")
        
        with open(FILE_URL_FALLITI, 'w', encoding='utf-8') as f:
            f.write("# Lista URL (da parquet_autorevoli_filtrati) con testo vuoto o nullo\n")
            f.write("# Totale: " + str(len(urls_falliti_series)) + "\n")
            f.write("# ------------------------------------------------------------------\n")
            # Salva la Series nel file, un URL per riga
            urls_falliti_series.to_csv(f, index=False, header=False, lineterminator='\n')

        print(f" -> File TXT '{FILE_URL_FALLITI}' salvato.")
        print("\n--- ✅ OPERAZIONE COMPLETATA ---")
        print("Il tuo dataset pulito è ora in 'parquet_final_con_testo'.")
        print("La lista per lo scraping è nel file .txt.")

    except Exception as e:
        print(f"ERRORE INASPETTATO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()