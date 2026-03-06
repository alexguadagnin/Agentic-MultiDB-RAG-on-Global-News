import pandas as pd
from pathlib import Path
import sys
import os
import shutil  # Per creare/rimuovere la cartella di output
import multiprocessing as mp
from functools import partial
import numpy as np
from tqdm import tqdm
import traceback

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
    PROJECT_ROOT = PROJECT_ROOT_FALLBACK
    print(f"Uso path di fallback: RAW_DATA_DIR_NGRAMS={RAW_DATA_DIR_NGRAMS}")

# Path al dataset PULITO (NOSTRO INPUT)
PATH_SORGENTE_PULITO = RAW_DATA_DIR_NGRAMS / "parquet_dati_puliti"

# Path al dataset FINALE UNICO (NOSTRO OUTPUT)
PATH_OUTPUT_UNICO = RAW_DATA_DIR_NGRAMS / "parquet_dati_puliti_unici"


# --- 2. FUNZIONE WORKER PER IL SALVATAGGIO ---

def save_chunk_worker(indexed_chunk: tuple, output_path: Path):
    """
    Funzione eseguita da un worker per salvare un singolo chunk in Parquet.
    """
    index, chunk = indexed_chunk
    pid = os.getpid()
    
    try:
        filename = output_path / f"part-{index:05d}.parquet"
        chunk.to_parquet(
            filename,
            index=False,
            engine='pyarrow',
            compression='snappy'
        )
        # print(f"[PID: {pid}] Chunk {index} salvato in {filename}") # Rimuovi commento per debug
        return (True, filename)
    except Exception as e:
        print(f"[PID: {pid}] ERRORE salvataggio chunk {index}: {e}")
        traceback.print_exc()
        return (False, str(e))

# --- 3. ESECUZIONE ---

def rimuovi_duplicati_e_salva():
    print(f"\n--- AVVIO RIMOZIONE DUPLICATI ---")
    print(f"Sorgente: {PATH_SORGENTE_PULITO}")
    print(f"Destinazione: {PATH_OUTPUT_UNICO}")

    try:
        if not PATH_SORGENTE_PULITO.is_dir():
            print(f"ERRORE CRITICO: Cartella sorgente non trovata: {PATH_SORGENTE_PULITO}")
            return

        # 1. Prepara la cartella di output
        if PATH_OUTPUT_UNICO.exists():
            print(f" -> Rimuovo la cartella di output unica esistente...")
            shutil.rmtree(PATH_OUTPUT_UNICO)
        
        PATH_OUTPUT_UNICO.mkdir(parents=True, exist_ok=False)
        print(f" -> Cartella di output creata: {PATH_OUTPUT_UNICO}")

        # 2. Carica l'INTERO dataset (tutte le colonne)
        print(f" -> Caricamento di {PATH_SORGENTE_PULITO} in memoria...")
        print("    (Questo potrebbe richiedere tempo e RAM)")
        df = pd.read_parquet(PATH_SORGENTE_PULITO, engine='pyarrow')
        
        total_rows = len(df)
        print(f" -> Caricamento completato. {total_rows:,} righe caricate.")

        # 3. Rimuovi i duplicati
        print(" -> Rimozione duplicati basati sulla colonna 'url' (keep='first')...")
        df_unici = df.drop_duplicates(subset=['url'], keep='first')
        final_rows = len(df_unici)
        removed_rows = total_rows - final_rows
        
        print("\n--- STATISTICHE RIMOZIONE ---")
        print(f" Righe originali: {total_rows:,}")
        print(f" Righe rimosse: {removed_rows:,}")
        print(f" Righe finali uniche: {final_rows:,}")
        
        if final_rows == total_rows:
            print(" -> Nessun duplicato trovato. (Strano, ma OK)")
        
        # 4. Suddividi e salva in parallelo (per un output analogo)
        num_cores = max(1, mp.cpu_count() - 1)
        if final_rows < num_cores * 10 and final_rows > 0:
            num_cores = 1
            
        print(f"\n -> Divisione dati unici in {num_cores} blocchi...")
        df_chunks = np.array_split(df_unici, num_cores)
        
        # Lista di tuple (indice, chunk)
        chunks_with_index = list(enumerate(df_chunks))

        print(f" -> Avvio salvataggio parallelo su {num_cores} core...")
        
        mp_context = mp.get_context('spawn')
        
        # Usa 'partial' per "iniettare" il path di output nel worker
        worker_func = partial(save_chunk_worker, output_path=PATH_OUTPUT_UNICO)
        
        with mp_context.Pool(processes=num_cores) as pool:
            results = list(tqdm(
                pool.imap(worker_func, chunks_with_index), 
                total=len(df_chunks),
                desc="Salvataggio blocchi unici"
            ))
        
        print(" -> Salvataggio parallelo completato.")
        
        # Controlla se ci sono stati errori
        errori = [r for r in results if not r[0]]
        if errori:
            print(f"ATTENZIONE: Si sono verificati {len(errori)} errori durante il salvataggio.")
            for e in errori[:5]:
                print(f" - {e[1]}")
        
        print("\n--- ✅ OPERAZIONE COMPLETATA ---")
        print(f"Dataset UNICO (come collezione di file Parquet) salvato con successo in:")
        print(f"{PATH_OUTPUT_UNICO}")

    except MemoryError:
        print("\n--- ERRORE DI MEMORIA ---")
        print("ERRORE CRITICO: Memoria esaurita (OOM).")
        print("Il dataset completo (con tutte le colonne) non entra nei 32GB di RAM.")
        print("Dovrai usare un approccio 'out-of-core' (es. Dask o Polars) per questo step.")
        
    except Exception as e:
        print(f"\nERRORE INASPETTATO: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    rimuovi_duplicati_e_salva()