import pandas as pd
from pathlib import Path
import sys
import os
import re # Usato per la pulizia finale
from urllib.parse import urlparse
from typing import List, Dict, Tuple
from collections import Counter
from tqdm import tqdm
import multiprocessing as mp
from functools import partial
import numpy as np
import traceback # Per log dettagliati
import regex
import shutil # Aggiunto per rimuovere la cartella di output

# ---
# STEP 1: IMPORTA LE TUE FUNZIONI DI PULIZIA
try:
    from Hybrid_RAG.data_processing.cleaning_functions import (
        clean_abc_es, clean_afp_com, clean_asahi_com, clean_bbc_com, clean_bloomberg_com,
        clean_clarin_com, clean_corriere_it, clean_dn_se, clean_elcomercio_pe,
        clean_elmundo_es, clean_elpais_com, clean_eltiempo_com, clean_eluniversal_com_mx,
        clean_emol_com, clean_faz_net, clean_folha_uol_com_br, clean_hindustantimes_com,
        clean_ilfattoquotidiano_it, clean_ilsole24ore_com, clean_japantimes_co_jp,
        clean_lanacion_com_ar, clean_lastampa_it, clean_latimes_com, clean_lefigaro_fr,
        clean_lematin_ma, clean_lemonde_fr, clean_liberation_fr, clean_mainichi_jp,
        clean_nrc_nl, clean_nytimes_com, clean_nzz_ch, clean_oglobo_globo_com,
        clean_reforma_com, clean_repubblica_it, clean_rt_com, clean_smh_com_au,
        clean_spiegel_de, clean_standaard_be, clean_sueddeutsche_de, clean_svd_se,
        clean_tass_ru, clean_thehindu_com, clean_yomiuri_co_jp, clean_zeit_de
    )
    print("Funzioni di pulizia (basate su 'regex') importate con successo.")
except ImportError as e:
    print(f"ERRORE CRITICO: Impossibile importare le funzioni di pulizia.")
    print(f"Dettaglio: {e}")
    # Per eseguire il codice senza le funzioni (solo per test strutturale), commenta 'exit()':
    # exit() 
    pass # Lasciamo un 'pass' per permettere all'esempio di caricare

# --- FINE SEZIONE IMPORT ---
# --- 2. CONFIGURAZIONE PATH ---
try:
    # Si assume che Hybrid_RAG.constants esista e definisca RAW_DATA_DIR_NGRAMS
    from Hybrid_RAG.constants import RAW_DATA_DIR_NGRAMS
    print(f"Path Cartella Genitore Ngrams (da constants): {RAW_DATA_DIR_NGRAMS}")
except ImportError:
    # FALLBACK se Hybrid_RAG.constants non è disponibile
    print("ATTENZIONE: Impossibile importare da Hybrid_RAG.constants.")
    PROJECT_ROOT_FALLBACK = Path("D:/progetto-rag-gdelt")
    DATA_DIR_FALLBACK = PROJECT_ROOT_FALLBACK / "data"
    RAW_DATA_DIR_NGRAMS = DATA_DIR_FALLBACK / "gdelt_ngrams"
    print(f"Uso path di fallback: RAW_DATA_DIR_NGRAMS={RAW_DATA_DIR_NGRAMS}")

PATH_SORGENTE = RAW_DATA_DIR_NGRAMS / "parquet_dati_completi"
# PATH_OUTPUT_PULITO sarà una CARTELLA contenente file parquet
PATH_OUTPUT_PULITO = RAW_DATA_DIR_NGRAMS / "parquet_dati_puliti" 


# --- 3. DOMINI E MAPPA FUNZIONI ---
siti_autorevoli = {
    "abc.es": "ABC", "afp.com": "Agence France-Presse", "asahi.com": "Asahi Shimbun",
    "bbc.com": "BBC", "bloomberg.com": "Bloomberg", "clarin.com": "Clarín",
    "corriere.it": "Corriere della Sera", "dn.se": "Dagens Nyheter", "elcomercio.pe": "El Comercio (Perù)",
    "elmundo.es": "El Mundo", "elpais.com": "El Pais", "eltiempo.com": "El Tiempo (Colombia)",
    "eluniversal.com.mx": "El Universal (Mexico)", "emol.com": "El Mercurio (Chile)",
    "faz.net": "Frankfurter Allgemeine Zeitung", "folha.uol.com.br": "Folha de São Paulo",
    "hindustantimes.com": "Hindustan Times", "ilfattoquotidiano.it": "Il Fatto Quotidiano",
    "ilsole24ore.com": "Il Sole 24 Ore", "japantimes.co_jp": "The Japan Times",
    "lanacion.com.ar": "La Nación (Argentina)", "lastampa.it": "La Stampa",
    "latimes.com": "Los Angeles Times", "lefigaro.fr": "Le Figaro", "lematin.ma": "Le Matin",
    "lemonde.fr": "Le Monde", "liberation.fr": "Libération", "mainichi.jp": "Mainichi Shimbun",
    "nrc.nl": "NRC Handelsblad", "nytimes.com": "New York Times", "nzz.ch": "Neue Zürcher Zeitung",
    "oglobo.globo.com": "O Globo", "reforma.com": "Reforma", "repubblica.it": "La Repubblica",
    "rt.com": "Russia Today", "smh.com.au": "The Sydney Morning Herald",
    "spiegel.de": "Der Spiegel", "standaard.be": "De Standaard",
    "sueddeutsche.de": "Süddeutsche Zeitung", "svd.se": "Svenska Dagbladet", "tass.ru": "TASS",
    "thehindu.com": "The Hindu", "yomiuri.co.jp": "Yomiuri Shimbun", "zeit.de": "Die Zeit"
}
allowed_domains_set = set(siti_autorevoli.keys())

# Mappa le stringhe dei domini con le funzioni importate
function_mapping = {
    "abc.es": globals().get('clean_abc_es'), "afp.com": globals().get('clean_afp_com'), 
    "asahi.com": globals().get('clean_asahi_com'), "bbc.com": globals().get('clean_bbc_com'), 
    "bloomberg.com": globals().get('clean_bloomberg_com'), "clarin.com": globals().get('clean_clarin_com'),
    "corriere.it": globals().get('clean_corriere_it'), "dn.se": globals().get('clean_dn_se'), 
    "elcomercio.pe": globals().get('clean_elcomercio_pe'), "elmundo.es": globals().get('clean_elmundo_es'), 
    "elpais.com": globals().get('clean_elpais_com'), "eltiempo.com": globals().get('clean_eltiempo_com'),
    "eluniversal.com.mx": globals().get('clean_eluniversal_com_mx'), "emol.com": globals().get('clean_emol_com'), 
    "faz.net": globals().get('clean_faz_net'), "folha.uol.com.br": globals().get('clean_folha_uol_com_br'), 
    "hindustantimes.com": globals().get('clean_hindustantimes_com'), 
    "ilfattoquotidiano.it": globals().get('clean_ilfattoquotidiano_it'), 
    "ilsole24ore.com": globals().get('clean_ilsole24ore_com'), "japantimes.co_jp": globals().get('clean_japantimes_co_jp'),
    "lanacion.com.ar": globals().get('clean_lanacion_com_ar'), "lastampa.it": globals().get('clean_lastampa_it'),
    "latimes.com": globals().get('clean_latimes_com'), "lefigaro.fr": globals().get('clean_lefigaro_fr'), 
    "lematin.ma": globals().get('clean_lematin_ma'), "lemonde.fr": globals().get('clean_lemonde_fr'), 
    "liberation.fr": globals().get('clean_liberation_fr'), "mainichi.jp": globals().get('clean_mainichi_jp'), 
    "nrc.nl": globals().get('clean_nrc_nl'), "nytimes.com": globals().get('clean_nytimes_com'), 
    "nzz.ch": globals().get('clean_nzz_ch'), "oglobo.globo.com": globals().get('clean_oglobo_globo_com'), 
    "reforma.com": globals().get('clean_reforma_com'), "repubblica.it": globals().get('clean_repubblica_it'), 
    "rt.com": globals().get('clean_rt_com'), "smh.com.au": globals().get('clean_smh_com_au'),
    "spiegel.de": globals().get('clean_spiegel_de'), "standaard.be": globals().get('clean_standaard_be'),
    "sueddeutsche.de": globals().get('clean_sueddeutsche_de'), "svd.se": globals().get('clean_svd_se'), 
    "tass.ru": globals().get('clean_tass_ru'), "thehindu.com": globals().get('clean_thehindu_com'), 
    "yomiuri.co.jp": globals().get('clean_yomiuri_co_jp'), "zeit.de": globals().get('clean_zeit_de')
}
print(f"Mappa di {len(function_mapping)} funzioni di pulizia caricata.")


# --- 4. FUNZIONI DI APPLICAZIONE (WORKER) ---

def get_base_domain(url: str) -> str:
    if not isinstance(url, str): return "dominio_sconosciuto"
    try:
        netloc = urlparse(url).netloc
        for domain in allowed_domains_set:
            if netloc == domain or netloc.endswith("." + domain):
                return domain
        return "dominio_non_autorevole"
    except Exception:
        return "dominio_malformato"

def apply_robust_cleaner_to_row(row: pd.Series) -> str:
    """
    Funzione robusta che pulisce una SINGOLA riga.
    """
    text = row['text']
    domain = row['domain_root']
    
    if not isinstance(text, str) or pd.isna(text) or text == '':
        return None 

    clean_func = function_mapping.get(domain)
    
    if clean_func:
        # Nessun try/except qui, il worker fallirà/si bloccherà sulla riga problematica
        # Questo è il comportamento richiesto nel codice originale.
        return clean_func(text)
    else:
        # Ritorna il testo originale se non c'è una funzione di pulizia specifica
        return text


def process_dataframe_chunk_and_save(indexed_chunk: Tuple[int, pd.DataFrame], output_path: Path) -> Dict:
    """
    Funzione ESEGUITA DAL WORKER. 
    Processa il chunk e lo salva direttamente come file Parquet nella cartella di output.
    """
    index, df_chunk = indexed_chunk
    worker_pid = os.getpid()
    
    print(f"\n[Worker PID: {worker_pid}] Avvio elaborazione chunk {index} di {len(df_chunk)} righe.")
    
    # 1. Mappa domini
    df_chunk['domain_root'] = df_chunk['url'].apply(get_base_domain)
    
    # 2. Applica la pulizia riga per riga con log dettagliati
    original_rows = len(df_chunk)
    # Rimuoviamo l'eccessivo logging riga per riga qui per non intasare l'output,
    # ma lo lasciamo in apply_robust_cleaner_to_row se necessario.
    df_chunk['text'] = df_chunk.apply(apply_robust_cleaner_to_row, axis=1)
    
    # 3. Elimina le righe vuote
    df_chunk = df_chunk.dropna(subset=['text'])
    df_chunk = df_chunk[df_chunk['text'] != '']
    
    # 4. Rimuovi colonna temporanea
    df_chunk = df_chunk.drop(columns=['domain_root'])
    
    final_rows = len(df_chunk)
    print(f"[Worker PID: {worker_pid}] Chunk {index} completato. Righe rimanenti: {final_rows}.")
    
    # 5. Salva il chunk come file Parquet
    # Formato standard Spark/Dask per file multipli in un dataset Parquet
    filename = output_path / f"part-{index:05d}.parquet" 
    
    try:
        # Usiamo il filtro sulle colonne originali per coerenza (assumendo che 'url' e 'text' esistano)
        final_columns = [col for col in df_chunk.columns if col != 'domain_root'] 
        df_chunk[final_columns].to_parquet(
            filename,
            index=False,
            engine='pyarrow',
            compression='snappy'
        )
        print(f"[Worker PID: {worker_pid}] Salvataggio {filename} riuscito.")
        # Ritorna i metadati per il tracciamento
        return {"success": True, "rows_before": original_rows, "rows_after_cleaning": final_rows, "path": str(filename)}
    except Exception as e:
        print(f"[Worker PID: {worker_pid}] ERRORE CRITICO durante il salvataggio del chunk {index}: {e}")
        # Stampa lo stack trace completo per l'errore di salvataggio
        traceback.print_exc(file=sys.stdout)
        return {"success": False, "error": str(e), "path": str(filename)}


# --- 5. ESECUZIONE (con PANDAS + MULTIPROCESSING) ---

def run_cleaning_pipeline_parallel():
    print(f"\n--- AVVIO PIPELINE DI PULIZIA (PANDAS + PARALLELO + SALVATAGGIO IN CARTELLA) ---")
    print(f"Sorgente: {PATH_SORGENTE}")
    print(f"Destinazione (CARTELLA): {PATH_OUTPUT_PULITO}")
    print("ATTENZIONE: NESSUN TIMEOUT. Lo script si bloccherà sulla riga problematica.")

    try:
        if not PATH_SORGENTE.is_dir():
            print(f"ERRORE CRITICO: Cartella sorgente non trovata: {PATH_SORGENTE}")
            return

        # 1. Prepara la cartella di output
        if PATH_OUTPUT_PULITO.exists():
            print(f" -> Rimuovo la cartella di output esistente: {PATH_OUTPUT_PULITO}")
            shutil.rmtree(PATH_OUTPUT_PULITO)
        
        PATH_OUTPUT_PULITO.mkdir(parents=True, exist_ok=False)
        print(f" -> Cartella di output creata: {PATH_OUTPUT_PULITO}")


        print(f" -> Caricamento di {PATH_SORGENTE} in memoria (può richiedere tempo)...")
        df = pd.read_parquet(PATH_SORGENTE, engine='pyarrow')
        print(f" -> Caricamento completato. {len(df):,} righe caricate.")
        original_count = len(df)

        # Determina il numero di core 
        num_cores = max(1, mp.cpu_count() - 1)
        if original_count < num_cores * 10 and original_count > 0:
            num_cores = 1
        
        print(f" -> Divisione dati in {num_cores} blocchi per {num_cores} processi worker...")
        df_chunks = np.array_split(df, num_cores)

        # 2. Creazione di una lista di tuple (indice, chunk)
        chunks_with_index = list(enumerate(df_chunks))

        print(f" -> Avvio pulizia parallela e salvataggio su {num_cores} core...")
        
        mp_context = mp.get_context('spawn')
        
        # Uso 'partial' per iniettare il PATH_OUTPUT_PULITO nella funzione worker
        worker_func = partial(process_dataframe_chunk_and_save, output_path=PATH_OUTPUT_PULITO)
        
        with mp_context.Pool(processes=num_cores) as pool:
            # results conterrà i dizionari di metadati restituiti dai worker
            results = list(tqdm(
                pool.imap(worker_func, chunks_with_index), 
                total=len(df_chunks),
                desc="Pulizia e salvataggio blocchi paralleli"
            ))

        print(" -> Pulizia parallela e salvataggio completato.")

        # 3. Calcolo delle righe finali dai metadati
        final_count = sum(r.get('rows_after_cleaning', 0) for r in results if r['success'])
        
        print(f" -> Rimosse {original_count - final_count:,} righe vuote.")
        print(f" -> Righe totali rimanenti (salvate): {final_count:,}")

        # Non serve pd.concat() o un salvataggio finale, poiché i worker hanno salvato i file.
        
        print("\n--- ✅ OPERAZIONE COMPLETATA ---")
        print(f"Dataset pulito (come collezione di file Parquet) salvato con successo in:")
        print(f"{PATH_OUTPUT_PULITO}")

    except Exception as e:
        print(f"\nERRORE INASPETTATO NELLA PIPELINE PRINCIPALE: {e}")
        traceback.print_exc(file=sys.stdout)

if __name__ == "__main__":
    run_cleaning_pipeline_parallel()