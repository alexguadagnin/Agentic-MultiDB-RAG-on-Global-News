import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import pandas as pd
from pathlib import Path
import os
import sys
import re 
from tqdm import tqdm 
from collections import Counter 
from botocore.exceptions import ClientError 
from urllib.parse import urlparse 

# --- 1. CONFIGURAZIONE DEI PATH ---

# Importa le costanti ESATTE dal tuo file
try:
    from Hybrid_RAG.constants import RAW_DATA_DIR_NGRAMS
    print(f"Path Cartella Genitore Ngrams (da constants): {RAW_DATA_DIR_NGRAMS}")
except ImportError:
    print("ATTENZIONE: Impossibile importare da Hybrid_RAG.constants.")
    PROJECT_ROOT_FALLBACK = Path("D:/progetto-rag-gdelt")
    DATA_DIR_FALLBACK = PROJECT_ROOT_FALLBACK / "data"
    RAW_DATA_DIR_NGRAMS = DATA_DIR_FALLBACK / "gdelt_ngrams"
    print(f"Uso path di fallback: RAW_DATA_DIR_NGRAMS={RAW_DATA_DIR_NGRAMS}")

# --- Path Sorgenti (i tuoi dati N-Gram completi) ---
PATH_ORIGINALE = RAW_DATA_DIR_NGRAMS / "parquet_data_locale"
PATH_FIX = RAW_DATA_DIR_NGRAMS / "parquet_fix_locale"

# --- Path Filtro (il CSV con gli URL autorevoli) ---
CSV_CON_URL_AUTOREVOLI = Path.cwd() / "url_metadata_filtered.csv"
if not CSV_CON_URL_AUTOREVOLI.exists():
    try:
        from Hybrid_RAG.constants import PROJECT_ROOT
        CSV_CON_URL_AUTOREVOLI = PROJECT_ROOT / "url_metadata_filtered.csv"
    except ImportError:
        CSV_CON_URL_AUTOREVOLI = RAW_DATA_DIR_NGRAMS.parent.parent / "url_metadata_filtered.csv"

# --- Path Destinazione (la nuova terza cartella) ---
PATH_OUTPUT = RAW_DATA_DIR_NGRAMS / "parquet_autorevoli_filtrati"


# --- 2. FUNZIONI (incluse quelle mancanti dal codice precedente) ---

# Definiamo siti_autorevoli, altrimenti la funzione filtro non ha riferimenti
siti_autorevoli = {
    "New York Times": "nytimes.com", "Washington Post": "washingtonpost.com",
    "Wall Street Journal": "wsj.com", "Los Angeles Times": "latimes.com",
    "USA Today": "usatoday.com", "Bloomberg": "bloomberg.com", "BBC": "bbc.com",
    "The Guardian": "theguardian.com", "The Times": "thetimes.co.uk", "Financial Times": "ft.com",
    "The Independent": "independent.co.uk", "The Telegraph": "telegraph.co.uk",
    "Le Monde": "lemonde.fr", "Le Figaro": "lefigaro.fr", "Libération": "liberation.fr",
    "Der Spiegel": "spiegel.de", "Frankfurter Allgemeine Zeitung": "faz.net", "Die Zeit": "zeit.de",
    "Süddeutsche Zeitung": "sueddeutsche.de", "Corriere della Sera": "corriere.it",
    "La Repubblica": "repubblica.it", "Il Sole 24 Ore": "ilsole24ore.com", "La Stampa": "lastampa.it",
    "Il Fatto Quotidiano": "ilfattoquotidiano.it", "El Pais": "elpais.com", "El Mundo": "elmundo.es",
    "ABC": "abc.es", "Russia Today": "rt.com", "TASS": "tass.ru", "Dagens Nyheter": "dn.se",
    "Svenska Dagbladet": "svd.se", "Le Soir": "lesoir.be", "De Standaard": "standaard.be",
    "NRC Handelsblad": "nrc.nl", "De Volkskrant": "volkskrant.nl", "Neue Zürcher Zeitung": "nzz.ch",
    "The Japan Times": "japantimes.co.jp", "Asahi Shimbun": "asahi.com", "Mainichi Shimbun": "mainichi.jp",
    "Yomiuri Shimbun": "yomiuri.co.jp", "China Daily": "chinadaily.com.cn",
    "South China Morning Post": "scmp.com", "Global Times": "globaltimes.cn", "The Hindu": "thehindu.com",
    "Times of India": "timesofindia.indiatimes.com", "Hindustan Times": "hindustantimes.com",
    "The Korea Herald": "koreaherald.com", "The Korea Times": "koreatimes.co.kr",
    "Straits Times": "straitstimes.com", "Bangkok Post": "bangkokpost.com", "The Star": "thestar.com.my",
    "Clarín": "clarin.com", "La Nación (Argentina)": "lanacion.com.ar", "O Globo": "oglobo.globo.com",
    "Folha de São Paulo": "folha.uol.com.br", "El Comercio (Perù)": "elcomercio.pe",
    "El Universal (Mexico)": "eluniversal.com.mx", "Reforma": "reforma.com",
    "El Tiempo (Colombia)": "eltiempo.com", "El Mercurio (Chile)": "emol.com",
    "Mail & Guardian": "mg.co.za", "News24": "news24.com", "Daily Nation (Kenya)": "nation.africa",
    "The Guardian Nigeria": "guardian.ng", "Al Ahram": "ahram.org.eg", "Le Matin": "lematin.ma",
    "The Sydney Morning Herald": "smh.com.au", "The Australian": "theaustralian.com.au",
    "New Zealand Herald": "nzherald.co.nz", "Reuters": "reuters.com", "Associated Press": "apnews.com",
    "Agence France-Presse": "afp.com", "Politico": "politico.com"
}
allowed_domains_set = set(siti_autorevoli.values())

def is_allowed_correctly(url_netloc: str, allowed_domains: set) -> bool:
    for allowed_domain in allowed_domains:
        if url_netloc == allowed_domain or url_netloc.endswith("." + allowed_domain):
            return True
    return False

def is_allowed_flawed(url_netloc: str, allowed_domains: set) -> bool:
    domain = url_netloc.replace("www.", "")
    return domain in allowed_domains
    
# --- 3. FUNZIONE PRINCIPALE ---

def filtra_parquet_per_lista_url():
    print("--- AVVIO FILTRO DATASET PARQUET ---")

    # --- STEP 1: Carica la lista degli URL autorevoli in memoria ---
    print(f"\nStep 1/3: Caricamento lista URL autorevoli da '{CSV_CON_URL_AUTOREVOLI}'...")
    if not CSV_CON_URL_AUTOREVOLI.exists():
        print(f"ERRORE CRITICO: File CSV '{CSV_CON_URL_AUTOREVOLI}' non trovato.")
        return
        
    try:
        # Leggiamo il CSV
        ddf_csv = dd.read_csv(
            str(CSV_CON_URL_AUTOREVOLI),
            usecols=['url'],
            dtype={'url': 'str'},
            encoding='utf-8',
            on_bad_lines='skip'
        )
        
        print(" -> Calcolo URL autorevoli...")
        with ProgressBar():
            valid_urls_series = ddf_csv['url'].dropna().compute()
        valid_urls_set = set(valid_urls_series)
        
        if not valid_urls_set:
            print("ERRORE: Nessun URL valido trovato nel file CSV. Arresto.")
            return
            
        print(f" -> Caricati {len(valid_urls_set):,} URL autorevoli unici in memoria.")

    except Exception as e:
        print(f"ERRORE durante la lettura del file CSV '{CSV_CON_URL_AUTOREVOLI}': {e}")
        return

    # --- STEP 2: Trova e carica i file Parquet sorgente ---
    print(f"\nStep 2/3: Ricerca file Parquet sorgente...")
    
    LISTA_FILE_PARQUET = []
    if PATH_ORIGINALE.is_dir():
        files_originali = list(PATH_ORIGINALE.glob('*.parquet'))
        LISTA_FILE_PARQUET.extend(files_originali)
        print(f" -> Trovati {len(files_originali)} file in: {PATH_ORIGINALE}")
    else:
        print(f"ATTENZIONE: Cartella originale NON trovata in: {PATH_ORIGINALE}")

    if PATH_FIX.is_dir():
        files_fix = list(PATH_FIX.glob('*.parquet'))
        LISTA_FILE_PARQUET.extend(files_fix)
        print(f" -> Trovati {len(files_fix)} file in: {PATH_FIX}")
    else:
        print(f"ATTENZIONE: Cartella FIX NON trovata in: {PATH_FIX}")

    if not LISTA_FILE_PARQUET:
         print("\nERRORE CRITICO: Nessun file Parquet trovato da cui leggere. Arresto.")
         return
    
    print(f" -> Totale file Parquet da processare: {len(LISTA_FILE_PARQUET)}")

    try:
        ddf_all_parquet = dd.read_parquet(
            [str(p) for p in LISTA_FILE_PARQUET],
            engine='pyarrow'
            # Non specifichiamo le colonne qui, le leggiamo tutte
        )
        print(f" -> Colonne caricate dai Parquet: {list(ddf_all_parquet.columns)}")
    
    except Exception as e:
        print(f"ERRORE: Impossibile leggere i file Parquet. Dettaglio: {e}")
        return

    # --- STEP 3: Filtra e Salva il nuovo dataset ---
    print(f"\nStep 3/3: Filtro e salvataggio in corso...")
    print(f" -> Destinazione: {PATH_OUTPUT}")

    try:
        os.makedirs(PATH_OUTPUT, exist_ok=True)

        # Filtra il Dask DataFrame
        filtered_ddf = ddf_all_parquet[ddf_all_parquet['url'].isin(valid_urls_set)]
        
        print(" -> Avvio scrittura su disco (può richiedere tempo)...")
        with ProgressBar():
            filtered_ddf.to_parquet(
                PATH_OUTPUT,
                write_index=False,
                engine='pyarrow',
                compute=True
            )
        
        print("\n--- ✅ OPERAZIONE COMPLETATA ---")
        print(f"File Parquet filtrati salvati con successo in:")
        print(f"{PATH_OUTPUT}")

    except Exception as e:
        print(f"\nERRORE durante il filtro o il salvataggio: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    filtra_parquet_per_lista_url()