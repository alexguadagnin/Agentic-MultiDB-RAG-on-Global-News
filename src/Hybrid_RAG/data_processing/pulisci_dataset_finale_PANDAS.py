import pandas as pd
from pathlib import Path
import sys
import os
import regex as re
from urllib.parse import urlparse
from typing import List, Dict, Tuple # Importa i tipi
from collections import Counter
from tqdm import tqdm

# ---
# STEP 1: IMPORTA LE TUE FUNZIONI DI PULIZIA
# ---
# Assicurati che questo path di import sia corretto
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
    print("Funzioni di pulizia importate con successo.")
except ImportError as e:
    print(f"ERRORE CRITICO: Impossibile importare le funzioni di pulizia.")
    print(f"Dettaglio: {e}")
    exit()
# --- FINE SEZIONE IMPORT ---


# --- 2. CONFIGURAZIONE PATH ---
try:
    from Hybrid_RAG.constants import RAW_DATA_DIR_NGRAMS
    print(f"Path Cartella Genitore Ngrams (da constants): {RAW_DATA_DIR_NGRAMS}")
except ImportError:
    print("ATTENZIONE: Impossibile importare da Hybrid_RAG.constants.")
    PROJECT_ROOT_FALLBACK = Path("D:/progetto-rag-gdelt")
    DATA_DIR_FALLBACK = PROJECT_ROOT_FALLBACK / "data"
    RAW_DATA_DIR_NGRAMS = DATA_DIR_FALLBACK / "gdelt_ngrams"
    print(f"Uso path di fallback: RAW_DATA_DIR_NGRAMS={RAW_DATA_DIR_NGRAMS}")

PATH_SORGENTE = RAW_DATA_DIR_NGRAMS / "parquet_dati_completi"
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
    "ilsole24ore.com": "Il Sole 24 Ore", "japantimes.co.jp": "The Japan Times",
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

function_mapping = {
    "abc.es": clean_abc_es, "afp.com": clean_afp_com, "asahi.com": clean_asahi_com,
    "bbc.com": clean_bbc_com, "bloomberg.com": clean_bloomberg_com, "clarin.com": clean_clarin_com,
    "corriere.it": clean_corriere_it, "dn.se": clean_dn_se, "elcomercio.pe": clean_elcomercio_pe,
    "elmundo.es": clean_elmundo_es, "elpais.com": clean_elpais_com, "eltiempo.com": clean_eltiempo_com,
    "eluniversal.com.mx": clean_eluniversal_com_mx, "emol.com": clean_emol_com, "faz.net": clean_faz_net,
    "folha.uol.com.br": clean_folha_uol_com_br, "hindustantimes.com": clean_hindustantimes_com,
    "ilfattoquotidiano.it": clean_ilfattoquotidiano_it, "ilsole24ore.com": clean_ilsole24ore_com,
    "japantimes.co.jp": clean_japantimes_co_jp, "lanacion.com.ar": clean_lanacion_com_ar,
    "lastampa.it": clean_lastampa_it, "latimes.com": clean_latimes_com, "lefigaro.fr": clean_lefigaro_fr,
    "lematin.ma": clean_lematin_ma, "lemonde.fr": clean_lemonde_fr, "liberation.fr": clean_liberation_fr,
    "mainichi.jp": clean_mainichi_jp, "nrc.nl": clean_nrc_nl, "nytimes.com": clean_nytimes_com,
    "nzz.ch": clean_nzz_ch, "oglobo.globo.com": clean_oglobo_globo_com, "reforma.com": clean_reforma_com,
    "repubblica.it": clean_repubblica_it, "rt.com": clean_rt_com, "smh.com.au": clean_smh_com_au,
    "spiegel.de": clean_spiegel_de, "standaard.be": clean_standaard_be,
    "sueddeutsche.de": clean_sueddeutsche_de, "svd.se": clean_svd_se, "tass.ru": clean_tass_ru,
    "thehindu.com": clean_thehindu_com, "yomiuri.co.jp": clean_yomiuri_co_jp, "zeit.de": clean_zeit_de
}
print(f"Mappa di {len(function_mapping)} funzioni di pulizia caricata.")


# --- 4. FUNZIONI DI APPLICAZIONE (WORKER) ---

# Dizionario globale per loggare gli errori solo una volta
error_log_cache = {}

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

def apply_robust_cleaner(row: pd.Series) -> str:
    """
    Funzione helper robusta da applicare riga per riga (axis=1).
    Cattura gli errori delle funzioni di pulizia.
    """
    text = row['text']
    domain = row['domain_root']
    
    if not isinstance(text, str) or pd.isna(text) or text == '':
        return None 

    clean_func = function_mapping.get(domain)
    
    if clean_func:
        try:
            return clean_func(text) # Esegui la pulizia
        except re.error as e:
            error_key = f"{clean_func.__name__}:{e}"
            if error_key not in error_log_cache:
                print(f"\n!!! ERRORE REGEX in {clean_func.__name__}: {e}. Il testo originale verrà restituito. !!!")
                error_log_cache[error_key] = True
            return text # Restituisci il testo originale non pulito
        except Exception as e:
            error_key = f"{clean_func.__name__}:GENERIC:{e}"
            if error_key not in error_log_cache:
                print(f"\n!!! ERRORE GENERICO in {clean_func.__name__}: {e}. Il testo originale verrà restituito. !!!")
                error_log_cache[error_key] = True
            return text
    else:
        return text

# --- 5. ESECUZIONE (con PANDAS) ---

def run_cleaning_pipeline_pandas():
    print(f"\n--- AVVIO PIPELINE DI PULIZIA (con PANDAS) ---")
    print(f"Sorgente: {PATH_SORGENTE}")
    print(f"Destinazione: {PATH_OUTPUT_PULITO}")
    print("ATTENZIONE: Le righe con testo vuoto/nullo dopo la pulizia verranno eliminate.")

    try:
        if not PATH_SORGENTE.is_dir():
            print(f"ERRORE CRITICO: Cartella sorgente non trovata: {PATH_SORGENTE}")
            return
            
        os.makedirs(PATH_OUTPUT_PULITO, exist_ok=True)

        # --- STEP 1: Carica l'intero dataset in RAM con Pandas ---
        print(f" -> Caricamento di {PATH_SORGENTE} in memoria (può richiedere tempo)...")
        # Pandas legge automaticamente tutti i file Parquet in una cartella
        df = pd.read_parquet(PATH_SORGENTE, engine='pyarrow')
        print(f" -> Caricamento completato. {len(df):,} righe caricate.")

        # --- STEP 2: Applica la pulizia ---
        print(" -> Avvio mappatura domini...")
        df['domain_root'] = df['url'].apply(get_base_domain)
        
        print(f" -> Avvio pulizia di {len(df):,} righe (può richiedere tempo)...")
        # Inizializza tqdm per Pandas
        tqdm.pandas(desc="Pulizia righe")
        # Applica la funzione robusta (catturerà l'errore \p)
        df['text'] = df.progress_apply(apply_robust_cleaner, axis=1)
        
        print(" -> Pulizia completata.")

        # --- STEP 3: Filtra le righe vuote ---
        print(" -> Rimozione righe con testo vuoto/nullo...")
        original_count = len(df)
        df = df.dropna(subset=['text'])
        df = df[df['text'] != '']
        final_count = len(df)
        print(f" -> Rimosse {original_count - final_count:,} righe vuote.")
        print(f" -> Righe rimanenti: {final_count:,}")

        # --- STEP 4: Pulisci e Salva ---
        # Rimuovi la colonna temporanea
        df = df.drop(columns=['domain_root'])
        
        print(f" -> Salvataggio del dataset pulito in {PATH_OUTPUT_PULITO}...")
        df.to_parquet(
            PATH_OUTPUT_PULITO,
            write_index=False,
            engine='pyarrow',
            compression='snappy' # Aggiungi compressione
        )

        print("\n--- ✅ OPERAZIONE COMPLETATA ---")
        print(f"Dataset pulito (senza righe vuote) salvato con successo in:")
        print(f"{PATH_OUTPUT_PULITO}")

    except Exception as e:
        print(f"\nERRORE INASPETTATO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Assicurati di avere le dipendenze:
    # pip install pandas pyarrow tqdm
    
    run_cleaning_pipeline_pandas()