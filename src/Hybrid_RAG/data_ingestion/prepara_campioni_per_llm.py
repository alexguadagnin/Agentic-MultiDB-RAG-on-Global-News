import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from pathlib import Path
import pandas as pd
import sys
import os
from urllib.parse import urlparse
import json

# --- 1. CONFIGURAZIONE ---

# Importa i path
try:
    from Hybrid_RAG.constants import RAW_DATA_DIR_NGRAMS, PROJECT_ROOT
    print(f"Path Cartella Genitore Ngrams (da constants): {RAW_DATA_DIR_NGRAMS}")
except ImportError:
    print("ATTENZIONE: Impossibile importare da Hybrid_RAG.constants.")
    PROJECT_ROOT_FALLBACK = Path("D:/progetto-rag-gdelt")
    DATA_DIR_FALLBACK = PROJECT_ROOT_FALLBACK / "data"
    RAW_DATA_DIR_NGRAMS = DATA_DIR_FALLBACK / "gdelt_ngrams"
    PROJECT_ROOT = PROJECT_ROOT_FALLBACK
    print(f"Uso path di fallback: RAW_DATA_DIR_NGRAMS={RAW_DATA_DIR_NGRAMS}, PROJECT_ROOT={PROJECT_ROOT}")

# Path al dataset finale e pulito
PATH_SORGENTE = RAW_DATA_DIR_NGRAMS / "parquet_dati_completi"

# File di output per i campioni
FILE_OUTPUT_CAMPIONI = PROJECT_ROOT / "campioni_per_llm.jsonl"

# Quanti campioni per dominio
N_CAMPIONI = 10
# Lunghezza snippet di testo (per non superare i limiti di token)
SNIPPET_SIZE = 2000000

# --- 2. LISTA DOMINI (Necessaria per mappare i sottodomini) ---
siti_autorevoli = {
    # Nord America
    "New York Times": "nytimes.com", "Washington Post": "washingtonpost.com",
    "Wall Street Journal": "wsj.com", "Los Angeles Times": "latimes.com",
    "USA Today": "usatoday.com", "Bloomberg": "bloomberg.com",
    # Europa
    "BBC": "bbc.com", "The Guardian": "theguardian.com", "The Times": "thetimes.co.uk",
    "Financial Times": "ft.com", "The Independent": "independent.co.uk",
    "The Telegraph": "telegraph.co.uk", "Le Monde": "lemonde.fr", "Le Figaro": "lefigaro.fr",
    "Libération": "liberation.fr", "Der Spiegel": "spiegel.de",
    "Frankfurter Allgemeine Zeitung": "faz.net", "Die Zeit": "zeit.de",
    "Süddeutsche Zeitung": "sueddeutsche.de", "Corriere della Sera": "corriere.it",
    "La Repubblica": "repubblica.it", "Il Sole 24 Ore": "ilsole24ore.com", "La Stampa": "lastampa.it",
    "Il Fatto Quotidiano": "ilfattoquotidiano.it", "El Pais": "elpais.com", "El Mundo": "elmundo.es",
    "ABC": "abc.es", "Russia Today": "rt.com", "TASS": "tass.ru", "Dagens Nyheter": "dn.se",
    "Svenska Dagbladet": "svd.se", "Le Soir": "lesoir.be", "De Standaard": "standaard.be",
    "NRC Handelsblad": "nrc.nl", "De Volkskrant": "volkskrant.nl",
    "Neue Zürcher Zeitung": "nzz.ch",
    # Asia
    "The Japan Times": "japantimes.co.jp", "Asahi Shimbun": "asahi.com", "Mainichi Shimbun": "mainichi.jp",
    "Yomiuri Shimbun": "yomiuri.co.jp", "China Daily": "chinadaily.com.cn",
    "South China Morning Post": "scmp.com", "Global Times": "globaltimes.cn", "The Hindu": "thehindu.com",
    "Times of India": "timesofindia.indiatimes.com", "Hindustan Times": "hindustantimes.com",
    "The Korea Herald": "koreaherald.com", "The Korea Times": "koreatimes.co.kr",
    "Straits Times": "straitstimes.com", "Bangkok Post": "bangkokpost.com", "The Star": "thestar.com.my",
    # America Latina
    "Clarín": "clarin.com", "La Nación (Argentina)": "lanacion.com.ar", "O Globo": "oglobo.globo.com",
    "Folha de São Paulo": "folha.uol.com.br", "El Comercio (Perù)": "elcomercio.pe",
    "El Universal (Mexico)": "eluniversal.com.mx", "Reforma": "reforma.com",
    "El Tiempo (Colombia)": "eltiempo.com", "El Mercurio (Chile)": "emol.com",
    # Africa
    "Mail & Guardian": "mg.co.za", "News24": "news24.com", "Daily Nation (Kenya)": "nation.africa",
    "The Guardian Nigeria": "guardian.ng", "Al Ahram": "ahram.org.eg", "Le Matin": "lematin.ma",
    # Oceania
    "The Sydney Morning Herald": "smh.com.au", "The Australian": "theaustralian.com.au",
    "New Zealand Herald": "nzherald.co.nz",
    # Agenzie
    "Reuters": "reuters.com", "Associated Press": "apnews.com",
    "Agence France-Presse": "afp.com", "Politico": "politico.com"
}
# Invertiamo il dizionario per mappare i valori (domini) ai nomi (chiavi)
domains_to_names = {v: k for k, v in siti_autorevoli.items()}

def get_base_domain(url: str) -> str:
    """
    Estrae il dominio radice (es. 'bbc.com') da un URL complesso 
    (es. 'https://www.bbc.com/news/123').
    Restituisce None se non è un dominio autorevole.
    """
    if not isinstance(url, str):
        return None
    try:
        netloc = urlparse(url).netloc
        for domain in domains_to_names.keys():
            if netloc == domain or netloc.endswith("." + domain):
                return domain # Restituisce il dominio radice (es. 'bbc.com')
        return None # Non è un dominio autorevole
    except Exception:
        return None

# --- 3. ESECUZIONE ---

def prepara_campioni():
    print(f"--- AVVIO PREPARAZIONE CAMPIONI PER LLM ---")
    print(f"Lettura da: {PATH_SORGENTE}")

    try:
        if not PATH_SORGENTE.is_dir():
            print(f"ERRORE CRITICO: Cartella sorgente non trovata: {PATH_SORGENTE}")
            return

        # Carica solo le colonne necessarie
        ddf = dd.read_parquet(
            PATH_SORGENTE,
            columns=['url', 'text'],
            engine='pyarrow'
        )

        # Rimuovi righe senza testo
        ddf = ddf.dropna(subset=['text'])
        # Crea snippet per ridurre i dati
        ddf['text_snippet'] = ddf['text'].str.slice(0, SNIPPET_SIZE)
        
        # Mappa gli URL al loro dominio radice autorevole
        ddf['domain'] = ddf['url'].apply(get_base_domain, meta=('domain', 'str'))
        
        # Rimuovi righe che non mappano a un dominio (non dovrebbe succedere, ma per sicurezza)
        ddf = ddf.dropna(subset=['domain'])
        
        print(" -> Avvio campionamento (5 elementi per dominio)...")
        with ProgressBar():
            # 'head(N)' è il modo più efficiente in Dask per prendere N elementi per gruppo
            campioni_ddf = ddf.groupby('domain').head(N_CAMPIONI)
            # Calcola e porta i risultati in memoria (sarà un DataFrame Pandas)
            campioni_df = campioni_ddf.compute()

        print(f" -> Campionamento completato. Trovati {len(campioni_df)} campioni totali.")

        # --- Salvataggio in JSONL ---
        print(f" -> Salvataggio campioni in {FILE_OUTPUT_CAMPIONI}...")
        
        # Riorganizza i dati per il salvataggio
        output_data = []
        # Raggruppa i risultati (ora in Pandas) per dominio
        for domain_root, group in campioni_df.groupby('domain'):
            domain_name = domains_to_names.get(domain_root, domain_root) # Ottieni il nome leggibile
            
            # Prepara gli snippet di testo
            text_samples = []
            for _, row in group.iterrows():
                text_samples.append({
                    "url": row['url'],
                    "text_snippet": row['text_snippet']
                })
            
            # Aggiungi un oggetto per riga nel file JSONL
            output_data.append({
                "domain_name": domain_name,
                "domain_root": domain_root,
                "samples": text_samples
            })

        # Scrivi su file JSON Lines (un dizionario per riga)
        with open(FILE_OUTPUT_CAMPIONI, 'w', encoding='utf-8') as f:
            for item in output_data:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')

        print(f"--- ✅ CAMPIONI PRONTI ---")
        print(f"File '{FILE_OUTPUT_CAMPIONI}' creato con {len(output_data)} domini.")
        print("Ora puoi usare 'Script 2' per inviare questi dati a un LLM.")

    except Exception as e:
        print(f"\nERRORE INASPETTATO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    prepara_campioni()