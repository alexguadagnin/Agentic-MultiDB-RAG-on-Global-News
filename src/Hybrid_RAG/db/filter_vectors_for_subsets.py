import psycopg2
import pandas as pd
import glob
import os
import sys
import shutil
from pathlib import Path
from tqdm import tqdm

# --- CONFIGURAZIONE ---
# Percorso base assoluto (quello che funzionava)
BASE_PATH = Path(r"...") 
SOURCE_PARQUET_DIR = BASE_PATH / "embedding_output"

# Credenziali SQL
DB_CONFIG = {
    'user': 'gdelt_admin',
    'password': 'strong_password_123',
    'host': 'localhost',
    'port': '5432'
}

# Subset attivi
SUBSETS = {
    "xs": "gdelt_xs",
    "s":  "gdelt_s",
    "m":  "gdelt_m",
    "l":  "gdelt_l",
    "xl": "gdelt_xl"
}

def get_valid_urls_from_sql(dbname):
    """Recupera URL 'normalizzati' (senza https://) dal DB."""
    print(f"🔌 Connessione a '{dbname}' per recuperare gli URL validi...")
    try:
        conn = psycopg2.connect(dbname=dbname, **DB_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT DocIdentifier_Normalized FROM ARTICLE")
        valid_urls = set(row[0] for row in cur.fetchall())
        conn.close()
        print(f"✅ Trovati {len(valid_urls)} articoli validi in {dbname}.")
        return valid_urls
    except Exception as e:
        print(f"❌ Errore connessione DB {dbname}: {e}")
        return set()

def normalize_url(url):
    """Rimuove il protocollo per far combaciare i dati."""
    if not isinstance(url, str): return ""
    return url.replace("https://", "").replace("http://", "")

def filter_parquet(tag, dbname):
    print(f"\n" + "="*60)
    print(f"--- GENERAZIONE FILE PARQUET PER: {tag.upper()} ---")
    print(f"="*60)
    
    # 1. Recupera la lista degli URL
    valid_urls = get_valid_urls_from_sql(dbname)
    if not valid_urls:
        print("⚠️ Nessun URL trovato (o errore DB). Salto.")
        return

    # 2. Prepara output
    output_dir = BASE_PATH / f"embedding_output_{tag}"
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 3. Leggi e Filtra
    if not SOURCE_PARQUET_DIR.exists():
        print(f"❌ ERRORE: Cartella sorgente non trovata: {SOURCE_PARQUET_DIR}")
        return

    parquet_files = sorted(glob.glob(str(SOURCE_PARQUET_DIR / "*.parquet")))
    total_vectors_saved = 0
    
    print(f"📂 Filtraggio {len(parquet_files)} file Parquet...")
    
    for f in tqdm(parquet_files, unit="file", desc=f"Subset {tag.upper()}"):
        try:
            df = pd.read_parquet(f)
            
            # --- LOGICA DI FILTRO CORRETTA (NORMALIZZAZIONE) ---
            # Per ogni riga:
            # 1. Prendi l'URL dal payload
            # 2. Togli 'https://' o 'http://'
            # 3. Controlla se è nel set valid_urls
            
            mask = []
            for row in df['payload']:
                if isinstance(row, dict):
                    raw_url = row.get('url', '')
                    # 🔥 LA FIX È QUI: Pulisci l'URL prima di controllare
                    clean_url = raw_url.replace("https://", "").replace("http://", "")
                    mask.append(clean_url in valid_urls)
                else:
                    mask.append(False)
            
            filtered_df = df[mask]
            
            if not filtered_df.empty:
                out_path = output_dir / Path(f).name
                filtered_df.to_parquet(out_path, index=False)
                total_vectors_saved += len(filtered_df)
                
        except Exception as e:
            print(f"⚠️ Errore file {Path(f).name}: {e}")

    print(f"\n✅ COMPLETATO {tag.upper()}!")
    print(f"📊 Vettori salvati: {total_vectors_saved}")
    print(f"💾 Cartella creata: {output_dir}")

if __name__ == "__main__":
    for tag, dbname in SUBSETS.items():
        filter_parquet(tag, dbname)