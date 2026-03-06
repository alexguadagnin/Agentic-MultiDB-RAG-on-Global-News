import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from pathlib import Path
import pandas as pd
import sys
import os
import re 
from tqdm import tqdm 
from collections import Counter 
from botocore.exceptions import ClientError
from urllib.parse import urlparse
from typing import List, Dict, Tuple # Import corretto

##############################################################
# ----- CREA PAEQUET COMPLETO (LOCALI + FIX + SCRAPING) -----
##############################################################

# --- 1. CONFIGURAZIONE PATH ---

# Tentativo di importare la root del progetto e i path
try:
    from Hybrid_RAG.constants import PROJECT_ROOT, RAW_DATA_DIR_NGRAMS
    print(f"Path Cartella Genitore Ngrams (da constants): {RAW_DATA_DIR_NGRAMS}")
    print(f"Path Root Progetto (da constants): {PROJECT_ROOT}")
except ImportError:
    print("ATTENZIONE: Impossibile importare da Hybrid_RAG.constants.")
    PROJECT_ROOT_FALLBACK = Path("D:/progetto-rag-gdelt")
    DATA_DIR_FALLBACK = PROJECT_ROOT_FALLBACK / "data"
    RAW_DATA_DIR_NGRAMS = DATA_DIR_FALLBACK / "gdelt_ngrams"
    PROJECT_ROOT = PROJECT_ROOT_FALLBACK
    print(f"Uso path di fallback: RAW_DATA_DIR_NGRAMS={RAW_DATA_DIR_NGRAMS}, PROJECT_ROOT={PROJECT_ROOT}")

# --- Sorgenti ---
PATH_NGRAM_ORIGINALE = RAW_DATA_DIR_NGRAMS / "parquet_data_locale"
PATH_NGRAM_FIX = RAW_DATA_DIR_NGRAMS / "parquet_fix_locale"
FILE_JL_SCRAPING = PROJECT_ROOT / "src" / "Hybrid_RAG" / "news_scraper" / "output.jl"
CSV_MASTER_LIST = PROJECT_ROOT / "url_metadata_filtered.csv"

# --- Destinazioni ---
PATH_OUTPUT_FINALE = RAW_DATA_DIR_NGRAMS / "parquet_dati_completi"
FILE_URL_MANCANTI = PROJECT_ROOT / "url_ancora_mancanti.txt"


# --- 2. FUNZIONE DI ELABORAZIONE ---
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

def transform_dict(original_dict: Dict) -> Dict:
    transformed_data = {}
    for url, entries in original_dict.items():
        transformed_entries = []
        for entry in entries:
            pre = entry.get('pre', '') or ''
            ngram = entry.get('ngram', '') or ''
            post = entry.get('post', '') or ''
            sentence = ' '.join([pre, ngram, post])
            pos_val = entry.get('pos', 999)
            try: pos_int = int(pos_val)
            except (ValueError, TypeError): pos_int = 999
            if pos_int < 20 and " / " in sentence:
                parts = sentence.split(" / ")
                if len(parts) > 1:
                    sentence = " / ".join(parts[1:])
            transformed_entries.append({
                'date': entry.get('date', 'YYYY-MM-DD'),
                'lang': entry.get('lang', 'un'),
                'type': entry.get('type', -1),
                'pos': pos_int,
                'sentence': sentence.strip()
            })
        transformed_data[url] = transformed_entries
    return transformed_data

def reconstruct_sentence(fragments: List[str], positions: List[int] = None) -> str:
    if not fragments: return ""
    if len(fragments) == 1: return fragments[0]
    if positions:
        indexed_fragments = sorted(zip(positions, fragments), key=lambda x: x[0])
    else:
        indexed_fragments = [(i, f) for i, f in enumerate(fragments)]
    ordered_fragments = [f for pos, f in indexed_fragments if f]
    if not ordered_fragments: return ""
    result_text = ordered_fragments[0]
    used_indices_in_ordered_list = {0} 
    
    while len(used_indices_in_ordered_list) < len(ordered_fragments):
        best_overlap = -1
        best_fragment_index = -1
        append_at_end = True
        
        for i in range(len(ordered_fragments)):
            if i in used_indices_in_ordered_list: continue
            fragment_text = ordered_fragments[i]
            max_check = min(len(result_text), len(fragment_text), 30)
            
            for k in range(max_check, 0, -1):
                if result_text.endswith(fragment_text[:k]):
                    if k > best_overlap:
                        best_overlap = k; best_fragment_index = i; append_at_end = True
                    break
            for k in range(max_check, 0, -1):
                if result_text.startswith(fragment_text[-k:]):
                     if k > best_overlap:
                        best_overlap = k; best_fragment_index = i; append_at_end = False
                     break
        
        if best_fragment_index != -1:
            fragment_to_add = ordered_fragments[best_fragment_index]
            if append_at_end:
                result_text += fragment_to_add[best_overlap:]
            else:
                result_text = fragment_to_add[:-best_overlap] + result_text
            used_indices_in_ordered_list.add(best_fragment_index)
        else:
            break
            
    return result_text

def remove_overlap(text: str) -> str:
    if len(text) < 50: return text
    n = len(text)
    for k in range(n // 2, 5, -1):
        if text.startswith(text[-k:]):
            return text[k:]
    return text

def process_article(url_entries_tuple):
    try:
        url, entries = url_entries_tuple
        if not entries: return None
        valid_entries = []
        for e in entries:
            try: e['pos'] = int(e.get('pos', 999)); valid_entries.append(e)
            except (ValueError, TypeError): e['pos'] = 999; valid_entries.append(e)
        valid_entries.sort(key=lambda x: x['pos'])
        sentences = [e.get('sentence', '') for e in valid_entries]
        positions = [e['pos'] for e in valid_entries]
        reconstructed_sentences = reconstruct_sentence(sentences, positions)
        text = remove_overlap(reconstructed_sentences)
        textok = text.replace("|", " ").replace('"', " ").strip()
        textok = re.sub(r'\s+', ' ', textok)
        if len(textok) < 150: return None
        first_date = valid_entries[0].get('date', 'YYYY-MM-DD')[:10]
        return {"url": url, "text": textok, "date": first_date}
    except Exception as e:
        print(f"ERROR in process_article for {url}: {e}")
        return None

def process_skipped_article(url_entries_tuple):
    url, entries = url_entries_tuple
    date = entries[0]['date'][:10] if entries and 'date' in entries[0] else 'YYYY-MM-DD'
    return {"url": url, "text": "", "date": date}

# --- 3. FUNZIONE PRINCIPALE DI CONSOLIDAMENTO ---

def consolida_dati():
    print("--- AVVIO CONSOLIDAMENTO DATASET ---")
    
    # Verifica che tutti i file di input esistano
    if not (PATH_NGRAM_ORIGINALE.is_dir() and os.listdir(str(PATH_NGRAM_ORIGINALE))) and \
       not (PATH_NGRAM_FIX.is_dir() and os.listdir(str(PATH_NGRAM_FIX))):
        print(f"ERRORE: Le cartelle Parquet N-Gram sono vuote o non trovate.")
        return
        
    if not FILE_JL_SCRAPING.exists():
        print(f"ERRORE: File di scraping '{FILE_JL_SCRAPING}' non trovato.")
        return
        
    if not CSV_MASTER_LIST.exists():
        print(f"ERRORE: File master CSV '{CSV_MASTER_LIST}' non trovato.")
        return

    print("\nStep 1/5: Caricamento Lista URL Master e Metadati (da CSV)...")
    try:
        # Usiamo Dask per leggere il CSV (provando prima UTF-8, poi UTF-16)
        try:
            ddf_meta = dd.read_csv(
                str(CSV_MASTER_LIST),
                encoding='utf-8',
                on_bad_lines='skip',
                dtype={'url': 'str', 'tone': 'float'}
            )
        except UnicodeDecodeError:
            print(" -> Lettura UTF-8 fallita. Tento con UTF-16...")
            ddf_meta = dd.read_csv(
                str(CSV_MASTER_LIST),
                encoding='utf-16',
                on_bad_lines='skip',
                dtype={'url': 'str', 'tone': 'float'}
            )
        
        ddf_meta = ddf_meta.set_index('url')
        print(f" -> Caricato CSV Master. Colonne: {list(ddf_meta.columns)}")
    except Exception as e:
        print(f"ERRORE lettura CSV Master: {e}")
        return

    print("\nStep 2/5: Caricamento Dati N-Gram (da Parquet)...")
    try:
        LISTA_FILE_PARQUET_NGRAM = []
        print(" -> Ricerca file Parquet N-Gram...")
        if PATH_NGRAM_ORIGINALE.is_dir():
            files_originali = list(PATH_NGRAM_ORIGINALE.glob('*.parquet'))
            LISTA_FILE_PARQUET_NGRAM.extend(files_originali)
            print(f" -> Trovati {len(files_originali)} file in: {PATH_NGRAM_ORIGINALE.name}")
        if PATH_NGRAM_FIX.is_dir():
            files_fix = list(PATH_NGRAM_FIX.glob('*.parquet'))
            LISTA_FILE_PARQUET_NGRAM.extend(files_fix)
            print(f" -> Trovati {len(files_fix)} file in: {PATH_NGRAM_FIX.name}")

        if not LISTA_FILE_PARQUET_NGRAM:
             print("\nERRORE CRITICO: Nessun file Parquet N-Gram trovato. Arresto.")
             return
        
        print(f" -> Totale file Parquet N-Gram da caricare: {len(LISTA_FILE_PARQUET_NGRAM)}")
        
        paths_str = [str(p) for p in LISTA_FILE_PARQUET_NGRAM]
        ddf_ngram = dd.read_parquet(
            paths_str,
            columns=['url', 'text', 'date'],
            engine='pyarrow'
        ).rename(columns={'text': 'text_ngram', 'date': 'date_ngram'}).set_index('url')
        
        print(f" -> Caricati dati N-Gram. Colonne: {list(ddf_ngram.columns)}")
    except Exception as e:
        print(f"ERRORE lettura Parquet N-Gram: {e}")
        return

    print("\nStep 3/5: Caricamento Dati Scraping (da .jl)...")
    try:
        ddf_scraped = dd.read_json(
            str(FILE_JL_SCRAPING),
            lines=True,
            dtype={'url': 'str', 'title': 'str', 'body_text': 'str', 'author': 'str', 'date': 'str'}
        ).rename(columns={
            'body_text': 'text_scraped',
            'title': 'title_scraped',
            'author': 'author_scraped',
            'date': 'date_scraped'
        })
        
        # Prima rimuovi i duplicati sulla colonna 'url', POI imposta l'indice
        ddf_scraped = ddf_scraped.drop_duplicates(subset=['url'], keep='last')
        ddf_scraped = ddf_scraped.set_index('url')
        
        print(f" -> Caricati dati Scraped. Colonne: {list(ddf_scraped.columns)}")
    except Exception as e:
        print(f"ERRORE lettura file .jl: {e}")
        import traceback
        traceback.print_exc()
        return

    print("\nStep 4/5: Unione e consolidamento dei dati...")
    try:
        ddf_merged = ddf_meta.join(ddf_ngram, how='left')
        ddf_final = ddf_merged.join(ddf_scraped, how='left')

        # Consolidamento
        ddf_final['text'] = ddf_final['text_scraped'].fillna(ddf_final['text_ngram'])
        ddf_final['title'] = ddf_final['title_scraped']
        ddf_final['author'] = ddf_final['author_scraped']
        ddf_final['date'] = ddf_final['date'].fillna(ddf_final['date_scraped']).fillna(ddf_final['date_ngram'])
        
        colonne_output = [
            'text', 'title', 'author', 'date', 
            'enhancedthemes', 'allnames', 'locations', 'tone'
        ]
        
        colonne_finali_da_salvare = [col for col in colonne_output if col in ddf_final.columns]
                
        ddf_output = ddf_final[colonne_finali_da_salvare].reset_index()
        
        task_salva_parquet = ddf_output.to_parquet(
            PATH_OUTPUT_FINALE,
            write_index=False,
            engine='pyarrow',
            compute=False 
        )
        
        ddf_mancanti = ddf_final[
            ddf_final['text'].isnull() | (ddf_final['text'] == '')
        ]
        task_url_mancanti = ddf_mancanti.index.compute()

        print("Avvio elaborazione in parallelo (Salvataggio Parquet e Calcolo URL mancanti)...")
        with ProgressBar():
            (risultati_parquet, urls_mancanti_series) = dd.compute(
                task_salva_parquet,
                task_url_mancanti
            )
            
        print(" -> Dati Parquet finali salvati con successo.")
    
    except Exception as e:
        print(f"\nERRORE durante l'unione o il salvataggio: {e}")
        import traceback
        traceback.print_exc()
        return

    # --- STEP 5: Scrittura file TXT mancanti ---
    try:
        total_mancanti = len(urls_mancanti_series)
        print(f"\nStep 5/5: Salvataggio di {total_mancanti:,} URL ancora mancanti...")
        
        with open(FILE_URL_MANCANTI, 'w', encoding='utf-8') as f:
            f.write(f"# Lista URL (da {CSV_MASTER_LIST}) per cui il testo è ancora mancante\n")
            f.write(f"# (non trovato in N-Gram e non trovato nello scraping)\n")
            f.write(f"# Totale: {total_mancanti}\n")
            f.write("# ------------------------------------------------------------------\n")
            pd.Series(urls_mancanti_series, name="url").to_csv(f, index=False, header=False, lineterminator='\n')
        
        print(f" -> File TXT '{FILE_URL_MANCANTI}' salvato.")
        
    except Exception as e:
        print(f"ERRORE durante la scrittura del file TXT '{FILE_URL_MANCANTI}': {e}")

    print("\n--- ✅ OPERAZIONE COMPLETATA ---")
    print(f"Dataset finale consolidato salvato in: {PATH_OUTPUT_FINALE}")

if __name__ == "__main__":
    consolida_dati()