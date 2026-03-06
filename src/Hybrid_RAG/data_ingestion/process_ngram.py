import json
import csv
from collections import defaultdict
import re
from typing import List, Dict, Tuple
from tqdm import tqdm
import multiprocessing as mp
from functools import partial
import boto3  # Per AWS S3
from botocore.config import Config 
from urllib.parse import urlparse 
import io 
import os 
import pandas as pd 

# --- 1. SITI AUTOREVOLI ---
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
    "La Repubblica": "repubblica.it", "Il Sole 24 Ore": "ilsole24ore.com", 
    "La Stampa": "lastampa.it", "Il Fatto Quotidiano": "ilfattoquotidiano.it", 
    "El Pais": "elpais.com", "El Mundo": "elmundo.es", "ABC": "abc.es", 
    "Russia Today": "rt.com", "TASS": "tass.ru", "Dagens Nyheter": "dn.se", 
    "Svenska Dagbladet": "svd.se", "Le Soir": "lesoir.be", "De Standaard": "standaard.be", 
    "NRC Handelsblad": "nrc.nl", "De Volkskrant": "volkskrant.nl", 
    "Neue Zürcher Zeitung": "nzz.ch",
    # Asia
    "The Japan Times": "japantimes.co.jp", "Asahi Shimbun": "asahi.com", 
    "Mainichi Shimbun": "mainichi.jp", "Yomiuri Shimbun": "yomiuri.co.jp", 
    "China Daily": "chinadaily.com.cn", "South China Morning Post": "scmp.com", 
    "Global Times": "globaltimes.cn", "The Hindu": "thehindu.com", 
    "Times of India": "timesofindia.indiatimes.com", "Hindustan Times": "hindustantimes.com", 
    "The Korea Herald": "koreaherald.com", "The Korea Times": "koreatimes.co.kr", 
    "Straits Times": "straitstimes.com", "Bangkok Post": "bangkokpost.com", 
    "The Star": "thestar.com.my",
    # America Latina
    "Clarín": "clarin.com", "La Nación (Argentina)": "lanacion.com.ar", 
    "O Globo": "oglobo.globo.com", "Folha de São Paulo": "folha.uol.com.br", 
    "El Comercio (Perù)": "elcomercio.pe", "El Universal (Mexico)": "eluniversal.com.mx", 
    "Reforma": "reforma.com", "El Tiempo (Colombia)": "eltiempo.com", 
    "El Mercurio (Chile)": "emol.com",
    # Africa
    "Mail & Guardian": "mg.co.za", "News24": "news24.com", 
    "Daily Nation (Kenya)": "nation.africa", "The Guardian Nigeria": "guardian.ng", 
    "Al Ahram": "ahram.org.eg", "Le Matin": "lematin.ma",
    # Oceania
    "The Sydney Morning Herald": "smh.com.au", "The Australian": "theaustralian.com.au", 
    "New Zealand Herald": "nzherald.co.nz",
    # Agenzie
    "Reuters": "reuters.com", "Associated Press": "apnews.com", 
    "Agence France-Presse": "afp.com", "Politico": "politico.com"
}
allowed_domains_set = set(siti_autorevoli.values())


# --- 2. FUNZIONI DI ELABORAZIONE (Invariate) ---

def transform_dict(original_dict: Dict) -> Dict:
    transformed_data = {}
    for url, entries in original_dict.items():
        transformed_entries = []
        for entry in entries:
            sentence = ' '.join([entry['pre'], entry['ngram'], entry['post']])
            if int(entry['pos']) < 20 and " / " in sentence:
                parts = sentence.split(" / ")
                if len(parts) > 1:
                    sentence = " / ".join(parts[1:])
            transformed_entries.append({
                'date': entry['date'], 'lang': entry['lang'], 'type': entry['type'],
                'pos': entry['pos'], 'sentence': sentence
            })
        transformed_data[url] = transformed_entries
    return transformed_data

def reconstruct_sentence(fragments: List[str], positions: List[int] = None) -> str:
    if not fragments: return ""
    if len(fragments) == 1: return fragments[0]
    pos_map = {}
    if positions:
        pos_map = {i: pos for i, pos in enumerate(positions)}
    words_list = [fragment.split() for fragment in fragments]
    result_words = words_list[0]
    used = {0}
    while len(used) < len(fragments):
        best_overlap = 0
        best_fragment = -1
        best_is_prefix = False
        for i in range(len(fragments)):
            if i in used: continue
            words = words_list[i]
            min_len = min(len(result_words), len(words))
            if positions is None or pos_map.get(i, 0) + 10 >= pos_map.get(0, 0):
                for k in range(min_len, 0, -1):
                    if result_words[-k:] == words[:k] and k > best_overlap:
                        best_overlap = k; best_fragment = i; best_is_prefix = False; break
            if positions is None or pos_map.get(i, 0) - 10 <= pos_map.get(0, 0):
                for k in range(min_len, 0, -1):
                    if result_words[:k] == words[-k:] and k > best_overlap:
                        best_overlap = k; best_fragment = i; best_is_prefix = True; break
        if best_fragment == -1: break
        if best_is_prefix:
            result_words = words_list[best_fragment][:-best_overlap] + result_words
        else:
            result_words = result_words + words_list[best_fragment][best_overlap:]
        used.add(best_fragment)
    return ' '.join(result_words)

def remove_overlap(text: str) -> str:
    if len(text) < 2: return text
    max_check_len = len(text) // 2
    max_overlap_len = 0
    for i in range(1, max_check_len + 1):
        if text[:i] == text[-i:]: max_overlap_len = i
    if max_overlap_len > 0: return text[max_overlap_len:]
    return text

def process_article(url_entries_tuple):
    try:
        url, entries = url_entries_tuple
        entries.sort(key=lambda x: x['pos'])
        sentences = [entry['sentence'] for entry in entries]
        positions = [entry['pos'] for entry in entries]
        group_positions = [positions[i] for i in range(len(sentences))]
        reconstructed_sentences = reconstruct_sentence(sentences, group_positions)
        text = remove_overlap(reconstructed_sentences)
        textok = text.replace("|", " ").replace('"', " ").strip()
        textok = re.sub(r'\s+', ' ', textok)
        return {"url": url, "text": textok, "date": entries[0]['date'][:10]}
    except Exception as e:
        print(f"Errore durante la ricostruzione di {url}: {e}")
        return None

def process_skipped_article(url_entries_tuple):
    url, entries = url_entries_tuple
    return {"url": url, "text": "", "date": entries[0]['date'][:10]}


# --- 3. FUNZIONE PRINCIPALE ROBUSTA (MODIFICATA PER PARQUET) ---

def main_robust_process_s3(s3_input_bucket, s3_input_prefix,
                           s3_output_bucket, s3_output_prefix,
                           allowed_domains, num_processes=None):
    """
    Funzione robusta: legge da S3, processa e scrive 
    file Parquet multipli su S3.
    """
    
    # --- Setup Checkpointing ---
    CHECKPOINT_FILE = 'processed_files_checkpoint.log'
    processed_files = set()
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            processed_files = {line.strip() for line in f}
        print(f"Caricati {len(processed_files)} file già processati dal log.")
    except FileNotFoundError:
        print("Nessun file di checkpoint trovato, si parte da zero.")
    
    # --- Apri il log dei checkpoint (l'unico file locale) ---
    with open(CHECKPOINT_FILE, 'a') as log_file:
        
        if num_processes is None:
            # Usa tutti i core tranne uno per lasciare il PC utilizzabile
            num_processes = max(1, mp.cpu_count() - 1)
        
        # ACCELERATE_ENDPOINT = "https://hybrid-rag-gdelt-bucket.s3-accelerate.amazonaws.com"
        
        # --- MODIFICA 1: Risoluzione conflitto Boto3 ---
        # Diciamo esplicitamente a boto3 di NON usare il flag accelerate
        # (perché stiamo già fornendo l'URL endpoint)
        # s3_config = Config(s3={'use_accelerate_endpoint': False})
        # s3_client = boto3.client('s3', endpoint_url=ACCELERATE_ENDPOINT, config=s3_config)
        s3_client = boto3.client('s3')


        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=s3_input_bucket, Prefix=s3_input_prefix)
        
        # Il pool di processi viene creato una sola volta
        with mp.Pool(processes=num_processes) as pool:
            print(f"Avvio scansione S3 con {num_processes} processi...")
            
            for page in tqdm(pages, desc="Scansione Pagine S3"):
                files_to_process = []
                
                for file_summary in page.get('Contents', []):
                    file_key = file_summary['Key']
                    if file_key in processed_files: continue
                    if not file_key.endswith('.json'): continue
                    files_to_process.append(file_key)
                
                for file_key in tqdm(files_to_process, desc="Elaborazione File", leave=False):
                    try:
                        # --- Elaborazione File-per-File ---
                        articles_to_reconstruct = defaultdict(list)
                        articles_to_skip = defaultdict(list)
                        
                        obj = s3_client.get_object(Bucket=s3_input_bucket, Key=file_key)
                        with io.TextIOWrapper(obj['Body'], encoding='utf-8') as f:
                            for line in f:
                                try:
                                    entry = json.loads(line)
                                    url = entry.get("url")
                                    if not url: continue
                                    netloc = urlparse(url).netloc
                                    # Estrae il dominio principale controllando se finisce con uno dei domini permessi
                                    is_allowed = False
                                    for allowed_domain in allowed_domains:
                                        if netloc == allowed_domain or netloc.endswith("." + allowed_domain):
                                            is_allowed = True
                                            break

                                    if is_allowed:
                                        # Il filtro lingua mancava qui
                                        if (entry.get("type") == 1):
                                            articles_to_reconstruct[url].append(entry)
                                        else:
                                            articles_to_skip[url].append(entry)
                                except json.JSONDecodeError:
                                    continue
                        
                        # --- Trasforma ed Elabora (solo dati di questo file) ---
                        transformed_articles = transform_dict(articles_to_reconstruct)
                        work_items_reconstruct = list(transformed_articles.items())
                        work_items_skip = list(articles_to_skip.items())
                        results = []
                        
                        if work_items_reconstruct:
                            process_func = partial(process_article)
                            for result in pool.imap_unordered(process_func, work_items_reconstruct, chunksize=10):
                                if result: results.append(result)
                        
                        for item in work_items_skip:
                            results.append(process_skipped_article(item))
                        
                        # --- SCRITTURA SU S3 IN PARQUET ---
                        if results:
                            df = pd.DataFrame(results)
                            
                            base_name = os.path.basename(file_key)
                            output_name = base_name.replace('.json', '.parquet')
                            
                            # Assicura che il prefisso finisca con /
                            if not s3_output_prefix.endswith('/'):
                                s3_output_prefix += '/'
                                
                            output_s3_path = f"s3://{s3_output_bucket}/{s3_output_prefix}{output_name}"
                            
                            # --- MODIFICA 3: Risoluzione conflitto Pandas/s3fs ---
                            """
                            pandas_storage_options = {
                                'use_accelerate_endpoint': False, # Diciamo a s3fs di non usare il flag
                                'client_kwargs': {
                                    'endpoint_url': ACCELERATE_ENDPOINT # Usiamo solo l'URL
                                }
                            }

                            df.to_parquet(
                                output_s3_path, 
                                index=False, 
                                engine='pyarrow', 
                                storage_options=pandas_storage_options
                            )
                            """
                            df.to_parquet(
                                output_s3_path, 
                                index=False, 
                                engine='pyarrow' 
                                # Niente storage_options
                            )

                        # --- Checkpoint: Scriviamo sul log CHE ABBIAMO FINITO QUESTO FILE ---
                        log_file.write(f"{file_key}\n")
                        log_file.flush()
                        processed_files.add(file_key)

                    except Exception as e:
                        print(f"Errore critico durante l'elaborazione del file {file_key}: {e}")
                        print("Lo script verrà interrotto, ma riprenderà da questo file al prossimo avvio.")
                        return

    print("Elaborazione completata.")


# --- 4. ESECUZIONE ---

if __name__ == "__main__":
    
    # --- Parametri di Configurazione ---
    S3_INPUT_BUCKET = "hybrid-rag-gdelt-bucket"
    S3_INPUT_PREFIX = ""
    
    S3_OUTPUT_BUCKET = "hybrid-rag-gdelt-bucket" # Stesso bucket o uno diverso
    S3_OUTPUT_PREFIX = "gdelt_reconstructed_parquet/" # Un prefisso (cartella) diverso
    
    
    main_robust_process_s3(
        s3_input_bucket=S3_INPUT_BUCKET,
        s3_input_prefix=S3_INPUT_PREFIX,
        s3_output_bucket=S3_OUTPUT_BUCKET,
        s3_output_prefix=S3_OUTPUT_PREFIX,
        allowed_domains=allowed_domains_set,
        num_processes=None # Usa (N-1) core
    )