import json
import csv
from collections import defaultdict
import re
from typing import List, Dict, Tuple
from tqdm import tqdm
import multiprocessing as mp
from functools import partial
import boto3
from urllib.parse import urlparse
import io
import os
import pandas as pd
import threading 
import queue
import time


# --- 1. SITI AUTOREVOLI ---
# (Definiti globalmente per semplicità)
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


# --- 2. FUNZIONI FILTRO "DELTA" ---

def is_allowed_correctly(url_netloc: str, allowed_domains: set) -> bool:
    """
    Il filtro CORRETTO che avremmo dovuto usare:
    Controlla se il netloc è il dominio o un suo sottodominio.
    """
    for allowed_domain in allowed_domains:
        if url_netloc == allowed_domain or url_netloc.endswith("." + allowed_domain):
            return True
    return False

def is_allowed_flawed(url_netloc: str, allowed_domains: set) -> bool:
    """
    Il filtro FALLATO originale: rimuove 'www.' 
    e cerca una corrispondenza esatta.
    """
    domain = url_netloc.replace("www.", "")
    return domain in allowed_domains


# --- 3. FUNZIONI DI ELABORAZIONE (Invariate) ---
# Queste funzioni DEVONO essere globali per essere 
# usate dalla pool di multiprocessing.

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
        return None

def process_skipped_article(url_entries_tuple):
    url, entries = url_entries_tuple
    return {"url": url, "text": "", "date": entries[0]['date'][:10]}


# --- 4. WORKER THREADS PER LA PIPELINE ---

def worker_downloader(s3_input_bucket: str, 
                      file_key_queue: queue.Queue, 
                      processing_queue: queue.Queue):
    """
    Worker Thread: Legge dalla coda dei file, scarica da S3, 
    mette il contenuto nella coda di elaborazione.
    """
    # I client Boto3 NON sono thread-safe, ne creiamo uno per thread
    s3_client = boto3.client('s3')
    
    while True:
        file_key = file_key_queue.get()
        if file_key is None: # "Poison pill"
            file_key_queue.task_done()
            break
        
        try:
            obj = s3_client.get_object(Bucket=s3_input_bucket, Key=file_key)
            content = obj['Body'].read() 
            processing_queue.put((file_key, content))
        except Exception as e:
            print(f"Errore DOWNLOAD file {file_key}: {e}")
        
        file_key_queue.task_done()

def worker_processor(processing_queue: queue.Queue, 
                     upload_queue: queue.Queue, 
                     mp_pool: mp.Pool, 
                     allowed_domains: set):
    """
    Worker Thread: Legge dalla coda di elaborazione, parsa il contenuto,
    usa la mp.Pool per la ricostruzione (CPU-bound),
    mette il DataFrame nella coda di upload.
    
    """
    while True:
        item = processing_queue.get()
        if item is None: # "Poison pill"
            processing_queue.task_done()
            break
            
        file_key, content = item
        
        try:
            articles_to_reconstruct = defaultdict(list)
            articles_to_skip = defaultdict(list)
            
            with io.TextIOWrapper(io.BytesIO(content), encoding='utf-8') as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                        url = entry.get("url")
                        if not url: continue
                        
                        netloc = urlparse(url).netloc
                        
                        # --- <<< Logica Filtro "DELTA" >>> ---
                        # CONDIZIONE: 
                        # 1. Deve passare il filtro CORRETTO (è un sottodominio valido)
                        # E
                        # 2. Deve FALLIRE il filtro FALLATO (è stato saltato la prima volta)
                        
                        if is_allowed_correctly(netloc, allowed_domains) and \
                           not is_allowed_flawed(netloc, allowed_domains):
                            
                            # È un sottodominio che ci siamo persi! Processiamolo.
                            if (entry.get("type") == 1):
                                articles_to_reconstruct[url].append(entry)
                            else:
                                articles_to_skip[url].append(entry)

                    except json.JSONDecodeError:
                        continue
            
            # --- Elaborazione (light CPU) ---
            transformed_articles = transform_dict(articles_to_reconstruct)
            work_items_reconstruct = list(transformed_articles.items())
            work_items_skip = list(articles_to_skip.items())
            results = []
            
            # --- Elaborazione (Heavy CPU) ---
            if work_items_reconstruct:
                process_func = partial(process_article)
                for result in mp_pool.imap_unordered(process_func, work_items_reconstruct, chunksize=10):
                    if result: results.append(result)
            
            for item_skip in work_items_skip:
                results.append(process_skipped_article(item_skip))
            
            if results:
                df = pd.DataFrame(results)
                upload_queue.put((file_key, df))
            
        except Exception as e:
            print(f"Errore PROCESSO file {file_key}: {e}")
        
        processing_queue.task_done()

def worker_uploader(s3_output_bucket: str, 
                    s3_output_prefix: str, 
                    upload_queue: queue.Queue, 
                    checkpoint_queue: queue.Queue):
    """
    Worker Thread: Legge dalla coda di upload, carica il Parquet su S3,
    mette il file_key nella coda di checkpoint.
    """
    s3_client = boto3.client('s3')
    
    if not s3_output_prefix.endswith('/'):
        s3_output_prefix += '/'
        
    while True:
        item = upload_queue.get()
        if item is None: # "Poison pill"
            upload_queue.task_done()
            break
            
        file_key, df = item
        
        try:
            base_name = os.path.basename(file_key)
            output_name = base_name.replace('.json', '.parquet')
            output_s3_path = f"s3://{s3_output_bucket}/{s3_output_prefix}{output_name}"

            df.to_parquet(
                output_s3_path, 
                index=False, 
                engine='pyarrow'
            )
            
            checkpoint_queue.put(file_key)
            
        except Exception as e:
            print(f"Errore UPLOAD file {file_key} (path: {output_s3_path}): {e}")
        
        upload_queue.task_done()

def worker_checkpointer(checkpoint_queue: queue.Queue, 
                        log_file_handle: io.TextIOWrapper, 
                        pbar: tqdm):
    """
    Worker Thread (SINGOLO): Legge dalla coda di checkpoint, 
    scrive sul file di log e aggiorna la progress bar.
    """
    while True:
        file_key = checkpoint_queue.get()
        if file_key is None: # "Poison pill"
            checkpoint_queue.task_done()
            break
            
        try:
            log_file_handle.write(f"{file_key}\n")
            log_file_handle.flush() # Forza la scrittura su disco
            pbar.update(1)
        except Exception as e:
            print(f"Errore CHECKPOINT file {file_key}: {e}")
            
        checkpoint_queue.task_done()


# --- 5. WORKER DI MONITORAGGIO (Invariato) ---

def worker_monitor(stop_event: threading.Event, 
                   pbar: tqdm,
                   queues_dict: Dict[str, queue.Queue], 
                   interval: int = 10):
    """
    Worker Thread (SINGOLO): Stampa la dimensione delle code 
    periodicamente.
    """
    while not stop_event.is_set():
        try:
            status_parts = []
            for name, q in queues_dict.items():
                status_parts.append(f"{name}: {q.qsize():<4}") 
            
            pbar.write(f"STATUS | {' | '.join(status_parts)}")
            
            stop_event.wait(interval)
            
        except Exception as e:
            pbar.write(f"Errore MONITOR: {e}")
            time.sleep(interval)


# --- 6. FUNZIONE PRINCIPALE (Modificata per il DELTA) ---

def main_pipeline_process_s3_DELTA(s3_input_bucket, s3_input_prefix,
                                 s3_output_bucket, s3_output_prefix,
                                 allowed_domains, 
                                 num_downloaders=10, 
                                 num_processors=4,
                                 num_uploaders=10):
    
    CHECKPOINT_FILE = 'processed_files_FIX_checkpoint.log'
    processed_files = set()
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            processed_files = {line.strip() for line in f}
        print(f"Caricati {len(processed_files)} file già processati dal log DELTA.")
    except FileNotFoundError:
        print("Nessun file di checkpoint DELTA trovato, si parte da zero.")
    
    # --- Setup Code ---
    file_key_queue = queue.Queue(maxsize=num_downloaders * 2)
    processing_queue = queue.Queue(maxsize=num_processors * 2)
    upload_queue = queue.Queue(maxsize=num_uploaders * 2)
    checkpoint_queue = queue.Queue()
    
    with open(CHECKPOINT_FILE, 'a') as log_file:
        
        num_mp_cores = max(1, mp.cpu_count() - 2)
        print(f"Avvio M-Pool condivisa con {num_mp_cores} core.")
        with mp.Pool(processes=num_mp_cores) as mp_pool:
            
            pbar = tqdm(total=0, desc="Elaborazione file DELTA", unit="file")
            threads = []

            # --- Avvia il monitor ---
            monitor_stop_event = threading.Event()
            queues_dict = {
                "Download": file_key_queue,
                "Process": processing_queue,
                "Upload": upload_queue,
                "Checkpoint": checkpoint_queue
            }
            mon_thread = threading.Thread(
                target=worker_monitor, 
                args=(monitor_stop_event, pbar, queues_dict, 10),
                daemon=True
            )
            mon_thread.start()
            
            # --- Avvia i worker ---
            # 1. Checkpointer (1 thread)
            cp_thread = threading.Thread(
                target=worker_checkpointer, 
                args=(checkpoint_queue, log_file, pbar),
                daemon=True
            )
            cp_thread.start()
            threads.append(cp_thread)

            # 2. Uploaders (N thread)
            for _ in range(num_uploaders):
                t = threading.Thread(
                    target=worker_uploader,
                    args=(s3_output_bucket, s3_output_prefix, upload_queue, checkpoint_queue),
                    daemon=True
                )
                t.start()
                threads.append(t)
            
            # 3. Processors (N thread)
            for _ in range(num_processors):
                t = threading.Thread(
                    target=worker_processor,
                    args=(processing_queue, upload_queue, mp_pool, allowed_domains),
                    daemon=True
                )
                t.start()
                threads.append(t)
                
            # 4. Downloaders (N thread)
            for _ in range(num_downloaders):
                t = threading.Thread(
                    target=worker_downloader,
                    args=(s3_input_bucket, file_key_queue, processing_queue),
                    daemon=True
                )
                t.start()
                threads.append(t)

            # --- 5. Main Thread: Riempie la coda di download ---
            print("Avvio scansione S3 per trovare nuovi file...")
            s3_client = boto3.client('s3')
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=s3_input_bucket, Prefix=s3_input_prefix)
            
            total_files_found = 0
            for page in tqdm(pages, desc="Scansione Pagine S3"):
                for file_summary in page.get('Contents', []):
                    file_key = file_summary['Key']
                    if file_key not in processed_files and file_key.endswith('.json'):
                        file_key_queue.put(file_key)
                        total_files_found += 1
            
            print(f"Scansione S3 completata. Trovati {total_files_found} nuovi file da scansionare per il DELTA.")
            pbar.total = total_files_found
            pbar.refresh() 

            # --- 6. Avvia l'arresto controllato ---
            for _ in range(num_downloaders):
                file_key_queue.put(None)
            
            print("In attesa del completamento dei download...")
            file_key_queue.join()
            print("Tutti i download completati.")

            for _ in range(num_processors):
                processing_queue.put(None)
                
            print("In attesa del completamento dei processi...")
            processing_queue.join()
            print("Tutti i processi completati.")
            
            for _ in range(num_uploaders):
                upload_queue.put(None)

            print("In attesa del completamento degli upload...")
            upload_queue.join()
            print("Tutti gli upload completati.")
            
            checkpoint_queue.put(None)
            print("In attesa del completamento del checkpoint...")
            checkpoint_queue.join()
            print("Checkpoint completato.")
            
            for t in threads:
                t.join()

            pbar.close()
            
            print("Arresto del monitor...")
            monitor_stop_event.set()
            mon_thread.join()
            
    print("Elaborazione pipeline DELTA completata.")


# --- 7. ESECUZIONE (Modificata per il DELTA) ---

if __name__ == "__main__":
    
    # --- Parametri di Configurazione ---
    S3_INPUT_BUCKET = "hybrid-rag-gdelt-bucket"
    S3_INPUT_PREFIX = "" # Legge gli STESSI JSON di input
    
    S3_OUTPUT_BUCKET = "hybrid-rag-gdelt-bucket"
    
    S3_OUTPUT_PREFIX = "gdelt_reconstructed_parquet_FIX/"
    
    main_pipeline_process_s3_DELTA(
        s3_input_bucket=S3_INPUT_BUCKET,
        s3_input_prefix=S3_INPUT_PREFIX,
        s3_output_bucket=S3_OUTPUT_BUCKET,
        s3_output_prefix=S3_OUTPUT_PREFIX,
        allowed_domains=allowed_domains_set,
        num_downloaders=40, # Esempio: 40 thread per scaricare
        num_processors=8,   # Esempio: 8 thread per parsare (che usano la mp_pool)
        num_uploaders=10    # Esempio: 10 thread per caricare
    )