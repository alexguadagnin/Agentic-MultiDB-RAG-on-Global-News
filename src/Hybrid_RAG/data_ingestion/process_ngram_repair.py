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
import shutil # Per shutil.disk_usage e shutil.move
from pathlib import Path

# --- 1. CONFIGURAZIONE ---

# Path alla cartella locale da usare come buffer 
LOCAL_BUFFER_PATH = Path("...")
# Limite in GB per il buffer
MAX_BUFFER_GB = 500
# Path al file di checkpoint
CHECKPOINT_FILE = '...\processed_files_FIX_checkpoint.log' 

# Converti GB in Byte
MAX_BUFFER_BYTES = MAX_BUFFER_GB * (1024**3)

# Parametri S3
S3_INPUT_BUCKET = "hybrid-rag-gdelt-bucket"
S3_INPUT_PREFIX = "gdelt_ngrams/" 
S3_OUTPUT_BUCKET = "hybrid-rag-gdelt-bucket"
S3_OUTPUT_PREFIX = "gdelt_reconstructed_parquet_FIX/"

# Parametri Pipeline
NUM_S3_DOWNLOADERS = 10 # Thread per scaricare da S3 in parallelo
NUM_LOCAL_READERS = 12  # Thread che leggono da SSD (veloce) e filtrano
NUM_PROCESSORS = 8      # Thread che usano la MP Pool per CPU pesante
NUM_UPLOADERS = 10      # Thread che caricano su S3


# --- 2. SITI AUTOREVOLI ---
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


# --- 3. FUNZIONI FILTRO "DELTA" ---
def is_allowed_correctly(url_netloc: str, allowed_domains: set) -> bool:
    """Il filtro CORRETTO che include i sottodomini."""
    for allowed_domain in allowed_domains:
        if url_netloc == allowed_domain or url_netloc.endswith("." + allowed_domain):
            return True
    return False

def is_allowed_flawed(url_netloc: str, allowed_domains: set) -> bool:
    """Il filtro FALLATO originale."""
    domain = url_netloc.replace("www.", "")
    return domain in allowed_domains

# --- 4. FUNZIONI DI ELABORAZIONE ---
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
    if not words_list or not words_list[0]: return "" # Check if list or first element is empty
    result_words = words_list[0]
    used = {0}
    
    # Sort fragments based on position for potentially better initial guess
    # This assumes positions correspond to fragments index before splitting words
    if positions:
         sorted_indices = sorted(range(len(fragments)), key=lambda k: positions[k])
         if sorted_indices[0] != 0: # If the first fragment is not the one with the smallest pos
             first_index = sorted_indices[0]
             result_words = words_list[first_index]
             used = {first_index}
             # Swap 0 and first_index in positions mapping if needed for logic below
             # Or adjust the logic to start from first_index properly.
             # Simpler: just ensure the loop considers all fragments.

    while len(used) < len(fragments):
        best_overlap = 0
        best_fragment_index = -1
        append_at_end = True # True to append, False to prepend

        current_result_len = len(result_words)

        for i in range(len(fragments)):
            if i in used: continue

            current_fragment_words = words_list[i]
            if not current_fragment_words: continue # Skip empty fragments

            current_fragment_len = len(current_fragment_words)
            overlap_check_len = min(current_result_len, current_fragment_len, 15) # Limit check length

            # Check for appending (suffix of result matches prefix of fragment)
            for k in range(overlap_check_len, 0, -1):
                if result_words[-k:] == current_fragment_words[:k]:
                    if k > best_overlap:
                        best_overlap = k
                        best_fragment_index = i
                        append_at_end = True
                    break # Best possible suffix/prefix overlap found for this i

            # Check for prepending (prefix of result matches suffix of fragment)
            # Only consider prepending if it offers a strictly better overlap
            for k in range(overlap_check_len, 0, -1):
                 if result_words[:k] == current_fragment_words[-k:]:
                     if k > best_overlap: # Strictly better overlap needed to switch to prepend
                        best_overlap = k
                        best_fragment_index = i
                        append_at_end = False
                     break # Best possible prefix/suffix overlap found for this i

        if best_fragment_index == -1:
            # No overlap found, maybe append the largest remaining fragment?
            # Or just break. Breaking might be safer.
            break # Or find the next best fragment based on position?

        if append_at_end:
            result_words.extend(words_list[best_fragment_index][best_overlap:])
        else: # Prepend
            result_words = words_list[best_fragment_index][:-best_overlap] + result_words
        used.add(best_fragment_index)

    return ' '.join(result_words)


def remove_overlap(text: str) -> str:
    if len(text) < 50: return text # Skip short texts
    
    # More robust check for repeated phrases (simple version)
    words = text.split()
    n = len(words)
    if n < 10: return text

    # Check for large block overlaps (e.g., last half matches first half)
    half = n // 2
    if half > 10 and words[:half] == words[-half:]:
        return " ".join(words[half:])
    if half > 10 and words[n-half:] == words[:half]: # Check other direction too
         return " ".join(words[:-half])

    # Check for smaller, significant overlaps at start/end (e.g., 5-15 words)
    for k in range(min(15, n // 3), 4, -1): # Check overlaps from 15 down to 5 words
        if words[:k] == words[-k:]:
            # Found overlap at start and end
            return " ".join(words[k:]) # Keep the second half

    return text

def process_article(url_entries_tuple):
    try:
        url, entries = url_entries_tuple
        if not entries: return None

        # Ensure 'pos' is treated as integer for sorting
        entries.sort(key=lambda x: int(x.get('pos', 0)))
        sentences = [entry['sentence'] for entry in entries]
        positions = [int(entry.get('pos', 0)) for entry in entries]

        reconstructed_sentences = reconstruct_sentence(sentences, positions)
        text = remove_overlap(reconstructed_sentences)
        textok = text.replace("|", " ").replace('"', " ").strip()
        textok = re.sub(r'\s+', ' ', textok)

        if len(textok) < 100: # Increase minimum length threshold
             # print(f"Skipping short reconstruction for {url} (length: {len(textok)})")
             return None

        first_date = entries[0].get('date', 'YYYY-MM-DD')[:10]
        return {"url": url, "text": textok, "date": first_date}
    except Exception as e:
        print(f"ERROR in process_article for {url}: {e}")
        return None

def process_skipped_article(url_entries_tuple):
    url, entries = url_entries_tuple
    date = entries[0]['date'][:10] if entries and 'date' in entries[0] else 'YYYY-MM-DD'
    return {"url": url, "text": "", "date": date}

# --- 5. FUNZIONI HELPER ---

def get_dir_size_on_startup(path: Path) -> int:
    """Calcola la dimensione totale *all'avvio*. Lento, ma eseguito solo una volta."""
    try:
        total_size = 0
        print("Calcolo dimensione iniziale del buffer (può richiedere tempo)...")
        files_to_check = list(path.glob('**/*'))
        for f in tqdm(files_to_check, desc="Scanning buffer"):
            if f.is_file() and (f.name.endswith('.json') or f.name.endswith('.downloading')):
                try: total_size += f.stat().st_size
                except FileNotFoundError: continue
        gb_size = total_size / (1024**3)
        print(f"Dimensione iniziale buffer: {gb_size:.2f} GB")
        return total_size
    except Exception as e:
        print(f"Errore calcolo dimensione iniziale: {e}")
        return 0

def safe_s3_key_to_local(s3_key: str) -> str:
    """Converte una S3 key in un nome file locale sicuro."""
    return s3_key.replace('/', '__')

def local_to_safe_s3_key(local_name: str) -> str:
    """Riconverte un nome file locale nella S3 key originale."""
    base_name = local_name.rsplit('.', 1)[0] if '.' in local_name else local_name
    return base_name.replace('__', '/')


# --- 6. BUFFER MANAGER ---

class BufferManager:
    def __init__(self, initial_size_bytes: int, max_size_bytes: int):
        self.current_size = initial_size_bytes
        self.max_size = max_size_bytes
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)

    def get_current_size(self) -> int:
        with self.lock: return self.current_size

    def wait_for_space(self, size_needed: int, stop_event: threading.Event):
        with self.lock:
            while (self.current_size + size_needed) > self.max_size:
                if stop_event.is_set() or not self.condition.wait(timeout=5.0):
                    if stop_event.is_set(): return False
            self.current_size += size_needed
            return True

    def release_space(self, size_released: int):
        with self.lock:
            self.current_size -= size_released
            if self.current_size < 0: self.current_size = 0
            self.condition.notify_all()


# --- 7. PIPELINE 1: DOWNLOADER (PRODUCER) ---

def worker_s3_lister(
    stop_event: threading.Event,
    download_queue: queue.Queue,
    processed_files_set: set,          # <<< Set passato qui
    # processed_files_lock: threading.Lock, # Lock non necessario per leggere il set
    s3_input_bucket: str,
    s3_input_prefix: str,
    pbar_s3_scan: tqdm
):
    """
    Worker Thread (SINGOLO): Scansiona S3, controlla checkpoint (robusto)
    e mette (s3_key_originale, file_size) in coda per il download.
    """
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2').paginate(
        Bucket=s3_input_bucket, Prefix=s3_input_prefix
    )

    try:
        pbar_s3_scan.write(f"DEBUG Lister: Dimensione set checkpoint: {len(processed_files_set)}")

        for page in paginator:
            if stop_event.is_set(): break

            page_keys_info = [] # Lista di tuple (key_originale_CON_json, size)
            if 'Contents' in page:
                for file_summary in page['Contents']:
                     s3_key_original = file_summary['Key']
                     file_size = file_summary['Size']
                     if s3_key_original.endswith('.json') and file_size > 0:
                         page_keys_info.append((s3_key_original, file_size))

            keys_to_queue = []
            # La lettura dal set è thread-safe, non serve lock
            for s3_key_original, file_size in page_keys_info:
                 # <<< MODIFICA: Controllo robusto (sia con che senza .json) >>>
                 s3_key_normalized = s3_key_original.replace('.json', '')
                 # Controlla se ALMENO UNA delle versioni è nel set
                 if s3_key_original not in processed_files_set and \
                    s3_key_normalized not in processed_files_set:
                     keys_to_queue.append((s3_key_original, file_size))
                 # <<< FINE MODIFICA >>>

            for item in keys_to_queue:
                if stop_event.is_set(): break
                download_queue.put(item)
                pbar_s3_scan.update(1)

            if stop_event.is_set(): break

    except Exception as e: pbar_s3_scan.write(f"ErroRE CRITICO S3 Lister: {e}")
    finally:
        pbar_s3_scan.write("S3 Lister: Scansione S3 completata.")
        pbar_s3_scan.close()
        for _ in range(NUM_S3_DOWNLOADERS): download_queue.put(None)


def worker_local_downloader(
    stop_event: threading.Event,
    download_queue: queue.Queue,
    buffer_manager: BufferManager,
    local_buffer_path: Path,
    s3_input_bucket: str,
    pbar_download: tqdm
):
    """ VERSIONE ROBUSTA """
    s3_client = boto3.client('s3')
    # Ottimizzazione Boto3 (opzionale, ma può aiutare)
    transfer_config = boto3.s3.transfer.TransferConfig(
        multipart_threshold=100 * 1024 * 1024, # 100MB
        max_concurrency=10,
        multipart_chunksize=25 * 1024 * 1024, # 25MB
        use_threads=True
    )

    while True:
        item = download_queue.get()
        if item is None:
            download_queue.task_done()
            break

        s3_key, file_size = item
        if stop_event.is_set():
            download_queue.task_done()
            break

        local_filename = safe_s3_key_to_local(s3_key) + ".json"
        final_path = local_buffer_path / local_filename
        temp_path = local_buffer_path / f"{local_filename}.downloading"
        space_reserved = False

        try:
            if final_path.exists():
                download_queue.task_done()
                continue
            if temp_path.exists():
                 pbar_download.write(f"WARN: Trovato {temp_path}. Verrà sovrascritto.")
                 try: os.remove(temp_path)
                 except OSError as e_del: pbar_download.write(f"WARN: Impossibile rimuovere {temp_path}: {e_del}")

            if not buffer_manager.wait_for_space(file_size, stop_event):
                download_queue.task_done()
                break
            space_reserved = True

            s3_client.download_file(s3_input_bucket, s3_key, str(temp_path), Config=transfer_config)
            shutil.move(str(temp_path), str(final_path))
            pbar_download.update(1)

        except Exception as e:
            pbar_download.write(f"ERRORE download/spostamento {s3_key}: {e}")
            if space_reserved: buffer_manager.release_space(file_size)
            if temp_path and temp_path.exists():
                try: os.remove(temp_path)
                except Exception as e_del: pbar_download.write(f"ERRORE: Impossibile pulire {temp_path}: {e_del}")
        finally:
            download_queue.task_done()


# --- 8. PIPELINE 2: IL PROCESSORE (CONSUMER) ---

def worker_local_scanner(
    stop_event: threading.Event,
    local_file_queue: queue.Queue,
    local_buffer_path: Path,
    pbar_scanner: tqdm
):
    """Worker Thread (SINGOLO): Scansiona la cartella locale"""
    processed_locally = set()
    scan_interval = 5

    while not stop_event.is_set():
        files_found_this_scan = 0
        try:
            current_files_in_dir = {f.name for f in local_buffer_path.glob('*.json')}
            new_files = current_files_in_dir - processed_locally

            for f_name in new_files:
                f_path = local_buffer_path / f_name
                if f_path.exists():
                    local_file_queue.put(f_path)
                    processed_locally.add(f_name)
                    files_found_this_scan += 1

            if files_found_this_scan > 0:
                pbar_scanner.total = len(processed_locally) # Aggiorna il totale atteso
                pbar_scanner.update(0) # Aggiorna display

            processed_locally &= current_files_in_dir
            time.sleep(scan_interval)

        except Exception as e:
            pbar_scanner.write(f"Errore Scanner Locale: {e}")
            time.sleep(scan_interval)

    pbar_scanner.write("Scanner Locale: Rilevato stop event. Uscita.")
    pbar_scanner.close()


def worker_local_reader_filter(
    stop_event: threading.Event,
    local_file_queue: queue.Queue,
    processing_queue: queue.Queue,
    buffer_manager: BufferManager,
    allowed_domains: set,
    pbar_scanner: tqdm
):
    """Worker Thread (MULTIPLO): Legge da SSD, filtra, cancella e notifica."""
    while not (stop_event.is_set() and local_file_queue.empty()):
        try: local_path = local_file_queue.get(timeout=1)
        except queue.Empty: continue

        s3_key = local_to_safe_s3_key(local_path.name)
        relevant_entries = []
        file_size = 0
        processed_successfully = False

        try:
            try: file_size = local_path.stat().st_size
            except FileNotFoundError:
                local_file_queue.task_done()
                pbar_scanner.update(1)
                continue

            with open(local_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                        url = entry.get("url")
                        if not url: continue
                        netloc = urlparse(url).netloc
                        if is_allowed_correctly(netloc, allowed_domains) and \
                           not is_allowed_flawed(netloc, allowed_domains):
                            relevant_entries.append(entry)
                    except json.JSONDecodeError: continue

            if relevant_entries:
                articles_to_process = defaultdict(list)
                for entry in relevant_entries: articles_to_process[entry['url']].append(entry)
                processing_queue.put((s3_key, articles_to_process))
            else:
                processing_queue.put((s3_key, None))
            processed_successfully = True

        except FileNotFoundError: print(f"WARN: File {local_path} scomparso durante lettura.")
        except Exception as e: print(f"Errore LETTURA file {local_path}: {e}")
        finally:
            if processed_successfully:
                try:
                    os.remove(local_path)
                    buffer_manager.release_space(file_size)
                except FileNotFoundError: print(f"WARN: File {local_path} non trovato per cancellazione.")
                except Exception as e: print(f"Errore CANCELLAZIONE file {local_path}: {e}")
            pbar_scanner.update(1)
            local_file_queue.task_done()


def worker_processor(
    stop_event: threading.Event,
    processing_queue: queue.Queue,
    upload_queue: queue.Queue,
    mp_pool: mp.Pool
):
    """Worker Thread (MULTIPLO): Ricostruisce articoli"""
    while not (stop_event.is_set() and processing_queue.empty()):
        try: item = processing_queue.get(timeout=1)
        except queue.Empty: continue

        s3_key, articles_to_process_dict = item

        if articles_to_process_dict is None:
            upload_queue.put((s3_key, None))
            processing_queue.task_done()
            continue

        try:
            articles_to_reconstruct = defaultdict(list)
            articles_to_skip = defaultdict(list)
            for url, entries in articles_to_process_dict.items():
                for entry in entries:
                    if entry.get("type") == 1: articles_to_reconstruct[url].append(entry)
                    else: articles_to_skip[url].append(entry)

            transformed_articles = transform_dict(articles_to_reconstruct)
            work_items_reconstruct = list(transformed_articles.items())
            work_items_skip = list(articles_to_skip.items())
            results = []

            if work_items_reconstruct:
                process_func = partial(process_article)
                chunk_size = max(1, len(work_items_reconstruct) // (mp_pool._processes * 2)) # Dynamic chunk size
                processor_results = mp_pool.imap_unordered(process_func, work_items_reconstruct, chunksize=chunk_size)
                results.extend(filter(None, processor_results))

            for item_skip in work_items_skip: results.append(process_skipped_article(item_skip))

            if results:
                df = pd.DataFrame(results)
                upload_queue.put((s3_key, df))
            else:
                upload_queue.put((s3_key, None))

        except Exception as e: print(f"Errore PROCESSO {s3_key}: {e}")
        finally: processing_queue.task_done()


def worker_uploader(
    stop_event: threading.Event,
    upload_queue: queue.Queue,
    checkpoint_queue: queue.Queue,
    s3_output_bucket: str,
    s3_output_prefix: str
):
    """Worker Thread (MULTIPLO): Carica Parquet"""
    s3_client = boto3.client('s3')
    if not s3_output_prefix.endswith('/'): s3_output_prefix += '/'

    while not (stop_event.is_set() and upload_queue.empty()):
        try: item = upload_queue.get(timeout=1)
        except queue.Empty: continue

        s3_key, df = item
        upload_successful = False
        try:
            if df is not None and not df.empty:
                output_name = safe_s3_key_to_local(s3_key) + ".parquet"
                output_s3_path = f"s3://{s3_output_bucket}/{s3_output_prefix}{output_name}"
                df.to_parquet(output_s3_path, index=False, engine='pyarrow')
            upload_successful = True
        except Exception as e: print(f"Errore UPLOAD {s3_key}: {e}")
        finally:
            if upload_successful: checkpoint_queue.put(s3_key) # Passa la chiave ORIGINALE (con .json)
            upload_queue.task_done()


def worker_checkpointer(
    stop_event: threading.Event,
    checkpoint_queue: queue.Queue,
    log_file_handle: io.TextIOWrapper,
    processed_files_set: set,
    processed_files_lock: threading.Lock,
    pbar_processed: tqdm
):
    """
    Worker Thread (SINGOLO): Scrive sul log la chiave normalizzata.
    """
    while not (stop_event.is_set() and checkpoint_queue.empty()):
        try: s3_key_original = checkpoint_queue.get(timeout=1) # Riceve chiave CON .json
        except queue.Empty: continue

        try:
            # <<< MODIFICA: Normalizza prima di controllare/scrivere >>>
            s3_key_normalized = s3_key_original.replace('.json', '')
            # <<< FINE MODIFICA >>>

            with processed_files_lock:
                # <<< MODIFICA: Usa chiave normalizzata per check, aggiungi entrambe al set, scrivi normalizzata >>>
                if s3_key_normalized not in processed_files_set:
                    processed_files_set.add(s3_key_normalized)
                    processed_files_set.add(s3_key_original) # Aggiungi anche l'originale al set in memoria
                    log_file_handle.write(f"{s3_key_normalized}\n") # Scrivi normalizzata
                    log_file_handle.flush()
                # <<< FINE MODIFICA >>>

            pbar_processed.update(1) # Aggiorna pbar per ogni file che arriva qui
        except Exception as e: pbar_processed.write(f"Errore CHECKPOINT {s3_key_original}: {e}")
        finally: checkpoint_queue.task_done()


def worker_monitor(
    stop_event: threading.Event,
    queues_dict: Dict[str, queue.Queue],
    buffer_manager: BufferManager,
    interval: int = 20
):
    """Worker Thread (SINGOLO): Stampa lo stato."""
    while not stop_event.is_set():
        try:
            status_parts = [f"{name}: {q.qsize():<4}" for name, q in queues_dict.items()]
            buffer_size_bytes = buffer_manager.get_current_size()
            buffer_size_gb = buffer_size_bytes / (1024**3)
            buffer_perc = (buffer_size_bytes / MAX_BUFFER_BYTES * 100) if MAX_BUFFER_BYTES > 0 else 0
            status_line = f"STATUS | {' | '.join(status_parts)} | Buffer SSD: {buffer_size_gb:.2f}/{MAX_BUFFER_GB} GB ({buffer_perc:.1f}%)"
            print(status_line)
            stop_event.wait(interval)
        except Exception as e:
            print(f"Errore MONITOR: {e}")
            time.sleep(interval)


# --- 9. FUNZIONE PRINCIPALE ---

def main():

    if not LOCAL_BUFFER_PATH.exists():
        print(f"ERRORE: La cartella buffer non esiste: {LOCAL_BUFFER_PATH}")
        print("Creala e riavvia lo script.")
        return

    print(f"Buffer locale: {LOCAL_BUFFER_PATH}")
    print(f"Limite buffer: {MAX_BUFFER_GB} GB ({MAX_BUFFER_BYTES} bytes)")

    processed_files_set = set()
    processed_files_lock = threading.Lock()

    try:
        print(f"Lettura checkpoint da: {CHECKPOINT_FILE}")
        with open(CHECKPOINT_FILE, 'r') as f:
            # <<< MODIFICA: Caricamento robusto >>>
            lines_read = 0
            for line in f:
                original_key = line.strip()
                if original_key:
                    lines_read += 1
                    processed_files_set.add(original_key)
                    normalized_key = original_key.replace('.json', '')
                    processed_files_set.add(normalized_key)
            # <<< FINE MODIFICA >>>
        print(f"Lette {lines_read} righe dal log DELTA. Caricati {len(processed_files_set)} elementi unici nel set di checkpoint.")
    except FileNotFoundError: print("Nessun file di checkpoint DELTA trovato.")

    initial_buffer_size = get_dir_size_on_startup(LOCAL_BUFFER_PATH)
    buffer_manager = BufferManager(initial_buffer_size, MAX_BUFFER_BYTES)

    # Code
    download_queue = queue.Queue(maxsize=NUM_S3_DOWNLOADERS * 3)
    local_file_queue = queue.Queue(maxsize=NUM_LOCAL_READERS * 3)
    processing_queue = queue.Queue(maxsize=NUM_PROCESSORS * 3)
    upload_queue = queue.Queue(maxsize=NUM_UPLOADERS * 3)
    checkpoint_queue = queue.Queue()

    stop_event = threading.Event()

    # Barre di Avanzamento
    pbar_s3_scan = tqdm(desc="File S3 Trovati (Nuovi)", unit="file", position=0, dynamic_ncols=True)
    pbar_download = tqdm(desc="File Scaricati su SSD", unit="file", position=1, dynamic_ncols=True)
    pbar_scanner = tqdm(desc="File Letti da SSD", unit="file", position=2, dynamic_ncols=True, total=0)
    pbar_processed = tqdm(desc="File Checkpointati", unit="file", position=3, dynamic_ncols=True)


    with open(CHECKPOINT_FILE, 'a') as log_file:

        num_mp_cores = max(1, mp.cpu_count() - 2)
        print(f"Avvio M-Pool condivisa con {num_mp_cores} core.")

        with mp.Pool(processes=num_mp_cores) as mp_pool:

            all_threads = [] # Lista per join finale

            # Monitor
            queues_dict = {
                "S3List": download_queue, "Local": local_file_queue,
                "Process": processing_queue, "Upload": upload_queue,
                "ChkPoint": checkpoint_queue
            }
            mon_thread = threading.Thread(target=worker_monitor, args=(stop_event, queues_dict, buffer_manager, 30), daemon=True)
            mon_thread.start()

            # --- CONSUMER Pipeline ---
            cp_thread = threading.Thread(target=worker_checkpointer, args=(stop_event, checkpoint_queue, log_file, processed_files_set, processed_files_lock, pbar_processed), daemon=True)
            cp_thread.start(); all_threads.append(cp_thread)

            uploader_threads = []
            for _ in range(NUM_UPLOADERS):
                t = threading.Thread(target=worker_uploader, args=(stop_event, upload_queue, checkpoint_queue, S3_OUTPUT_BUCKET, S3_OUTPUT_PREFIX), daemon=True)
                t.start(); uploader_threads.append(t)
            all_threads.extend(uploader_threads)

            processor_threads = []
            for _ in range(NUM_PROCESSORS):
                t = threading.Thread(target=worker_processor, args=(stop_event, processing_queue, upload_queue, mp_pool), daemon=True)
                t.start(); processor_threads.append(t)
            all_threads.extend(processor_threads)

            reader_threads = []
            for _ in range(NUM_LOCAL_READERS):
                t = threading.Thread(target=worker_local_reader_filter, args=(stop_event, local_file_queue, processing_queue, buffer_manager, allowed_domains_set, pbar_scanner), daemon=True)
                t.start(); reader_threads.append(t)
            all_threads.extend(reader_threads)

            scan_thread = threading.Thread(target=worker_local_scanner, args=(stop_event, local_file_queue, LOCAL_BUFFER_PATH, pbar_scanner), daemon=True)
            scan_thread.start(); all_threads.append(scan_thread)

            # --- PRODUCER Pipeline ---
            downloader_threads = []
            for _ in range(NUM_S3_DOWNLOADERS):
                t = threading.Thread(target=worker_local_downloader, args=(stop_event, download_queue, buffer_manager, LOCAL_BUFFER_PATH, S3_INPUT_BUCKET, pbar_download), daemon=True)
                t.start(); downloader_threads.append(t)
            all_threads.extend(downloader_threads)

            lister_thread = threading.Thread(
                target=worker_s3_lister,
                args=(stop_event,
                      download_queue,
                      processed_files_set,
                      S3_INPUT_BUCKET,
                      S3_INPUT_PREFIX,
                      pbar_s3_scan),
                daemon=True
            )
            lister_thread.start(); all_threads.append(lister_thread)

            # --- Gestione Arresto ---
            print("Pipeline avviata. Premi CTRL+C per un arresto controllato.")
            try:
                lister_thread.join()
                print("S3 Lister terminato.")

                print("Attesa fine download...")
                download_queue.join()
                for t in downloader_threads: t.join()
                print("Download su SSD completati.")

                print("In attesa svuotamento coda file locali (lettura)...")
                local_file_queue.join()
                print("- Coda file locali svuotata.")

                stop_event.set()
                print("Stop event settato. Attesa terminazione workers...")

                print("In attesa svuotamento coda processo...")
                processing_queue.join()
                print("- Coda processo svuotata.")
                print("In attesa svuotamento coda upload...")
                upload_queue.join()
                print("- Coda upload svuotata.")
                print("In attesa svuotamento coda checkpoint...")
                checkpoint_queue.join()
                print("- Coda checkpoint svuotata.")

                print("Tutte le code sono vuote. Arresto pulito.")

            except KeyboardInterrupt:
                print("\nRichiesta [CTRL+C]... Avvio arresto controllato...")
                stop_event.set()

            print("Attesa terminazione thread...")
            # Unisci prima i downloader e lister per sicurezza
            if lister_thread.is_alive(): lister_thread.join(timeout=5)
            for t in downloader_threads:
                 if t.is_alive(): t.join(timeout=10)
            # Poi il resto
            for t in reversed(all_threads):
                 if t and t.is_alive() and t not in downloader_threads and t != lister_thread:
                     t.join(timeout=15)

            print("Arresto del monitor...")
            mon_thread.join(timeout=5)

            print("Chiusura barre di avanzamento...")
            pbar_s3_scan.close()
            pbar_download.close()
            pbar_scanner.close()
            pbar_processed.close()
            print("Barre chiuse.")

    print("Elaborazione pipeline con buffer locale completata.")


# --- 10. ESECUZIONE ---

if __name__ == "__main__":
    main()