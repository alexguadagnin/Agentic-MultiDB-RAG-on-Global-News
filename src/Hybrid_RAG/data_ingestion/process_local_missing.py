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
import shutil # Per shutil.disk_usage
from pathlib import Path

# --- 1. CONFIGURAZIONE ---

# Path alla cartella locale da usare come buffer
LOCAL_BUFFER_PATH = Path("...")
# Limite in GB per il buffer
MAX_BUFFER_GB = 500
# Path al file di checkpoint
CHECKPOINT_FILE = 'processed_files_FIX_checkpoint.log'

# Converti GB in Byte
MAX_BUFFER_BYTES = MAX_BUFFER_GB * (1024**3)

# Parametri S3
S3_INPUT_BUCKET = "hybrid-rag-gdelt-bucket"
S3_INPUT_PREFIX = ""
S3_OUTPUT_BUCKET = "hybrid-rag-gdelt-bucket"
S3_OUTPUT_PREFIX = "gdelt_reconstructed_parquet_FIX/"

# Parametri Pipeline
NUM_S3_DOWNLOADERS = 12 # Thread per scaricare da S3 in parallelo
NUM_LOCAL_READERS = 12  # Thread che leggono da SSD (veloce) e filtrano
NUM_PROCESSORS = 12      # Thread che usano la MP Pool per CPU pesante
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

# --- 4. FUNZIONI DI ELABORAZIONE (Invariate) ---
# (Queste rimangono globali per il multiprocessing)

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

# --- 5. FUNZIONI HELPER ---

def get_dir_size_on_startup(path: Path) -> int:
    """Calcola la dimensione totale *all'avvio*. Lento, ma eseguito solo una volta."""
    try:
        total_size = 0
        print("Calcolo dimensione iniziale del buffer (può richiedere tempo)...")
        for f in path.glob('**/*'):
            # Considera solo file JSON completati o in corso di download
            if f.is_file() and (f.name.endswith('.json') or f.name.endswith('.downloading')):
                try:
                    total_size += f.stat().st_size
                except FileNotFoundError:
                    # Il file potrebbe essere stato cancellato nel frattempo, ignoralo
                    continue
        print(f"Dimensione iniziale buffer: {total_size / (1024**3):.2f} GB")
        return total_size
    except Exception as e:
        print(f"Errore calcolo dimensione iniziale: {e}")
        return 0

def safe_s3_key_to_local(s3_key: str) -> str:
    """Converte una S3 key in un nome file locale sicuro."""
    return s3_key.replace('/', '__')

def local_to_safe_s3_key(local_name: str) -> str:
    """Riconverte un nome file locale nella S3 key originale."""
    return local_name.replace('.json', '').replace('__', '/')


# --- 6. NUOVO: BUFFER MANAGER ---

class BufferManager:
    """
    Classe thread-safe per gestire lo stato del buffer
    senza usare os.walk() in loop.
    """
    def __init__(self, initial_size_bytes: int, max_size_bytes: int):
        self.current_size = initial_size_bytes
        self.max_size = max_size_bytes
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)

    def get_current_size(self) -> int:
        with self.lock:
            return self.current_size

    def wait_for_space(self, size_needed: int, stop_event: threading.Event):
        """
        Chiamato da un downloader. Attende finché c'è spazio.
        Restituisce True se c'è spazio, False se lo script si sta fermando.
        """
        with self.lock:
            while (self.current_size + size_needed) > self.max_size:
                # Usa un timeout per controllare periodicamente lo stop_event
                if stop_event.is_set() or not self.condition.wait(timeout=5.0):
                    if stop_event.is_set():
                        return False # Interrotto
                    # Se il timeout scade ma non c'è stop_event, continua ad aspettare
            # Abbiamo spazio! Occupalo
            self.current_size += size_needed
            return True

    def release_space(self, size_released: int):
        """
        Chiamato da un reader dopo aver cancellato un file.
        Libera spazio e sveglia i downloader in attesa.
        """
        with self.lock:
            self.current_size -= size_released
            if self.current_size < 0:
                self.current_size = 0 # Sicurezza
            # Sveglia *tutti* i downloader in attesa
            self.condition.notify_all()


# --- 7. PIPELINE 1: IL DOWNLOADER (PRODUCER) ---

def worker_s3_lister(
    stop_event: threading.Event,
    download_queue: queue.Queue,
    processed_files_set: set,
    processed_files_lock: threading.Lock,
    s3_input_bucket: str,
    s3_input_prefix: str,
    pbar_s3_scan: tqdm
):
    """
    Worker Thread (SINGOLO): Scansiona S3, controlla checkpoint
    e mette (s3_key, file_size) in coda per il download.
    """
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2').paginate(
        Bucket=s3_input_bucket, Prefix=s3_input_prefix
    )

    try:
        for page in paginator:
            if stop_event.is_set():
                pbar_s3_scan.write("S3 Lister: Rilevato stop event.")
                break

            for file_summary in page.get('Contents', []):
                if stop_event.is_set(): break

                s3_key = file_summary['Key']
                file_size = file_summary['Size']

                if not s3_key.endswith('.json') or file_size == 0:
                    continue

                with processed_files_lock:
                    if s3_key in processed_files_set:
                        continue

                download_queue.put((s3_key, file_size))
                pbar_s3_scan.update(1)

    except Exception as e:
        pbar_s3_scan.write(f"ErroRE CRITICO S3 Lister: {e}")
    finally:
        pbar_s3_scan.write("S3 Lister: Scansione S3 completata.")
        pbar_s3_scan.close()
        # Segnala ai downloader che non ci sono più file da S3
        for _ in range(NUM_S3_DOWNLOADERS):
            download_queue.put(None)


def worker_local_downloader(
    stop_event: threading.Event,
    download_queue: queue.Queue,
    buffer_manager: BufferManager,
    local_buffer_path: Path,
    s3_input_bucket: str,
    pbar_download: tqdm
):
    """
    <<< VERSIONE ROBUSTA >>>
    Worker Thread (MULTIPLO): Legge (s3_key, size) dalla coda,
    gestisce file esistenti, aspetta spazio e scarica.
    """
    s3_client = boto3.client('s3')

    while True:
        item = download_queue.get()
        if item is None:
            download_queue.task_done()
            break

        s3_key, file_size = item
        if stop_event.is_set():
            download_queue.task_done()
            break

        local_filename = safe_s3_key_to_local(s3_key)
        final_path = local_buffer_path / local_filename
        temp_path = local_buffer_path / f"{local_filename}.downloading"
        space_reserved = False # Flag per tracciare se abbiamo chiamato wait_for_space

        try:
            # --- Fase 1: Controllo Preliminare ---
            
            # Controlla se il file finale (.json) esiste già
            if final_path.exists():
                # Se esiste, presumiamo sia valido (da un run precedente interrotto DOPO il download).
                # Lo scanner locale lo troverà e lo processerà. Saltiamo il download.
                # pbar_download.write(f"INFO: File {local_filename} già presente. Download saltato.")
                download_queue.task_done()
                continue # Prendi il prossimo file dalla coda

            # Controlla se esiste un file temporaneo (.downloading) orfano
            if temp_path.exists():
                pbar_download.write(f"WARN: Trovato file temporaneo orfano {temp_path}. Verrà sovrascritto.")
                # Non è necessario cancellarlo ora, download_file lo sovrascriverà.

            # --- Fase 2: Attesa Spazio ---
            
            # Aspetta che ci sia spazio nel buffer (bloccante)
            if not buffer_manager.wait_for_space(file_size, stop_event):
                # Script fermato mentre aspettavamo spazio
                download_queue.task_done()
                break # Esci dal loop del worker
            space_reserved = True # Lo spazio è ora riservato per noi

            # --- Fase 3: Download ---
            
            s3_client.download_file(s3_input_bucket, s3_key, str(temp_path))

            # --- Fase 4: Finalizzazione (Rename/Move) ---
            
            # Usa shutil.move per maggiore robustezza (sovrascrive se necessario)
            shutil.move(str(temp_path), str(final_path))
            pbar_download.update(1) # Aggiorna la barra solo se il download ha successo

        except Exception as e:
            pbar_download.write(f"ERRORE durante download/spostamento {s3_key}: {e}")
            # Se lo spazio era stato riservato, liberalo
            if space_reserved:
                buffer_manager.release_space(file_size)
            # Tenta di pulire il file temporaneo se esiste ancora
            if temp_path and temp_path.exists():
                try:
                    os.remove(temp_path)
                except Exception as e_del:
                     pbar_download.write(f"ERRORE: Impossibile cancellare file temporaneo {temp_path} dopo errore: {e_del}")
        finally:
            # Segnala sempre che abbiamo finito con questo item della coda
            download_queue.task_done()


# --- 8. PIPELINE 2: IL PROCESSORE (CONSUMER) ---

def worker_local_scanner(
    stop_event: threading.Event,
    local_file_queue: queue.Queue,
    local_buffer_path: Path,
    pbar_scanner: tqdm
):
    """Worker Thread (SINGOLO): Scansiona la cartella locale (Invariato)"""
    processed_locally = set()
    scan_interval = 5 # Scansiona più frequentemente

    while not stop_event.is_set():
        files_found = 0
        try:
            # Lista i file *prima* per evitare race condition con la cancellazione
            current_files = {f for f in os.listdir(local_buffer_path) if f.endswith('.json')}
            new_files = current_files - processed_locally

            for f_name in new_files:
                # Controlla se il file esiste ancora prima di aggiungerlo
                f_path = local_buffer_path / f_name
                if f_path.exists():
                    local_file_queue.put(f_path)
                    processed_locally.add(f_name)
                    files_found += 1

            if files_found > 0:
                pbar_scanner.update(files_found)

            # Rimuovi dal set i file che non esistono più (cancellati dal reader)
            processed_locally &= current_files

            # Aspetta prima della prossima scansione
            time.sleep(scan_interval)

        except Exception as e:
            pbar_scanner.write(f"Errore Scanner Locale: {e}")
            time.sleep(scan_interval) # Aspetta anche in caso di errore

    pbar_scanner.write("Scanner Locale: Rilevato stop event. Uscita.")
    pbar_scanner.close()


def worker_local_reader_filter(
    stop_event: threading.Event,
    local_file_queue: queue.Queue,
    processing_queue: queue.Queue,
    buffer_manager: BufferManager,
    allowed_domains: set
):
    """
    Worker Thread (MULTIPLO): Legge da SSD, filtra, cancella
    e notifica il BufferManager.
    """
    while not (stop_event.is_set() and local_file_queue.empty()):
        try:
            local_path = local_file_queue.get(timeout=1)
        except queue.Empty:
            continue

        s3_key = local_to_safe_s3_key(local_path.name)
        relevant_entries = []
        file_size = 0
        processed_successfully = False # Flag per sapere se cancellare

        try:
            # Ottieni la dimensione PRIMA di processare
            # Gestisci FileNotFoundError se il file viene cancellato prima
            try:
                file_size = local_path.stat().st_size
            except FileNotFoundError:
                local_file_queue.task_done()
                continue # File non più esistente

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

                    except json.JSONDecodeError:
                        continue

            if relevant_entries:
                articles_to_process = defaultdict(list)
                for entry in relevant_entries:
                    articles_to_process[entry['url']].append(entry)
                processing_queue.put((s3_key, articles_to_process))
            else:
                processing_queue.put((s3_key, None))

            processed_successfully = True # Lettura completata

        except FileNotFoundError:
            # File cancellato mentre lo stavamo leggendo? Ignora.
            print(f"WARN: File {local_path} scomparso durante la lettura.")
        except Exception as e:
            print(f"Errore LETTURA file {local_path}: {e}")
        finally:
            # Cancella e libera spazio SOLO se abbiamo letto con successo
            if processed_successfully:
                try:
                    os.remove(local_path)
                    buffer_manager.release_space(file_size)
                except FileNotFoundError:
                     # Potrebbe essere stato cancellato da un altro thread?
                     # O il file_size era sbagliato? Meglio non rilasciare spazio per sicurezza.
                     print(f"WARN: File {local_path} non trovato durante la cancellazione.")
                except Exception as e:
                    print(f"Errore CANCELLAZIONE file {local_path}: {e}")
                    # Non rilasciare spazio se la cancellazione fallisce

            local_file_queue.task_done()


def worker_processor(
    stop_event: threading.Event,
    processing_queue: queue.Queue,
    upload_queue: queue.Queue,
    mp_pool: mp.Pool
):
    """Worker Thread (MULTIPLO): Ricostruisce articoli (Invariato)"""
    while not (stop_event.is_set() and processing_queue.empty()):
        try:
            item = processing_queue.get(timeout=1)
        except queue.Empty:
            continue

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
                    if (entry.get("type") == 1):
                        articles_to_reconstruct[url].append(entry)
                    else:
                        articles_to_skip[url].append(entry)

            transformed_articles = transform_dict(articles_to_reconstruct)
            work_items_reconstruct = list(transformed_articles.items())
            work_items_skip = list(articles_to_skip.items())
            results = []

            if work_items_reconstruct:
                process_func = partial(process_article)
                # Usa un timeout anche qui per evitare blocchi indefiniti
                for result in mp_pool.imap_unordered(process_func, work_items_reconstruct, chunksize=10):
                     if result: results.append(result)

            for item_skip in work_items_skip:
                results.append(process_skipped_article(item_skip))

            if results:
                df = pd.DataFrame(results)
                upload_queue.put((s3_key, df))
            else:
                upload_queue.put((s3_key, None))

        except Exception as e:
            print(f"Errore PROCESSO {s3_key}: {e}")

        processing_queue.task_done()


def worker_uploader(
    stop_event: threading.Event,
    upload_queue: queue.Queue,
    checkpoint_queue: queue.Queue,
    s3_output_bucket: str,
    s3_output_prefix: str
):
    """Worker Thread (MULTIPLO): Carica Parquet (Invariato)"""
    s3_client = boto3.client('s3')

    if not s3_output_prefix.endswith('/'):
        s3_output_prefix += '/'

    while not (stop_event.is_set() and upload_queue.empty()):
        try:
            item = upload_queue.get(timeout=1)
        except queue.Empty:
            continue

        s3_key, df = item

        try:
            if df is not None:
                output_name = safe_s3_key_to_local(s3_key).replace('.json', '.parquet')
                output_s3_path = f"s3://{s3_output_bucket}/{s3_output_prefix}{output_name}"
                df.to_parquet(output_s3_path, index=False, engine='pyarrow')

            checkpoint_queue.put(s3_key)

        except Exception as e:
            print(f"Errore UPLOAD {s3_key}: {e}")

        upload_queue.task_done()

def worker_checkpointer(
    stop_event: threading.Event,
    checkpoint_queue: queue.Queue,
    log_file_handle: io.TextIOWrapper,
    processed_files_set: set,
    processed_files_lock: threading.Lock,
    pbar_processed: tqdm
):
    """Worker Thread (SINGOLO): Scrive sul log (Invariato)"""
    while not (stop_event.is_set() and checkpoint_queue.empty()):
        try:
            s3_key = checkpoint_queue.get(timeout=1)
        except queue.Empty:
            continue

        try:
            with processed_files_lock:
                if s3_key not in processed_files_set:
                    processed_files_set.add(s3_key)
                    log_file_handle.write(f"{s3_key}\n")
                    log_file_handle.flush()

            pbar_processed.update(1)
        except Exception as e:
            pbar_processed.write(f"Errore CHECKPOINT {s3_key}: {e}")

        checkpoint_queue.task_done()


def worker_monitor(
    stop_event: threading.Event,
    queues_dict: Dict[str, queue.Queue],
    buffer_manager: BufferManager,
    interval: int = 20
):
    """Worker Thread (SINGOLO): Stampa lo stato leggendo dal BufferManager."""
    while not stop_event.is_set():
        try:
            status_parts = []
            for name, q in queues_dict.items():
                status_parts.append(f"{name}: {q.qsize():<4}")

            buffer_size_bytes = buffer_manager.get_current_size()
            buffer_size_gb = buffer_size_bytes / (1024**3)
            # Evita divisione per zero se MAX_BUFFER_GB è 0
            buffer_perc = (buffer_size_bytes / MAX_BUFFER_BYTES * 100) if MAX_BUFFER_BYTES > 0 else 0


            status_line = (
                f"STATUS | {' | '.join(status_parts)} | "
                f"Buffer SSD: {buffer_size_gb:.2f}/{MAX_BUFFER_GB} GB ({buffer_perc:.1f}%)"
            )
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
    print(f"Limite buffer: {MAX_BUFFER_GB} GB")

    processed_files_set = set()
    processed_files_lock = threading.Lock()

    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            processed_files_set = {line.strip() for line in f}
        print(f"Caricati {len(processed_files_set)} file già processati dal log DELTA.")
    except FileNotFoundError:
        print("Nessun file di checkpoint DELTA trovato, si parte da zero.")

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
    pbar_s3_scan = tqdm(desc="File S3 Trovati", unit="file", position=0, dynamic_ncols=True)
    pbar_download = tqdm(desc="File Scaricati su SSD", unit="file", position=1, dynamic_ncols=True)
    pbar_scanner = tqdm(desc="File Letti da SSD", unit="file", position=2, dynamic_ncols=True)
    pbar_processed = tqdm(desc="File Processati/Scritti", unit="file", position=3, dynamic_ncols=True)

    with open(CHECKPOINT_FILE, 'a') as log_file:

        num_mp_cores = max(1, mp.cpu_count() - 2)
        print(f"Avvio M-Pool condivisa con {num_mp_cores} core.")

        with mp.Pool(processes=num_mp_cores) as mp_pool:

            all_threads = [] # Lista per tenere traccia di tutti i thread

            # Avvia il monitor
            queues_dict = {
                "Download": download_queue,
                "Locale": local_file_queue,
                "Process": processing_queue,
                "Upload": upload_queue,
                "Checkpoint": checkpoint_queue
            }
            mon_thread = threading.Thread(
                target=worker_monitor,
                args=(stop_event, queues_dict, buffer_manager, 30),
                daemon=True
            )
            mon_thread.start()
            # Non aggiungiamo il monitor a all_threads per gestirlo separatamente alla fine

            # --- Avvia Pipeline CONSUMER ---
            # Checkpointer
            cp_thread = threading.Thread(
                target=worker_checkpointer,
                args=(stop_event, checkpoint_queue, log_file,
                      processed_files_set, processed_files_lock, pbar_processed),
                daemon=True
            )
            cp_thread.start()
            all_threads.append(cp_thread)

            # Uploaders
            uploader_threads = []
            for _ in range(NUM_UPLOADERS):
                t = threading.Thread(
                    target=worker_uploader,
                    args=(stop_event, upload_queue, checkpoint_queue,
                          S3_OUTPUT_BUCKET, S3_OUTPUT_PREFIX),
                    daemon=True
                )
                t.start()
                uploader_threads.append(t)
            all_threads.extend(uploader_threads)

            # Processors
            processor_threads = []
            for _ in range(NUM_PROCESSORS):
                t = threading.Thread(
                    target=worker_processor,
                    args=(stop_event, processing_queue, upload_queue, mp_pool),
                    daemon=True
                )
                t.start()
                processor_threads.append(t)
            all_threads.extend(processor_threads)

            # Local Readers
            reader_threads = []
            for _ in range(NUM_LOCAL_READERS):
                t = threading.Thread(
                    target=worker_local_reader_filter,
                    args=(stop_event, local_file_queue, processing_queue,
                          buffer_manager, allowed_domains_set),
                    daemon=True
                )
                t.start()
                reader_threads.append(t)
            all_threads.extend(reader_threads)

            # Local Scanner
            scan_thread = threading.Thread(
                target=worker_local_scanner,
                args=(stop_event, local_file_queue, LOCAL_BUFFER_PATH, pbar_scanner),
                daemon=True
            )
            scan_thread.start()
            all_threads.append(scan_thread)

            # --- Avvia Pipeline PRODUCER ---
            # Local Downloaders
            downloader_threads = []
            for _ in range(NUM_S3_DOWNLOADERS):
                t = threading.Thread(
                    target=worker_local_downloader,
                    args=(stop_event, download_queue, buffer_manager,
                          LOCAL_BUFFER_PATH, S3_INPUT_BUCKET, pbar_download),
                    daemon=True
                )
                t.start()
                downloader_threads.append(t)
            all_threads.extend(downloader_threads)

            # S3 Lister (avviato per ultimo)
            lister_thread = threading.Thread(
                target=worker_s3_lister,
                args=(stop_event, download_queue, processed_files_set,
                      processed_files_lock, S3_INPUT_BUCKET, S3_INPUT_PREFIX,
                      pbar_s3_scan),
                daemon=True # Daemon=True per non bloccare l'uscita in caso di errore
            )
            lister_thread.start()
            all_threads.append(lister_thread)

            # --- Gestione Arresto ---
            print("Pipeline avviata. Premi CTRL+C per un arresto controllato.")
            try:
                # Aspetta che il lister finisca (o venga interrotto)
                lister_thread.join()

                # Se il lister finisce normalmente, aspetta che i downloader finiscano
                if not stop_event.is_set():
                    print("Scansione S3 completata. In attesa fine download...")
                    download_queue.join() # Attende che tutti i None siano processati
                    print("Download su SSD completati.")

                # A questo punto, sia che sia finito normalmente o interrotto,
                # diciamo a tutti gli altri di fermarsi (se non già fatto da CTRL+C)
                stop_event.set()

                print("In attesa arresto pipeline di processo...")
                # Aspetta lo svuotamento delle code in ordine inverso
                local_file_queue.join()
                print("- Coda file locali svuotata.")
                processing_queue.join()
                print("- Coda processo svuotata.")
                upload_queue.join()
                print("- Coda upload svuotata.")
                checkpoint_queue.join()
                print("- Coda checkpoint svuotata.")

                print("Tutte le code sono vuote. Arresto pulito.")

            except KeyboardInterrupt:
                print("\nRichiesta [CTRL+C]... Avvio arresto controllato...")
                print("Attesa completamento operazioni in corso...")
                stop_event.set()

            # Aspetta tutti i thread worker (il lister è già terminato)
            print("Attesa terminazione thread...")
            for t in all_threads:
                 if t != lister_thread and t.is_alive():
                     t.join(timeout=10) # Dagli un timeout per evitare blocchi

            print("Arresto del monitor...")
            mon_thread.join(timeout=5)

            # Chiudi le progress bar
            pbar_s3_scan.close()
            pbar_download.close()
            pbar_scanner.close()
            pbar_processed.close()

    print("Elaborazione pipeline con buffer locale completata.")


# --- 10. ESECUZIONE ---

if __name__ == "__main__":
    main()