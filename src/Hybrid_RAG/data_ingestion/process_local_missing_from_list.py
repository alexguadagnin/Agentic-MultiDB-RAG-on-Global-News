import json
import csv
from collections import defaultdict
import re
from typing import List, Dict, Tuple
from tqdm import tqdm
import multiprocessing as mp
from functools import partial
import boto3
from botocore.exceptions import ClientError # Per head_object error
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

LOCAL_BUFFER_PATH = Path("...") # Path alla cartella locale da usare come buffer (DEVE ESISTERE)
MAX_BUFFER_GB = 500 # Limite in GB per il buffer
CHECKPOINT_FILE = 'processed_files_FIX_checkpoint.log' # Path al file di checkpoint
TODO_FILE = 'files_to_process.txt' # File con la lista da fare (nella stessa cartella dello script)

MAX_BUFFER_BYTES = MAX_BUFFER_GB * (1024**3) # Converti GB in Byte

# Parametri S3
S3_INPUT_BUCKET = "hybrid-rag-gdelt-bucket"
S3_OUTPUT_BUCKET = "hybrid-rag-gdelt-bucket"
S3_OUTPUT_PREFIX = "gdelt_reconstructed_parquet_FIX/" # Cartella di output su S3

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
            # Assicura che pre, ngram, post siano stringhe
            pre = entry.get('pre', '') or ''
            ngram = entry.get('ngram', '') or ''
            post = entry.get('post', '') or ''
            sentence = ' '.join([pre, ngram, post])

            pos_val = entry.get('pos', 999) # Default alto se manca 'pos'
            try:
                pos_int = int(pos_val)
            except (ValueError, TypeError):
                pos_int = 999 # Default se non convertibile

            if pos_int < 20 and " / " in sentence:
                parts = sentence.split(" / ")
                if len(parts) > 1:
                    sentence = " / ".join(parts[1:]) # Prendi dal secondo elemento in poi
            transformed_entries.append({
                'date': entry.get('date', 'YYYY-MM-DD'), # Default se manca date
                'lang': entry.get('lang', 'un'),        # Default se manca lang
                'type': entry.get('type', -1),          # Default se manca type
                'pos': pos_int,                         # Usa valore intero o default
                'sentence': sentence.strip()            # Rimuovi spazi extra
            })
        transformed_data[url] = transformed_entries
    return transformed_data

def reconstruct_sentence(fragments: List[str], positions: List[int] = None) -> str:
    if not fragments: return ""
    if len(fragments) == 1: return fragments[0]

    # Combina frammenti con posizioni e ordina
    if positions:
        indexed_fragments = sorted(zip(positions, fragments), key=lambda x: x[0])
    else:
        # Se non ci sono posizioni, non possiamo ordinarli in modo affidabile
        # Potremmo provare a unirli in ordine, ma l'overlap è più robusto
        indexed_fragments = [(i, f) for i, f in enumerate(fragments)] # Mantieni ordine originale

    # Logica di overlap (leggermente semplificata ma robusta)
    ordered_fragments = [f for pos, f in indexed_fragments]
    if not ordered_fragments: return ""

    result_text = ordered_fragments[0]
    used_indices = {indexed_fragments[0][0]} # Usa l'indice originale se disponibile, altrimenti 0

    for _ in range(1, len(ordered_fragments)): # Itera per unire i frammenti rimanenti
        best_overlap = -1 # Inizia con -1 per indicare nessun overlap valido trovato
        best_fragment_text = None
        best_fragment_idx = -1
        append_at_end = True

        for i in range(len(ordered_fragments)):
             # Usa l'indice corretto per controllare 'used_indices'
            original_index = indexed_fragments[i][0] if positions else i
            if original_index in used_indices: continue

            fragment_text = ordered_fragments[i]
            if not fragment_text: continue

            # Controllo overlap semplificato (cerca la sottostringa più lunga)
            # Questo è meno preciso del word-overlap ma più veloce e robusto a errori
            max_check = min(len(result_text), len(fragment_text), 30) # Limita check a 30 chars

            # Check Suffix(result) vs Prefix(fragment) -> Appendi
            for k in range(max_check, 0, -1):
                if result_text.endswith(fragment_text[:k]):
                    if k > best_overlap:
                        best_overlap = k
                        best_fragment_text = fragment_text
                        best_fragment_idx = original_index
                        append_at_end = True
                    break # Trovato miglior overlap per append

            # Check Prefix(result) vs Suffix(fragment) -> Prependi
            for k in range(max_check, 0, -1):
                if result_text.startswith(fragment_text[-k:]):
                     if k > best_overlap: # Sovrascrivi solo se l'overlap è strettamente migliore
                        best_overlap = k
                        best_fragment_text = fragment_text
                        best_fragment_idx = original_index
                        append_at_end = False
                     break # Trovato miglior overlap per prepend

        if best_fragment_idx != -1:
            if append_at_end:
                result_text += best_fragment_text[best_overlap:]
            else: # Prepend
                result_text = best_fragment_text[:-best_overlap] + result_text
            used_indices.add(best_fragment_idx)
        else:
            # Nessun overlap trovato con nessun frammento rimanente, unisci il prossimo ordinato?
            # Per ora, fermiamoci qui per evitare di unire cose a caso.
            break

    return result_text

def remove_overlap(text: str) -> str:
    if len(text) < 50: return text
    n = len(text)
    # Cerca l'overlap più lungo possibile (fino a metà stringa)
    # che sia all'inizio e alla fine
    for k in range(n // 2, 5, -1): # Da metà lunghezza giù fino a 6 caratteri
        if text.startswith(text[-k:]):
            # Trovato overlap: prendi la parte dopo l'overlap iniziale
            return text[k:]
    return text # Nessun overlap significativo trovato

def process_article(url_entries_tuple):
    try:
        url, entries = url_entries_tuple
        if not entries: return None

        # Assicura che 'pos' sia un intero e gestisci errori/mancanze
        valid_entries = []
        for e in entries:
            try:
                e['pos'] = int(e.get('pos', 999)) # Converte o assegna default
                valid_entries.append(e)
            except (ValueError, TypeError):
                e['pos'] = 999 # Assegna default se conversione fallisce
                valid_entries.append(e)

        valid_entries.sort(key=lambda x: x['pos'])
        sentences = [e.get('sentence', '') for e in valid_entries] # Prendi sentence o stringa vuota
        positions = [e['pos'] for e in valid_entries]

        reconstructed_sentences = reconstruct_sentence(sentences, positions)
        text = remove_overlap(reconstructed_sentences)
        textok = text.replace("|", " ").replace('"', " ").strip()
        textok = re.sub(r'\s+', ' ', textok)

        # Filtro lunghezza più stringente
        if len(textok) < 150:
             # print(f"Skipping short/failed reconstruction for {url} (length: {len(textok)})")
             return None

        first_date = valid_entries[0].get('date', 'YYYY-MM-DD')[:10]
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
    try:
        total_size = 0
        print("Calcolo dimensione iniziale del buffer (può richiedere tempo)...")
        files_to_check = list(path.glob('*.json')) + list(path.glob('*.downloading'))
        for f in tqdm(files_to_check, desc="Scanning buffer"):
            if f.is_file(): # Già filtrato da glob
                try: total_size += f.stat().st_size
                except FileNotFoundError: continue
        gb_size = total_size / (1024**3)
        print(f"Dimensione iniziale buffer: {gb_size:.2f} GB")
        return total_size
    except Exception as e:
        print(f"Errore calcolo dimensione iniziale: {e}")
        return 0

def safe_s3_key_to_local(s3_key: str) -> str:
    # Rimuove .json se presente prima di sostituire /
    base_key = s3_key.replace('.json', '')
    return base_key.replace('/', '__')

def local_to_safe_s3_key(local_name: str) -> str:
    # Rimuove .json se presente prima di sostituire __
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

# <<< worker_s3_lister RIMOSSO >>>

def worker_local_downloader(
    stop_event: threading.Event,
    download_queue: queue.Queue, # Riceve (s3_key_CON_json, file_size)
    buffer_manager: BufferManager,
    local_buffer_path: Path,
    s3_input_bucket: str,
    pbar_download: tqdm
):
    """ VERSIONE ROBUSTA + BOTO3 OTTIMIZZATO """
    s3_client = boto3.client('s3')
    transfer_config = boto3.s3.transfer.TransferConfig(
        multipart_threshold=100 * 1024 * 1024, max_concurrency=10,
        multipart_chunksize=25 * 1024 * 1024, use_threads=True
    )

    while True:
        item = download_queue.get()
        if item is None:
            download_queue.task_done()
            break

        s3_key, file_size = item # Riceve chiave CON .json
        if stop_event.is_set():
            download_queue.task_done()
            break

        # Usa la chiave CON .json per creare i nomi file locali
        local_filename = safe_s3_key_to_local(s3_key) + ".json" # Aggiunge sempre .json
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


# --- 8. PIPELINE 2: PROCESSORE (CONSUMER) ---

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
            # Usa glob per efficienza
            current_files_in_dir_paths = {f for f in local_buffer_path.glob('*.json')}
            current_files_in_dir_names = {f.name for f in current_files_in_dir_paths}
            new_file_names = current_files_in_dir_names - processed_locally

            for f_name in new_file_names:
                f_path = local_buffer_path / f_name
                # Verifica esistenza (potrebbe essere cancellato nel frattempo)
                if f_path.exists():
                    local_file_queue.put(f_path)
                    processed_locally.add(f_name)
                    files_found_this_scan += 1

            if files_found_this_scan > 0:
                 # Aggiorna il totale solo se necessario (più preciso)
                 if pbar_scanner.total is None or len(processed_locally) > pbar_scanner.total:
                     pbar_scanner.total = len(processed_locally)
                 pbar_scanner.refresh()


            # Pulisci il set processed_locally dai file non più esistenti
            processed_locally &= current_files_in_dir_names

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

        # Ottieni la chiave S3 originale (con .json) dal nome file locale
        s3_key_original = local_to_safe_s3_key(local_path.name) + ".json"
        relevant_entries = []
        file_size = 0
        processed_successfully = False

        try:
            try: file_size = local_path.stat().st_size
            except FileNotFoundError:
                local_file_queue.task_done(); pbar_scanner.update(1); continue

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
                processing_queue.put((s3_key_original, articles_to_process))
            else:
                processing_queue.put((s3_key_original, None)) # File letto ma vuoto
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
            pbar_scanner.update(1) # Aggiorna pbar scanner DOPO aver tentato il processo/cancellazione
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

        s3_key, articles_to_process_dict = item # Riceve chiave CON .json
        if articles_to_process_dict is None:
            upload_queue.put((s3_key, None)) # Passa chiave CON .json
            processing_queue.task_done(); continue
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
                # Adatta chunksize dinamicamente
                chunk_size = max(1, len(work_items_reconstruct) // (mp_pool._processes * 2 if mp_pool._processes else 2))
                processor_results = mp_pool.imap_unordered(process_func, work_items_reconstruct, chunksize=chunk_size)
                results.extend(filter(None, processor_results))

            for item_skip in work_items_skip: results.append(process_skipped_article(item_skip))

            if results:
                df = pd.DataFrame(results)
                upload_queue.put((s3_key, df)) # Passa chiave CON .json
            else:
                upload_queue.put((s3_key, None)) # Passa chiave CON .json

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

        s3_key, df = item # Riceve chiave CON .json
        upload_successful = False
        try:
            if df is not None and not df.empty:
                # Usa chiave CON .json per creare nome output normalizzato
                output_name = safe_s3_key_to_local(s3_key) + ".parquet"
                output_s3_path = f"s3://{s3_output_bucket}/{s3_output_prefix}{output_name}"
                df.to_parquet(output_s3_path, index=False, engine='pyarrow')
            upload_successful = True
        except Exception as e: print(f"Errore UPLOAD {s3_key}: {e}")
        finally:
            if upload_successful: checkpoint_queue.put(s3_key) # Passa chiave CON .json
            upload_queue.task_done()

def worker_checkpointer(
    stop_event: threading.Event,
    checkpoint_queue: queue.Queue,
    log_file_handle: io.TextIOWrapper,
    processed_files_set: set,
    processed_files_lock: threading.Lock,
    pbar_processed: tqdm
):
    """ Worker Thread (SINGOLO): Scrive sul log la chiave normalizzata. """
    while not (stop_event.is_set() and checkpoint_queue.empty()):
        try: s3_key_original = checkpoint_queue.get(timeout=1) # Riceve chiave CON .json
        except queue.Empty: continue
        try:
            s3_key_normalized = s3_key_original.replace('.json', '')
            with processed_files_lock:
                # Controlla se la VERSIONE NORMALIZZATA è già stata AGGIUNTA al set IN MEMORIA
                # (potrebbe essere stata aggiunta all'avvio o da un'iterazione precedente)
                if s3_key_normalized not in processed_files_set:
                    processed_files_set.add(s3_key_normalized)
                    # Aggiungi anche la versione originale al set in memoria per sicurezza
                    # durante questa run, sebbene il lister non dovrebbe più mandarla qui
                    processed_files_set.add(s3_key_original)
                    # Scrivi SOLO la versione normalizzata nel file di log
                    log_file_handle.write(f"{s3_key_normalized}\n")
                    log_file_handle.flush()
            # Aggiorna pbar SEMPRE, perché conta i file che hanno completato il ciclo
            pbar_processed.update(1)
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
        return

    print(f"Buffer locale: {LOCAL_BUFFER_PATH}")
    print(f"Limite buffer: {MAX_BUFFER_GB} GB ({MAX_BUFFER_BYTES} bytes)")

    processed_files_set = set()
    processed_files_lock = threading.Lock() # Lock ancora utile per scrittura checkpoint

    try:
        print(f"Lettura checkpoint da: {CHECKPOINT_FILE}")
        with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f: # Specifica encoding
            lines_read = 0
            for line in f:
                original_key = line.strip()
                if original_key:
                    lines_read += 1
                    processed_files_set.add(original_key)
                    normalized_key = original_key.replace('.json', '')
                    processed_files_set.add(normalized_key)
        print(f"Lette {lines_read} righe dal log DELTA. Caricati {len(processed_files_set)} elementi unici nel set di checkpoint.")
    except FileNotFoundError: print(f"Nessun file di checkpoint DELTA trovato in {CHECKPOINT_FILE}.")
    except Exception as e: print(f"Errore lettura checkpoint {CHECKPOINT_FILE}: {e}")


    initial_buffer_size = get_dir_size_on_startup(LOCAL_BUFFER_PATH)
    buffer_manager = BufferManager(initial_buffer_size, MAX_BUFFER_BYTES)

    # --- Code ---
    download_queue = queue.Queue(maxsize=NUM_S3_DOWNLOADERS * 3)
    local_file_queue = queue.Queue(maxsize=NUM_LOCAL_READERS * 3)
    processing_queue = queue.Queue(maxsize=NUM_PROCESSORS * 3)
    upload_queue = queue.Queue(maxsize=NUM_UPLOADERS * 3)
    checkpoint_queue = queue.Queue()

    stop_event = threading.Event()

    # --- Barre di Avanzamento ---
    pbar_download = tqdm(desc="File Scaricati su SSD", unit="file", position=0, dynamic_ncols=True)
    pbar_scanner = tqdm(desc="File Letti da SSD", unit="file", position=1, dynamic_ncols=True, total=0)
    pbar_processed = tqdm(desc="File Checkpointati", unit="file", position=2, dynamic_ncols=True)

    # --- Leggi la lista TODO ---
    files_to_process_list = []
    todo_file_path = Path(TODO_FILE) # Cerca nella cartella corrente
    if not todo_file_path.exists():
        # Prova a cercarlo nella root del progetto se non trovato
        script_dir = Path(__file__).parent
        project_root_todo = script_dir.parent / TODO_FILE
        if project_root_todo.exists():
             todo_file_path = project_root_todo
        else:
             print(f"ERRORE: File '{TODO_FILE}' non trovato né nella cartella corrente né nella root del progetto. Esegui prima 'generate_todo_list.py'. Arresto.")
             return

    try:
        print(f"Lettura lista file da processare da: {todo_file_path}")
        with open(todo_file_path, 'r', encoding='utf-8') as f_todo:
            files_to_process_list = [line.strip() for line in f_todo if line.strip()]
        print(f"Trovati {len(files_to_process_list)} file nella lista 'to-do'.")
    except Exception as e:
        print(f"ERRORE durante la lettura di '{todo_file_path}': {e}")
        return

    if not files_to_process_list:
        print("Nessun file da processare trovato nella lista 'to-do'. Verifica che 'generate_todo_list.py' abbia trovato differenze.")
        # Processa comunque i file nel buffer locale prima di uscire
        # return # Rimosso per permettere pulizia buffer
        pass # Continua per pulire il buffer

    # --- Filtra la lista TODO contro il checkpoint caricato ---
    # Questo passaggio è cruciale se il checkpoint è stato aggiornato DOPO la creazione del TODO
    keys_actually_to_do = []
    print("Filtraggio lista 'to-do' contro checkpoint attuale...")
    for normalized_key in tqdm(files_to_process_list, desc="Filtraggio TODO"):
        original_key = normalized_key + ".json"
        # Controlla se *entrambe* le versioni NON sono nel set caricato
        if normalized_key not in processed_files_set and original_key not in processed_files_set:
            keys_actually_to_do.append(normalized_key) # Aggiungi la chiave normalizzata

    num_actually_to_do = len(keys_actually_to_do)
    print(f"File effettivamente da processare dopo filtro checkpoint: {num_actually_to_do}")

    # Imposta i totali delle barre
    pbar_download.total = num_actually_to_do
    pbar_processed.total = num_actually_to_do # Anche la barra finale avrà questo totale
    pbar_download.refresh()
    pbar_processed.refresh()


    # --- Ottieni le dimensioni dei file da S3 (solo per quelli da fare) ---
    s3_client_main = boto3.client('s3')
    items_for_download_queue = []
    if num_actually_to_do > 0:
        print("Recupero dimensioni file da S3 per i file mancanti...")
        for normalized_key in tqdm(keys_actually_to_do, desc="Recupero info S3"):
            if stop_event.is_set(): break
            original_key = normalized_key + ".json"
            try:
                response = s3_client_main.head_object(Bucket=S3_INPUT_BUCKET, Key=original_key)
                file_size = response['ContentLength']
                if file_size > 0:
                    items_for_download_queue.append((original_key, file_size))
                else: print(f"WARN: File {original_key} su S3 ha dimensione 0.")
            except ClientError as e:
                if e.response['Error']['Code'] == '404': print(f"ERRORE: File {original_key} da lista TODO non trovato su S3!")
                else: print(f"ERRORE S3 head_object per {original_key}: {e}")
            except Exception as e: print(f"ERRORE recupero info per {original_key}: {e}")

        # Aggiorna totali barre con numero effettivo trovato su S3
        actual_files_found_on_s3 = len(items_for_download_queue)
        print(f"Pronto a scaricare {actual_files_found_on_s3} file validi da S3.")
        if actual_files_found_on_s3 != num_actually_to_do:
             print(f"ATTENZIONE: Il numero di file trovati su S3 ({actual_files_found_on_s3}) non corrisponde al numero da fare ({num_actually_to_do}). Alcuni file potrebbero mancare su S3.")
             pbar_download.total = actual_files_found_on_s3
             pbar_processed.total = actual_files_found_on_s3 # Usa numero reale
             pbar_download.refresh()
             pbar_processed.refresh()

    if not items_for_download_queue and num_actually_to_do > 0:
        print("ERRORE: Nessun file valido trovato su S3 corrispondente alla lista 'to-do'. Verifica S3 e la lista.")
        # Continua per pulire il buffer
        pass
    elif not items_for_download_queue and num_actually_to_do == 0:
         print("Nessun nuovo file da scaricare.")
         # Continua per pulire il buffer


    # --- Popola la coda di download (in thread separato) ---
    def populate_download_queue():
        if items_for_download_queue:
            print("Popolamento coda di download...")
            for item in items_for_download_queue:
                if stop_event.is_set(): break
                download_queue.put(item)
            print("Coda di download popolata.")
        else:
             print("Nessun file da mettere in coda per il download.")
        # Metti le poison pills SEMPRE per fermare i downloader
        for _ in range(NUM_S3_DOWNLOADERS):
            download_queue.put(None)

    populator_thread = threading.Thread(target=populate_download_queue, daemon=True)


    # --- Avvio Pipeline ---
    with open(CHECKPOINT_FILE, 'a', encoding='utf-8') as log_file: # Usa encoding anche qui
        num_mp_cores = max(1, mp.cpu_count() - 2)
        print(f"Avvio M-Pool condivisa con {num_mp_cores} core.")
        with mp.Pool(processes=num_mp_cores) as mp_pool:

            all_threads = []

            # Monitor
            queues_dict = {
                "Download": download_queue, "Local": local_file_queue,
                "Process": processing_queue, "Upload": upload_queue,
                "ChkPoint": checkpoint_queue
            }
            mon_thread = threading.Thread(target=worker_monitor, args=(stop_event, queues_dict, buffer_manager, 30), daemon=True)
            mon_thread.start()

            # CONSUMER Pipeline
            cp_thread = threading.Thread(target=worker_checkpointer, args=(stop_event, checkpoint_queue, log_file, processed_files_set, processed_files_lock, pbar_processed), daemon=True); cp_thread.start(); all_threads.append(cp_thread)
            uploader_threads = [];
            for _ in range(NUM_UPLOADERS): t = threading.Thread(target=worker_uploader, args=(stop_event, upload_queue, checkpoint_queue, S3_OUTPUT_BUCKET, S3_OUTPUT_PREFIX), daemon=True); t.start(); uploader_threads.append(t); all_threads.extend(uploader_threads)
            processor_threads = [];
            for _ in range(NUM_PROCESSORS): t = threading.Thread(target=worker_processor, args=(stop_event, processing_queue, upload_queue, mp_pool), daemon=True); t.start(); processor_threads.append(t); all_threads.extend(processor_threads)
            reader_threads = [];
            for _ in range(NUM_LOCAL_READERS): t = threading.Thread(target=worker_local_reader_filter, args=(stop_event, local_file_queue, processing_queue, buffer_manager, allowed_domains_set, pbar_scanner), daemon=True); t.start(); reader_threads.append(t); all_threads.extend(reader_threads)
            scan_thread = threading.Thread(target=worker_local_scanner, args=(stop_event, local_file_queue, LOCAL_BUFFER_PATH, pbar_scanner), daemon=True); scan_thread.start(); all_threads.append(scan_thread)

            # PRODUCER Pipeline (solo Downloaders)
            downloader_threads = [];
            for _ in range(NUM_S3_DOWNLOADERS): t = threading.Thread(target=worker_local_downloader, args=(stop_event, download_queue, buffer_manager, LOCAL_BUFFER_PATH, S3_INPUT_BUCKET, pbar_download), daemon=True); t.start(); downloader_threads.append(t); all_threads.extend(downloader_threads)

            # Avvia popolamento coda
            populator_thread.start()

            # Gestione Arresto
            print("Pipeline avviata per processare la lista 'to-do'. Premi CTRL+C per un arresto controllato.")
            try:
                populator_thread.join() # Aspetta che la coda sia popolata

                print("Attesa fine download...")
                download_queue.join()
                for t in downloader_threads: t.join()
                print("Download su SSD completati.")

                print("In attesa svuotamento coda file locali (lettura)...")
                # Dobbiamo aspettare che lo scanner finisca E la coda sia vuota
                # Lo scanner si fermerà solo con stop_event
                # Quindi aspettiamo prima che la coda si svuoti
                local_file_queue.join()
                print("- Coda file locali svuotata.")

                stop_event.set() # Diciamo a tutti (scanner incluso) di fermarsi
                print("Stop event settato. Attesa terminazione workers...")

                # Aspetta le code rimanenti e i join dei thread
                print("In attesa svuotamento coda processo...")
                processing_queue.join(); print("- Coda processo svuotata.")
                print("In attesa svuotamento coda upload...")
                upload_queue.join(); print("- Coda upload svuotata.")
                print("In attesa svuotamento coda checkpoint...")
                checkpoint_queue.join(); print("- Coda checkpoint svuotata.")

                print("Tutte le code sono vuote. Arresto pulito.")

            except KeyboardInterrupt:
                print("\nRichiesta [CTRL+C]... Avvio arresto controllato...")
                stop_event.set()

            # Attesa terminazione thread
            print("Attesa terminazione thread...")
            # Unisci prima i downloader
            for t in downloader_threads:
                 if t.is_alive(): t.join(timeout=10)
            # Poi il resto (escluso il populator già terminato)
            joinable_threads = [t for t in all_threads if t != populator_thread]
            for t in reversed(joinable_threads):
                 if t and t.is_alive():
                     t.join(timeout=15)

            print("Arresto del monitor...")
            mon_thread.join(timeout=5)

            print("Chiusura barre di avanzamento...")
            pbar_download.close()
            pbar_scanner.close()
            pbar_processed.close()
            print("Barre chiuse.")

    print("Elaborazione pipeline basata su lista 'to-do' completata.")


# --- 10. ESECUZIONE ---

if __name__ == "__main__":
    main()