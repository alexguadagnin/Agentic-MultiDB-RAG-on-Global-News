import os
import re
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from Hybrid_RAG.constants import DATA_DIR

# --- CONFIGURAZIONE LOCALE ---
# Il percorso locale da cui leggere i file di input rimane invariato
CARTELLA_INPUT = DATA_DIR / 'gdelt_event'  # La cartella locale dove si trovano i tuoi file .csv.zip

# --- CONFIGURAZIONE AMAZON S3 ---
# Modifica queste variabili con la tua configurazione AWS S3
S3_BUCKET_NOME = "hybrid-rag-gdelt-bucket"
S3_CARTELLA_OUTPUT = "gdelt_ngrams/"

# --- IMPOSTAZIONI DI PROCESSO ---
MAX_WORKERS = 20 

# Inizializza il client S3
s3_client = boto3.client('s3')

# --- FUNZIONE WORKER PER DOWNLOAD E UPLOAD SU S3 ---

def download_and_upload_to_s3(url: str, bucket: str, s3_key: str) -> str:
    """
    Scarica un file da un URL e lo carica direttamente su S3 senza salvarlo localmente.
    Restituisce una stringa di stato.
    """
    filename = os.path.basename(s3_key)

    # 1. Controlla se l'oggetto esiste già su S3 prima di scaricarlo
    try:
        s3_client.head_object(Bucket=bucket, Key=s3_key)
        return f"✅ Già presente in S3: {filename}"
    except ClientError as e:
        # Se l'errore è 404, il file non esiste e possiamo procedere.
        # Altri errori (es. 403 Forbidden) verranno sollevati.
        if e.response['Error']['Code'] != '404':
            return f"❌ Errore S3 (controllo esistenza): {filename} - {e}"

    # 2. Se non esiste, scarica il file dall'URL di origine
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status() # Lancia eccezione per errori HTTP (es. 404, 500)

        # 3. Carica il contenuto del file (in-memory) direttamente su S3
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=response.content
        )
        return f"⬆️ Caricato su S3: {filename}"

    except requests.exceptions.HTTPError:
        return f"⚠️  Non trovato su GDELT (404): {filename}"
    except requests.exceptions.RequestException as e:
        return f"❌ Errore di connessione per {filename}: {e}"
    except ClientError as e:
        return f"❌ Errore S3 (upload) per {filename}: {e}"

# --- SCRIPT PRINCIPALE ---

def scarica_ngrams_e_carica_su_s3():
    """
    Scansiona i file GDELT da una cartella locale e carica in parallelo
    i file n-gram corrispondenti su Amazon S3.
    """
    print("Avvio dello script di download e upload parallelo su S3... 🚀")

    # 1. Trova i timestamp unici leggendo i file dalla cartella di input LOCALE
    try:
        files_esistenti = os.listdir(CARTELLA_INPUT)
    except FileNotFoundError:
        print(f"ERRORE: La cartella di input locale '{CARTELLA_INPUT}' non è stata trovata.")
        return

    timestamps_intervallo = set()
    regex_timestamp = re.compile(r'(\d{14})')
    for filename in files_esistenti:
        if match := regex_timestamp.search(filename):
            timestamps_intervallo.add(match.group(1))

    if not timestamps_intervallo:
        print(f"Nessun file con timestamp valido trovato in '{CARTELLA_INPUT}'.")
        return

    print(f"Trovati {len(timestamps_intervallo)} intervalli di 15 minuti da elaborare.")

    # 2. Genera la lista di tutti i task (URL da scaricare e percorso S3 di destinazione)
    tasks_to_process = []
    data_inizio_ngrams = datetime.strptime("20200101000000", "%Y%m%d%H%M%S")

    for ts_inizio_str in sorted(list(timestamps_intervallo)):
        start_time = datetime.strptime(ts_inizio_str, "%Y%m%d%H%M%S")

        if start_time < data_inizio_ngrams:
            continue
            
        # Per ogni intervallo di 15 min, genera i 15 task da 1 min
        for i in range(15):
            current_time = start_time + timedelta(minutes=i)
            ts_minuto_str = current_time.strftime("%Y%m%d%H%M%S")
            
            nome_file_ngram = f"{ts_minuto_str}.webngrams.json.gz"
            url_ngram = f"http://data.gdeltproject.org/gdeltv3/webngrams/{nome_file_ngram}"
            
            # La destinazione non è più un percorso locale, ma una "chiave" S3
            s3_key_output = f"{S3_CARTELLA_OUTPUT}{nome_file_ngram}"
            
            tasks_to_process.append((url_ngram, s3_key_output))
    
    if not tasks_to_process:
        print("Nessun file da scaricare nel periodo di tempo valido (dopo il 01-01-2020).")
        return
        
    print(f"Generata una lista di {len(tasks_to_process)} file n-gram da scaricare e caricare su S3.")
    print(f"Avvio del processo con un massimo di {MAX_WORKERS} worker paralleli...")

    # 3. Esegui i task di download e upload in parallelo
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Crea un "future" per ogni task
        future_to_task = {
            executor.submit(download_and_upload_to_s3, url, S3_BUCKET_NOME, s3_key): (url, s3_key)
            for url, s3_key in tasks_to_process
        }
        
        # Stampa i risultati man mano che vengono completati
        for i, future in enumerate(as_completed(future_to_task)):
            result = future.result()
            print(f"[{i + 1}/{len(tasks_to_process)}] {result}")

    print("\nOperazione completata! 👍")


if __name__ == '__main__':
    scarica_ngrams_e_carica_su_s3()