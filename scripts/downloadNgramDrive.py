import os
import re
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
#from Hybrid_RAG.constants import DATA_DIR



# --- CONFIGURAZIONE ---

# Modifica questi percorsi in base alla tua configurazione

CARTELLA_INPUT = r"...."  # La cartella dove si trovano i tuoi file .csv.zip
CARTELLA_OUTPUT = Path(r"...")

MAX_WORKERS = 22

# --- FUNZIONE WORKER PER IL DOWNLOAD ---
def download_file(url: str, output_path: Path) -> str:

    """
    Scarica un singolo file e restituisce una stringa di stato.
    Questa funzione è progettata per essere eseguita in un thread separato.
    """

    filename = output_path.name

    # Controlla se il file esiste già prima di iniziare il download
    if output_path.exists():
        return f"✅ Già presente: {filename}"
    try:
        response = requests.get(url, stream=True, timeout=30)
        # Lancia un'eccezione per errori HTTP (es. 404 Not Found, 500 Server Error)
        response.raise_for_status() 

        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return f"⚡️ Scaricato: {filename}"

    except requests.exceptions.HTTPError as e:
        # Gestisce specificamente gli errori HTTP come il 404
        return f"⚠️  Non trovato (404): {filename}"
    
    except requests.exceptions.RequestException as e:
        # Gestisce altri errori di rete (timeout, problemi di connessione)
        return f"❌ Errore di connessione per {filename}: {e}"

# --- SCRIPT PRINCIPALE ---
def scarica_ngrams_parallelo():
    """
    Scansiona i file GDELT v2 e scarica in parallelo tutti i file n-gram
    corrispondenti per ogni intervallo di 15 minuti.
    """

    print("Avvio dello script di download parallelo... 🚀")

    # 1. Crea la cartella di output se non esiste
    CARTELLA_OUTPUT.mkdir(parents=True, exist_ok=True)

    # 2. Trova i timestamp unici degli intervalli di 15 minuti
    try:
        files_esistenti = os.listdir(CARTELLA_INPUT)
    except FileNotFoundError:
        print(f"ERRORE: La cartella di input '{CARTELLA_INPUT}' non è stata trovata.")
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

    # 3. Genera la lista di tutti i file da scaricare
    tasks_to_download = []
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
            percorso_output = CARTELLA_OUTPUT / nome_file_ngram

            tasks_to_download.append((url_ngram, percorso_output))

    if not tasks_to_download:
        print("Nessun file da scaricare nel periodo di tempo valido (dopo il 01-01-2020).")
        return

    print(f"Generata una lista di {len(tasks_to_download)} file n-gram da scaricare.")
    print(f"Avvio del download con un massimo di {MAX_WORKERS} worker paralleli...")

    # 4. Esegui i download in parallelo
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Crea un "future" per ogni task di download
        future_to_task = {executor.submit(download_file, url, path): (url, path) for url, path in tasks_to_download}

        # Stampa i risultati man mano che i download vengono completati
        for i, future in enumerate(as_completed(future_to_task)):
            result = future.result()
            print(f"[{i + 1}/{len(tasks_to_download)}] {result}")

    print("\nOperazione completata! 👍")


if __name__ == '__main__':
    scarica_ngrams_parallelo()