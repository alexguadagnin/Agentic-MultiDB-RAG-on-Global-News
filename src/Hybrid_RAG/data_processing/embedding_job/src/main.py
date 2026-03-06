import os
import glob
import logging
import pandas as pd
import uuid
import torch
import time
from pathlib import Path
from sentence_transformers import SentenceTransformer
from langchain_text_splitters import RecursiveCharacterTextSplitter

# Importa la configurazione
import config

# --- Setup del Logging ---
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# --- Funzioni Helper per Checkpoint e Salvataggio ---

def create_dirs():
    """Crea tutte le directory necessarie se non esistono."""
    Path(config.OUTPUT_DIR).mkdir(exist_ok=True)
    Path(config.PROCESSED_DIR).mkdir(exist_ok=True)
    Path(config.CHECKPOINT_DIR).mkdir(exist_ok=True)
    log.info("Struttura directory verificata.")

def read_checkpoint(checkpoint_path):
    """Legge l'indice dal file di checkpoint."""
    if not checkpoint_path.exists():
        return 0
    try:
        with open(checkpoint_path, 'r') as f:
            start_index = int(f.read().strip())
        log.info(f"Ripristino da checkpoint: inizio dalla riga {start_index}")
        return start_index
    except Exception as e:
        log.error(f"Errore nel leggere checkpoint {checkpoint_path}: {e}. Rinizio da 0.")
        return 0

def save_checkpoint(checkpoint_path, index):
    """Salva l'indice nel file di checkpoint."""
    try:
        with open(checkpoint_path, 'w') as f:
            f.write(str(index))
    except Exception as e:
        log.error(f"FALLIMENTO nel salvare checkpoint su {checkpoint_path}: {e}")

# ==============================================================================
# NUOVA FUNZIONE DI SALVATAGGIO (V12)
# ==============================================================================
def save_batch_to_parquet(points_list, original_filename_stem, batch_start_index):
    """
    Salva un batch di punti in un file Parquet *UNIVOCO*.
    Non usa più 'append', che era la causa del bug.
    """
    if not points_list:
        return True # Nessun lavoro da fare = successo

    df = pd.DataFrame(points_list)
    
    # Formatta l'indice del batch (es. 0 -> "000000", 1000 -> "001000")
    batch_id = f"{batch_start_index:06d}" 
    
    # Nuovo nome file: es. "part-00000_batch_001000.parquet"
    output_filename = f"{original_filename_stem}_batch_{batch_id}.parquet"
    output_path = Path(config.OUTPUT_DIR) / output_filename
    
    try:
        # Scrittura atomica semplice. Niente più 'append'.
        df.to_parquet(output_path, engine='pyarrow') 
        return True # Ritorna SUCCESSO
    except Exception as e:
        log.error(f"FALLIMENTO nel salvare Parquet su {output_path}: {e}")
        return False # Ritorna FALLIMENTO
# ==============================================================================

def get_files_to_process():
    """Ottiene la lista dei file da processare."""
    all_input_files = set(f.name for f in Path(config.INPUT_DIR).glob("*.parquet"))
    processed_files = set(f.name for f in Path(config.PROCESSED_DIR).glob("*.parquet"))
    
    files_to_process = sorted(list(all_input_files - processed_files))
    
    log.info(f"Trovati {len(all_input_files)} file totali.")
    log.info(f"{len(processed_files)} file già processati in /data/processed_parquet.")
    log.info(f"{len(files_to_process)} file rimanenti da processare.")
    
    return files_to_process

# --- Funzioni Principali della Pipeline ---

def load_model(model_name):
    """Carica il modello di embedding sulla GPU."""
    try:
        log.info(f"Caricamento modello '{model_name}' su GPU (cuda)...")
        model = SentenceTransformer(model_name, device="cuda", trust_remote_code=True)
        log.info("Modello caricato con successo.")
        return model
    except Exception as e:
        log.error(f"Errore critico durante il caricamento del modello: {e}")
        raise e

def process_file(filename, model, text_splitter):
    """
    Processa un singolo file Parquet con logica di checkpoint granulare.
    """
    input_file_path = Path(config.INPUT_DIR) / filename
    checkpoint_file_path = Path(config.CHECKPOINT_DIR) / f"{filename}.txt"
    original_stem = Path(filename).stem # es. "part-00000"

    log.info(f"Inizio processamento file: {filename}")

    try:
        df = pd.read_parquet(input_file_path)
        total_rows = len(df)
        log.info(f"File '{filename}' caricato. Articoli totali: {total_rows}")
    except Exception as e:
        log.error(f"Impossibile leggere il file Parquet {filename}: {e}. Salto il file.")
        return

    start_index = read_checkpoint(checkpoint_file_path)
    if start_index >= total_rows:
        log.info(f"File '{filename}' già completamente processato (secondo checkpoint). Salto.")
        return

    df_to_process = df.iloc[start_index:]
    
    qdrant_points_buffer = []
    rows_processed_since_checkpoint = 0
    start_time = time.time()

    for current_index, row in df_to_process.iterrows():
        try:
            text = row[config.TEXT_COLUMN]
            if not text or not isinstance(text, str):
                log.warning(f"[{filename}] Riga {current_index}: Testo mancante o non valido. Salto.")
                continue
            
            metadata = row.drop(config.TEXT_COLUMN).to_dict()
            metadata["original_text_preview"] = text[:200] 
            
            chunks = text_splitter.split_text(text)
            if not chunks:
                continue
            
            embeddings = model.encode(
                chunks,
                batch_size=config.EMBEDDING_BATCH_SIZE,
                show_progress_bar=False 
            )
            
            for i, (chunk_text, vector) in enumerate(zip(chunks, embeddings)):
                qdrant_points_buffer.append({
                    "id": str(uuid.uuid4()), 
                    "vector": vector.tolist(),
                    "payload": {**metadata, "chunk_text": chunk_text, "chunk_number": i}
                })
            
            rows_processed_since_checkpoint += 1

        except Exception as e:
            log.error(f"[{filename}] FALLIMENTO CRITICO riga {current_index}: {e}. Salto riga.")

        # --- LOGICA DI CHECKPOINT V12 (CORRETTA) ---
        if rows_processed_since_checkpoint >= config.CHECKPOINT_EVERY_N_ROWS:
            end_time = time.time()
            rate = config.CHECKPOINT_EVERY_N_ROWS / (end_time - start_time)
            
            log.info(f"[{filename}] Progresso: {current_index + 1} / {total_rows} "
                     f"(Performance: {rate:.2f} articoli/sec)")
            
            log.info(f"[{filename}] Salvataggio checkpoint su disco...")

            # Calcola l'indice di riga da cui è partito questo batch
            batch_start_row_index = (current_index + 1) - rows_processed_since_checkpoint
            
            # Chiama la NUOVA funzione di salvataggio
            save_success = save_batch_to_parquet(
                qdrant_points_buffer,
                original_stem,
                batch_start_row_index
            )
            
            if save_success:
                save_checkpoint(checkpoint_file_path, current_index + 1)
            else:
                log.error(f"[{filename}] Checkpoint NON aggiornato a {current_index + 1} "
                          f"a causa di un errore di salvataggio Parquet. Il job ritenterà questo batch al riavvio.")
            
            qdrant_points_buffer.clear()
            rows_processed_since_checkpoint = 0
            start_time = time.time()

    # --- SALVATAGGIO FINALE V12 (CORRETTO) ---
    if qdrant_points_buffer:
        log.info(f"[{filename}] Salvataggio batch finale ({len(qdrant_points_buffer)} punti)...")
        
        # Calcola l'indice di riga da cui è partito questo batch finale
        batch_start_row_index = total_rows - len(qdrant_points_buffer)
        
        save_batch_to_parquet(
            qdrant_points_buffer,
            original_stem,
            batch_start_row_index
        )
        qdrant_points_buffer.clear()

    log.info(f"Completato file: {filename}. Eseguo pulizia...")
    if checkpoint_file_path.exists():
        os.remove(checkpoint_file_path)
    
    os.rename(input_file_path, Path(config.PROCESSED_DIR) / filename)
    log.info(f"Spostato {filename} in /data/processed_parquet.")


def main():
    """Punto di ingresso principale del Job V-Max."""
    log.info("--- Inizio Job di Embedding Batch (V-Max) ---")
    
    create_dirs()
    
    model = load_model(config.MODEL_NAME)
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=config.CHUNK_SIZE,
        chunk_overlap=config.CHUNK_OVERLAP
    )
    
    files_to_process = get_files_to_process()
    
    if not files_to_process:
        log.info("Nessun file nuovo da processare. Uscita.")
        return

    log.info(f"Trovati {len(files_to_process)} file da processare.")
    
    for i, filename in enumerate(files_to_process):
        log.info(f"--- Inizio File {i+1}/{len(files_to_process)}: {filename} ---")
        try:
            process_file(filename, model, text_splitter)
            log.info(f"--- Completato File {i+1}/{len(files_to_process)}: {filename} ---")
        except Exception as e:
            log.error(f"FALLIMENTO IRRECUPERABILE su file {filename}: {e}")
            log.error("Passo al file successivo...")
            
    log.info("--- Job di Embedding Batch (V-Max) Completato ---")

if __name__ == "__main__":
    main()