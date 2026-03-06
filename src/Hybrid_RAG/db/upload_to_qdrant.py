import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct
import glob
import sys
import time
import json
from pathlib import Path
from tqdm import tqdm
import numpy as np

# ==============================================================================
# 1. LOGICA DI IMPORT (Per trovare 'constants.py')
# ==============================================================================
try:
    SRC_ROOT = Path(__file__).parent.parent.parent.resolve()
    if str(SRC_ROOT) not in sys.path:
        sys.path.append(str(SRC_ROOT))
    from Hybrid_RAG.constants import RAW_DATA_DIR_NGRAMS
except ImportError:
    print("❌ ERRORE CRITICO: Impossibile importare 'constants.py'.")
    sys.exit(1)
# ==============================================================================

# --- CONFIGURAZIONE ---
EMBEDDING_DATA_DIR = RAW_DATA_DIR_NGRAMS / "embedding_output"
COLLECTION_NAME = "gdelt_articles"
VECTOR_DIMENSION = 1024
STATE_FILE = Path(__file__).parent / "upload_state.json"
# ----------------------

def connect_to_qdrant():
    """
    Si connette a Qdrant in locale usando HTTP/REST con timeout.
    """
    print("Tentativo di connessione a Qdrant (localhost:6333 - REST)...")
    try:
        client = QdrantClient(
            host="localhost", 
            port=6333, 
            https=False,
            timeout=60.0
        ) 
        
        client.get_collections()
        print("✅ Connessione a Qdrant stabilita.")
        return client
    except Exception as e:
        print(f"❌ FALLIMENTO: Impossibile connettersi a Qdrant.")
        print(f"Errore: {e}")
        sys.exit(1)

def create_collection_if_not_exists(client):
    """Crea la collezione solo se non esiste già."""
    print(f"Verifica della collezione: '{COLLECTION_NAME}'")
    try:
        if not client.collection_exists(collection_name=COLLECTION_NAME):
            print("Collezione non trovata. Creazione in corso...")
            client.create_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=VectorParams(
                    size=VECTOR_DIMENSION,
                    distance=Distance.COSINE
                )
            )
            print("✅ Collezione creata con successo.")
        else:
            print("✅ Collezione già esistente.")
    except Exception as e:
        print(f"❌ FALLIMENTO: Impossibile creare la collezione: {e}")
        sys.exit(1)

def load_state():
    """Carica i file già processati dal file di checkpoint."""
    if not STATE_FILE.exists():
        return set()
    try:
        with open(STATE_FILE, 'r') as f:
            processed_files = set(json.load(f))
        print(f"Checkpoint trovato. {len(processed_files)} file già caricati.")
        return processed_files
    except Exception as e:
        print(f"Attenzione: Impossibile leggere il file di stato: {e}. Rinizio da zero.")
        return set()

def save_state(processed_files_set):
    """Salva l'elenco aggiornato dei file processati."""
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(list(processed_files_set), f, indent=2)
    except Exception as e:
        print(f"❌ ERRORE CRITICO: Impossibile salvare lo stato su {STATE_FILE}: {e}")

def get_files_to_upload():
    """Ottiene la lista di tutti i file parquet di output."""
    path_str = str(EMBEDDING_DATA_DIR / "*.parquet")
    all_files = glob.glob(path_str)
    if not all_files:
        print(f"❌ ERRORE: Nessun file .parquet trovato in {EMBEDDING_DATA_DIR}")
        sys.exit(1)
    return sorted(all_files)

def convert_to_point_structs(dict_points):
    """Converte i dizionari in oggetti PointStruct per Qdrant."""
    point_structs = []
    
    for point_dict in dict_points:
        try:
            # Estrai i campi obbligatori
            point_id = point_dict['id']
            vector = point_dict['vector']
            payload = point_dict['payload']
            
            # Converti numpy array in lista se necessario
            if hasattr(vector, 'tolist'):
                vector = vector.tolist()
            
            # Crea il PointStruct
            point_struct = PointStruct(
                id=point_id,
                vector=vector,
                payload=payload
            )
            point_structs.append(point_struct)
            
        except KeyError as e:
            print(f"❌ Campo mancante nel punto: {e}")
            continue
        except Exception as e:
            print(f"❌ Errore nella conversione del punto: {e}")
            continue
    
    return point_structs

# --- Esecuzione Principale ---
if __name__ == "__main__":
    start_job_time = time.time()
    
    client = connect_to_qdrant()
    create_collection_if_not_exists(client)
    all_files = get_files_to_upload()
    processed_files_set = load_state()
    
    files_to_process = [
        f for f in all_files if Path(f).name not in processed_files_set
    ]
    
    if not files_to_process:
        print("\n--- ✅ TUTTO GIÀ CARICATO ---")
        sys.exit(0)

    print(f"\nInizio caricamento di {len(files_to_process)} file batch (su {len(all_files)} totali)...")
    
    total_points_uploaded = 0
    for filepath in tqdm(files_to_process, desc="Caricamento batch", unit="file"):
        file_name = Path(filepath).name
        
        try:
            tqdm.write(f"\n[DEBUG] File: {file_name}")
            tqdm.write("[DEBUG]... (A) Caricamento file Parquet in RAM...")
            df = pd.read_parquet(filepath)
            
            tqdm.write("[DEBUG]... (B) Conversione DataFrame in lista di dizionari...")
            dict_points = df.to_dict(orient='records')
            
            if not dict_points:
                tqdm.write(f"Attenzione: File {file_name} è vuoto. Salto.")
                continue

            tqdm.write("[DEBUG]... (B.1) Conversione in PointStruct...")
            points = convert_to_point_structs(dict_points)
            
            if not points:
                tqdm.write(f"❌ Nessun punto valido nel file {file_name}. Salto.")
                continue

            tqdm.write(f"[DEBUG] Primo punto convertito: ID={points[0].id}, Vector_len={len(points[0].vector)}")
            tqdm.write(f"[DEBUG]... (C) Invio di {len(points)} punti a Qdrant...")
            
            # Upload con batch ragionevoli
            batch_size = 100
            successful_uploads = 0
            
            for i in range(0, len(points), batch_size):
                batch = points[i:i + batch_size]
                batch_num = i//batch_size + 1
                total_batches = (len(points)-1)//batch_size + 1
                
                tqdm.write(f"[DEBUG] Upload batch {batch_num}/{total_batches} ({len(batch)} punti)")
                
                try:
                    client.upload_points(
                        collection_name=COLLECTION_NAME,
                        points=batch,
                        wait=True
                    )
                    successful_uploads += len(batch)
                    tqdm.write(f"[DEBUG] Batch {batch_num} completato con successo")
                    
                except Exception as batch_error:
                    tqdm.write(f"[DEBUG] ❌ Errore nel batch {batch_num}: {batch_error}")
                    # Riprova con batch più piccoli
                    micro_batch_size = 20
                    for j in range(0, len(batch), micro_batch_size):
                        micro_batch = batch[j:j + micro_batch_size]
                        try:
                            client.upload_points(
                                collection_name=COLLECTION_NAME,
                                points=micro_batch,
                                wait=True
                            )
                            successful_uploads += len(micro_batch)
                            tqdm.write(f"[DEBUG] Micro-batch {j//micro_batch_size + 1} completato")
                        except Exception as micro_error:
                            tqdm.write(f"[DEBUG] ❌ Errore fatale nel micro-batch: {micro_error}")
                            raise micro_error
            
            tqdm.write(f"[DEBUG]... (D) Upload completato: {successful_uploads}/{len(points)} punti")
            processed_files_set.add(file_name)
            save_state(processed_files_set)
            total_points_uploaded += successful_uploads
            tqdm.write(f"[DEBUG]... (E) Checkpoint salvato per {file_name}.")

        except Exception as e:
            print(f"\n❌ ERRORE durante il processamento del file {file_name}: {e}")
            print("Lo script si fermerà. Alla prossima esecuzione, riprenderà da questo file.")
            sys.exit(1)

    end_job_time = time.time()
    print("\n--- CARICAMENTO COMPLETATO ---")
    print(f"✅ Caricati {total_points_uploaded} nuovi vettori.")
    print(f"Tempo totale: {end_job_time - start_job_time:.2f} secondi.")
    print("Controlla la dashboard di Qdrant: http://localhost:6333/dashboard")