import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import sys
from pathlib import Path

try:
    SRC_ROOT = Path(__file__).parent.parent.parent.resolve()
    if str(SRC_ROOT) not in sys.path:
        sys.path.append(str(SRC_ROOT))
    from Hybrid_RAG.constants import RAW_DATA_DIR_NGRAMS
except ImportError:
    print("❌ ERRORE CRITICO: Impossibile importare 'constants.py'.")
    sys.exit(1)


# --- CONFIGURAZIONE ---
# Assicurati che Elastic sia raggiungibile qui
ELASTIC_URL = "http://localhost:9200" 
INDEX_NAME = "news_chunks"

PARQUET_FILE = RAW_DATA_DIR_NGRAMS / "elasticsearch_data.parquet" 
# ----------------------

def yield_chunks_from_parquet(file_path):
    """
    Generatore che legge il file Parquet e formatta i dati
    per la Bulk API di Elasticsearch.
    """
    print(f"--- Inizio lettura del file: {file_path} ---")
    
    try:
        df = pd.read_parquet(file_path)
        print(f"File Parquet letto. Numero di chunk da caricare: {len(df)}")
    except FileNotFoundError:
        print(f"ERRORE: File non trovato: {file_path}")
        print("Per favore, inserisci il nome corretto del file Parquet nello script.")
        sys.exit(1) # Esce dallo script
    except Exception as e:
        print(f"ERRORE durante la lettura del file Parquet: {e}")
        sys.exit(1)

    
    for index, row in df.iterrows():
        # Questo è il formato richiesto da helpers.bulk
        # _id DEVE essere il tuo id_chunk per mappare Qdrant
        yield {
            "_op_type": "index",
            "_index": INDEX_NAME,
            "_id": row["id_chunk"],  # Fondamentale!
            "_source": {
                # Stiamo usando id_chunk come _id, quindi salvarlo
                # in _source è ridondante, ma lo teniamo per coerenza.
                "id_chunk": row["id_chunk"], 
                "chunk_text": row["chunk_text"]
            }
        }
        
    print(f"--- Lettura file completata. {len(df)} chunk pronti per l'invio. ---")

# --- ESECUZIONE PRINCIPALE ---
if __name__ == "__main__":
    try:
        # 1. Connettiti a Elasticsearch
        print(f"Tentativo di connessione a Elasticsearch: {ELASTIC_URL}...")
        es = Elasticsearch(ELASTIC_URL, request_timeout=5) 

        # Controlla la connessione con un debug più approfondito
        try:
            es.ping()
            print(f"Connessione a Elasticsearch ({ELASTIC_URL}) riuscita.")
        except Exception as ping_error:
            print(f"\n--- ERRORE PING ---")
            print(f"es.ping() ha fallito. Errore originale: {ping_error}")
            print("Questo di solito è un problema di proxy, firewall o di rete.")
            print("----------------------\n")
            raise ValueError(f"Connessione a Elasticsearch fallita! Controlla che sia in esecuzione su {ELASTIC_URL}")

        # 2. Crea un generatore
        chunk_generator = yield_chunks_from_parquet(PARQUET_FILE)
        
        # 3. Esegui il caricamento bulk
        print("Inizio caricamento bulk su Elasticsearch... (potrebbe richiedere tempo)")
        success, errors = bulk(es, chunk_generator, raise_on_error=False, request_timeout=60, max_retries=3)
        
        print("\n--- CARICAMENTO COMPLETATO ---")
        print(f"Documenti caricati con successo: {success}")
        
        if errors:
            print(f"Errori riscontrati: {len(errors)}")
            print("Esempio primi 5 errori:")
            for i, error in enumerate(errors[:5]):
                print(f"  Errore {i+1}: {error}")
        else:
            print("Nessun errore riscontrato. Dati caricati correttamente!")

    except Exception as e:
        print(f"\nERRORE CRITICO durante il processo: {e}")
        print("Assicurati che Elasticsearch sia in esecuzione e che il nome del file Parquet sia corretto.")