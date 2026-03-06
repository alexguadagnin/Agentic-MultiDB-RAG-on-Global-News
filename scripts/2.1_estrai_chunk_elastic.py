from elasticsearch import Elasticsearch
import json
import time 
from tqdm import tqdm 

# --- Configurazione ---
ES_HOST = "http://localhost:9200"
INDEX_NAME = "news_chunks"
OUTPUT_FILE = "random_chunks.jsonl"

# --- Strategia di Campionamento ---
NUM_QUERIES = 100       # Quante "sonde" inviamo
SIZE_PER_QUERY = 100    # Quanti chunk per sonda
NUM_CHUNKS_TO_FETCH = NUM_QUERIES * SIZE_PER_QUERY # Totale 10.000

print(f"Strategia di campionamento: {NUM_QUERIES} query da {SIZE_PER_QUERY} chunk ciascuna.")

# --- Connessione a Elastic ---
try:
    es = Elasticsearch(ES_HOST)
    es.info()
    print("Connesso a Elasticsearch.")
except Exception as e:
    print(f"Errore di connessione a Elasticsearch: {e}")
    exit(1)

# --- Loop di estrazione ---
all_hits = {} 
print(f"Inizio estrazione di {NUM_CHUNKS_TO_FETCH} chunk...")

try:
    for i in tqdm(range(NUM_QUERIES), desc="Esecuzione sonde"):
        seed = f"seed_{i}_{time.time_ns()}" 

        query = {
            "size": SIZE_PER_QUERY,
            "query": {
                "function_score": {
                    "query": {"match_all": {}},
                    "random_score": {
                        "seed": seed,       
                        "field": "_seq_no"  
                    }
                }
            },
            # --- 💡 CORREZIONE CHIAVE ---
            # Uso i campi corretti che mi hai fornito.
            "_source": ["id_chunk", "chunk_text"] 
        }
        
        resp = es.search(
            index=INDEX_NAME,
            body=query
        )
        
        for hit in resp['hits']['hits']:
            # Aggiungiamo il documento intero per evitare duplicati
            # Usiamo _id come chiave
            all_hits[hit['_id']] = hit['_source']

    # --- Salvataggio ---
    count = len(all_hits)
    print(f"Estrazione completata. Trovati {count} chunk unici.")
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for chunk_data in all_hits.values():
            # chunk_data conterrà {"id_chunk": ..., "chunk_text": ...}
            f.write(json.dumps(chunk_data) + '\n')

    print(f"Salvataggio completato in '{OUTPUT_FILE}'.")
    print(f"Ora esegui lo script '2_filter_rich_chunks.py'.")

except Exception as e:
    print(f"Errore durante l'esecuzione della query: {e}")
    if "index_not_found_exception" in str(e):
        print(f"ERRORE: L'indice '{INDEX_NAME}' non esiste. Verifica il nome.")