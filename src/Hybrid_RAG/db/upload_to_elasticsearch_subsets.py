import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import sys
import glob
from pathlib import Path
from tqdm import tqdm
import numpy as np

# --- CONFIGURAZIONE ---
BASE_PATH = Path(r"...")
ELASTIC_URL = "http://localhost:9200"

SUBSETS = {
    "xs": {"dir": BASE_PATH / "embedding_output_xs", "index": "news_chunks_xs"},
    "s":  {"dir": BASE_PATH / "embedding_output_s",  "index": "news_chunks_s"},
    "m":  {"dir": BASE_PATH / "embedding_output_m",  "index": "news_chunks_m"},
    "l":  {"dir": BASE_PATH / "embedding_output_l",  "index": "news_chunks_l"},
    "xl": {"dir": BASE_PATH / "embedding_output_xl", "index": "news_chunks_xl"},
}

def connect_to_elastic():
    print(f"🔌 Connessione a Elasticsearch ({ELASTIC_URL})...")
    try:
        # Timeout aumentato a 60s per connessioni lente
        es = Elasticsearch(ELASTIC_URL, request_timeout=60)
        if not es.ping():
            raise ValueError("Ping fallito.")
        return es
    except Exception as e:
        print(f"❌ Errore connessione Elastic: {e}")
        sys.exit(1)

def clean_value(val):
    """Pulisce i valori per renderli digeribili a JSON/Elastic"""
    if pd.isna(val) or val is None:
        return ""
    return str(val)

def generate_actions(df, index_name):
    for _, row in df.iterrows():
        try:
            doc_id = row['id']
            payload = row['payload']
            
            if not isinstance(payload, dict): continue
            
            chunk_text = clean_value(payload.get('chunk_text'))
            if not chunk_text: continue

            # Costruiamo il documento pulito
            source_doc = {
                "chunk_text": chunk_text,
                "url": clean_value(payload.get('url')),
                "title": clean_value(payload.get('title')),
                "date": clean_value(payload.get('date'))
            }

            yield {
                "_index": index_name,
                "_id": doc_id, 
                "_source": source_doc
            }
        except Exception:
            continue

def process_subset(tag, config, es):
    input_dir = config['dir']
    index_name = config['index']

    print(f"\n" + "="*60)
    print(f"🚀 ELASTIC UPLOAD: {tag.upper()}")
    print(f"="*60)

    if not input_dir.exists():
        print(f"⚠️ Cartella non trovata: {input_dir}")
        return

    parquet_files = sorted(glob.glob(str(input_dir / "*.parquet")))
    if not parquet_files:
        print("⚠️ Nessun file parquet trovato.")
        return

    # 1. Reset Indice
    if es.indices.exists(index=index_name):
        print(f"🧹 Elimino indice '{index_name}'...")
        es.indices.delete(index=index_name)
    
    es.indices.create(index=index_name, body={
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "chunk_text": {"type": "text"},
                "url": {"type": "keyword"},
                "date": {"type": "date", "ignore_malformed": True} # Ignora date rotte
            }
        }
    })

    # 2. Caricamento Bulk
    total_docs = 0
    print(f"📥 Caricamento di {len(parquet_files)} file...")

    for filepath in tqdm(parquet_files, desc=f"Index {tag.upper()}", unit="file"):
        try:
            df = pd.read_parquet(filepath)
            
            # Generatore azioni
            actions = generate_actions(df, index_name)
            
            # UPLOAD con gestione errori dettagliata
            # Usiamo .options() per passare il timeout in modo moderno
            success, errors = bulk(
                es.options(request_timeout=120), # Timeout lungo per sicurezza
                actions, 
                raise_on_error=False,
                stats_only=False # Ci serve per vedere i dettagli degli errori
            )
            
            total_docs += success
            
            if errors:
                print(f"\n⚠️ {len(errors)} errori nel file {Path(filepath).name}")
                # Stampa il primo errore per capire cosa succede
                first_error = errors[0]
                print(f"   ESEMPIO ERRORE: {first_error}")

        except Exception as e:
            print(f"❌ Errore lettura file {Path(filepath).name}: {e}")

    # Refresh finale
    es.indices.refresh(index=index_name)
    print(f"✅ SUCCESSO {tag.upper()}. Indicizzati {total_docs} documenti.")

if __name__ == "__main__":
    es_client = connect_to_elastic()
    
    # Scommenta SOLO XS per testare se ora va, poi fai gli altri
    for tag, config in SUBSETS.items():
        process_subset(tag, config, es_client)