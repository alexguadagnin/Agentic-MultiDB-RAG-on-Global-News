import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct
import glob
import sys
import time
from pathlib import Path
from tqdm import tqdm

# --- CONFIGURAZIONE ---
BASE_PATH = Path(r"...")

# Definiamo i subset e le collezioni di destinazione
SUBSETS = {
    "xs": {"dir": BASE_PATH / "embedding_output_xs", "collection": "gdelt_articles_xs"},
    "s":  {"dir": BASE_PATH / "embedding_output_s",  "collection": "gdelt_articles_s"},
    "m":  {"dir": BASE_PATH / "embedding_output_m",  "collection": "gdelt_articles_m"},
    "l":  {"dir": BASE_PATH / "embedding_output_l",  "collection": "gdelt_articles_l"},
    "xl": {"dir": BASE_PATH / "embedding_output_xl", "collection": "gdelt_articles_xl"},
}

VECTOR_DIMENSION = 1024

def connect_to_qdrant():
    print("🔌 Connessione a Qdrant (localhost:6333)...")
    try:
        client = QdrantClient(host="localhost", port=6333, timeout=60.0)
        # Test connessione
        client.get_collections()
        return client
    except Exception as e:
        print(f"❌ Impossibile connettersi a Qdrant: {e}")
        print("Assicurati che il container Docker 'qdrant-db' sia acceso!")
        sys.exit(1)

def convert_to_point_structs(dict_points):
    point_structs = []
    for point_dict in dict_points:
        try:
            point_id = point_dict['id']
            vector = point_dict['vector']
            payload = point_dict['payload']
            
            # Conversione sicura numpy -> list
            if hasattr(vector, 'tolist'): 
                vector = vector.tolist()
            
            point_structs.append(PointStruct(id=point_id, vector=vector, payload=payload))
        except Exception:
            continue
    return point_structs

def process_subset(tag, config, client):
    input_dir = config['dir']
    collection_name = config['collection']
    
    print(f"\n" + "="*60)
    print(f"🚀 UPLOAD SUBSET: {tag.upper()}")
    print(f"📂 Cartella: {input_dir}")
    print(f"📦 Collezione Target: {collection_name}")
    print(f"="*60)

    # 1. Verifica Cartella
    if not input_dir.exists():
        print(f"⚠️ Cartella non trovata: {input_dir}")
        return

    parquet_files = sorted(glob.glob(str(input_dir / "*.parquet")))
    if not parquet_files:
        print(f"⚠️ Nessun file parquet trovato in {input_dir}.")
        return

    # 2. Reset Collezione (PULIZIA TOTALE per i test)
    if client.collection_exists(collection_name):
        print(f"🧹 La collezione '{collection_name}' esiste già. La elimino e ricreo...")
        client.delete_collection(collection_name)
    
    client.create_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(size=VECTOR_DIMENSION, distance=Distance.COSINE)
    )

    # 3. Caricamento
    total_uploaded = 0
    print(f"📥 Inizio caricamento di {len(parquet_files)} file...")

    for filepath in tqdm(parquet_files, desc=f"Upload {tag.upper()}", unit="file"):
        try:
            df = pd.read_parquet(filepath)
            dict_points = df.to_dict(orient='records')
            points = convert_to_point_structs(dict_points)
            
            if not points: continue

            # Batch Upload (Ottimizzato a 250 punti per volta)
            batch_size = 250
            for i in range(0, len(points), batch_size):
                batch = points[i:i + batch_size]
                client.upload_points(
                    collection_name=collection_name,
                    points=batch,
                    wait=False # False è più veloce per caricamenti massivi
                )
                total_uploaded += len(batch)
                
        except Exception as e:
            print(f"❌ Errore file {Path(filepath).name}: {e}")

    # Ottimizzazione finale post-caricamento (indicizzazione)
    client.update_collection(collection_name=collection_name, optimizer_config={"indexing_threshold": 20000})
    
    print(f"✅ SUCCESSO {tag.upper()}. Caricati {total_uploaded} vettori in '{collection_name}'.")

if __name__ == "__main__":
    client = connect_to_qdrant()
    
    # Esegue il ciclo su tutti i subset
    for tag, config in SUBSETS.items():
        process_subset(tag, config, client)