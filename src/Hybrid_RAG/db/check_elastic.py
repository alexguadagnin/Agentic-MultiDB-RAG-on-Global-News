from elasticsearch import Elasticsearch
import json

# --- CONFIGURAZIONE ---
ELASTIC_URL = "http://localhost:9200"
INDEX_NAME = "news_chunks"
# ----------------------

def check_index():
    try:
        # 1. Connessione
        es = Elasticsearch(ELASTIC_URL)
        
        if not es.ping():
            print(f"Impossibile connettersi a {ELASTIC_URL}")
            return

        # 2. Conteggio totale documenti
        count = es.count(index=INDEX_NAME)['count']
        print(f"\n=== STATISTICHE INDICE: '{INDEX_NAME}' ===")
        print(f"Totale documenti (chunk) trovati: {count}")
        
        if count == 0:
            print("L'indice è vuoto!")
            return

        # 3. Recupera 3 documenti a caso per ispezionarli
        # Usiamo una query 'match_all' con dimensione 3
        response = es.search(
            index=INDEX_NAME,
            body={
                "size": 3,
                "query": {"match_all": {}}
            }
        )

        print(f"\n=== ESEMPIO DI 3 CHUNK A CASO ===")
        hits = response['hits']['hits']
        
        for i, hit in enumerate(hits):
            doc_id = hit['_id']
            source = hit['_source']
            text = source.get('chunk_text', 'NESSUN TESTO TROVATO')
            
            print(f"\n--- Documento #{i+1} (ID: {doc_id}) ---")
            print(f"LUNGHEZZA TESTO: {len(text)} caratteri")
            print(f"ANTEPRIMA TESTO: {text[:200]}...") # Primi 200 caratteri
            # Se vuoi vedere tutto il testo, togli [:200]
            
    except Exception as e:
        print(f"Errore: {e}")

if __name__ == "__main__":
    check_index()