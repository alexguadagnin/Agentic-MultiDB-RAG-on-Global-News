import json
import ahocorasick # pip install pyahocorasick
import time
from tqdm import tqdm

# --- 1. CONFIGURAZIONE ---
# File generato dallo Script 1 (di estrazione)
INPUT_FILE = "random_chunks.jsonl" 

# Ora usiamo la mappa PULITA che abbiamo appena generato
ENTITY_MAP_FILE = "D:\\progetto-rag-gdelt\\data\\knowledge_graph\\clean_entity_map.json" 

# Il nostro output finale
OUTPUT_FILE = "training_set_candidates.jsonl" # Sovrascriveremo quello vecchio
MIN_UNIQUE_ENTITIES_FOUND = 2

# --- 2. Costruisci il Motore di Ricerca Aho-Corasick ---
print(f"Caricamento 'clean_entity_map.json' da {ENTITY_MAP_FILE}...")
print("Questo potrebbe richiedere ~60-80 secondi...")
start_time = time.time()

A = ahocorasick.Automaton()
try:
    with open(ENTITY_MAP_FILE, 'r', encoding='utf-8') as f:
        clean_map = json.load(f)
    
    # Aggiungiamo solo le chiavi pulite
    for dirty_name in clean_map.keys():
        A.add_word(dirty_name, dirty_name)
            
    A.make_automaton()
    print(f"Motore di ricerca Aho-Corasick PULITO costruito in {time.time() - start_time:.2f}s")
    print(f"Caricate {len(clean_map)} entità pulite nel motore.")

except FileNotFoundError:
    print(f"ERRORE: File 'clean_entity_map.json' non trovato al percorso: {ENTITY_MAP_FILE}")
    print("Assicurati di aver eseguito prima lo script '1_clean_entity_map.py'.")
    exit(1)
except Exception as e:
    print(f"Errore durante il caricamento o la costruzione dell'automa: {e}")
    exit(1)


# --- 3. Elabora i Chunk Estratti ---
print(f"Inizio filtro chunk (usando mappa pulita) da '{INPUT_FILE}'...")
rich_chunks = []
total_chunks_processed = 0

try:
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line in tqdm(f, desc="Filtraggio Chunk (Pulito)"):
            total_chunks_processed += 1
            chunk = json.loads(line)
            
            chunk_text = chunk.get("chunk_text")
            if not chunk_text:
                continue

            # Cerca solo le entità pulite
            found_entities = set()
            for end_index, original_value in A.iter(chunk_text):
                found_entities.add(original_value)
            
            # Filtra per la nostra euristica
            if len(found_entities) >= MIN_UNIQUE_ENTITIES_FOUND:
                chunk["found_entities_list"] = list(found_entities)
                rich_chunks.append(chunk)

    # --- 4. Salvataggio ---
    print(f"Analisi completata.")
    print(f"Elaborati {total_chunks_processed} chunk.")
    print(f"Trovati {len(rich_chunks)} chunk 'ricchi' (con entità pulite).")

    if not rich_chunks:
        print("Nessun chunk ricco trovato. Questo è improbabile ma possibile.")
        exit()

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for chunk in rich_chunks:
            f.write(json.dumps(chunk) + '\n')
            
    print(f"File dei candidati (PULITO) salvato in '{OUTPUT_FILE}'.")

except FileNotFoundError:
    print(f"ERRORE: File '{INPUT_FILE}' non trovato. Esegui prima lo script 1 (estrazione).")
except Exception as e:
    print(f"Errore durante l'elaborazione: {e}")