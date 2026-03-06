import json
import time
from tqdm import tqdm

# --- 1. CONFIGURAZIONE ---
INPUT_DIRTY_MAP = "D:\\progetto-rag-gdelt\\data\\knowledge_graph\\entity_map.json"
OUTPUT_CANONICAL_LOOKUP = "D:\\progetto-rag-gdelt\\data\\knowledge_graph\\canonical_lookup.json"
ACRONYM_WHITELIST = {"UN", "US", "EU", "UK", "MP", "PM"}

print("Avvio creazione Canonical Lookup (Dizionario 'Oro')...")
start_time = time.time()

try:
    print(f"Caricamento mappa 'sporca' da {INPUT_DIRTY_MAP}...")
    with open(INPUT_DIRTY_MAP, 'r', encoding='utf-8') as f:
        dirty_map = json.load(f)
except Exception as e:
    print(f"ERRORE: Impossibile caricare {INPUT_DIRTY_MAP}. Dettagli: {e}"); exit(1)

canonical_lookup = {}

print("Estrazione e pulizia delle entità canoniche (Wikidata)...")
for entity_data in tqdm(dirty_map.values(), desc="Pulizia entità"):
    
    if not isinstance(entity_data, dict) or not all(k in entity_data for k in ["id", "name", "type", "source"]):
        continue

    # --- FILTRI SOTA ---
    # 1. Fonte: Solo Wikidata
    if entity_data.get("source") != "wikidata": continue
    # 2. Tipo: Solo Actor e Location
    if entity_data.get("type") not in ["Actor", "Location"]: continue
    # 3. ID: Non custom
    entity_id = entity_data.get("id")
    if isinstance(entity_id, str) and entity_id.startswith("custom:"): continue
    # 4. Nome Canonico: Deve esistere ed essere valido
    canonical_name = entity_data.get("name")
    if not canonical_name or not isinstance(canonical_name, str): continue
    # 5. Lunghezza: Rimuovi rumore breve (mantenendo acronimi)
    if len(canonical_name) <= 2 and canonical_name.upper() not in ACRONYM_WHITELIST: continue
        
    # ✅ Entità "Oro" trovata. Mappa: nome_canonico_minuscolo -> ID
    canonical_lookup[canonical_name.lower()] = entity_id

end_time = time.time()
print(f"\n--- CREAZIONE DIZIONARIO 'ORO' COMPLETATA ---")
print(f"Tempo impiegato: {end_time - start_time:.2f} secondi.")
print(f"Entità 'Oro' uniche trovate: {len(canonical_lookup):,}")

# --- 3. SALVATAGGIO ---
try:
    print(f"\nSalvataggio dizionario 'oro' in {OUTPUT_CANONICAL_LOOKUP}...")
    with open(OUTPUT_CANONICAL_LOOKUP, 'w', encoding='utf-8') as f:
        json.dump(canonical_lookup, f, ensure_ascii=False)
    print("Salvataggio completato.")
except Exception as e:
    print(f"ERRORE: Impossibile salvare il file. Dettagli: {e}")