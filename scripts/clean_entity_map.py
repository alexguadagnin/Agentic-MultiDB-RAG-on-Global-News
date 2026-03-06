import json
import time
from tqdm import tqdm

# --- 1. CONFIGURAZIONE ---
INPUT_DIRTY_MAP = "D:\\progetto-rag-gdelt\\data\\knowledge_graph\\entity_map.json"
OUTPUT_CLEAN_MAP = "D:\\progetto-rag-gdelt\\data\\knowledge_graph\\clean_entity_map.json"

STOP_LIST = {
    "president", "economist", "son", "american", "libertad", "robot", 
    "audience", "derbi", "amba", "lome", "espa", "satra", "premier",
    "government", "official", "spokesperson", "minister", "senator", 
    "representative", "police", "military", "army", "soldier", "officer",
    "doctor", "phd", "prof", "professor",
    "news", "media", "press", "company", "organization", "team", "group",
    "man", "woman", "people", "father", "mother", "citizen", "citizens",
    "general", "chief", "director", "manager", "ceo", "cfo",
    "rt", "http", "https", "www", "com", "net", "org", "twitter", "facebook"
}
ACRONYM_WHITELIST = {"UN", "US", "EU", "UK", "MP", "PM"}

# --- 2. PROCESSO DI PULIZIA ---
print("Avvio processo di pulizia della Entity Map (v2)...")
start_time = time.time()

try:
    print(f"Caricamento mappa 'sporca' da {INPUT_DIRTY_MAP}...")
    with open(INPUT_DIRTY_MAP, 'r', encoding='utf-8') as f:
        dirty_map = json.load(f)
    print(f"Caricate {len(dirty_map):,} voci.")
except Exception as e:
    print(f"ERRORE: Impossibile caricare {INPUT_DIRTY_MAP}. Dettagli: {e}")
    exit(1)

clean_map = {}
removed_count = 0

for key, value in tqdm(dirty_map.items(), desc="Pulizia voci (v2)"):
    
    if not isinstance(value, dict) or not all(k in value for k in ["id", "name", "type", "source"]):
        removed_count += 1
        continue

    key_lower = key.lower()
    canonical_name_lower = (value.get("name") or "").lower()
    entity_id = value.get("id") # Prendiamo l'ID

    # --- Inizio Filtri Aggiornati ---

    # 1. Filtra per Fonte (Rimuove 'custom:actor:', ecc.)
    if value.get("source") != "wikidata":
        removed_count += 1
        continue

    # 💡 --- NUOVO FILTRO CHIRURGICO --- 💡
    # Se l'ID è una stringa e inizia con "custom:", è spazzatura.
    if isinstance(entity_id, str) and entity_id.startswith("custom:"):
        removed_count += 1
        continue
    # -----------------------------------

    # 2. Filtra per Tipo (Mantiene solo Actor e Location)
    if value.get("type") not in ["Actor", "Location"]:
        removed_count += 1
        continue
        
    # 3. Filtra per Stop-List (Controlla sia la chiave che il nome)
    if key_lower in STOP_LIST or canonical_name_lower in STOP_LIST:
        removed_count += 1
        continue
    
    # 4. Filtra per Lunghezza (Rimuove rumore come "a", "de", "el", "la")
    if len(key) <= 2 and key.upper() not in ACRONYM_WHITELIST:
        removed_count += 1
        continue

    # ✅ Se è sopravvissuto a tutti i filtri, è pulito!
    clean_map[key] = value

end_time = time.time()

print("\n--- PULIZIA COMPLETATA (v2) ---")
print(f"Tempo impiegato: {end_time - start_time:.2f} secondi.")
print(f"Voci originali: {len(dirty_map):,}")
print(f"Voci rimosse:   {removed_count:,}")
print(f"Voci pulite:    {len(clean_map):,}")

# --- 3. SALVATAGGIO ---
try:
    print(f"\nSalvataggio mappa 'pulita' in {OUTPUT_CLEAN_MAP}...")
    with open(OUTPUT_CLEAN_MAP, 'w', encoding='utf-8') as f:
        json.dump(clean_map, f, ensure_ascii=False) 
    print("Salvataggio completato.")
except Exception as e:
    print(f"ERRORE: Impossibile salvare il file pulito. Dettagli: {e}")

print("\nProssimo passo: Esegui lo script '2_filter_rich_chunks.py' (modificato).")