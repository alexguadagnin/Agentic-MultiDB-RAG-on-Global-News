import json
import random
import os
import time
from openai import OpenAI
from tqdm import tqdm

# --- 1. CONFIGURAZIONE ---
INPUT_CHUNKS_FILE = "random_chunks.jsonl" 
OUTPUT_TRAINING_FILE = "training_data_pure_text.jsonl" # Nome nuovo per chiarezza
NUM_SAMPLES_TO_LABEL = 2000

# --- Configurazione Novita.ai ---
NOVITA_API_KEY = "..."
NOVITA_BASE_URL = "https://api.novita.ai/openai"
MODEL_NAME = "meta-llama/llama-3.3-70b-instruct"

# --- Gestione Rate Limit (20 RPM) ---
# 60 secondi / 20 richieste = 3 secondi. Facciamo 3.2 per sicurezza assoluta.
SECONDS_PER_REQUEST = 3.2

RELATION_SCHEMA = [
    "MEETS_WITH", "NEGOTIATES_WITH", "SIGNS_AGREEMENT", "SUPPORTS", "CRITICIZES",
    "ATTACKS", "PROVIDES_AID_TO", "PROTESTS_AGAINST", "ACCUSES", "INVESTIGATES", 
    "DENIES", "TRAVELS_TO"
]

# --- 2. CONFIGURAZIONE API ---
try:
    if NOVITA_API_KEY == "LA_TUA_CHIAVE_API_NOVITA_QUI" or not NOVITA_API_KEY:
        raise ValueError("La chiave API Novita non è stata impostata.")
    
    client = OpenAI(api_key=NOVITA_API_KEY, base_url=NOVITA_BASE_URL)
    print("Client OpenAI (per Novita.ai) configurato.")
except Exception as e:
    print(f"ERRORE: Configurazione API Novita fallita. {e}")
    exit(1)

# --- 3. PREPARAZIONE DATI ---
print(f"Caricamento chunk casuali da '{INPUT_CHUNKS_FILE}'...")
candidates = []
try:
    with open(INPUT_CHUNKS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            candidates.append(json.loads(line))
    print(f"Trovati {len(candidates)} chunk.")
except Exception as e:
    print(f"ERRORE: Impossibile caricare '{INPUT_CHUNKS_FILE}': {e}"); exit(1)

if len(candidates) < NUM_SAMPLES_TO_LABEL:
    samples = candidates
else:
    samples = random.sample(candidates, NUM_SAMPLES_TO_LABEL)
print(f"Campionati {len(samples)} chunk per l'etichettatura.")

# --- 4. PROMPT (TESTO PURO) ---
# Nessun dizionario, nessuna forzatura. Solo estrazione pura.
SYSTEM_PROMPT = f"""
You are an expert Relation Extraction system. Your task is to extract semantic relationships from the provided text.

RULES:
1.  **Language:** The input text can be in ANY language. Analyze it in its original language.
2.  **Entities:** Extract the Subject and Object exactly as they appear in the text (text literals). Do not translate entity names.
3.  **Relations:** The relation MUST be one of the following (in English): {RELATION_SCHEMA}.
4.  **Format:** Return ONLY a valid JSON object with a "triples" key containing a list.
    Format: {{"triples": [{{"subject": "Entity Name", "relation": "RELATION", "object": "Entity Name"}}]}}
5.  If no relevant relation is found from the allowed list, return {{"triples": []}}.
6.  Do not output markdown blocks (like ```json). Just the raw JSON string.
"""

USER_PROMPT_TEMPLATE = """
Text to analyze:
"{text}"
"""

# --- 5. HELPER PARSING ---
def extract_json_from_text(text):
    """Pulisce l'output di Llama per trovare il JSON."""
    try:
        text = text.strip()
        # Rimuovi markdown se presente
        if text.startswith("```json"): text = text[7:]
        if text.endswith("```"): text = text[:-3]
        
        start = text.find('{')
        end = text.rfind('}') + 1
        if start != -1 and end != 0:
            return json.loads(text[start:end])
        return None
    except: return None

# --- 6. ESECUZIONE BATCH ---
final_training_data = []
print(f"Inizio etichettatura di {len(samples)} chunk...")
print(f"Tempo stimato: ~{(len(samples) * SECONDS_PER_REQUEST)/60:.1f} minuti.")

for chunk in tqdm(samples, desc="Etichettatura"):
    chunk_text = chunk.get("chunk_text")
    if not chunk_text: continue

    user_prompt = USER_PROMPT_TEMPLATE.format(text=chunk_text)
    
    # Retry logic per errori API (non per JSON brutti)
    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0,
                max_tokens=500
            )
            
            content = response.choices[0].message.content
            data = extract_json_from_text(content)
            
            if data and "triples" in data and isinstance(data["triples"], list):
                for triple in data["triples"]:
                    # Validazione minima: devono esserci i campi e la relazione deve essere valida
                    if (triple.get("subject") and triple.get("object") and 
                        triple.get("relation") in RELATION_SCHEMA):
                        
                        # Aggiungiamo il contesto originale per il training
                        triple["context_text"] = chunk_text
                        final_training_data.append(triple)
            
            break # Successo, usciamo dal loop retry

        except Exception as e:
            print(f"\nErrore API: {e}. Riprovo...")
            time.sleep(2) # Breve pausa extra per errore
    
    # --- RATE LIMITING OBBLIGATORIO ---
    time.sleep(SECONDS_PER_REQUEST)

# --- 7. SALVATAGGIO ---
print(f"\nEtichettatura completata. Estratte {len(final_training_data)} triple.")

with open(OUTPUT_TRAINING_FILE, 'w', encoding='utf-8') as f:
    for item in final_training_data:
        f.write(json.dumps(item, ensure_ascii=False) + '\n')

print(f"File salvato: {OUTPUT_TRAINING_FILE}")
print("Ora esegui 'verify_training_data_simple.py' per controllare la qualità.")