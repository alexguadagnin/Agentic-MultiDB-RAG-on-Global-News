import json
import torch
from elasticsearch import Elasticsearch
from setfit import SetFitModel
from gliner import GLiNER # pip install gliner
from rapidfuzz import process
import time
import os

# --- CONFIGURAZIONE ---
# Il tuo modello addestrato su RunPod
MODEL_PATH = "scripts/model/my_relation_extractor_SOTA_Final" 

# Il dizionario "Oro" (servirà solo per trovare l'ID DOPO che il NER ha trovato il nome)
CANONICAL_LOOKUP_FILE = "D:\\progetto-rag-gdelt\\data\\knowledge_graph\\canonical_lookup.json"

ES_HOST = "http://localhost:9200"
INDEX_NAME = "news_chunks"
NUM_SAMPLES = 10 
CONFIDENCE_THRESHOLD = 0.5 

print("\n" + "="*60)
print("🧪 TEST INFERENZA SOTA (GLiNER + SetFit)")
print("="*60)

device = "cuda" if torch.cuda.is_available() else "cpu"

# 1. CARICAMENTO MODELLI
print(f"\n[1/4] Caricamento Modelli su {device}...")

try:
    # A. Modello Relazioni (Il tuo)
    rel_model = SetFitModel.from_pretrained(MODEL_PATH, local_files_only=True, trust_remote_code=True)
    rel_model.to(device)
    
    # Carica labels
    label_map_path = os.path.join(MODEL_PATH, "label_map.json")
    with open(label_map_path, 'r') as f:
        label_map = json.load(f)
    id2label = {int(k): v for k, v in label_map["id2label"].items()}
    print("✅ Modello Relazioni caricato.")

    # B. Modello NER (GLiNER - Multilingue e Potente)
    # Usiamo un modello medio-piccolo perfetto per la 4070
    print("   Scaricamento GLiNER (una tantum)...")
    ner_model = GLiNER.from_pretrained("urchade/gliner_multi-v2.1")
    ner_model.to(device)
    print("✅ Modello NER (GLiNER) caricato.")

except Exception as e:
    print(f"❌ Errore caricamento: {e}"); exit(1)

# 2. CARICAMENTO DIZIONARIO (Per convertire Nomi -> ID)
print(f"\n[2/4] Caricamento Dizionario ID...")
try:
    with open(CANONICAL_LOOKUP_FILE, 'r', encoding='utf-8') as f:
        canonical_lookup = json.load(f)
    canonical_names_list = list(canonical_lookup.keys())
    print(f"✅ Dizionario caricato ({len(canonical_names_list)} entità).")
except Exception as e:
    print(f"❌ Errore dizionario: {e}"); exit(1)

# 3. RECUPERO DATI
print(f"\n[3/4] Recupero chunk casuali...")
es = Elasticsearch(ES_HOST)
resp = es.search(index=INDEX_NAME, body={"size": NUM_SAMPLES, "query": {"function_score": {"random_score": {}}}, "_source": ["chunk_text", "id_chunk"]})
hits = resp['hits']['hits']

print(f"\n--- INIZIO ANALISI ({len(hits)} Chunk) ---\n")

for i, hit in enumerate(hits):
    text = hit['_source'].get('chunk_text', '')
    print(f"🔹 CHUNK {i+1}")
    print(f"📝 TESTO: \"{text[:100].replace(chr(10), ' ')}...\"")
    
    # --- FASE A: NER (Intelligente) ---
    # Chiediamo a GLiNER di trovare solo Persone, Luoghi e Organizzazioni
    labels = ["person", "location", "organization"]
    entities = ner_model.predict_entities(text, labels, threshold=0.3)
    
    # Estraiamo il testo delle entità trovate
    found_names = [e['text'] for e in entities]
    # Rimuovi duplicati mantenendo l'ordine
    found_names = list(dict.fromkeys(found_names))
    
    print(f"🔎 ENTITÀ NER: {found_names}")
    
    if len(found_names) < 2:
        print("⚠️ < 2 entità. Skip.")
        print("-" * 60); continue

    # --- FASE B: PREDIZIONE RELAZIONI ---
    batch_inputs = []
    pairs_meta = []
    
    # Limitiamo a 5 per test
    test_names = found_names[:5]
    
    for s_name in test_names:
        for o_name in test_names:
            if s_name == o_name: continue
            
            # Entities First
            model_input = f"{s_name} [SEP] {o_name} [SEP] {text}"
            batch_inputs.append(model_input)
            pairs_meta.append((s_name, o_name))

    if not batch_inputs: continue

    # Inferenza
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with torch.no_grad():
            probs = rel_model.predict_proba(batch_inputs)
    
    probs_tensor = torch.tensor(probs)
    max_scores, max_indices = torch.max(probs_tensor, dim=1)
    
    found_rel = False
    for k in range(len(batch_inputs)):
        score = max_scores[k].item()
        if score >= CONFIDENCE_THRESHOLD:
            label = id2label[max_indices[k].item()]
            s_name, o_name = pairs_meta[k]
            
            # --- FASE C: LINKING (Solo se c'è una relazione!) ---
            # Ora cerchiamo l'ID solo per le entità valide
            # 1. Cerca esatto
            s_id = canonical_lookup.get(s_name.lower())
            o_id = canonical_lookup.get(o_name.lower())
            
            # 2. Se fallisce, Fuzzy (opzionale, qui stampiamo solo se trovato)
            s_status = f"ID: {s_id}" if s_id else "ID: NON TROVATO (Fuzzy necessario)"
            o_status = f"ID: {o_id}" if o_id else "ID: NON TROVATO (Fuzzy necessario)"

            print(f"   ✅ RELAZIONE: ({s_name}) --[{label}]--> ({o_name})")
            print(f"      Score: {score:.4f} | {s_status} -> {o_status}")
            found_rel = True
            
    if not found_rel:
        print("   ❌ Nessuna relazione rilevante.")

    print("-" * 60)