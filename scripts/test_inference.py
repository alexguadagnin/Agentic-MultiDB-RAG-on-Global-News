import json
import torch
import ahocorasick
from elasticsearch import Elasticsearch
from setfit import SetFitModel
import time
import os

# --- CONFIGURAZIONE ---
# Assicurati che questo percorso sia corretto (la cartella scompattata)
MODEL_PATH = "scripts/model/my_relation_extractor_SOTA_Final" 

ENTITY_MAP_FILE = "D:\\progetto-rag-gdelt\\data\\knowledge_graph\\clean_entity_map.json"
ES_HOST = "http://localhost:9200"
INDEX_NAME = "news_chunks"
NUM_SAMPLES = 10 
CONFIDENCE_THRESHOLD = 0.5 

print("\n" + "="*60)
print("🧪 AVVIO TEST INFERENZA (Sanity Check - v2)")
print("="*60)

# 1. CARICAMENTO MODELLO E ETICHETTE
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"\n[1/3] Caricamento Modello su {device}...")

try:
    # A. Carica il modello
    model = SetFitModel.from_pretrained(MODEL_PATH, local_files_only=True, trust_remote_code=True)
    model.to(device)
    
    # B. 💡 FIX CRITICO: Carica la mappa delle etichette manualmente
    label_map_path = os.path.join(MODEL_PATH, "label_map.json")
    with open(label_map_path, 'r') as f:
        label_map = json.load(f)
    
    # id2label nel JSON ha chiavi stringa ("0": "SUPPORTS"), dobbiamo gestirlo
    id2label = {int(k): v for k, v in label_map["id2label"].items()}
    
    print(f"✅ Modello caricato. Classi: {list(id2label.values())}")

except Exception as e:
    print(f"❌ Errore caricamento modello/labels: {e}")
    print(f"Verifica che {MODEL_PATH} contenga 'config.json', 'model.safetensors' e 'label_map.json'")
    exit(1)

# 2. CARICAMENTO MAPPA
print(f"\n[2/3] Caricamento Entity Map e costruzione automa...")
start_map = time.time()
A = ahocorasick.Automaton()
try:
    with open(ENTITY_MAP_FILE, 'r', encoding='utf-8') as f:
        clean_map = json.load(f)
    
    count = 0
    for dirty_name, data in clean_map.items():
        if 'id' in data:
            # Filtro runtime per pulire l'output del test (ignora parole < 4 char se non sono UPPER)
            if len(dirty_name) < 4 and not dirty_name.isupper():
                continue
            A.add_word(dirty_name, (data['id'], data.get('name', dirty_name)))
            count += 1
    A.make_automaton()
    print(f"✅ Automa pronto con {count} entità ({time.time()-start_map:.1f}s).")
except Exception as e:
    print(f"❌ Errore mappa: {e}"); exit(1)

# 3. ESTRAZIONE E TEST
print(f"\n[3/3] Recupero {NUM_SAMPLES} chunk casuali da Elasticsearch...")
es = Elasticsearch(ES_HOST)
query = {
    "size": NUM_SAMPLES,
    "query": {
        "function_score": {
            "query": {"match_all": {}},
            "random_score": {} 
        }
    },
    "_source": ["chunk_text", "id_chunk"]
}

resp = es.search(index=INDEX_NAME, body=query)
hits = resp['hits']['hits']

print(f"\n--- INIZIO ANALISI ({len(hits)} Chunk) ---\n")

for i, hit in enumerate(hits):
    chunk_id = hit['_id']
    text = hit['_source'].get('chunk_text', '')
    
    # Pulizia visuale del testo (rimuove a capo eccessivi)
    display_text = text.replace("\n", " ")[:150]
    
    print(f"🔹 CHUNK {i+1}")
    print(f"📝 TESTO: \"{display_text}...\"")
    
    # A. Trova Entità
    found_entities = {}
    for end_index, (eid, ename) in A.iter(text):
        found_entities[eid] = ename
    
    ids = list(found_entities.keys())
    names = list(found_entities.values())
    
    print(f"🔎 ENTITÀ: {names}")
    
    if len(ids) < 2:
        print("⚠️ < 2 entità. Skip.")
        print("-" * 60)
        continue

    # B. Predizione
    batch_inputs = []
    pairs_meta = []
    
    test_ids = ids[:5] # Limitiamo il test a 5 entità per leggibilità
    
    for s_idx in range(len(test_ids)):
        for o_idx in range(len(test_ids)):
            if s_idx == o_idx: continue
            
            subj_id = test_ids[s_idx]
            obj_id = test_ids[o_idx]
            subj_name = found_entities[subj_id]
            obj_name = found_entities[obj_id]
            
            # Format: Entities First
            model_input = f"{subj_name} [SEP] {obj_name} [SEP] {text}"
            batch_inputs.append(model_input)
            pairs_meta.append((subj_name, obj_name))

    if not batch_inputs: continue

    # Inferenza
    # Suppress warning about tensor copy
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with torch.no_grad():
            probs = model.predict_proba(batch_inputs)
    
    probs_tensor = torch.tensor(probs)
    max_scores, max_indices = torch.max(probs_tensor, dim=1)
    
    found_rel = False
    for k in range(len(batch_inputs)):
        score = max_scores[k].item()
        if score >= CONFIDENCE_THRESHOLD:
            class_idx = max_indices[k].item()
            
            # 💡 FIX: Usiamo il dizionario caricato manualmente
            label = id2label[class_idx] 
            
            s_name, o_name = pairs_meta[k]
            print(f"   ✅ RELAZIONE: ({s_name}) --[{label}]--> ({o_name}) (Score: {score:.4f})")
            found_rel = True
            
    if not found_rel:
        print("   ❌ Nessuna relazione rilevante.")

    print("-" * 60)

print("\nTest Completato.")