import json
import csv
import torch
import time
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from setfit import SetFitModel
from gliner import GLiNER
from rapidfuzz import process, fuzz # Il segreto per il linking
from tqdm import tqdm
import os

# --- 1. CONFIGURAZIONE ---
MODEL_PATH = "scripts/model/my_relation_extractor_SOTA_Final" 
CANONICAL_LOOKUP_FILE = "D:\\progetto-rag-gdelt\\data\\knowledge_graph\\canonical_lookup.json"
OUTPUT_FILE = "D:\\progetto-rag-gdelt\\data\\knowledge_graph\\extracted_relations.csv"
ES_HOST = "http://localhost:9200"
INDEX_NAME = "news_chunks"

BATCH_SIZE = 32 
CONFIDENCE_THRESHOLD = 0.6
FUZZY_THRESHOLD = 85 # Se > 85%, accettiamo il link (es. "Regno Unito" -> "United Kingdom" potrebbe richiedere traduzione, ma fuzzy aiuta con typo)

# --- 2. CARICAMENTO RISORSE ---
print("--- FASE 1: Inizializzazione ---")
device = "cuda" if torch.cuda.is_available() else "cpu"

# A. Modelli
print(f"--> Caricamento Modelli su {device}...")
rel_model = SetFitModel.from_pretrained(MODEL_PATH, local_files_only=True, trust_remote_code=True)
rel_model.to(device)

# Carica mappa label (importante!)
label_map_path = os.path.join(MODEL_PATH, "label_map.json")
with open(label_map_path, 'r') as f:
    label_map = json.load(f)
id2label = {int(k): v for k, v in label_map["id2label"].items()}

# GLiNER
ner_model = GLiNER.from_pretrained("urchade/gliner_multi-v2.1")
ner_model.to(device)
print("✅ Modelli caricati.")

# B. Dizionario
print("--> Caricamento Dizionario ID...")
with open(CANONICAL_LOOKUP_FILE, 'r', encoding='utf-8') as f:
    canonical_lookup = json.load(f)
canonical_names_list = list(canonical_lookup.keys()) # Per fuzzy matching
print(f"✅ Dizionario pronto ({len(canonical_names_list)} entità).")

# C. Elastic
es = Elasticsearch(ES_HOST)

# --- 3. FUNZIONE LINKING ---
def get_best_id(text_name):
    """Tenta Exact Match, poi Fuzzy Match."""
    if not text_name: return None
    text_lower = text_name.lower()
    
    # 1. Exact
    if text_lower in canonical_lookup:
        return canonical_lookup[text_lower]
    
    # 2. Fuzzy (Lento ma potente)
    # Nota: Su 300k articoli, il fuzzy su 1M di chiavi è LENTO.
    # Ottimizzazione: Se il nome è molto corto, salta fuzzy.
    if len(text_lower) < 4: return None
    
    match = process.extractOne(text_lower, canonical_names_list, scorer=fuzz.ratio, score_cutoff=FUZZY_THRESHOLD)
    if match:
        return canonical_lookup[match[0]]
    return None

# --- 4. PIPELINE ---
print("\n--- FASE 2: Estrazione di Massa ---")

batch_inputs, batch_meta = [], []
total_saved = 0

with open(OUTPUT_FILE, 'w', encoding='utf-8', newline='') as f_out:
    writer = csv.writer(f_out)
    writer.writerow(["source_id", "target_id", "relation", "score", "chunk_id"])

    # Scan efficiente
    es_scan = scan(es, index=INDEX_NAME, query={"query": {"match_all": {}}}, size=200, _source=["chunk_text"])

    for doc in tqdm(es_scan, desc="Processing", mininterval=1.0):
        text = doc['_source'].get('chunk_text', '')
        chunk_id = doc['_id']
        if not text or len(text) < 20: continue

        # 1. NER
        entities = ner_model.predict_entities(text, ["person", "location", "organization"], threshold=0.3)
        names = list(set(e['text'] for e in entities))
        
        if len(names) < 2: continue
        if len(names) > 8: names = names[:8] # Limitiamo combinatoria

        # 2. Linking (Pre-calcolo ID per velocità)
        # Risolviamo gli ID *prima* di creare le coppie per scartare entità sconosciute
        valid_entities = {} # Nome -> ID
        for name in names:
            eid = get_best_id(name)
            if eid: valid_entities[name] = eid
        
        valid_names = list(valid_entities.keys())
        if len(valid_names) < 2: continue

        # 3. Genera Coppie (solo tra entità linkate!)
        for i in range(len(valid_names)):
            for j in range(len(valid_names)):
                if i == j: continue
                
                s_name = valid_names[i]
                o_name = valid_names[j]
                
                batch_inputs.append(f"{s_name} [SEP] {o_name} [SEP] {text}")
                batch_meta.append((valid_entities[s_name], valid_entities[o_name], chunk_id))

        # 4. Inferenza Batch
        if len(batch_inputs) >= BATCH_SIZE:
            with torch.no_grad():
                probs = rel_model.predict_proba(batch_inputs)
            
            probs = torch.tensor(probs)
            max_scores, max_indices = torch.max(probs, dim=1)
            
            for k in range(len(batch_inputs)):
                if max_scores[k] >= CONFIDENCE_THRESHOLD:
                    label = id2label[max_indices[k].item()]
                    s_id, o_id, c_id = batch_meta[k]
                    writer.writerow([s_id, o_id, label, f"{max_scores[k]:.4f}", c_id])
                    total_saved += 1
            
            batch_inputs, batch_meta = [], []

    # Ultimo batch
    if batch_inputs:
        with torch.no_grad():
            probs = rel_model.predict_proba(batch_inputs)
        probs = torch.tensor(probs)
        max_scores, max_indices = torch.max(probs, dim=1)
        for k in range(len(batch_inputs)):
            if max_scores[k] >= CONFIDENCE_THRESHOLD:
                label = id2label[max_indices[k].item()]
                s_id, o_id, c_id = batch_meta[k]
                writer.writerow([s_id, o_id, label, f"{max_scores[k]:.4f}", c_id])
                total_saved += 1

print(f"\n✅ FINITO. Salvate {total_saved} relazioni in {OUTPUT_FILE}")