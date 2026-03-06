import pandas as pd
import torch
import ahocorasick
import json
import csv
import os
import gc
import numpy as np
from setfit import SetFitModel
from tqdm import tqdm
from itertools import permutations
from collections import deque

# --- CONFIGURAZIONE ---
INPUT_PARQUET = "scripts/remaining_chunks.parquet"
ENTITY_MAP_FILE = "data/knowledge_graph/clean_entity_map.json"
MODEL_PATH = "scripts/model/my_relation_extractor_SOTA_Final"
OUTPUT_FILE = "data/knowledge_graph/local_extracted_relations.csv"
CHECKPOINT_FILE = "data/knowledge_graph/inference_checkpoint.txt"

BATCH_SIZE = 128  
CONFIDENCE_THRESHOLD = 0.7
MAX_ENTITIES_PER_CHUNK = 6  
MIN_TEXT_LENGTH = 20
CHECKPOINT_INTERVAL = 1000 

# --- FUNZIONI RESUME ---
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            try:
                return int(f.read().strip())
            except:
                return 0
    return 0

def save_checkpoint(index):
    with open(CHECKPOINT_FILE, 'w') as f:
        f.write(str(index))

# --- FUNZIONI CORE ---
def extract_entities_optimized(text, automaton, max_entities=MAX_ENTITIES_PER_CHUNK):
    """Estrazione entità rapida con limite."""
    entities = {}
    for _, (eid, ename) in automaton.iter(text):
        entities[eid] = ename
        if len(entities) >= max_entities:
            break 
    return entities

def generate_entity_pairs(entities_dict):
    """Genera permutazioni (A, B) e (B, A)."""
    ids = list(entities_dict.keys())
    if len(ids) < 2:
        return []
    return list(permutations(ids, 2))

def process_batch_efficiently(model, batch_inputs, batch_meta, writer, f_out,
                            id2label, confidence_threshold, device):
    """Inferenza pura su GPU con scrittura immediata."""
    if not batch_inputs:
        return 0
    
    with torch.no_grad():
        probs = model.predict_proba(batch_inputs)
    
    if isinstance(probs, torch.Tensor):
        probs_tensor = probs.to(device)
    else:
        probs_tensor = torch.tensor(probs, device=device)
    
    max_scores, max_indices = torch.max(probs_tensor, dim=1)
    mask = max_scores >= confidence_threshold
    valid_indices = torch.nonzero(mask).squeeze(1)
    
    saved_count = len(valid_indices)
    
    if saved_count > 0:
        valid_indices_cpu = valid_indices.cpu().numpy()
        scores_cpu = max_scores[valid_indices].cpu().numpy()
        classes_cpu = max_indices[valid_indices].cpu().numpy()
        
        rows_to_write = []
        for i, idx in enumerate(valid_indices_cpu):
            s, o, c = batch_meta[idx]
            lbl = id2label[classes_cpu[i]]
            score_val = scores_cpu[i]
            rows_to_write.append([s, o, lbl, f"{score_val:.4f}", c])
        
        writer.writerows(rows_to_write)
        f_out.flush() 
    
    return saved_count

# --- MAIN ---
if __name__ == "__main__":
    print("--- AVVIO INFERENZA LOCALE (OTTIMIZZATA & SICURA v3) ---")

    start_idx = load_checkpoint()
    print(f"--> Ultimo indice salvato: {start_idx}")

    # 1. Modello
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"--> Caricamento Modello su {device}...")
    try:
        model = SetFitModel.from_pretrained(MODEL_PATH, local_files_only=True, trust_remote_code=True)
        model.to(device)
        
        # --- 💡 FIX DEFINITIVO: Accesso ai componenti interni ---
        # SetFitModel non ha .parameters(), ma i suoi componenti sì.
        
        # 1. Congela il Body (Sentence Transformer)
        if hasattr(model, "model_body"):
            model.model_body.eval()
            for param in model.model_body.parameters():
                param.requires_grad = False
        
        # 2. Congela la Head (Classifier)
        if hasattr(model, "model_head"):
            # Alcune head di sklearn non hanno eval/parameters, gestiamo l'eccezione
            if hasattr(model.model_head, "eval"):
                model.model_head.eval()
            if hasattr(model.model_head, "parameters"):
                for param in model.model_head.parameters():
                    param.requires_grad = False
            
        # Carica Label Map
        label_map_path = os.path.join(MODEL_PATH, "label_map.json")
        with open(label_map_path, 'r') as f:
            label_map = json.load(f)
        id2label = {int(k): v for k, v in label_map["id2label"].items()}
        
        print("✅ Modello ottimizzato per inferenza.")

    except Exception as e:
        print(f"❌ ERRORE Modello: {e}"); exit(1)

    # 2. Automa
    print("--> Caricamento Entity Map...")
    A = ahocorasick.Automaton()
    try:
        with open(ENTITY_MAP_FILE, 'r', encoding='utf-8') as f:
            clean_map = json.load(f)
        for name, data in clean_map.items():
            if 'id' in data:
                A.add_word(name, (data['id'], data.get('name', name)))
        A.make_automaton()
        del clean_map
        gc.collect()
    except Exception as e:
        print(f"❌ ERRORE Mappa: {e}"); exit(1)

    # 3. Dati
    print(f"--> Caricamento Dati...")
    try:
        df = pd.read_parquet(INPUT_PARQUET)
    except Exception as e:
        print(f"❌ ERRORE Parquet: {e}"); exit(1)
        
    total_rows = len(df)

    if start_idx >= total_rows:
        print("✅ Lavoro completato.")
        exit(0)

    df_to_process = df.iloc[start_idx:]
    print(f"--> Da processare: {len(df_to_process)} (Saltati {start_idx})")
    
    del df
    gc.collect()

    # --- LOOP PRINCIPALE ---
    batch_inputs = []
    batch_meta = []
    total_saved_session = 0
    file_mode = 'a' if start_idx > 0 else 'w'

    print("--> Inizio elaborazione...")

    with open(OUTPUT_FILE, file_mode, encoding='utf-8', newline='') as f_out:
        writer = csv.writer(f_out)
        
        if start_idx == 0:
            writer.writerow(["source_id", "target_id", "relation", "score", "chunk_id"])

        for relative_i, row in tqdm(enumerate(df_to_process.itertuples(index=False)), 
                                   total=len(df_to_process), desc="Inferenza"):
            
            absolute_idx = start_idx + relative_i 
            text = row.chunk_text
            chunk_id = row.id_chunk
            
            if isinstance(text, str) and len(text) >= MIN_TEXT_LENGTH:
                found_entities = extract_entities_optimized(text, A)
                
                entity_pairs = generate_entity_pairs(found_entities)
                
                for s_id, o_id in entity_pairs:
                    s_name = found_entities[s_id]
                    o_name = found_entities[o_id]
                    batch_inputs.append(f"{s_name} [SEP] {o_name} [SEP] {text}")
                    batch_meta.append((s_id, o_id, chunk_id))

                if len(batch_inputs) >= BATCH_SIZE:
                    saved = process_batch_efficiently(
                        model, batch_inputs, batch_meta, writer, f_out,
                        id2label, CONFIDENCE_THRESHOLD, device
                    )
                    total_saved_session += saved
                    batch_inputs, batch_meta = [], []

            # Checkpoint Periodico
            if relative_i % CHECKPOINT_INTERVAL == 0 and relative_i > 0:
                save_checkpoint(absolute_idx + 1)

        # Residuo Finale
        if batch_inputs:
            saved = process_batch_efficiently(
                model, batch_inputs, batch_meta, writer, f_out,
                id2label, CONFIDENCE_THRESHOLD, device
            )
            total_saved_session += saved

        save_checkpoint(total_rows)

    print(f"\n✅ SESSIONE COMPLETATA. Salvate {total_saved_session} nuove relazioni.")