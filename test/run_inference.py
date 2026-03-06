import os
import json
import time
import requests
from typing import List, Any

# CONFIGURAZIONE
GOLDEN_PATH = "test/golden_dataset_300_final.json"
OUT_JSONL = "test/rag_inference_results.jsonl"
RAG_ENDPOINT = "http://localhost:8000/query"
SLEEP_BETWEEN_CALLS = 1.0 # Secondi. Fondamentale per non stressare il container.

def clean_and_normalize_contexts(contexts):
    cleaned = []
    for ctx in contexts:
        if not isinstance(ctx, str): continue
        
        # Rimuoviamo log di sistema e messaggi di errore del Router
        if any(x in ctx for x in ["Router Decision", "Grader Decision", "Rewrote query", "No data found", "Error:", "Dati tecnici"]):
            continue
            
        # Se è un risultato SQL o Grafo pulito, o un testo, lo teniamo
        cleaned.append(ctx)
    
    # Se alla fine è vuoto, restituiamo una lista vuota (Ragas capirà che il context era vuoto)
    return cleaned

def run_inference():
    if not os.path.exists(GOLDEN_PATH):
        print("❌ Dataset input non trovato!")
        return

    # Carica dataset
    with open(GOLDEN_PATH, "r", encoding="utf-8") as f:
        dataset = json.load(f)
    
    # Checkpoint: Carica ID già processati
    processed_ids = set()
    if os.path.exists(OUT_JSONL):
        with open(OUT_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                try: 
                    processed_ids.add(json.loads(line)['id'])
                except: pass
    
    print(f"🔄 Resume: {len(processed_ids)} già processati su {len(dataset)}.")

    # Loop Inference
    with open(OUT_JSONL, "a", encoding="utf-8") as f_out:
        for i, item in enumerate(dataset):
            if item['id'] in processed_ids: continue
            
            print(f"🚀 [{i+1}/{len(dataset)}] Asking ({item['category']}): {item['question'][:60]}...")
            
            try:
                resp = requests.post(RAG_ENDPOINT, json={"question": item['question']}, timeout=1200)
                
                if resp.status_code == 200:
                    data = resp.json()

                    # --- 🚨 DEBUG DIAGNOSTICO ---
                    if i == 0: 
                        print("\n🔍 --- DEBUG API RESPONSE ---")
                        print(f"KEYS: {list(data.keys())}")
                        # Verifica se c'è il campo nuovo
                        if "retrieved_contexts" in data:
                            print(f"✅ TROVATO 'retrieved_contexts' con {len(data['retrieved_contexts'])} elementi.")
                            print(f"Esempio: {str(data['retrieved_contexts'])[:100]}...")
                        else:
                            print("❌ 'retrieved_contexts' NON TROVATO nella risposta API!")
                        print("-------------------------------\n")
                    # -----------------------------
                    
                    # 1. Recuperiamo la risposta
                    final_answer = data.get("final_answer") or data.get("answer", "No Answer")

                    # 2. Recuperiamo i CONTESTI REALI (Priorità al nuovo campo)
                    # Se nodes.py è aggiornato, userà 'retrieved_contexts'.
                    # Se non lo è ancora, fallback su 'reasoning_trace' ma pulito.
                    raw_contexts = data.get("retrieved_contexts", [])
                    
                    if not raw_contexts:
                        # Fallback se il campo nuovo manca (per compatibilità)
                        print("⚠️ Using fallback contexts (reasoning_trace)")
                        raw_contexts = clean_and_normalize_contexts(data.get("reasoning_trace", []))

                    # Assicuriamoci che siano stringhe per Ragas
                    final_contexts = [str(c) for c in raw_contexts]

                    result_row = {
                        "id": item['id'],
                        "category": item['category'],
                        "question": item['question'],
                        "ground_truth": item['ground_truth'], 
                        "answer": final_answer, 
                        "contexts": final_contexts,  # <--- QUESTO È QUELLO CHE CONTA
                        "gold_contexts": item.get("gold_contexts", []) 
                    }
                    
                    f_out.write(json.dumps(result_row, ensure_ascii=False) + "\n")
                    f_out.flush()
                else:
                    print(f"❌ API Error {resp.status_code}: {resp.text}")
                    # Salviamo comunque l'errore per non bloccare il loop infinito su un item rotto
                    err_row = {
                        "id": item['id'], "category": item['category'], 
                        "question": item['question'], "ground_truth": item['ground_truth'],
                        "answer": f"ERROR API {resp.status_code}", "contexts": []
                    }
                    f_out.write(json.dumps(err_row, ensure_ascii=False) + "\n")
                    f_out.flush()
                
            except Exception as e:
                print(f"❌ Exception: {e}")
                time.sleep(5) # Pause più lunga in caso di crash
            
            time.sleep(SLEEP_BETWEEN_CALLS)

    print("✅ Inference completata.")

if __name__ == "__main__":
    run_inference()