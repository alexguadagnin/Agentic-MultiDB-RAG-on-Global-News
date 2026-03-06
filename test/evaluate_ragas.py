import os
import json
import time
import pandas as pd
from datasets import Dataset
from pathlib import Path  
from dotenv import load_dotenv
from ragas import evaluate
from ragas.metrics import (
    context_precision,
    context_recall,
    faithfulness,
    answer_relevancy,
    answer_correctness
)
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

# --- CARICAMENTO ENV ---
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

if not os.getenv("OPENAI_API_KEY"):
    print("❌ ERRORE: OPENAI_API_KEY non trovata. Controlla il percorso del .env o la chiave.")
    exit(1)

# --- CONFIGURAZIONE ---
JUDGE_MODEL = "gpt-4o-mini" 
BATCH_SIZE = 2
INFERENCE_FILE = "test/rag_inference_results.jsonl"
REPORT_FILE = "test/ragas_report_final.csv"

def load_processed_ids():
    if os.path.exists(REPORT_FILE):
        try:
            df = pd.read_csv(REPORT_FILE)
            if 'id' in df.columns:
                return set(df['id'].tolist())
        except Exception as e:
            print(f"⚠️ Errore lettura CSV esistente: {e}")
    return set()

def run_evaluation():
    if not os.path.exists(INFERENCE_FILE):
        print("❌ File inference non trovato.")
        return

    # Carica tutti i dati
    all_data = []
    with open(INFERENCE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                all_data.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    
    # Filtra quelli già fatti
    processed_ids = load_processed_ids()
    data_to_process = [d for d in all_data if d['id'] not in processed_ids]
    
    print(f"📊 Totale items: {len(all_data)}. Già processati: {len(processed_ids)}. Da valutare: {len(data_to_process)}")
    
    if not data_to_process:
        print("✅ Tutto già valutato!")
        return

    # --- CONFIGURAZIONE GIUDICE (CORAZZATA) ---
    # 1. Timeout alto per evitare crash su contesti lunghi
    # 2. Max Retries per gestire errori di rete temporanei
    llm_judge = ChatOpenAI(
        model=JUDGE_MODEL,
        temperature=0,          # Più deterministico
        request_timeout=240,    # 4 minuti di tempo prima di andare in timeout
        max_retries=3           # Riprova 3 volte se fallisce
    )
    
    embeddings = OpenAIEmbeddings(
        model="text-embedding-3-small",
        request_timeout=120     # Timeout anche per gli embeddings
    )
    
    metrics = [
        context_precision,
        context_recall,
        faithfulness,
        answer_relevancy,
        answer_correctness
    ]

    # Batch Loop
    total_batches = (len(data_to_process) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for i in range(0, len(data_to_process), BATCH_SIZE):
        batch = data_to_process[i : i + BATCH_SIZE]
        current_batch_num = i // BATCH_SIZE + 1
        print(f"\n⚖️  Processing Batch {current_batch_num}/{total_batches} ({len(batch)} items)...")
        
        # Prepara dataset Ragas
        # Nota: ground_truth in Ragas spesso si aspetta una lista di stringhe per ogni item
        # Quindi trasformiamo la stringa singola in lista [str] se necessario, ma di solito str funziona.
        ds = Dataset.from_dict({
            "question": [d["question"] for d in batch],
            "answer": [d["answer"] for d in batch],
            "contexts": [d["contexts"] for d in batch],
            "ground_truth": [str(d.get("ground_truth", "")) for d in batch]
        })

        try:
            results = evaluate(
                dataset=ds,
                metrics=metrics,
                llm=llm_judge,
                embeddings=embeddings,
                #max_workers=4,  
                raise_exceptions=False  # Se una riga fallisce, mette NaN invece di crashare tutto
            )
            
            # Converti in DataFrame
            df_batch = results.to_pandas()
            
            # Reintegra i metadati originali (ID, Categoria)
            df_batch['id'] = [d['id'] for d in batch]
            df_batch['category'] = [d['category'] for d in batch]

            # Append su CSV
            is_new_file = not os.path.exists(REPORT_FILE)
            df_batch.to_csv(REPORT_FILE, mode='a', header=is_new_file, index=False)
            
            print(f"✅ Batch {current_batch_num} salvato con successo.")
            time.sleep(2) # Respiro per l'API
            
        except Exception as e:
            print(f"❌ Errore critico nel batch {current_batch_num}: {e}")
            print("⏳ Attendo 10 secondi e passo al prossimo batch...")
            time.sleep(10)
            continue

    print(f"\n🏆 Valutazione Completa. Risultati in {REPORT_FILE}")

if __name__ == "__main__":
    run_evaluation()