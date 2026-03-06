import pandas as pd
import ast
import os

# Configurazione file
CSV_FILE = "test/ragas_report_final.csv"

# MAPPING DELLE COLONNE REALI (Basato sul tuo header)
COLS = {
    "id": "id",
    "category": "category",
    "question": "user_input",            
    "answer": "response",                
    "ground_truth": "reference",         
    "contexts": "retrieved_contexts",    
    # Metriche
    "precision": "context_precision",
    "recall": "context_recall",
    "faithfulness": "faithfulness",
    "correctness": "answer_correctness"
}

def print_case(idx, row, type_label):
    print(f"\n{'='*20} {type_label} CASE #{idx} {'='*20}")
    
    # ID
    print(f"🆔 ID: {row.get(COLS['id'], 'N/A')}")
    
    # Domanda
    print(f"❓ Domanda: {row.get(COLS['question'], 'N/A')}")
    
    # Ground Truth
    gt = row.get(COLS['ground_truth'], 'N/A')
    print(f"🎯 Ground Truth: {gt}")
    
    # Risposta
    print(f"🤖 Risposta RAG: {row.get(COLS['answer'], 'N/A')}")
    
    # Gestione Contesti (Parsing sicuro da stringa a lista)
    raw_ctx = row.get(COLS['contexts'], "[]")
    try:
        # Se è una stringa che sembra una lista, convertila
        if isinstance(raw_ctx, str):
            ctx = ast.literal_eval(raw_ctx)
        else:
            ctx = raw_ctx if isinstance(raw_ctx, list) else [str(raw_ctx)]
    except:
        ctx = [str(raw_ctx)]
        
    print(f"📂 Contesti Recuperati ({len(ctx)}):")
    for c in ctx:
        # Pulisci e taglia per leggibilità
        text = str(c).replace('\n', ' ')
        print(f"   - {text[:200]}...") 

    print("-" * 20)
    print(f"📊 METRICHE:")
    # Usa .get() con valore di default 0.0 per sicurezza
    print(f"   Context Precision: {row.get(COLS['precision'], 0.0):.4f}")
    print(f"   Context Recall:    {row.get(COLS['recall'], 0.0):.4f}")
    print(f"   Faithfulness:      {row.get(COLS['faithfulness'], 0.0):.4f}")
    print(f"   Answer Correctness:{row.get(COLS['correctness'], 0.0):.4f}")

def analyze():
    if not os.path.exists(CSV_FILE):
        print(f"❌ File {CSV_FILE} non trovato.")
        return

    try:
        df = pd.read_csv(CSV_FILE)
        
        # Verifica veloce
        if COLS['category'] not in df.columns:
            print(f"❌ Errore: Colonna '{COLS['category']}' non trovata. Colonne presenti: {df.columns.tolist()}")
            return

        categories = df[COLS['category']].unique()

        for cat in categories:
            print(f"\n\n{'#'*30}")
            print(f"📢 ANALISI CATEGORIA: {cat}")
            print(f"{'#'*30}")
            
            subset = df[df[COLS['category']] == cat]
            
            # Ordiniamo per Answer Correctness (qualità finale)
            sort_col = COLS['correctness']
            if sort_col in df.columns:
                sorted_df = subset.sort_values(by=sort_col, ascending=False)
            else:
                sorted_df = subset
            
            # TOP 5
            print(f"\n🏆 --- I MIGLIORI 5 (Basato su Answer Correctness) ---")
            for i in range(min(5, len(sorted_df))):
                print_case(i+1, sorted_df.iloc[i], "BEST")
                
            # WORST 5
            print(f"\n💀 --- I PEGGIORI 5 (Basato su Answer Correctness) ---")
            worst_df = sorted_df.tail(5)
            for i in range(len(worst_df)):
                print_case(i+1, worst_df.iloc[i], "WORST")

    except Exception as e:
        print(f"❌ Errore durante l'analisi: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze()