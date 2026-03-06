import pandas as pd
import os

# CONFIGURAZIONE
INPUT_FILE = "test/ragas_report_final.csv"
OUTPUT_FILE = "test/ragas_critical_failures.csv"
THRESHOLD = 0.05 # Consideriamo "0" qualsiasi cosa sotto 0.05

def extract_failures():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ File non trovato: {INPUT_FILE}")
        return

    print(f"📂 Leggo {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    
    # Assicuriamoci che le colonne siano numeriche
    cols_to_check = ['answer_correctness', 'context_precision']
    for col in cols_to_check:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Logica di filtro: Se Correctness è ~0 OPPURE Precision è ~0
    # Questo cattura sia risposte sbagliate sia contesti spazzatura
    mask = (df['answer_correctness'] < THRESHOLD) | (df['context_precision'] < THRESHOLD)
    
    failures = df[mask].copy()
    
    # Ordiniamo per categoria per facilitare la lettura
    if 'category' in failures.columns:
        failures = failures.sort_values(by='category')

    # Salvataggio
    failures.to_csv(OUTPUT_FILE, index=False)
    
    print(f"\n📉 RISULTATO FILTRO (Soglia < {THRESHOLD})")
    print(f"   Totale righe analizzate: {len(df)}")
    print(f"   Totale casi critici:     {len(failures)} ({len(failures)/len(df)*100:.1f}%)")
    print(f"💾 Salvato in: {OUTPUT_FILE}")

    if 'category' in failures.columns:
        print("\n📊 Errori critici per Categoria:")
        print(failures['category'].value_counts())

if __name__ == "__main__":
    extract_failures()