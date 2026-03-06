import pandas as pd
import os

# CONFIGURAZIONE
INPUT_FILE = "test/ragas_report_final.csv"
OUTPUT_EXCEL = "test/analisi_risultati.xlsx"

def analyze():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ File non trovato: {INPUT_FILE}")
        return

    print(f"📂 Caricamento dati da {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    
    # Colonne delle metriche (quelle presenti nel tuo CSV)
    metric_cols = [
        'context_recall', 
        'context_precision', 
        'faithfulness', 
        'answer_relevancy', 
        'answer_correctness'
    ]
    
    # Filtriamo solo quelle che esistono davvero nel file
    existing_metrics = [c for c in metric_cols if c in df.columns]
    
    if not existing_metrics:
        print("❌ Nessuna metrica trovata nel CSV! Controlla i nomi delle colonne.")
        return

    print(f"📊 Analisi su {len(df)} domande completate.\n")

    # --- 1. MEDIA GLOBALE (Il voto finale) ---
    print("🏆 --- RISULTATI GLOBALI ---")
    global_means = df[existing_metrics].mean()
    print(global_means.round(3).to_string())
    print("-" * 30)

    # --- 2. ANALISI PER CATEGORIA (SQL vs GRAPH vs VECTOR) ---
    if 'category' in df.columns:
        print("\n📂 --- RISULTATI PER CATEGORIA ---")
        cat_means = df.groupby('category')[existing_metrics].mean()
        print(cat_means.round(3))
        print("-" * 30)
    
    # --- 3. ANALISI ERRORI (Dove hai preso 0 o voti bassi) ---
    # Consideriamo "Fallimento" se answer_correctness < 0.5
    if 'answer_correctness' in df.columns:
        failures = df[df['answer_correctness'] < 0.5]
        print(f"\n⚠️ --- CRITICITÀ ---")
        print(f"Domande con voto insufficiente (< 0.5): {len(failures)} su {len(df)} ({len(failures)/len(df)*100:.1f}%)")
        
        # Mostra le categorie peggiori
        if not failures.empty and 'category' in df.columns:
            print("Categorie con più errori:")
            print(failures['category'].value_counts())

    # --- SALVATAGGIO REPORT EXCEL ---
    try:
        with pd.ExcelWriter(OUTPUT_EXCEL) as writer:
            # Foglio 1: Medie
            global_means.to_frame("Global Averages").to_excel(writer, sheet_name="Global")
            if 'category' in df.columns:
                cat_means.to_excel(writer, sheet_name="By Category")
            
            # Foglio 2: Dati Completi
            df.to_excel(writer, sheet_name="Raw Data", index=False)
            
            # Foglio 3: I peggiori (per debug)
            if 'answer_correctness' in df.columns:
                failures.sort_values(by='answer_correctness').to_excel(writer, sheet_name="Low Scores", index=False)
                
        print(f"\n💾 Report Excel salvato in: {OUTPUT_EXCEL}")
        print("   (Aprilo per vedere i grafici e i dettagli!)")
        
    except Exception as e:
        print(f"⚠️ Impossibile salvare Excel (forse manca openpyxl?): {e}")

if __name__ == "__main__":
    analyze()