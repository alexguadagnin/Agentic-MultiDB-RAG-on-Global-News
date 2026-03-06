import json
import random
import os
from collections import defaultdict

# --- CONFIGURAZIONE ---
# Se il file è in una sottocartella, modifica qui (es. "test/golden_dataset...")
INPUT_FILE = "test/golden_dataset_300_final.json"
OUTPUT_FILE = "test/golden_sample_stratified.json"
SAMPLES_PER_CATEGORY = 10

def extract_stratified_sample():
    print(f"📂 Cerco il file: {INPUT_FILE}...")
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Errore: Il file '{INPUT_FILE}' non esiste.")
        return

    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        print(f"✅ File caricato. Totale elementi: {len(data)}")
        
        # 1. Raggruppa per Categoria
        grouped_data = defaultdict(list)
        for item in data:
            # Usa 'Unknown' se manca la categoria
            cat = item.get('category', 'Unknown')
            grouped_data[cat].append(item)
            
        final_sample = []
        
        print("\n--- ESTRAZIONE CAMPIONI ---")
        for cat, items in grouped_data.items():
            count = len(items)
            # Prendi 10 o tutti se sono meno di 10
            k = min(SAMPLES_PER_CATEGORY, count)
            
            subset = random.sample(items, k)
            final_sample.extend(subset)
            
            print(f"🔹 Categoria '{cat}': Trovati {count} -> Estratti {k}")
            
        # 2. Salvataggio
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_sample, f, indent=4, ensure_ascii=False)
            
        print(f"\n🎉 File generato: {OUTPUT_FILE}")
        print(f"📊 Totale campioni estratti: {len(final_sample)}")
        print("👉 Apri il file, copia il contenuto e incollalo nella chat per il Sanity Check!")
        
    except Exception as e:
        print(f"❌ Errore imprevisto: {e}")

if __name__ == "__main__":
    extract_stratified_sample()