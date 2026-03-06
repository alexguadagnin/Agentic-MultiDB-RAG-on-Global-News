import pandas as pd
import glob
import os

# --- CONFIGURAZIONE ---
FULL_DATA_FILE = "data/gdelt_ngrams/elasticsearch_data.parquet" # Il tuo file originale
RUNPOD_RESULTS_DIR = "runpod_results" # Dove hai estratto i CSV di RunPod
OUTPUT_FILE = "scripts/remaining_chunks.parquet" # Il file per la tua 4070

CSV_COLUMNS = ["source_id", "target_id", "relation", "score", "chunk_id"]

print("1. Lettura Parquet originale (Tutti i dati)...")
try:
    df_full = pd.read_parquet(FULL_DATA_FILE)
    print(f"   Totale chunk originali: {len(df_full)}")
except Exception as e:
    print(f"ERRORE: Non trovo il file {FULL_DATA_FILE}. {e}"); exit()

print("2. Scansione risultati RunPod...")
processed_ids = set()
# Cerca direttamente nella cartella, aggiusta il path se necessario (es. runpod_results/results/*.csv)
csv_files = glob.glob(os.path.join(RUNPOD_RESULTS_DIR, "**", "*.csv"), recursive=True)

if not csv_files:
    print(f"ATTENZIONE: Nessun CSV trovato in {RUNPOD_RESULTS_DIR}.")
else:
    print(f"   Trovati {len(csv_files)} file CSV.")
    for f in csv_files:
        try:
            # Leggiamo senza header e assegniamo i nomi
            df_temp = pd.read_csv(f, header=None, names=CSV_COLUMNS, on_bad_lines='skip')
            
            # Se per caso il file AVEVA un header (es. la riga 0), pandas lo avrà letto come dati.
            # Convertiamo chunk_id in stringa e rimuoviamo eventuali righe che contengono la parola "chunk_id"
            df_temp = df_temp[df_temp['chunk_id'] != 'chunk_id']
            
            current_ids = df_temp["chunk_id"].dropna().astype(str).tolist()
            processed_ids.update(current_ids)
            print(f"   - Letto {os.path.basename(f)}: {len(current_ids)} righe.")
        except pd.errors.EmptyDataError:
            print(f"   ⚠️ File vuoto ignorato: {f}")
        except Exception as e:
            print(f"   ⚠️ Errore lettura {f}: {e}")

    print(f"   Chunk unici già completati: {len(processed_ids)}")

# 3. Filtro (Sottrazione)
print("3. Calcolo del lavoro rimanente...")
df_full['id_chunk'] = df_full['id_chunk'].astype(str)

# Filtriamo
df_remaining = df_full[~df_full['id_chunk'].isin(processed_ids)]

count = len(df_remaining)
print(f"   Chunk da processare in locale: {count}")

if count == 0:
    print("🎉 Hai finito! Non c'è nulla da fare in locale.")
    # Puoi uscire, ma magari vuoi rigenerare il file vuoto per sicurezza
    # exit() 

# 4. Salvataggio
print(f"4. Salvataggio {OUTPUT_FILE}...")
df_remaining.to_parquet(OUTPUT_FILE)
print("✅ Pronto per l'inferenza locale.")