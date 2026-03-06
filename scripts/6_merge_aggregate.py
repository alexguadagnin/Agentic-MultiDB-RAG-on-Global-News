import pandas as pd
import glob
import os
import numpy as np

# --- CONFIGURAZIONE ---
INPUT_DIR = "data/knowledge_graph" 
OUTPUT_FILE = os.path.join(INPUT_DIR, "FINAL_RELATIONS_IMPORT.csv")

# Definizione esplicita delle colonne (basata sui tuoi dati)
COLUMNS = ["source_id", "target_id", "relation", "score", "chunk_id"]

# --- 1. CARICAMENTO E UNIONE ---
print(f"--- FASE 1: MERGE & AGGREGATE ---")
files = glob.glob(os.path.join(INPUT_DIR, "*extracted_relations*.csv"))
print(f"File trovati: {len(files)}")

if not files: print("❌ Nessun file trovato!"); exit()

dfs = []
for f in files:
    print(f"Leggo {os.path.basename(f)}...")
    try:
        # 💡 TRUCCO ROBUSTEZZA:
        # 1. header=None: Dice a Pandas "non cercare l'intestazione, leggi tutto come dati".
        # 2. names=COLUMNS: Assegna i nomi corretti manualmente.
        # 3. dtype=str: Legge tutto come stringa per evitare errori di conversione iniziali.
        df = pd.read_csv(f, names=COLUMNS, header=None, dtype=str, on_bad_lines='skip')
        
        # 💡 PULIZIA IBRIDA:
        # Se il file AVEVA un header (es. quello locale), la prima riga conterrà la stringa "source_id".
        # La rimuoviamo dinamicamente. Funziona sia per file con header che senza.
        df = df[df['source_id'] != 'source_id']
        df = df[df['score'] != 'score']
        
        # Convertiamo lo score in numero ora
        df['score'] = pd.to_numeric(df['score'], errors='coerce')
        
        # Rimuoviamo righe spazzatura (score NaN)
        original_len = len(df)
        df = df.dropna(subset=['score', 'source_id', 'target_id', 'relation'])
        
        print(f"   -> {len(df):,} righe valide (scartate {original_len - len(df)})")
        dfs.append(df)
        
    except Exception as e:
        print(f"⚠️ ERRORE FATALE su {f}: {e}")

if not dfs: exit()

full_df = pd.concat(dfs, ignore_index=True)
print(f"\nTotale relazioni grezze: {len(full_df):,}")

# --- 2. AGGREGAZIONE (IL SUPER-ARCO) ---
print("Aggregazione in corso...")

# A. Ordiniamo per Score decrescente
# Questo assicura che quando prendiamo i chunk_id, prendiamo quelli con la confidenza più alta per primi.
full_df.sort_values(by='score', ascending=False, inplace=True)

# B. Funzione di aggregazione custom vettorizzata
# Raggruppiamo per la tripla (Chi, Cosa, Chi)
grouped = full_df.groupby(['source_id', 'target_id', 'relation'])

# Definiamo come aggregare le colonne
# - chunk_id: prendiamo i primi 20 unici e li uniamo con '|'
# - score: prendiamo il massimo
# - weight: contiamo quante righe c'erano
def unique_chunks_join(series):
    # Prende i primi 20 ID unici (che sono i migliori grazie al sort) e li unisce
    return "|".join(series.unique()[:20])

print("   -> Calcolo statistiche (questo richiede RAM)...")

final_df = grouped.agg(
    max_score=('score', 'max'),
    weight=('chunk_id', 'count'),
    chunk_ids=('chunk_id', unique_chunks_join)
).reset_index()

final_count = len(final_df)
print(f"\n=== RISULTATO FINALE ===")
print(f"Relazioni Grezze:   {len(full_df):,}")
print(f"Relazioni Uniche:   {final_count:,} (Grafo Compresso)")
print(f"Fattore Riduzione:  {len(full_df)/final_count:.1f}x")

# --- 3. SALVATAGGIO ---
print(f"Salvataggio in {OUTPUT_FILE}...")
final_df.to_csv(OUTPUT_FILE, index=False)
print("✅ Fatto. Ora lancia 'phase_3_enrich_graph.py'.")