import json
import random

# Il file che hai appena generato
INPUT_FILE = "training_data_pure_text.jsonl"

print(f"Caricamento dati da {INPUT_FILE}...")
data = []
try:
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data.append(json.loads(line))
            except json.JSONDecodeError:
                continue # Salta righe corrotte se ce ne sono
    print(f"Caricati {len(data)} esempi validi.")
except FileNotFoundError:
    print(f"ERRORE: File '{INPUT_FILE}' non trovato."); exit()

if not data:
    print("Il file è vuoto!"); exit()

print("\n" + "="*60)
print("--- SANITY CHECK (Visualizza 10 Triple Testuali) ---")
print("="*60)

# Campiona 10 esempi casuali
samples = random.sample(data, min(10, len(data)))

for i, item in enumerate(samples):
    # Recupera i campi del formato "Testo Puro"
    subject = item.get('subject', 'N/A')
    relation = item.get('relation', 'N/A')
    obj = item.get('object', 'N/A')
    context = item.get('context_text', 'Nessun contesto salvato')

    print(f"\n[{i+1}] CONTESTO ORIGINALE (Primi 300 caratteri):")
    print(f"\"{context[:300]}...\"")
    print("-" * 20)
    print(f"--> TRIPLA ESTRATTA: ({subject}) --[{relation}]--> ({obj})")
    print("_" * 60)

print("\nVerifica completata.")