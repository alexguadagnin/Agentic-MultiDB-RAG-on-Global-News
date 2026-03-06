import json
import os

FILE_INPUT_ATTUALE = 'urls_rimanenti.txt'
FILE_OUTPUT_COMPLETATO = 'output.jl' 
NUOVO_FILE_INPUT = 'urls_da_processare.txt'

urls_gia_fatti = set()

print(f"Sto leggendo gli URL già completati da {FILE_OUTPUT_COMPLETATO}...")

# Leggi il file .jl (sicuro riga per riga)
if os.path.exists(FILE_OUTPUT_COMPLETATO):
    with open(FILE_OUTPUT_COMPLETATO, 'r', encoding='utf-8') as f:
        for riga in f:
            try:
                item = json.loads(riga)
                if 'url' in item:
                    urls_gia_fatti.add(item['url'])
            except json.JSONDecodeError:
                print(f"Riga malformata saltata: {riga[:50]}...")

print(f"Trovati {len(urls_gia_fatti)} URL unici già processati.")
print(f"Sto filtrando {FILE_INPUT_ATTUALE} per creare il nuovo file di input...")

urls_da_scrivere = []
totale_input = 0
with open(FILE_INPUT_ATTUALE, 'r', encoding='utf-8') as f:
    for riga in f:
        totale_input += 1
        url = riga.strip()
        if url and url not in urls_gia_fatti:
            urls_da_scrivere.append(url)

print(f"Input iniziale: {totale_input} URL.")
print(f"Rimanenti da processare: {len(urls_da_scrivere)} URL.")

# Scrivi il nuovo file di input
with open(NUOVO_FILE_INPUT, 'w', encoding='utf-8') as f:
    for url in urls_da_scrivere:
        f.write(url + '\n')

print(f"Creato file '{NUOVO_FILE_INPUT}' con successo.")