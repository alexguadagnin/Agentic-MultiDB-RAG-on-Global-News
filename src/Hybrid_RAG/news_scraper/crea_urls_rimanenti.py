import json
import os

# --- Nomi dei file ---
file_json_parziale = "output.jl"
file_urls_originali = "urls_da_processare.txt"
file_urls_rimanenti = "urls_top_rimasti.txt"

# --- Insieme per memorizzare gli URL già processati ---
urls_completati = set()
errori_lettura_json = 0
linee_lette_json = 0

print(f"[*] Sto leggendo gli URL completati da '{file_json_parziale}'...")

# --- Lettura Robusta del File JSON Parziale ---
# Cerchiamo di leggere riga per riga, gestendo errori se il JSON è invalido
try:
    with open(file_json_parziale, 'r', encoding='utf-8') as f_json:
        # Tentativo di leggere l'intero file se è piccolo e valido
        # Questo fallirà se il file è invalido o troppo grande
        try:
            data = json.load(f_json)
            for item in data:
                if isinstance(item, dict) and 'url' in item:
                    urls_completati.add(item['url'])
            linee_lette_json = len(data) # Approssimazione
            print(f"[+] Lettura JSON completata (metodo standard). Trovati {len(urls_completati)} URL unici.")

        except json.JSONDecodeError as e:
            print(f"[!] Errore nel caricamento JSON standard: {e}. Tento lettura linea per linea...")
            # Riavvolgi il file per leggerlo riga per riga
            f_json.seek(0)
            buffer = ""
            for linea in f_json:
                linee_lette_json += 1
                buffer += linea.strip()
                # Rimuovi virgole iniziali/finali e parentesi quadre residue
                buffer = buffer.strip('[,] ')

                # Se il buffer sembra contenere un oggetto JSON valido
                if buffer.startswith('{') and buffer.endswith('}'):
                    try:
                        item = json.loads(buffer)
                        if isinstance(item, dict) and 'url' in item:
                            urls_completati.add(item['url'])
                        buffer = "" # Resetta il buffer dopo successo
                    except json.JSONDecodeError:
                        # Probabilmente un oggetto incompleto, continua ad accumulare
                        errori_lettura_json +=1
                        # Se il buffer diventa troppo grande senza successo, resettalo
                        if len(buffer) > 10000: # Limite arbitrario
                             print(f"[!] Buffer troppo grande senza successo alla linea ~{linee_lette_json}, resetto.")
                             buffer = ""
                             errori_lettura_json +=1
                elif buffer == '[' or buffer == ']': # Ignora parentesi su righe separate
                    buffer = ""
                # Se la linea non chiude un oggetto, continua ad accumulare nel buffer

            if errori_lettura_json > 0:
                print(f"[!] Durante la lettura linea per linea, si sono verificati {errori_lettura_json} errori di parsing (oggetti incompleti?).")
            print(f"[+] Lettura JSON linea per linea terminata. Letti circa {linee_lette_json} linee. Trovati {len(urls_completati)} URL unici.")

except FileNotFoundError:
    print(f"[!] Errore: File '{file_json_parziale}' non trovato. Assicurati che sia nella stessa cartella.")
    exit()
except Exception as e:
    print(f"[!] Errore imprevisto durante la lettura del JSON: {e}")
    exit()

if not urls_completati:
    print("[!] Attenzione: Nessun URL completato trovato nel file JSON. Il file è vuoto o illeggibile?")

# --- Lettura degli URL Originali e Scrittura dei Rimanenti ---
print(f"[*] Sto leggendo gli URL originali da '{file_urls_originali}'...")
urls_rimanenti_cont = 0
urls_originali_cont = 0

try:
    with open(file_urls_originali, 'r', encoding='utf-8') as f_orig, \
         open(file_urls_rimanenti, 'w', encoding='utf-8') as f_riman:

        for linea in f_orig:
            urls_originali_cont += 1
            url_originale = linea.strip()
            if url_originale and url_originale not in urls_completati:
                f_riman.write(url_originale + '\n')
                urls_rimanenti_cont += 1

    print(f"[+] Fatto! Creato il file '{file_urls_rimanenti}'.")
    print(f"    - URL originali letti: {urls_originali_cont}")
    print(f"    - URL completati trovati nel JSON: {len(urls_completati)}")
    print(f"    - URL scritti nel nuovo file: {urls_rimanenti_cont}")
    urls_saltati_stimati = urls_originali_cont - len(urls_completati) - urls_rimanenti_cont
    if urls_saltati_stimati > 0 :
         print(f"[!] Attenzione: Circa {urls_saltati_stimati} URL presenti nel JSON potrebbero non essere stati trovati nell'originale (o viceversa, a causa di duplicati/errori).")
    elif urls_rimanenti_cont + len(urls_completati) < urls_originali_cont:
         print(f"[!] Attenzione: la somma di URL completati e rimanenti ({urls_rimanenti_cont + len(urls_completati)}) è minore degli originali ({urls_originali_cont}). Potrebbero esserci duplicati nell'originale.")


except FileNotFoundError:
    print(f"[!] Errore: File '{file_urls_originali}' non trovato.")
except Exception as e:
    print(f"[!] Errore imprevisto durante la scrittura del nuovo file: {e}")