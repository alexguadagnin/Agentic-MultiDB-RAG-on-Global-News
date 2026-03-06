import json
import os

# --- Nomi dei file ---
FILE_JSON_VECCHIO = 'output_parziale_48h.json'  # Il file JSON array (forse rotto)
FILE_JL_NUOVO = 'output.jl'                   # Il file JSON Lines (quello robusto)
# ---------------------

print(f"Avvio unione (metodo alternativo): si leggerà da '{FILE_JSON_VECCHIO}' e si accoderà a '{FILE_JL_NUOVO}'...")

contatore = 0
try:
    with open(FILE_JL_NUOVO, 'a', encoding='utf-8') as f_jl:
        with open(FILE_JSON_VECCHIO, 'r', encoding='utf-8') as f_json:
            
            print("Lettura del file JSON parziale in corso (metodo robusto)...")
            
            # Saltiamo l'eventuale '[' iniziale
            char = f_json.read(1)
            while char and char.isspace(): # Salta spazi bianchi
                char = f_json.read(1)
            
            if char != '[':
                print("ATTENZIONE: Il file JSON non inizia con '['. Tento di leggerlo comunque.")
                # Rimettiamo il carattere letto, se non è una parentesi
                f_json.seek(0) 

            # Ora cerchiamo gli oggetti
            brace_level = 0
            current_object_str = ""
            
            while True:
                char = f_json.read(1)
                if not char:
                    # Fine del file (probabilmente interrotto)
                    if brace_level > 0 and current_object_str:
                        print("ATTENZIONE: Il file è terminato a metà di un oggetto. Ultimo oggetto non salvato.")
                    break
                
                if char == '{':
                    if brace_level == 0:
                        current_object_str = "{" # Inizia un nuovo oggetto
                    else:
                        current_object_str += char
                    brace_level += 1
                
                elif char == '}':
                    if brace_level > 0:
                        current_object_str += char
                        brace_level -= 1
                        
                        if brace_level == 0:
                            # Abbiamo un oggetto completo!
                            try:
                                # 1. Carica la stringa come dict Python
                                item = json.loads(current_object_str)
                                # 2. Scarica il dict come stringa JSON (per il file .jl)
                                riga_json = json.dumps(item, ensure_ascii=False)
                                # 3. Scrivi sul file .jl
                                f_jl.write(riga_json + '\n')
                                contatore += 1
                                current_object_str = "" # Resetta
                            except json.JSONDecodeError as e:
                                print(f"ATTENZIONE: Trovato un oggetto JSON malformato, lo salto. Errore: {e}")
                                current_object_str = ""
                                brace_level = 0 # Resetta per sicurezza
                
                elif brace_level > 0:
                    # Siamo dentro un oggetto, aggiungiamo il carattere
                    current_object_str += char
                
                # Ignoriamo virgole, spazi, ecc. quando siamo *tra* oggetti (brace_level == 0)

    print(f"\nOperazione completata con successo!")
    print(f"Aggiunti {contatore} articoli da '{FILE_JSON_VECCHIO}' a '{FILE_JL_NUOVO}'.")

except FileNotFoundError:
    print(f"ERRORE: Uno dei file non è stato trovato. Controlla i nomi:")
    print(f"- {FILE_JSON_VECCHIO}")
    print(f"- {FILE_JL_NUOVO}")
except Exception as e:
    print(f"Si è verificato un errore imprevisto: {e}")