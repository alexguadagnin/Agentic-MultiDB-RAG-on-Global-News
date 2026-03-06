import openai
from pathlib import Path
import json
import os
import time
from tqdm import tqdm # Importa tqdm per la barra di avanzamento
import sys # Importa sys per gestire l'import
from typing import List, Dict, Tuple
from Hybrid_RAG.constants import PROJECT_ROOT

# --- 1. CONFIGURAZIONE ---

# !!! INCOLLA QUI LA TUA API KEY DI OPENAI !!!
OPENAI_API_KEY = "..."
if OPENAI_API_KEY == "LA_TUA_API_KEY":
    print("ERRORE OPENAI_API_KEY")
    exit()

# Modello LLM da usare (come da tua specifica)
MODEL_NAME = "gpt-5" 

# File di input
FILE_CAMPIONI = PROJECT_ROOT / "campioni_per_llm.jsonl" 

# File di output dove salvare le istruzioni di pulizia
FILE_OUTPUT_ISTRUZIONI = PROJECT_ROOT / "istruzioni_pulizia_llm_openai.jsonl"


# --- 2. DEFINIZIONE PROMPT ---

def crea_prompt(domain_name: str, samples: List[Dict]) -> Tuple[str, str]:
    """
    Crea i prompt (system e user) per l'API OpenAI.
    """
    
    prompt_system = """
Sei un esperto di data cleaning e ingegneria dei dati per pipeline NLP.
Il tuo obiettivo è analizzare campioni di testo grezzo da articoli di notizie e identificare pattern di "rumore" da rimuovere.
Il testo pulito verrà usato per creare embeddings (vettori) per un database vettoriale.

Fornisci la tua risposta ESCLUSIVAMENTE in formato JSON valido.
Il JSON deve avere la seguente struttura:
{
  "patterns_da_rimuovere": [
    "regex o stringa esatta 1",
    "regex o stringa esatta 2"
  ],
  "altri_suggerimenti": "Una breve frase con consigli aggiuntivi (es. 'normalizzare minuscolo', 'rimuovere punteggiatura', 'testo sembra pulito', ecc.)"
}
"""

    prompt_user = f"""
Sto analizzando articoli dal dominio: {domain_name}

Ho estratto {len(samples)} campioni (snippet) di testo da questo dominio.
Analizza questi campioni e identifica pattern di testo ricorrenti (come piè di pagina, cookie banner, menu, pubblicità, 'leggi anche', 'copyright', 'iscriviti alla newsletter', ecc.) che dovrebbero essere rimossi per pulire il testo prima dell'embedding.
Il testo è in lingua originale (potrebbe non essere inglese).

Ecco i {len(samples)} campioni:
"""

    # Aggiungi i campioni al prompt
    for i, sample in enumerate(samples):
        prompt_user += f"\n--- CAMPIONE {i+1} (da {sample['url']}) ---\n"
        prompt_user += sample['text_snippet']
        prompt_user += "\n--- FINE CAMPIONE ---\n"
        
    prompt_user += "\nFornisci la tua analisi nel formato JSON richiesto."
    
    return prompt_system, prompt_user

# --- 3. ESECUZIONE ---

def analizza_con_llm():
    print(f"--- AVVIO ANALISI CAMPIONI CON LLM ({MODEL_NAME}) ---")
    
    try:
        # Inizializza il client di OpenAI
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        
        # Leggi i campioni dal file JSONL
        try:
            with open(FILE_CAMPIONI, 'r', encoding='utf-8') as f:
                campioni_per_dominio = [json.loads(line) for line in f]
        except FileNotFoundError:
            print(f"ERRORE: File campioni '{FILE_CAMPIONI}' non trovato.")
            print(f"Percorso cercato: {FILE_CAMPIONI.resolve()}")
            print("Assicurati di aver prima eseguito 'prepara_campioni_per_llm.py'.")
            return
        except json.JSONDecodeError:
            print(f"ERRORE: Il file '{FILE_CAMPIONI}' non è un JSONL valido. Rigeneralo.")
            return

        print(f"Trovati {len(campioni_per_dominio)} domini da analizzare. Avvio...")

        # Apri il file di output per scrivere i risultati man mano
        with open(FILE_OUTPUT_ISTRUZIONI, 'w', encoding='utf-8') as f_out:
            
            # Usa tqdm per creare una barra di avanzamento
            for item in tqdm(campioni_per_dominio, desc="Analizzando Domini"):
                domain_name = item['domain_name']
                domain_root = item['domain_root']
                samples = item['samples']
                
                try:
                    # Crea e invia il prompt
                    system_prompt, user_prompt = crea_prompt(domain_name, samples)
                    
                    response = client.chat.completions.create(
                        model=MODEL_NAME,
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt}
                        ],
                        # Chiedi all'API di forzare l'output in formato JSON
                        response_format={"type": "json_object"}
                    )
                    
                    # Estrai il JSON dalla risposta
                    risultato_json_str = response.choices[0].message.content
                    
                    # Salva il risultato (JSON su una riga) nel file di output
                    risultato_obj = json.loads(risultato_json_str)
                    risultato_obj['domain_root'] = domain_root
                    risultato_obj['domain_name'] = domain_name
                    
                    f_out.write(json.dumps(risultato_obj, ensure_ascii=False) + '\n')
                    
                except Exception as e:
                    # Stampa un errore più dettagliato
                    print(f" -> ERRORE durante l'analisi del dominio {domain_name}:")
                    print(f"    {type(e).__name__}: {e}")
                    error_obj = {
                        "domain_root": domain_root,
                        "domain_name": domain_name,
                        "error": str(e)
                    }
                    f_out.write(json.dumps(error_obj, ensure_ascii=False) + '\n')
                
                # Attendi 1 secondo per evitare di superare i limiti di quota (opzionale ma sicuro)
                time.sleep(1) 

        print(f"\n--- ✅ ANALISI LLM COMPLETATA ---")
        print(f"Tutte le istruzioni di pulizia sono state salvate in: {FILE_OUTPUT_ISTRUZIONI}")

    except openai.AuthenticationError:
        print("\nERRORE DI AUTENTICAZIONE: La tua API key di OpenAI non è valida.")
    except Exception as e:
        print(f"\nERRORE INASPETTATO (configurazione API?): {e}")

if __name__ == "__main__":
    analizza_con_llm()