import os
import requests
from Hybrid_RAG.constants import RAW_DATA_DIR_EVENT
# --- CONFIGURAZIONE ---
# Nome del file di testo che contiene i link
file_sorgente = RAW_DATA_DIR_EVENT / 'links.txt'

# Nome della cartella dove verranno salvati i file scaricati
cartella_download = RAW_DATA_DIR_EVENT
# --------------------

def scarica_file(url, cartella_destinazione):
    """
    Funzione per scaricare un singolo file da un URL in una cartella specifica.
    """
    # Prova a scaricare il file
    try:
        print(f"Tentativo di download da: {url}")
        
        # Effettua la richiesta GET all'URL. stream=True è importante per file grandi,
        # perché non carica l'intero contenuto in memoria in una volta.
        response = requests.get(url, stream=True)
        
        # Controlla se la richiesta ha avuto successo (es. status code 200 OK)
        response.raise_for_status()
        
        # Estrai il nome del file dall'URL (es. '20251001053000.translation.export.CSV.zip')
        nome_file = url.split('/')[-1]
        
        # Crea il percorso completo dove salvare il file
        percorso_completo = os.path.join(cartella_destinazione, nome_file)
        
        # Scrivi il contenuto del file sul disco in modalità binaria ('wb')
        with open(percorso_completo, 'wb') as f:
            # Scrivi il file a "pezzi" (chunks) per gestire file di grandi dimensioni
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        print(f"✅ Download completato: {nome_file}\n")
        
    except requests.exceptions.RequestException as e:
        # Gestisce errori di rete (es. link non valido, connessione assente)
        print(f"❌ Errore durante il download da {url}: {e}\n")


def main():
    """
    Funzione principale dello script.
    """
    # 1. Crea la cartella di download se non esiste già
    if not os.path.exists(cartella_download):
        print(f"Creo la cartella: '{cartella_download}'...")
        os.makedirs(cartella_download)

    # 2. Leggi il file di testo riga per riga
    try:
        with open(file_sorgente, 'r') as file:
            for riga in file:
                # Pulisci la riga da spazi bianchi iniziali/finali
                riga_pulita = riga.strip()
                
                # Se la riga non è vuota, procedi
                if riga_pulita:
                    # Dividi la riga in base agli spazi
                    parti = riga_pulita.split()
                    
                    # L'URL è il terzo elemento (indice 2)
                    if len(parti) >= 3:
                        url = parti[2]
                        # Controlla che sia un URL valido prima di scaricare
                        if url.startswith('http'):
                            scarica_file(url, cartella_download)
                        else:
                            print(f"⚠️ Ignorata riga non valida (URL non trovato): {riga_pulita}")
                            
    except FileNotFoundError:
        print(f"❌ Errore: File '{file_sorgente}' non trovato!")
        print("Assicurati che il file si trovi nella stessa cartella dello script.")

# Esegui la funzione principale quando lo script viene avviato
if __name__ == "__main__":
    main()