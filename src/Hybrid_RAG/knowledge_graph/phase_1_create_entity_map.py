import psycopg2
import spacy
import json
import re
import os
import sys
from tqdm import tqdm
from pathlib import Path
import psycopg2.extras

# --- 1. Setup dei Percorsi e Import ---

SRC_ROOT = Path(__file__).parent.parent.parent.resolve()
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

try:
    from Hybrid_RAG import constants
except ImportError:
    print(f"Errore: Impossibile importare 'constants.py'.")
    print(f"Verifica che il path '{SRC_ROOT}' sia corretto e contenga 'Hybrid_RAG/constants.py'")
    sys.exit(1)

# --- 2. Configurazione ---

POSTGRES_CONFIG = {
    'dbname': 'gdelt_rag_db',
    'user': 'gdelt_admin',
    'password': 'strong_password_123',
    'host': 'localhost',
    'port': '5432'
}

KG_DATA_DIR = constants.DATA_DIR / 'knowledge_graph'
OUTPUT_MAP_FILE = KG_DATA_DIR / 'entity_map.json'
UNMATCHED_LOG_FILE = KG_DATA_DIR / 'unmatched_entities.log'

# --- 3. Funzioni della Pipeline ---

def get_postgres_connection():
    """Si connette al database PostgreSQL."""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        return conn
    except psycopg2.OperationalError as e:
        print(f"Errore: Impossibile connettersi a PostgreSQL.\n{e}")
        print("Assicurati che Docker sia in esecuzione e che 'docker-compose up' sia attivo.")
        exit(1) # Esce se non può connettersi

def extract_dirty_entities(conn):
    """Estrae tutti i nomi unici di entità dal DB SQL."""
    print("Inizio estrazione entità 'sporche' da PostgreSQL...")
    dirty_entities = set()
    
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # 1. Da EVENT (colonne dirette)
    print("Step 1/2: Estrazione da tabella EVENT...")
    cursor.execute("""
        SELECT DISTINCT Actor1Name FROM EVENT WHERE Actor1Name IS NOT NULL
        UNION
        SELECT DISTINCT Actor2Name FROM EVENT WHERE Actor2Name IS NOT NULL
        UNION
        SELECT DISTINCT ActionGeo_Fullname FROM EVENT WHERE ActionGeo_Fullname IS NOT NULL;
    """)
    for row in tqdm(cursor.fetchall()):
        if row[0]:
            dirty_entities.add((row[0], 'UNKNOWN'))

    # 2. Da ARTICLE (colonne da splittare)
    print("Step 2/2: Estrazione da tabella ARTICLE (potrebbe richiedere tempo)...")
    cursor.close()
    ss_cursor = conn.cursor('article_stream', cursor_factory=psycopg2.extras.DictCursor)
    ss_cursor.execute("SELECT AllNames, Locations, EnhancedThemes FROM ARTICLE")
    
    clean_re = re.compile(r'[,0-9\(\):]+.*$')

    for row in tqdm(ss_cursor):
        if row['allnames']:
            for name in row['allnames'].split(';'):
                cleaned_name = clean_re.sub('', name).strip()
                if cleaned_name and len(cleaned_name) > 2:
                    dirty_entities.add((cleaned_name, 'ACTOR_OR_LOCATION'))
        
        if row['locations']:
            for loc in row['locations'].split(';'):
                cleaned_loc = clean_re.sub('', loc).strip()
                if cleaned_loc and len(cleaned_loc) > 2:
                    dirty_entities.add((cleaned_loc, 'LOCATION'))
        
        if row['enhancedthemes']:
            for theme in row['enhancedthemes'].split(';'):
                if theme and len(theme) > 2: 
                    dirty_entities.add((theme, 'THEME'))

    ss_cursor.close()
    conn.close() # Chiude la connessione dopo l'estrazione
    
    print(f"Estrazione completata. Trovate {len(dirty_entities)} entità uniche.")
    return dirty_entities

def setup_entity_linker(is_test=False):
    """Carica il modello spaCy e configura l'entity linker CORRETTAMENTE."""
    if not is_test:
        print("Caricamento modello spaCy 'en_core_web_lg'...")
    try:
        nlp = spacy.load("en_core_web_lg")
    except OSError:
        print("Errore: Modello 'en_core_web_lg' non trovato.")
        print("Esegui: python -m spacy download en_core_web_lg")
        exit(1)
    
    # --- SOLUZIONE ERRORE E030 ---
    # Aggiungi il sentencizer (leggero) per i confini delle frasi
    if "sentencizer" not in nlp.pipe_names:
        nlp.add_pipe("sentencizer", first=True)
        if not is_test:
            print("✓ 'sentencizer' aggiunto alla pipeline.")
    
    # --- CORREZIONE PRINCIPALE: usa "entityLinker" ---
    try:
        if "entityLinker" in nlp.pipe_names:
            nlp.remove_pipe("entityLinker")
        
        if not is_test:
            print("Configurazione entityLinker (spacy-entity-linker)...")
        
        nlp.add_pipe("entityLinker", last=True)
        
        if not is_test:
            print("✓ entityLinker configurato correttamente")
            
    except Exception as e:
        print(f"Errore nella configurazione di entityLinker: {e}")
        print("\nVerifica che:")
        print("1. Hai installato: pip install spacy-entity-linker")
        print("2. Hai scaricato la KB: python -m spacy_entity_linker download_knowledge_base")
        exit(1)
    
    return nlp

def normalize_string(s):
    """Normalizza una stringa per creare un ID custom."""
    if not s:
        return "unknown"
    s = str(s).lower().strip()
    s = re.sub(r'\s+', '_', s)
    s = re.sub(r'[^\w_]', '', s)
    return s

def create_entity_map(dirty_entities, nlp):
    """
    Crea la mappa di riconciliazione con entity linking funzionante.
    Versione STABILE (Single-Core).
    """
    print(f"Inizio Entity Linking su {len(dirty_entities)} entità...")
    entity_map = {}
    unmatched_log = []

    # Processa prima i temi (non usano entity linking)
    themes = {name for (name, type) in dirty_entities if type == 'THEME'}
    print(f"Processo {len(themes)} temi...")
    for theme_name in tqdm(themes, desc="Temi"):
        entity_id = "theme:" + normalize_string(theme_name)
        entity_map[theme_name] = {
            "id": entity_id,
            "name": theme_name,
            "type": "Theme",
            "source": "custom"
        }

    # Processa Attori e Luoghi con entity linking
    other_entities = {name for (name, type) in dirty_entities if type != 'THEME'}
    entity_names = list(other_entities)
    entity_types = {name: type for (name, type) in dirty_entities if type != 'THEME'}
    
    print(f"Processo {len(entity_names)} attori/luoghi con Entity Linking...")
    
    # Strategia di elaborazione
    batch_size = 500  # Dimensione del batch manuale
    total_batches = (len(entity_names) + batch_size - 1) // batch_size
    
    processed_count = 0
    entities_with_wikidata = 0
    
    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(entity_names))
        batch_names = entity_names[start_idx:end_idx]
        
        print(f"\nBatch {batch_idx + 1}/{total_batches} ({len(batch_names)} entità)")
        
        # --- ESECUZIONE SINGLE-CORE (STABILE E FUNZIONANTE) ---
        # Eseguiamo l'intera pipeline nlp.pipe in un unico passaggio.
        # Sarà lento (1M/ora) ma corretto.
        
        docs = nlp.pipe(batch_names, batch_size=20) 
            
        for doc in tqdm(docs, total=len(batch_names), desc=f"  - Batch {batch_idx + 1}", leave=False):
            original_name = doc.text
            entity_id = None
            canonical_name = original_name
            entity_type = "Unknown"
            wikidata_id = None
            
            # 🔍 Entity Linking
            if hasattr(doc._, 'linkedEntities'):
                linked_entities = doc._.linkedEntities
                if linked_entities:
                    entity = linked_entities[0]
                    wikidata_id = entity.get_id() # Es. 76
                    canonical_name = entity.get_label() # Es. Barack Obama
                    entities_with_wikidata += 1
            
            # 🔍 NER (se il linking fallisce o per il tipo)
            if not wikidata_id and doc.ents:
                ent = doc.ents[0]
                canonical_name = ent.text
                
                if ent.label_ in ["PERSON", "ORG", "NORP"]:
                    entity_type = "Actor"
                elif ent.label_ in ["GPE", "LOC", "FAC"]:
                    entity_type = "Location"
                else:
                    entity_type = "Unknown"

            # 🚀 Assegnazione ID Finale (Logica di Fallback)
            if wikidata_id:
                # Gestisce sia 76 (int) che "76" (str)
                entity_id = str(wikidata_id) 
                
                if entity_type == "Unknown":
                    gdel_type_guess = entity_types.get(original_name, 'UNKNOWN')
                    entity_type = "Location" if gdel_type_guess == 'LOCATION' else "Actor"
            else:
                norm_name = normalize_string(original_name)
                if entity_type == "Unknown":
                    gdel_type_guess = entity_types.get(original_name, 'UNKNOWN')
                    entity_type = "Location" if gdel_type_guess == 'LOCATION' else "Actor"
                
                entity_id = f"custom:{entity_type.lower()}:{norm_name}"
                unmatched_log.append(f"[{entity_type}] No Wikidata: '{original_name}' -> {entity_id}")

            entity_map[original_name] = {
                "id": entity_id,
                "name": canonical_name,
                "type": entity_type,
                "source": "wikidata" if wikidata_id else "custom"
            }
            
            processed_count += 1
            if processed_count % 5000 == 0:
                print(f"\nProgresso: {processed_count}/{len(entity_names)} "
                      f"({entities_with_wikidata} con Wikidata ID)")

    print(f"Entity Linking completato: {entities_with_wikidata}/{len(entity_names)} entità con Wikidata ID")
    return entity_map, unmatched_log


# --- 4. 🚀 NUOVA FUNZIONE: PRE-FLIGHT CHECK ---

def run_preflight_checks():
    """
    Esegue un test end-to-end rapido dei componenti critici.
    Se fallisce, esce con un errore. Se ha successo, restituisce gli handle nlp e conn.
    """
    print("="*50)
    print("--- 1. ESECUZIONE TEST PRE-FLIGHT ---")
    print("="*50)
    
    # --- Test 1: Connessione a PostgreSQL ---
    print("\n[TEST 1/3] Connessione a PostgreSQL...")
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        print("✓ Connessione a PostgreSQL riuscita.")
    except Exception as e:
        print(f"❌ TEST FALLITO: Impossibile connettersi a PostgreSQL.")
        print(f"Errore: {e}")
        exit(1)

    # --- Test 2: Accesso in Scrittura alla Directory ---
    print("\n[TEST 2/3] Accesso in scrittura alla directory...")
    try:
        if not KG_DATA_DIR.exists():
            KG_DATA_DIR.mkdir(parents=True)
            print(f"Creata directory: {KG_DATA_DIR}")
        
        test_file = KG_DATA_DIR / "preflight_test.tmp"
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        print(f"✓ Accesso in scrittura a '{KG_DATA_DIR}' riuscito.")
    except Exception as e:
        print(f"❌ TEST FALLITO: Impossibile scrivere in '{KG_DATA_DIR}'.")
        print("Verifica i permessi della cartella.")
        print(f"Errore: {e}")
        exit(1)

    # --- Test 3: Pipeline NLP (Caricamento e Test Logico) ---
    print("\n[TEST 3/3] Pipeline NLP (spaCy + entityLinker)...")
    try:
        # Carica la pipeline in "modalità test" (meno stampe)
        nlp = setup_entity_linker(is_test=True) 
        
        print("  - Esecuzione test logico (Barack Obama -> 76)...")
        test_doc = nlp("Barack Obama") # Processa un testo di prova
        
        if not hasattr(test_doc._, 'linkedEntities') or not test_doc._.linkedEntities:
            raise ValueError("L'attributo 'linkedEntities' è vuoto o non trovato.")
        
        entity = test_doc._.linkedEntities[0]
        entity_id = entity.get_id() # Questo restituisce "76"
        
        # --- 💡 LA CORREZIONE E' QUI ---
        # Il pacchetto restituisce "76", non "Q76". Accettiamo entrambi
        # per sicurezza, ma il test ora cerca l'ID corretto.
        if entity_id == 76 or entity_id == "76" or entity_id == "Q76": 
            print(f"✓ Test pipeline NLP riuscito ('Barack Obama' -> '{entity_id}')")
        else:
            raise ValueError(f"ID Errato. Atteso '76' (Barack Obama), ricevuto '{entity_id}'")

    except Exception as e:
        print(f"❌ TEST FALLITO: La pipeline NLP non funziona.")
        print("Causa probabile: L'Entity Linker o la Knowledge Base non sono caricati/trovati.")
        print(f"Errore: {e}")
        print("\nVerifica di aver eseguito (come Admin):")
        print("python -m spacy_entity_linker download_knowledge_base")
        exit(1)
    
    print("="*50)
    print("--- TUTTI I TEST PRE-FLIGHT RIUSCITI ---")
    print("="*50 + "\n")
    
    conn.close() # Chiudiamo la connessione di test
    return nlp # Passiamo l'oggetto nlp già caricato


# --- 5. Esecuzione (Aggiornata) ---

def main():
    """Esegue l'intera pipeline della Fase 1."""
    
    # 1. Esegui i test prima di qualsiasi altra cosa
    # 'nlp' è già caricato e testato, pronto all'uso.
    nlp = run_preflight_checks()
    
    print("--- 2. AVVIO PROCESSO PRINCIPALE ---")

    # 2. Extract (apre una nuova connessione per il processo)
    conn = get_postgres_connection()
    dirty_entities = extract_dirty_entities(conn) # Questa funzione chiude la sua connessione

    # 3. Transform (usa l'oggetto 'nlp' testato)
    entity_map, unmatched_log = create_entity_map(dirty_entities, nlp)

    # 4. Save
    print("\nSalvataggio file di output...")
    print(f"Salvataggio mappa in '{OUTPUT_MAP_FILE}'...")
    with open(OUTPUT_MAP_FILE, 'w', encoding='utf-8') as f:
        json.dump(entity_map, f, indent=2, ensure_ascii=False)

    print(f"Salvataggio log in '{UNMATCHED_LOG_FILE}'...")
    with open(UNMATCHED_LOG_FILE, 'w', encoding='utf-8') as f:
        f.write("\n".join(unmatched_log))

    print("\n" + "="*50)
    print("FASE 1 COMPLETATA CON SUCCESSO!")
    print("="*50)
    print(f"• Entità totali: {len(entity_map):,}")
    
    # Calcolo delle statistiche
    wikidata_count = sum(1 for e in entity_map.values() if e['source'] == 'wikidata')
    custom_count = len(entity_map) - wikidata_count
    
    print(f"• Con Wikidata ID: {wikidata_count:,}")
    print(f"• Con ID custom: {custom_count:,}")
    print(f"• File generati:")
    print(f"  - Mappa entità: {OUTPUT_MAP_FILE}")
    print(f"  - Log dettagli: {UNMATCHED_LOG_FILE}")

if __name__ == "__main__":
    main()