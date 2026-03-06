import json
import sys
import os
import time
import csv
import glob
from pathlib import Path
from neo4j import GraphDatabase
from tqdm import tqdm 

# --- CONFIGURAZIONE ---
NEO4J_CONFIG = {
    'uri': 'bolt://localhost:7687',
    'user': 'neo4j',
    'password': 'strong_password_neo4j'
}

BASE_DIR = Path(__file__).parent.parent.parent.parent.resolve()
DATA_DIR = BASE_DIR / "data" / "knowledge_graph"
ENTITY_MAP_FILE = DATA_DIR / "entity_map.json"
ENTITIES_CSV_DIR = DATA_DIR / "chunks_nodes_entities"

DIRS = {
    "nodes_event": DATA_DIR / "chunks_nodes_event",
    "nodes_article": DATA_DIR / "chunks_nodes_article",
    "rel_mentions": DATA_DIR / "chunks_rel_mentions",
    "rel_events": DATA_DIR / "chunks_rel_events",
    "rel_themes": DATA_DIR / "chunks_rel_themes",
    "rel_cats": DATA_DIR / "chunks_rel_cats",
    "nodes_entities": ENTITIES_CSV_DIR
}

# --- 1. GENERAZIONE CSV ENTITÀ UNICHE ---
def generate_entities_csv():
    print("\n--- 1. Generazione CSV Entità Uniche da JSON ---")
    if not ENTITIES_CSV_DIR.exists(): ENTITIES_CSV_DIR.mkdir(parents=True)
    
    output_file = ENTITIES_CSV_DIR / "all_entities.csv"
    if output_file.exists() and output_file.stat().st_size > 0:
        print("   -> File CSV entità già esistente. Salto generazione.")
        return

    # Pulizia se vuoto o parziale
    for f in ENTITIES_CSV_DIR.glob("*.csv"): os.remove(f)

    print(f"   -> Leggo {ENTITY_MAP_FILE}...")
    try:
        with open(ENTITY_MAP_FILE, 'r', encoding='utf-8') as f:
            entity_map = json.load(f)
    except Exception as e:
        print(f"❌ Errore lettura JSON: {e}"); sys.exit(1)
    
    print(f"   -> Scrivo {output_file}...")
    count = 0
    with open(output_file, 'w', encoding='utf-8', newline='') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(["entity_id", "name", "type"])
        
        # Usiamo tqdm anche qui per vedere il progresso della scrittura
        for key, data in tqdm(entity_map.items(), desc="Scrittura Entità"):
            if data.get('id') and data.get('type') in ['Actor', 'Location']:
                writer.writerow([data['id'], data['name'], data['type']])
                count += 1
                
    print(f"✓ Creato CSV con {count:,} entità uniche.")

# --- 2. UTILS ---
def create_indexes(driver):
    print("\n--- 2. Creazione Indici Neo4j ---")
    queries = [
        "CREATE CONSTRAINT event_id IF NOT EXISTS FOR (e:Event) REQUIRE e.GlobalEventID IS UNIQUE",
        "CREATE CONSTRAINT article_id IF NOT EXISTS FOR (a:Article) REQUIRE a.DocIdentifier_Normalized IS UNIQUE",
        "CREATE CONSTRAINT actor_id IF NOT EXISTS FOR (a:Actor) REQUIRE a.entityID IS UNIQUE",
        "CREATE CONSTRAINT loc_id IF NOT EXISTS FOR (l:Location) REQUIRE l.entityID IS UNIQUE",
        "CREATE CONSTRAINT theme_name IF NOT EXISTS FOR (t:Theme) REQUIRE t.name IS UNIQUE",
        "CREATE CONSTRAINT cat_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE",
        "CREATE INDEX entity_lookup IF NOT EXISTS FOR (n:Entity) ON (n.entityID)" 
    ]
    with driver.session() as session:
        for q in queries: session.run(q)
    print("✓ Indici creati.")

def get_url(folder_key, filename):
    return f"file:///knowledge_graph/{DIRS[folder_key].name}/{filename}"

# --- 3. CARICAMENTO INTELLIGENTE ---
def load_strategic(driver):
    print("\n--- 3. Importazione Strategica in Neo4j ---")
    
    with driver.session() as session:
        
        # A. CARICAMENTO NODI PRINCIPALI
        
        # 1. Entità (Actor/Location) - Unico File Grande
        print("\n>> Caricamento Nodi Entità (Actor/Location)...")
        t0 = time.time()
        session.run(f"""
            LOAD CSV WITH HEADERS FROM '{get_url("nodes_entities", "all_entities.csv")}' AS r
            CALL {{
                WITH r
                CALL apoc.merge.node([r.type, 'Entity'], {{entityID: r.entity_id}}, {{name: r.name}}) YIELD node
                RETURN count(node) AS c
            }} IN TRANSACTIONS OF 10000 ROWS
            RETURN count(*)
        """)
        print(f"   ✓ Completato in {time.time()-t0:.1f}s")
        
        # 2. Eventi
        files = sorted(glob.glob(str(DIRS["nodes_event"] / "*.csv")))
        for f in tqdm(files, desc="Nodi Eventi", unit="file"):
            session.run(f"""
                LOAD CSV WITH HEADERS FROM '{get_url('nodes_event', Path(f).name)}' AS r 
                CALL {{ 
                    WITH r 
                    MERGE (e:Event {{GlobalEventID: r.gid}}) 
                    SET e.Day=r.day, e.AvgTone=toFloat(r.tone) 
                }} IN TRANSACTIONS OF 5000 ROWS
                RETURN count(*)
            """)
            
        # 3. Articoli
        files = sorted(glob.glob(str(DIRS["nodes_article"] / "*.csv")))
        for f in tqdm(files, desc="Nodi Articoli", unit="file"):
            session.run(f"""
                LOAD CSV WITH HEADERS FROM '{get_url('nodes_article', Path(f).name)}' AS r 
                CALL {{ 
                    WITH r 
                    MERGE (a:Article {{DocIdentifier_Normalized: r.doc_id}}) 
                    SET a.url=r.url, a.Date=r.date 
                }} IN TRANSACTIONS OF 5000 ROWS
                RETURN count(*)
            """)

        # B. CARICAMENTO RELAZIONI
        
        # 4. Relazioni Eventi
        files = sorted(glob.glob(str(DIRS["rel_events"] / "*.csv")))
        for f in tqdm(files, desc="Relazioni Eventi", unit="file"):
            session.run(f"""
                LOAD CSV WITH HEADERS FROM '{get_url('rel_events', Path(f).name)}' AS r 
                CALL {{
                    WITH r
                    MATCH (e:Event {{GlobalEventID: r.gid}})
                    MATCH (Ent:Entity {{entityID: r.entity_id}}) 
                    MERGE (Ent)-[:INVOLVED_IN]->(e)
                }} IN TRANSACTIONS OF 5000 ROWS
                RETURN count(*)
            """)

        # 5. Relazioni Menzioni
        files = sorted(glob.glob(str(DIRS["rel_mentions"] / "*.csv")))
        for f in tqdm(files, desc="Relazioni Menzioni", unit="file"):
            session.run(f"""
                LOAD CSV WITH HEADERS FROM '{get_url('rel_mentions', Path(f).name)}' AS r 
                CALL {{
                    WITH r
                    MATCH (a:Article {{DocIdentifier_Normalized: r.doc_id}})
                    MATCH (e:Entity {{entityID: r.entity_id}})
                    MERGE (a)-[:MENTIONS]->(e)
                }} IN TRANSACTIONS OF 5000 ROWS
                RETURN count(*)
            """)

        # 6. Temi
        files = sorted(glob.glob(str(DIRS["rel_themes"] / "*.csv")))
        for f in tqdm(files, desc="Relazioni Temi", unit="file"):
            session.run(f"""
                LOAD CSV WITH HEADERS FROM '{get_url('rel_themes', Path(f).name)}' AS r 
                CALL {{ 
                    WITH r 
                    MATCH (a:Article {{DocIdentifier_Normalized: r.doc_id}}) 
                    MERGE (t:Theme {{name: r.theme_name}}) 
                    MERGE (a)-[:HAS_THEME]->(t) 
                }} IN TRANSACTIONS OF 5000 ROWS
                RETURN count(*)
            """)
            
        # 7. Categorie
        files = sorted(glob.glob(str(DIRS["rel_cats"] / "*.csv")))
        for f in tqdm(files, desc="Relazioni Categorie", unit="file"):
            session.run(f"""
                LOAD CSV WITH HEADERS FROM '{get_url('rel_cats', Path(f).name)}' AS r 
                CALL {{ 
                    WITH r 
                    MATCH (a:Article {{DocIdentifier_Normalized: r.doc_id}}) 
                    MERGE (c:Category {{name: r.cat_name}}) 
                    MERGE (a)-[:IN_CATEGORY]->(c) 
                }} IN TRANSACTIONS OF 5000 ROWS
                RETURN count(*)
            """)

# --- MAIN ---
if __name__ == "__main__":
    # 1. Prepara i dati mancanti
    generate_entities_csv()
    
    # 2. Carica in Neo4j
    try:
        print("\nConnessione a Neo4j...")
        driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
        driver.verify_connectivity()
        
        create_indexes(driver)
        load_strategic(driver)
        
        driver.close()
    except Exception as e:
        print(f"Errore Neo4j: {e}")
        sys.exit(1)

    print("\n✅ GRAFO RICOSTRUITO CORRETTAMENTE.")