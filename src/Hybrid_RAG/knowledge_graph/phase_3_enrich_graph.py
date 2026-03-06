import sys
import time
from pathlib import Path
from neo4j import GraphDatabase

# --- CONFIGURAZIONE ---
NEO4J_CONFIG = {
    'uri': 'bolt://localhost:7687',
    'user': 'neo4j',
    'password': 'strong_password_neo4j'
}

CSV_FILENAME = "FINAL_RELATIONS_IMPORT.csv"

def load_semantic_relations(driver):
    print(f"\n--- FASE 3: Caricamento Relazioni Semantiche (AGGREGATE) ---")
    start_time = time.time()

    file_url = f"file:///knowledge_graph/{CSV_FILENAME}"
    print(f"Lettura da: {file_url}")

    # Query SOTA Aggiornata
    query = """
    CALL apoc.periodic.iterate(
      "LOAD CSV WITH HEADERS FROM $url AS row RETURN row",
      "
      MATCH (s:Entity {entityID: row.source_id})
      MATCH (t:Entity {entityID: row.target_id})
      
      // Crea la relazione
      CALL apoc.create.relationship(s, row.relation, {
        score: toFloat(row.max_score),
        
        // Peso (Frequenza)
        weight: toInteger(row.weight),
        
        // Converti stringa 'id1|id2' in Array reale ['id1', 'id2']
        chunk_ids: split(row.chunk_ids, '|'),
        
        source: 'LLM-Qwen-Aggregation',
        last_updated: datetime()
      }, t) YIELD rel
      
      RETURN count(rel)
      ",
      {batchSize: 5000, parallel: true, params: {url: $file_url}}
    )
    YIELD batches, total, errorMessages
    RETURN batches, total, errorMessages
    """
    
    with driver.session() as session:
        result = session.run(query, file_url=file_url)
        record = result.single()
        print(f"✅ Completato. Relazioni create/aggiornate: {record['total']:,}")
        if record['errorMessages']: print(f"⚠️ Errori: {record['errorMessages']}")

    print(f"Tempo: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    try:
        driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
        load_semantic_relations(driver)
        driver.close()
    except Exception as e:
        print(f"❌ Errore: {e}")