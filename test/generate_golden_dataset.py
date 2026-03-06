import os
import sys
import json
import time
import random
import psycopg2
from pathlib import Path
from typing import Any, Dict, List
from neo4j import GraphDatabase
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
# Pydantic imports unificati
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field

# --- CONFIGURAZIONE ---
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

OUTPUT_FILE = "test/golden_dataset_300_final.json"
CHECKPOINT_EVERY = 5  # Salva ogni 5 domande generate
SLEEP_TIME = 1.0      # Secondi di pausa tra le chiamate

# Target per categoria
TARGET_SQL = 75
TARGET_GRAPH = 75
TARGET_VECTOR = 150

TARGET_TOTAL = TARGET_SQL + TARGET_GRAPH + TARGET_VECTOR

GEN_MODEL_NAME = "gpt-5-mini" 

# DB CONFIG
PG_CONN = f"dbname={os.getenv('POSTGRES_DB', 'gdelt_rag_db')} user={os.getenv('POSTGRES_USER', 'gdelt_admin')} password={os.getenv('POSTGRES_PASSWORD', 'strong_password_123')} host=localhost port=5432"
NEO4J_URI = "bolt://localhost:7687" 
NEO4J_AUTH = (os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "strong_password_neo4j"))

ES_HOST = "http://localhost:9200"
ES_INDEX = "news_chunks"
ES_TEXT_FIELD = "chunk_text"

PG_TABLE = "event"
PG_THEME_COL = "themes_human"
TARGET_LANGS = [
    "Italian", 
    "English", 
    "Spanish", 
    "French", 
    "German", 
    "Russian", 
    "Chinese" 
]

GDELT_SYSTEM_CONTEXT = """
SEI UN ESPERTO VALIDATORE DI SISTEMI RAG (Retrieval-Augmented Generation).
Stai costruendo un "Golden Dataset" per testare un sistema basato su GDELT.

IL TUO OBIETTIVO:
Generare coppie (Domanda, Risposta Ideale) realistiche, complesse e precise.

IL CONTESTO DEI DATI:
- Eventi globali (proteste, conflitti, diplomazia) dal 2025 in poi.

REGOLE DI GENERAZIONE:
1. TONO PROFESSIONALE: Evita domande banali.
2. NESSUNA AMBIGUITÀ: De-contestualizza sempre (es. usa nomi propri, non "lui").
3. RISPOSTA IDEALE: Deve essere una frase completa e naturale nella stessa lingua della domanda.
   - NO: "150"
   - SI: "Ci sono stati 150 eventi registrati."

Se il chunk è spazzatura, rispondi 'SKIP' nella domanda.
"""

# --- UTILS ---
class QAPair(BaseModel):
    question: str = Field(description="La domanda naturale nella lingua target.")
    answer: str = Field(description="La risposta ideale, naturale e completa, nella stessa lingua della domanda.")

def call_llm(specific_instructions: str, human_input: str):
    """Genera SIA la domanda CHE la risposta ideale."""
    
    full_system_prompt = f"{GDELT_SYSTEM_CONTEXT}\n\nISTRUZIONI SPECIFICHE:\n{specific_instructions}"
    
    for i in range(3):
        try:
            prompt = ChatPromptTemplate.from_messages([
                ("system", full_system_prompt), 
                ("human", human_input)
            ])
            
            # Usiamo QAPair qui
            llm = ChatOpenAI(model=GEN_MODEL_NAME, temperature=0.7).with_structured_output(QAPair)
            
            res = (prompt | llm).invoke({})
            time.sleep(SLEEP_TIME)
            
            # Ritorniamo l'oggetto intero
            return res 
        except Exception as e:
            print(f"⚠️ Errore LLM (retry {i+1}): {e}")
            time.sleep(2)
            
    return None 

def load_checkpoint():
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            try: return json.load(f)
            except: return []
    return []

def save_checkpoint(items):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"💾 Checkpoint: {len(items)} items salvati.")

def already_have(items, category):
    return len([x for x in items if x['category'] == category])

# --- GENERATORI ---

def generate_sql_items(items):
    current = already_have(items, "SQL")
    if current >= TARGET_SQL:
        print("✅ SQL completato.")
        return

    print(f"📊 SQL: Genero {TARGET_SQL - current} items (Robust Mode)...")
    try:
        conn = psycopg2.connect(PG_CONN)
    except Exception as e:
        print(f"❌ Errore PG: {e}"); return

    sql_instructions = (
        "Sei un generatore di Ground Truth rigoroso per un database SQL.\n"
        "Il tuo compito è trasformare un FATTO NUMERICO in una coppia Domanda/Risposta.\n\n"
        "REGOLA FERREA - NO ALLUCINAZIONI:\n"
        "- La domanda deve basarsi ESCLUSIVAMENTE sui filtri citati nel FATTO.\n"
        "- Se il fatto dice '50 eventi nel luogo Roma', NON inventare '50 proteste a Roma'. Scrivi '50 eventi registrati a Roma'.\n"
        "- Se il fatto dice 'Tema: health_society_welfare', NON scrivere 'Epidemia di influenza'. Scrivi 'Eventi legati a salute e welfare'.\n"
        "- Usa termini generici come 'eventi', 'registrazioni', 'casi' se non hai dettagli specifici sull'azione.\n\n"
        "Esempio Input: 'FATTO: 120 eventi tema economy_trade_industry'\n"
        "Esempio Domanda: 'Quanti eventi relativi a economia, commercio e industria sono stati registrati?' (CORRETTO)\n"
        "Esempio Domanda: 'Quanti crolli di borsa ci sono stati?' (ERRATO - Allucinazione specifica)\n"
    )

    while already_have(items, "SQL") < TARGET_SQL:
        mode = "THEME" if random.random() > 0.4 else "LOCATION"
        
        try:
            if mode == "LOCATION":
                q = f"""
                    SELECT actiongeo_fullname, count(DISTINCT globaleventid) 
                    FROM event 
                    WHERE day >= '2025-01-01' 
                    GROUP BY 1 HAVING count(DISTINCT globaleventid) BETWEEN 5 AND 500 
                    ORDER BY random() LIMIT 1
                """
                with conn.cursor() as cur:
                    cur.execute(q)
                    row = cur.fetchone()
                
                if not row: continue
                loc, count = row
                
                evidence = f"DB Result: {count} unique events occurred in '{loc}' since 2025."
                
                human_prompt = (
                    f"FATTO: Ci sono stati esattamente {count} eventi unici nel luogo '{loc}' dal 01/01/2025.\n"
                    f"TASK: Genera coppia Domanda/Risposta in {random.choice(TARGET_LANGS)}."
                )

            else: # THEME JOIN
                q_theme = f"""
                    SELECT unnest(string_to_array({PG_THEME_COL}, ';')) as theme 
                    FROM article a JOIN mention m ON a.docidentifier_normalized = m.mentionidentifier_normalized
                    JOIN event e ON m.globaleventid = e.globaleventid
                    WHERE e.day >= '2025-01-01'
                    GROUP BY 1 HAVING count(*) > 10
                    ORDER BY random() LIMIT 1
                """
                with conn.cursor() as cur:
                    cur.execute(q_theme)
                    res = cur.fetchone()
                if not res: continue
                theme = res[0].strip()
                if len(theme) < 4: continue

                q_count = f"""
                    SELECT count(DISTINCT e.globaleventid)
                    FROM event e
                    JOIN mention m ON e.globaleventid = m.globaleventid
                    JOIN article a ON m.mentionidentifier_normalized = a.docidentifier_normalized
                    WHERE e.day >= '2025-01-01' AND a.{PG_THEME_COL} ILIKE %s
                """
                with conn.cursor() as cur:
                    cur.execute(q_count, (f"%{theme}%",))
                    real_count = cur.fetchone()[0]
                
                if real_count == 0: continue
                evidence = f"DB Join Result: {real_count} events linked to theme '{theme}'."
                
                # --- FIX CRITICO: USIAMO LE VARIABILI GIUSTE QUI ---
                human_prompt = (
                    f"FATTO: Ci sono esattamente {real_count} eventi collegati al tema '{theme}'.\n"
                    f"TASK: Genera coppia Domanda/Risposta in {random.choice(TARGET_LANGS)}."
                )

            qa_pair = call_llm(sql_instructions, human_prompt)
            if not qa_pair or "SKIP" in qa_pair.question: continue

            items.append({
                "id": f"sql_{len(items)+1}",
                "category": "SQL",
                "expected_db": "sql_db",
                "question": qa_pair.question,       
                "ground_truth": qa_pair.answer,     
                "gold_contexts": [evidence]
            })
            print(f"  [SQL] {qa_pair.question[:50]}... -> {qa_pair.answer[:30]}...")
            
            if len(items) % CHECKPOINT_EVERY == 0: save_checkpoint(items)

        except Exception as e:
            print(f"⚠️ SQL Gen Error: {e}"); conn.rollback(); time.sleep(1)
    conn.close()

def generate_graph_items(items):
    current = already_have(items, "GRAPH")
    if current >= TARGET_GRAPH:
        print("✅ GRAPH completato.")
        return

    print(f"🕸️ GRAPH: Genero {TARGET_GRAPH - current} items (Relationship Prediction)...")
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    except:
        print("❌ Neo4j non raggiungibile"); return

    # --- BLACKLIST BASATA SUI TUOI LOG (Cruciale!) ---
    # Rimuoviamo stopwords, errori NER noti (tacchini, anatomia) e parole comuni.
    BAD_NODES = [
        # Errori GDELT specifici
        "meleagris", "anatomical structure", "centrism", "timeline", 
        "geographic region", "suede", "fad", "roe", "egg",
        # Parole comuni / Stopwords
        "state", "police", "city", "east", "west", "north", "south", 
        "government", "other", "group", "unknown", "date", "report",
        "president", "school", "road", "office", "military", "law",
        "university", "territory", "september", "october", "november", "december",
        "village", "district", "trade union", "committee", "part",
        # I nostri sospettati originali
        "birthday", "fine", "usage"
    ]

    while already_have(items, "GRAPH") < TARGET_GRAPH:
        try:
            # QUERY CYPHER "CHIRURGICA":
            # 1. Lunghezza > 4 (Evita 'fad', 'sea', 'sun', 'use')
            # 2. Esclude la Blacklist (case-insensitive)
            # 3. Esclude relazioni banali
            cypher = """
            MATCH (s:Entity)-[r]->(t:Entity) 
            WHERE size(s.name) > 4 AND size(t.name) > 4
              AND NOT toLower(s.name) IN $bad_nodes
              AND NOT toLower(t.name) IN $bad_nodes
              AND NOT type(r) IN ['MENTIONED', 'LINKED', 'RELATED_TO'] 
            RETURN s.name as s, type(r) as r, t.name as t 
            ORDER BY rand() LIMIT 1
            """
            
            with driver.session() as sess: 
                res = sess.run(cypher, bad_nodes=BAD_NODES).single()
            
            if not res: 
                # Se non trova nulla (raro ora), aspetta e riprova
                print("⚠️ Nessun nodo valido trovato nel campionamento, riprovo..."); time.sleep(0.5); continue
            
            s, r, t = res['s'], res['r'], res['t']
            
            # Filtri Python extra di sicurezza (URL, caratteri strani)
            if "http" in s or "http" in t or "//" in s or "@" in s: continue

            evidence = f"Graph Relation: {s} --[{r}]--> {t}"
            
            # --- PROMPT BLINDATO ---
            # Impediamo all'LLM di chiedere "Perché/Quando".
            # Deve chiedere solo "Che relazione c'è".
            graph_instructions = (
                "Sei un generatore di test per un Knowledge Graph.\n"
                "Il tuo compito è creare una domanda semplice sulla relazione tra due entità.\n\n"
                "REGOLA 1: Chiedi SOLO 'Qual è la relazione?' o 'Come interagiscono?'.\n"
                "REGOLA 2: NON chiedere 'Perché', 'Quando', 'Dove' o 'In che modo specifico'. Il grafo non ha questi dati.\n"
                "REGOLA 3: Usa i nomi esatti delle entità: " + f"'{s}' e '{t}'.\n"
                "Esempio Input: 'FATTO: Biden --[CRITICIZES]--> Putin'\n"
                "Esempio Domanda: 'Quale relazione esiste tra Biden e Putin secondo i dati?' (CORRETTA)\n"
                "Esempio Domanda: 'Perché Biden ha criticato Putin?' (ERRATA - Allucinazione)\n"
            )
            
            human_prompt = (
                f"FATTO GRAFO: {s} --[{r}]--> {t}\n"
                f"TASK: Genera coppia Domanda/Risposta in {random.choice(TARGET_LANGS)}."
            )
            
            qa_pair = call_llm(graph_instructions, human_prompt)
            if not qa_pair or "SKIP" in qa_pair.question: continue

            items.append({
                "id": f"graph_{len(items)+1}",
                "category": "GRAPH",
                "expected_db": "graph_db",
                "question": qa_pair.question,
                "ground_truth": qa_pair.answer,
                "gold_contexts": [evidence]
            })
            print(f"  [GRAPH] {qa_pair.question[:50]}... -> {qa_pair.answer[:30]}...")
            
            if len(items) % CHECKPOINT_EVERY == 0: save_checkpoint(items)
            
        except Exception as e:
            print(f"⚠️ Neo4j Error: {e}"); time.sleep(2)
    driver.close()

def generate_vector_items(items):
    current = already_have(items, "VECTOR")
    if current >= TARGET_VECTOR:
        print("✅ VECTOR completato.")
        return

    print(f"📚 VECTOR: Genero {TARGET_VECTOR - current} items (Light Mode)...")
    
    # Setup connessione più robusto
    try:
        es = Elasticsearch(
            ES_HOST, 
            request_timeout=30,
            max_retries=3,
            retry_on_timeout=True
        )
        if not es.ping():
            print("❌ Elastic non risponde al ping (Riavvia Docker!)."); return
    except Exception as e:
        print(f"❌ Errore Init Elastic: {e}"); return

    # Otteniamo il conteggio totale dei documenti per calcolare l'offset
    try:
        count_res = es.count(index=ES_INDEX)
        total_docs = count_res['count']
        print(f"📊 Totale documenti in Elastic: {total_docs}")
    except:
        total_docs = 1000 # Fallback

    consecutive_skips = 0

    while already_have(items, "VECTOR") < TARGET_VECTOR:
        try:
            # --- FIX QUERY: Strategia Offset Casuale (CPU Friendly) ---
            # Invece di ordinare tutto il DB (pesante), saltiamo a un punto a caso.
            max_offset = max(0, total_docs - 1)
            random_offset = random.randint(0, min(10000, max_offset)) # Limitiamo a 10k per performance
            
            resp = es.search(
                index=ES_INDEX, 
                body={
                    "query": {"match_all": {}}, # Query leggerissima
                    "from": random_offset,
                    "size": 1 
                }
            )
            
            if not resp['hits']['hits']: continue

            hit = resp['hits']['hits'][0]['_source']
            text = hit.get(ES_TEXT_FIELD, "")
            
            # --- FILTRI QUALITÀ ---
            if len(text) < 300: continue # Ignora testi troppo brevi
            
            # Estrazione "fatto chiave" centrale
            mid = len(text) // 2
            start = text.find(" ", mid) + 1
            end = text.find(".", start + 150)
            
            if start == 0 or end == -1: 
                fact_span = text[:200]
            else:
                fact_span = text[start:end].strip()

            if len(fact_span) < 20: continue

            # --- GENERAZIONE LLM ---
            vector_instructions = (
                "Sei un giornalista. Scrivi una domanda NATURALE basata sul 'FATTO CHIAVE'.\n"
                "La risposta deve essere contenuta nel testo.\n"
                "🚫 NO domande meta ('Cosa dice il testo?', 'Quali sono le prime parole?').\n"
                "✅ SI domande fattuali ('Perché X ha fatto Y?', 'Chi ha vinto Z?')."
            )

            human_prompt = (
                f"TESTO: \"{text[:2000]}...\"\n"
                f"FATTO CHIAVE: \"{fact_span}\"\n"
                f"TASK: Genera domanda e risposta in {random.choice(TARGET_LANGS)}."
            )
            
            qa_pair = call_llm(vector_instructions, human_prompt)

            if not qa_pair: continue 

            # Controllo anti-spazzatura
            bad_patterns = ["frase", "parole", "stringa", "phrase", "words", "text", "testo", "mention", "riporta", "cite"]
            is_bad = any(x in qa_pair.question.lower() for x in bad_patterns)

            if "SKIP" in qa_pair.question or is_bad:
                consecutive_skips += 1
                if consecutive_skips % 5 == 0:
                    print(f"  ⏭️ Skipped unnatural... ({consecutive_skips})")
                continue 
            
            consecutive_skips = 0

            items.append({
                "id": f"vec_{len(items)+1}",
                "category": "VECTOR",
                "expected_db": "vector_db",
                "question": qa_pair.question,
                "ground_truth": qa_pair.answer,
                "gold_contexts": [text]
            })
            print(f"  [VECTOR] {qa_pair.question[:60]}...")
            
            if len(items) % CHECKPOINT_EVERY == 0: save_checkpoint(items)

        except Exception as e:
            print(f"⚠️ Errore Loop Elastic: {e}")
            time.sleep(2)

if __name__ == "__main__":
    dataset = load_checkpoint()
    print(f"🔄 Checkpoint: {len(dataset)} items.")
    generate_sql_items(dataset)
    generate_graph_items(dataset)
    generate_vector_items(dataset)
    save_checkpoint(dataset)
    print("✨ Dataset Generato.")