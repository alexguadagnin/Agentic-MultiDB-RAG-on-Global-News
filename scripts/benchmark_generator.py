import os
import json
import time
import random
import psycopg2
from elasticsearch import Elasticsearch
from neo4j import GraphDatabase
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field

# --- CONFIGURAZIONE ---
TARGET_PER_CATEGORY = 5 # 5 SQL + 5 Graph + 5 Vector = 15 Domande per Subset
GEN_MODEL_NAME = "gpt-4o-mini" # Veloce ed economico per generare dataset

class QAPair(BaseModel):
    question: str = Field(description="La domanda naturale.")
    answer: str = Field(description="La risposta ideale.")

class BenchmarkGenerator:
    def __init__(self, subset_size):
        self.size = subset_size.lower()
        
        # --- CONFIGURAZIONE DINAMICA (Subset vs Full) ---
        if self.size == "full":
            # CASO FULL: Usa le configurazioni di PRODUZIONE
            print("📝 Generator: Configurato per FULL DATASET (Prod)")
            self.db_name = os.getenv('POSTGRES_DB', 'gdelt_rag_db')
            self.es_index = "news_chunks"
            self.neo4j_uri = "bolt://localhost:7687" # Porta Produzione
            self.neo4j_auth = (
                os.getenv("NEO4J_USER", "neo4j"), 
                os.getenv("NEO4J_PASSWORD", "strong_password_neo4j")
            )
        else:
            # CASO SUBSET: Usa le configurazioni di TEST
            # print(f"📝 Generator: Configurato per SUBSET '{self.size.upper()}'")
            self.db_name = f"gdelt_{self.size}"
            self.es_index = f"news_chunks_{self.size}"
            self.neo4j_uri = "bolt://localhost:7688" # Porta Test
            self.neo4j_auth = ("neo4j", "strong_password_test")

        # 1. Stringa di Connessione SQL
        self.pg_conn_str = (
            f"dbname={self.db_name} "
            f"user={os.getenv('POSTGRES_USER', 'gdelt_admin')} "
            f"password={os.getenv('POSTGRES_PASSWORD', 'strong_password_123')} "
            f"host=localhost port=5432"
        )

        # 2. Connessione Elastic
        self.es = Elasticsearch("http://localhost:9200", request_timeout=30)

        # 3. LLM per generare le domande
        self.llm = ChatOpenAI(model=GEN_MODEL_NAME, temperature=0.7)

    def generate_dataset(self, output_path):
        print(f"\n🧩 GENERAZIONE GOLDEN DATASET PER: {self.size.upper()}")
        items = []
        
        # Generazione sequenziale
        self._gen_sql(items)
        self._gen_graph(items)
        self._gen_vector(items)
        
        # Salvataggio
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        print(f"✅ Dataset salvato in {output_path} ({len(items)} domande)")
        return items

    def _call_llm(self, prompt_text):
        try:
            structured_llm = self.llm.with_structured_output(QAPair)
            return structured_llm.invoke(prompt_text)
        except Exception as e:
            print(f"⚠️ LLM Error: {e}")
            return None

    # --- GENERATORI SPECIFICI ---
    
    def _gen_sql(self, items):
        print(f"   📊 Generazione SQL ({TARGET_PER_CATEGORY})...")
        try:
            conn = psycopg2.connect(self.pg_conn_str)
            cur = conn.cursor()
            # Prendiamo eventi reali da QUESTO subset
            cur.execute("SELECT actiongeo_fullname, count(*) FROM event GROUP BY 1 HAVING count(*) > 2 ORDER BY random() LIMIT %s", (TARGET_PER_CATEGORY,))
            rows = cur.fetchall()
            
            for loc, count in rows:
                prompt = f"FATTO: Ci sono {count} eventi registrati a '{loc}'. Genera una domanda 'Quanti...' e la risposta."
                qa = self._call_llm(prompt)
                if qa:
                    items.append({
                        "id": f"{self.size}_sql_{len(items)}", "category": "SQL",
                        "question": qa.question, "ground_truth": qa.answer
                    })
            conn.close()
        except Exception as e:
            print(f"❌ SQL Gen Error: {e}")

    def _gen_graph(self, items):
        print(f"   🕸️ Generazione GRAPH ({TARGET_PER_CATEGORY})...")
        try:
            driver = GraphDatabase.driver(self.neo4j_uri, auth=self.neo4j_auth)
            with driver.session() as sess:
                # Query semplificata per trovare relazioni esistenti
                res = sess.run(f"""
                    MATCH (s:Entity)-[r]->(t:Entity) 
                    WHERE size(s.name) > 3 AND size(t.name) > 3
                    RETURN s.name as s, type(r) as r, t.name as t 
                    ORDER BY rand() LIMIT {TARGET_PER_CATEGORY}
                """).data()
                
                for row in res:
                    prompt = f"FATTO: {row['s']} ha relazione {row['r']} con {row['t']}. Genera domanda sulla relazione."
                    qa = self._call_llm(prompt)
                    if qa:
                        items.append({
                            "id": f"{self.size}_graph_{len(items)}", "category": "GRAPH",
                            "question": qa.question, "ground_truth": qa.answer
                        })
            driver.close()
        except Exception as e:
             print(f"❌ Graph Gen Error: {e}")

    def _gen_vector(self, items):
        print(f"   📚 Generazione VECTOR ({TARGET_PER_CATEGORY})...")
        try:
            # Prendi chunk a caso dall'indice specifico
            res = self.es.search(index=self.es_index, body={"query": {"match_all": {}}, "size": TARGET_PER_CATEGORY, "sort": [{"_script": {"type": "number", "script": "Math.random()", "order": "asc"}}]})
            
            for hit in res['hits']['hits']:
                text = hit['_source'].get('chunk_text', '')[:500]
                if len(text) < 50: continue
                
                prompt = f"TESTO: {text}... Genera una domanda fattuale la cui risposta è nel testo."
                qa = self._call_llm(prompt)
                if qa:
                    items.append({
                        "id": f"{self.size}_vec_{len(items)}", "category": "VECTOR",
                        "question": qa.question, "ground_truth": qa.answer
                    })
        except Exception as e:
             print(f"❌ Vector Gen Error: {e}")