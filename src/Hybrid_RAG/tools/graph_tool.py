import os
from typing import List, Dict, Any
from langchain_community.graphs import Neo4jGraph
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate

class GDELTGraphTool:
    def __init__(self):
        """
        url = os.getenv("NEO4J_URI", "bolt://neo4j-db:7687")
        username = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "strong_password_neo4j")
        """

        # ***** TEST *****
        # --- LOGICA DINAMICA PER I TEST ---
        test_size = os.getenv("GDELT_TEST_SIZE", "").lower()
        
        if test_size == "full":
            print(f"🧪 GRAPH TOOL: Using PRODUCTION Neo4j (Port 7687)")
            url = "bolt://localhost:7687"
            username = os.getenv("NEO4J_USER", "neo4j")
            password = os.getenv("NEO4J_PASSWORD", "strong_password_neo4j") # Password di PROD
        elif test_size:
            print(f"🧪 GRAPH TOOL: Switching to TEST CONTAINER (Port 7688)")
            url = "bolt://localhost:7688"
            username = "neo4j"
            password = "strong_password_test"
        else:
            url = os.getenv("NEO4J_URI", "bolt://localhost:7687")
            username = os.getenv("NEO4J_USER", "neo4j")
            password = os.getenv("NEO4J_PASSWORD", "strong_password_neo4j")
        # ***** FINE TEST *****

        try:
            self.graph = Neo4jGraph(
                url=url, 
                username=username, 
                password=password
            )
            self.graph.refresh_schema()
            print(f"✅ Neo4j Connected to {url}")
        except Exception as e:
            print(f"❌ Neo4j Error: {e}")
            self.graph = None

        # Usiamo GPT-4o per scrivere Cypher di alta qualità
        self.llm = ChatOpenAI(temperature=1, model="gpt-5-mini") 

    def run_query(self, question: str) -> Dict[str, Any]:
        """
        Esegue la query Cypher in modo ROBUSTO (Parsing manuale + Esecuzione Sequenziale).
        """
        if not self.graph:
            return {"error": "Neo4j not connected"}

        # 1. Definizione Prompt (AGGIORNATO CON REGOLA SU EXISTS)
        cypher_generation_template = """
        Sei un esperto Neo4j.
        
        SCHEMA:
        - Nodi: (n:Entity)
        - Relazioni: [:CRITICIZES], [:SUPPORTS], [:ATTACKS], [:MEETS_WITH], [:PROVIDES_AID_TO]
        
        REGOLE TASSATIVE:
        1. Genera SOLO query Cypher valide. Niente markdown, niente spiegazioni.
        2. Usa SEMPRE `WHERE toLower(n.name) CONTAINS toLower('parte_nome')` per la ricerca case-insensitive.
        3. Se servono più query, separale con un punto e virgola (;).
        4. LIMITA i risultati a 50.
        5. EVITA percorsi variabili lunghi (*1..3). Usa relazioni dirette.
        6. ⚠️ SINTASSI DEPRECATA: NON usare MAI `exists(n.prop)`. Usa SEMPRE `n.prop IS NOT NULL`.
        
        Domanda: {question}
        Query Cypher:"""
        
        prompt = PromptTemplate(
            template=cypher_generation_template,
            input_variables=["question"]
        )

        # 2. Generazione Query (Solo testo, senza esecuzione)
        chain = prompt | self.llm
        try:
            response = chain.invoke({"question": question})
            generated_text = response.content
            print(f"Generated Cypher:\n{generated_text}") # Debug
        except Exception as e:
            return {"error": f"LLM Generation Error: {e}"}

        # 3. Parsing e Pulizia (Il segreto della robustezza!)
        # Rimuoviamo markdown ```cypher ... ```
        cleaned_text = generated_text.replace("```cypher", "").replace("```", "")
        
        # Splittiamo le query per punto e virgola
        queries = [q.strip() for q in cleaned_text.split(";") if q.strip()]
        
        final_results = []
        chunk_ids = set()

        # 4. Esecuzione Manuale Sequenziale
        for query in queries:
            # Controllo base di sicurezza
            if not any(k in query.upper() for k in ["MATCH", "RETURN", "CALL"]):
                continue
                
            try:
                # Usiamo il metodo .query() esposto da Neo4jGraph di LangChain
                data = self.graph.query(query)
                
                if data:
                    final_results.extend(data)
                    # Estraiamo i chunk_ids se presenti
                    for record in data:
                        # Gestione flessibile: chunk_ids può essere nel record o dentro una relazione
                        for val in record.values():
                            if isinstance(val, dict) and 'chunk_ids' in val:
                                chunk_ids.update(val['chunk_ids'])
                            elif isinstance(val, list): # Lista di chunk_ids diretta
                                chunk_ids.update([x for x in val if isinstance(x, str) and len(x) > 5])
                                
            except Exception as e:
                print(f"⚠️ Errore parziale su query: {query[:50]}... -> {e}")
                continue # Continua con la prossima query!

        # 5. Formattazione Risultato
        if not final_results:
            return {"data": "Nessuna relazione trovata nel grafo.", "source": "neo4j"}
            
        # Convertiamo i risultati in stringa leggibile per l'LLM finale
        # e passiamo i chunk_ids al sistema RAG
        return {
            "data": str(final_results[:50]), # Limitiamo per non intasare il prompt
            "chunk_ids": list(chunk_ids),
            "source": "neo4j"
        }