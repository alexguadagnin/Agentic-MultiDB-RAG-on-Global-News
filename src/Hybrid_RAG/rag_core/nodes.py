import logging
import json
from typing import List, Dict, Any, Union

from Hybrid_RAG.rag_core.state import AgentState
from Hybrid_RAG.rag_core.router import SemanticRouter
from Hybrid_RAG.tools.sql_tool import GDELTSQLTool
from Hybrid_RAG.tools.graph_tool import GDELTGraphTool
from Hybrid_RAG.tools.vector_tool import HybridRetrievalTool
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field

# --- CONFIGURAZIONE LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RAG_NODE")

# --- INIZIALIZZAZIONE TOOLS ---
"""
try:
    router_engine = SemanticRouter()
    sql_tool = GDELTSQLTool()
    graph_tool = GDELTGraphTool()
    vector_tool = HybridRetrievalTool()
    llm_generator = ChatOpenAI(model="gpt-5-mini", temperature=1)
    logger.info("✅ Tools initialized successfully inside nodes.py")
except Exception as e:
    logger.error(f"❌ Error initializing tools: {e}")
    raise e
"""
# Variabili Globali (inizializzate a None)
router_engine = None
sql_tool = None
graph_tool = None
vector_tool = None
llm_generator = None

def initialize_tools(force_reload=False):
    """
    Inizializza o Ricarica i tool. 
    Fondamentale per lo switch tra dataset (XS, S, M...) durante i test.
    """
    global router_engine, sql_tool, graph_tool, vector_tool, llm_generator
    
    # Se sono già caricati e non forziamo il reload, usciamo
    if sql_tool and not force_reload:
        return

    logger.info("🔄 (Re)Initializing RAG Tools...")
    try:
        router_engine = SemanticRouter()
        sql_tool = GDELTSQLTool()        # Leggerà GDELT_TEST_SIZE
        graph_tool = GDELTGraphTool()      # Leggerà GDELT_TEST_SIZE
        vector_tool = HybridRetrievalTool() # Leggerà GDELT_TEST_SIZE
        llm_generator = ChatOpenAI(model="gpt-5-mini", temperature=1)
        logger.info("✅ Tools ready.")
    except Exception as e:
        logger.error(f"❌ Error initializing tools: {e}")
        raise e

# Chiamata iniziale (per quando lanci l'app normalmente)
initialize_tools()


# --- 1. NODO ROUTER ---
def router_node(state: AgentState):
    # Assicuriamoci che i tool siano pronti
    if not router_engine: initialize_tools()
    """Analizza la domanda e decide quali database interrogare."""
    question = state["question"]
    logger.info(f"🚦 ROUTING QUERY: '{question}'")
    
    try:
        decision = router_engine.route_question(question)
        targets = decision.datasources
        reasoning = decision.reasoning
    except Exception as e:
        logger.error(f"⚠️ Router Error: {e}. Fallback to Vector DB.")
        targets = ["vector_db"]
        reasoning = f"Router Failed: {str(e)}"

    log_msg = f"📍 Router Decision: {targets} | Reasoning: {reasoning}"
    logger.info(log_msg)

    return {
        "target_databases": targets,
        "steps_log": [log_msg]
    }

# --- 2. NODO SQL RETRIEVAL ---
def sql_retrieval_node(state: AgentState):
    """Esegue la query su PostgreSQL."""
    question = state["question"]
    logger.info("🛠️ EXEC SQL TOOL")
    
    try:
        response = sql_tool.run_query(question)
        if isinstance(response, dict):
            if response.get("status") == "error":
                data = f"SQL Execution Error: {response.get('error')}"
                query_used = "Unknown"
            else:
                query_used = response.get("generated_query", "Unknown Query")
                raw_result = response.get("result", "")
                data = str(raw_result)
        else:
            data = str(response)
            query_used = "Query not returned by tool"

        short_data = (data[:100] + '...') if len(data) > 100 else data
        log_msg = f"🗄️ SQL Query: `{query_used}` | Result: {short_data}"
        logger.info(log_msg)

    except Exception as e:
        data = f"SQL Node Crash: {str(e)}"
        log_msg = f"❌ SQL Node Failed: {str(e)}"
        logger.error(log_msg)

    return {
        "sql_data": data,
        "steps_log": [log_msg]
    }

# --- 3. NODO GRAPH RETRIEVAL ---
def graph_retrieval_node(state: AgentState):
    """Interroga Neo4j e recupera le prove testuali (Bridge)."""
    question = state["question"]
    logger.info("🕸️ EXEC GRAPH TOOL")
    
    try:
        graph_response = graph_tool.run_query(question)
        
        # 1. Recupero Dati Grezzi (Stringa o Lista)
        raw_data = graph_response.get("data", [])
        
        # 2. Recupero Chunk IDs (Ora il tool ce li dà già pronti!)
        all_chunk_ids = graph_response.get("chunk_ids", []) 
        
        if not raw_data or raw_data == "Nessuna relazione trovata nel grafo.":
            logger.info("🕸️ Graph returned no data.")
            return {
                "graph_data": ["Nessuna relazione trovata nel grafo."],
                "steps_log": ["🕸️ Graph: No data found."]
            }

        # Formattazione Dati per l'LLM
        formatted_info = [f"Graph Findings: {raw_data}"]

        # 3. Bridge verso il Testo (Se abbiamo ID)
        evidence_texts = []
        if all_chunk_ids:
            logger.info(f"🌉 Graph Bridge: Fetching text for {len(all_chunk_ids)} chunks...")
            # De-duplicazione
            unique_ids = list(set(all_chunk_ids))
            evidence_texts = vector_tool.get_chunks_by_ids(unique_ids)
        
        log_msg = f"🕸️ Graph found relations & bridged to {len(evidence_texts)} text chunks."
        logger.info(log_msg)
        
        final_graph_data = formatted_info + ["--- GRAPH EVIDENCE ---"] + evidence_texts

        return {
            "graph_data": final_graph_data,
            "steps_log": [log_msg]
        }

    except Exception as e:
        logger.error(f"❌ Graph Node Crash: {e}")
        return {
            "graph_data": [f"Error querying graph: {e}"],
            "steps_log": [f"❌ Graph Error: {e}"]
        }

# --- 4. NODO VECTOR RETRIEVAL ---
def vector_retrieval_node(state: AgentState):
    """Esegue la ricerca ibrida con Query Expansion (IT + EN)."""
    question = state["question"]
    logger.info("📚 EXEC VECTOR TOOL (Multi-Query)")
    
    # 1. TRADUZIONE (Cruciale per trovare il doc spagnolo)
    try:
        translation_llm = ChatOpenAI(model="gpt-5-mini", temperature=1)
        translated_query = translation_llm.invoke(
            f"Translate into English: {question}"
        ).content
        logger.info(f"🌍 Translated: {translated_query}")
    except:
        translated_query = question

    # 2. RICERCA DOPPIA
    results_it = vector_tool.run_hybrid_search(question, limit=10)
    
    if translated_query != question:
        results_en = vector_tool.run_hybrid_search(translated_query, limit=10)
    else:
        results_en = {"results": []}

    # 3. MERGE
    combined = list(set(
        results_it.get("results", []) + 
        results_en.get("results", [])
    ))
    
    debug_info = f"Merged IT+EN: {len(combined)} docs"
    logger.info(f"📚 {debug_info}")

    return {
        "vector_data": combined,
        "steps_log": [f"📚 Vector: {debug_info}"]
    }

# --- 5. NODO GRADER ---
class GradeResult(BaseModel):
    is_relevant: bool = Field(description="True se i dati trovati rispondono alla domanda, False altrimenti.")
    explanation: str = Field(description="Spiegazione breve del perché i dati sono sufficienti o meno.")

def grader_node(state: AgentState):
    """Valuta la qualità del recupero. Supporta Multilingua."""
    logger.info("⚖️ GRADING RETRIEVAL QUALITY")
    question = state["question"]
    
    sql = str(state.get('sql_data') or "No SQL data")
    graph = str(state.get('graph_data') or "No Graph data")
    
    raw_vector = state.get('vector_data') or []
    if isinstance(raw_vector, list):
        vector_str = "\n--- DOC SEPARATOR ---\n".join([str(x) for x in raw_vector])
    else:
        vector_str = str(raw_vector)
    
    # Context massivo (25k caratteri) per leggere tutti i 14 documenti
    context_str = f"""
    SQL DATA: {sql[:1000]}
    GRAPH DATA: {graph[:2000]}
    VECTOR DATA (Full Context): 
    {vector_str[:25000]} 
    """
    
    preview_len = len(vector_str)
    mid_point = preview_len // 2
    logger.info(f"🧐 GRADER INPUT SIZE: {preview_len} chars. Middle Preview: ...{vector_str[mid_point:mid_point+100]}...")
    
    grader_llm = ChatOpenAI(model="gpt-5-mini", temperature=1)
    structured_grader = grader_llm.with_structured_output(GradeResult)
    
    # Prompt Multilingua Sicuro (senza f-string per i dati)
    prompt = ChatPromptTemplate.from_messages([
        ("system", """Sei un valutatore esperto e MULTILINGUA. 
        Analizza se i documenti contengono informazioni utili per rispondere alla domanda.
        
        IMPORTANTE:
        1. I documenti possono essere in INGLESE, SPAGNOLO o altre lingue. Traduci mentalmente.
        2. Cerca concetti chiave pertinenti. Se trovi anche una sola frase utile, rispondi TRUE.
        """),
        ("human", "DOMANDA: {question}\n\nCONTESTO RECUPERATO:\n{context}")
    ])
    
    chain = prompt | structured_grader
    
    try:
        grade_result = chain.invoke({"question": question, "context": context_str})
        
        if not grade_result:
            logger.warning("⚠️ Grader returned empty response. Defaulting to True.")
            score = True
            reason = "Grader execution returned None."
        else:
            score = grade_result.is_relevant
            reason = grade_result.explanation
            
    except Exception as e:
        logger.error(f"⚠️ Grader Crash: {e}. Defaulting to True.")
        score = True 
        reason = f"Grader Logic Failed: {str(e)}"

    log_msg = f"⚖️ Grader Decision: {score} | Reason: {reason}"
    logger.info(log_msg)

    return {
        "is_answerable": score,
        "grade_reasoning": reason,
        "steps_log": [log_msg]
    }

# --- 6. NODO GENERATOR ---
def generator_node(state: AgentState):
    """Genera la risposta finale usando il suggerimento del Grader."""
    logger.info("✍️ GENERATING FINAL ANSWER (Direct Style)")
    question = state["question"]
    
    sql_data = str(state.get("sql_data", "N/A"))
    grader_hint = state.get("grade_reasoning", "Usa i dati forniti per rispondere.")
    
    raw_graph = state.get("graph_data", [])
    graph_str = "\n".join([str(x) for x in raw_graph]) if isinstance(raw_graph, list) else str(raw_graph)
        
    raw_vector = state.get("vector_data", [])
    # Pulizia: se è una lista di Document oggetti, estraiamo page_content, altrimenti stringa
    vector_list = []
    if isinstance(raw_vector, list):
        for doc in raw_vector:
            if hasattr(doc, 'page_content'):
                vector_list.append(doc.page_content)
            else:
                vector_list.append(str(doc))
    else:
        vector_list.append(str(raw_vector))
        
    vector_str = "\n--- DOC ---\n".join(vector_list)

    
    prompt = ChatPromptTemplate.from_messages([
        ("system", """Sei un assistente analista esperto su dati GDELT.
        Il tuo compito è rispondere alla domanda basandoti ESCLUSIVAMENTE sui contesti forniti, ma nascondendo la complessità tecnica all'utente.

        REGOLA D'ORO - GERARCHIA DELLE FONTI:
        1. **TESTO (Vector/Documents):** Se hai dei testi che contengono la risposta, USALI! Hanno la priorità assoluta.
        2. **DATI (SQL/Graph):** Usali per confermare numeri o relazioni, ma integrali nel discorso in modo fluido.

        STILE DI RISPOSTA (CRUCIALE PER IL PUNTEGGIO):
        1. 🚫 **NIENTE META-RIFERIMENTI:** Non dire MAI "Secondo il grafo", "Il database SQL mostra", "Con uno score di 0.9". L'utente non deve sapere come hai trovato i dati, rispondi in modo diretto, in linguaggio naturale, non prolisso.
        2. 🚫 **NIENTE LISTE TECNICHE:** Non elencare tuple grezze (es. `[(50,)]`). Trasformale in linguaggio naturale (es. "Sono stati registrati 50 casi").
        3. ✅ **NATURALEZZA:** Rispondi come un analista umano. Esempio: Invece di "Relazione: SUPPORTS", scrivi "L'entità X supporta l'entità Y".
        4. ✅ **LINGUA:** Rispondi RIGOROSAMENTE nella lingua della domanda.

        GESTIONE ERRORI E CONFLITTI:
        - **NO SEO/RICERCA:** Se il contesto fornito non è sufficiente per generare una risposta rispondi semplicemente "Dati non sufficienti", NON generare MAI suggerimenti per ricerche su Google.
        - **PRIORITÀ AL CONTENUTO:** Se SQL/Graph danno errore ("Error", "No data") MA il Testo ha la risposta, IGNORA l'errore tecnico e rispondi col testo.
        - **ONESTÀ:** Solo se TUTTI i contesti sono vuoti o irrilevanti, rispondi: "Le informazioni disponibili non sono sufficienti per rispondere a questa specifica richiesta."
        """),
        ("human", """
        DOMANDA: {question}
        
        DATI SQL: {sql_data}
        DATI GRAFO: {graph_str}
        DATI TESTUALI: {vector_data}
        
        Suggerimento Grader: {grader_hint}
        """)
    ])
    
    try:
        chain = prompt | llm_generator
        response = chain.invoke({
            "question": question,
            "grader_hint": grader_hint,
            "sql_data": sql_data,
            "graph_str": graph_str,
            "vector_data": vector_str
        })
        final_answer = response.content
        logger.info("✅ Final Answer Generated.")
        
    except Exception as e:
        logger.error(f"❌ Generator Crash: {e}")
        final_answer = "Error generating answer."

    # --- MODIFICA PER RAGAS: COSTRUIAMO IL CONTESTO REALE ---
    ragas_contexts = []
    
    # 1. Aggiungiamo i testi vettoriali (Puri, niente metadata)
    # vector_list è stato creato poche righe sopra nel tuo codice
    if vector_list:
        ragas_contexts.extend(vector_list)
        
    # 2. Aggiungiamo il grafo (Se presente)
    if raw_graph and isinstance(raw_graph, list):
        # Trasformiamo le tuple/dizionari in stringhe leggibili se non lo sono già
        for item in raw_graph:
            ragas_contexts.append(str(item))
    elif raw_graph and isinstance(raw_graph, str) and "Nessuna relazione" not in raw_graph:
        ragas_contexts.append(raw_graph)

    # 3. Aggiungiamo SQL (Se presente)
    if sql_data and "SQL Execution Error" not in sql_data and sql_data != "N/A":
        ragas_contexts.append(f"SQL Result: {sql_data}")

    # Se tutto è vuoto, mettiamo un placeholder per evitare crash di Ragas
    if not ragas_contexts:
        ragas_contexts = ["No context retrieved."]

    return {
        "final_answer": final_answer,
        "retrieved_contexts": ragas_contexts, # Per Ragas
        "steps_log": ["✅ Answer Generated Successfully"]
    }

# --- 7. NODO QUERY REWRITER ---
def transform_query_node(state: AgentState):
    """
    Riscrive la query se il retrieval ha fallito.
    """
    question = state["question"]
    feedback = state.get("grade_reasoning", "Nessun dato rilevante trovato.")
    retry_count = state.get("retry_count", 0) + 1 # Incrementiamo qui
    
    logger.info(f"🔄 REWRITING QUERY (Attempt {retry_count})...")
    
    # LLM per riscrivere
    llm = ChatOpenAI(model="gpt-5-mini", temperature=1)
    
    msg = [
        ("system", """Sei un esperto di ottimizzazione per motori di ricerca.
        La ricerca precedente ha fallito. Riscrivi la domanda per renderla più efficace.
        
        Consigli:
        - Se la domanda era troppo specifica, rendila più generica.
        - Se era troppo vaga, aggiungi dettagli chiave.
        - Rimuovi riferimenti temporali troppo stretti se necessario.
        """),
        ("human", f"Query Originale: {question}\nMotivo Fallimento: {feedback}\n\nNuova Query Ottimizzata:")
    ]
    
    try:
        new_question = llm.invoke(msg).content
        logger.info(f"✨ New Query: {new_question}")
    except:
        new_question = question # Fallback
        
    return {
        "question": new_question, # Sovrascriviamo la domanda nello stato
        "retry_count": retry_count,
        "steps_log": [f"🔄 Rewrote query to: '{new_question}'"]
    }