import time
import logging
from fastapi import FastAPI, HTTPException
from Hybrid_RAG.api.schemas import QueryRequest, AgentResponse
from Hybrid_RAG.rag_core.graph import app_graph

# Configurazione Logging API
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("API")

app = FastAPI(
    title="GDELT Agentic RAG",
    description="Backend per Tesi Magistrale - Hybrid Retrieval System with Self-Correction",
    version="1.0.0"
)

@app.get("/")
async def root():
    return {"status": "System Online", "module": "Agent Core v1.0"}

@app.post("/query", response_model=AgentResponse)
async def run_agent_query(request: QueryRequest):
    """
    Endpoint principale: riceve la domanda, attiva il grafo LangGraph e ritorna la risposta.
    """
    start_time = time.time()
    logger.info(f"🚀 New Request: {request.question}")
    
    try:
        # 1. INIZIALIZZAZIONE DELLO STATO
        # È fondamentale settare 'retry_count' a 0 qui per far partire il loop correttamente.
        initial_state = {
            "question": request.question,
            "retry_count": 0,          
            "steps_log": [],           
            
            # Inizializziamo a vuoto/None per evitare KeyError se qualche nodo legge prima di scrivere
            "target_databases": [],
            "sql_data": None,
            "vector_data": [],
            "graph_data": [],
            "is_answerable": None,
            "grade_reasoning": None,

            "retrieved_contexts": []
        }
        
        # 2. ESECUZIONE DEL GRAFO (The Brain)
        # .invoke() esegue tutto il flusso (Router -> Tools -> Grader -> [Loop?] -> Generator)
        final_state = app_graph.invoke(initial_state)
        
        # 3. CALCOLO METRICHE
        elapsed = time.time() - start_time
        
        # Estrazione dati per la risposta HTTP
        answer = final_state.get("final_answer", "Si è verificato un errore nella generazione della risposta.")
        sources = final_state.get("target_databases", [])
        trace = final_state.get("steps_log", [])
        
        logger.info(f"✅ Request completed in {elapsed:.2f}s")
        
        return AgentResponse(
            answer=answer,
            sources=sources,
            reasoning_trace=trace,
            execution_time=elapsed,
            retrieved_contexts=final_state.get("retrieved_contexts", [])
        )
        
    except Exception as e:
        logger.error(f"❌ API CRITICAL ERROR: {e}")
        # In produzione non si mandano gli stack trace, ma per la tesi è utile vedere l'errore
        raise HTTPException(status_code=500, detail=str(e))