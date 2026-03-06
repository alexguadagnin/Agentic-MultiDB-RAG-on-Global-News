from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

# --- RICHIESTA DELL'UTENTE ---
class QueryRequest(BaseModel):
    question: str = Field(..., description="La domanda in linguaggio naturale", example="Quali tensioni ci sono state tra Francia e Germania nel 2023?")
    chat_history: Optional[List[Dict[str, str]]] = Field(default=[], description="Storia della conversazione opzionale")

# --- RISPOSTA DELL'AGENTE ---
class AgentResponse(BaseModel):
    answer: str = Field(..., description="La risposta finale generata")
    sources: List[str] = Field(default=[], description="Fonti utilizzate (es. SQL, Articoli)")
    reasoning_trace: List[str] = Field(default=[], description="I passaggi logici eseguiti dall'agente (per debug/tesi)")
    execution_time: float = Field(..., description="Tempo di esecuzione in secondi")
    # Campo per RAGAS
    retrieved_contexts: List[str] = Field(default=[], description="Contesti reali (Text/Graph/SQL) passati all'LLM per la valutazione RAGAS")
