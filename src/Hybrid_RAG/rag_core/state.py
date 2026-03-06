from typing import TypedDict, List, Annotated, Optional, Any
import operator

class AgentState(TypedDict):
    """
    Stato globale del grafo. 
    Usiamo Optional ovunque per evitare KeyError se un nodo salta.
    """
    # INPUT
    question: str
    
    # INTERNAL LOGIC
    target_databases: Optional[List[str]]
    
    # DATA RETRIEVED
    sql_data: Optional[str]
    vector_data: Optional[List[str]]
    graph_data: Optional[List[str]] # O stringa, a seconda del nodo
    
    # LOGGING (Annotated con add unisce le liste dai vari nodi)
    steps_log: Annotated[List[str], operator.add]
    
    # EVALUATION
    is_answerable: Optional[bool]
    grade_reasoning: Optional[str]
    
    # OUTPUT
    final_answer: Optional[str]

    retry_count: int

    # RAGAS
    retrieved_contexts: List[str]