from langgraph.graph import StateGraph, END
from Hybrid_RAG.rag_core.state import AgentState
from Hybrid_RAG.rag_core.nodes import (
    router_node,
    sql_retrieval_node,
    graph_retrieval_node,
    vector_retrieval_node,
    grader_node,
    generator_node,
    transform_query_node 
)

workflow = StateGraph(AgentState)

# Nodi
workflow.add_node("router", router_node)
workflow.add_node("sql_retrieval", sql_retrieval_node)
workflow.add_node("graph_retrieval", graph_retrieval_node)
workflow.add_node("vector_retrieval", vector_retrieval_node)
workflow.add_node("grader", grader_node)
workflow.add_node("generator", generator_node)
workflow.add_node("transform_query", transform_query_node)

# Entry Point
workflow.set_entry_point("router")

# Routing Logic
def route_decision(state: AgentState):
    dbs = state.get("target_databases", [])
    next_nodes = []
    if "sql_db" in dbs: next_nodes.append("sql_retrieval")
    if "graph_db" in dbs: next_nodes.append("graph_retrieval")
    if "vector_db" in dbs: next_nodes.append("vector_retrieval")
    if not next_nodes: return ["vector_retrieval"]
    return next_nodes

workflow.add_conditional_edges(
    "router",
    route_decision,
    {"sql_retrieval": "sql_retrieval", "graph_retrieval": "graph_retrieval", "vector_retrieval": "vector_retrieval"}
)

# Convergenza
workflow.add_edge("sql_retrieval", "grader")
workflow.add_edge("graph_retrieval", "grader")
workflow.add_edge("vector_retrieval", "grader")

# --- LOGICA DEL LOOP (Check Grade) ---
def check_grade(state: AgentState):
    is_ok = state.get("is_answerable", False)
    retries = state.get("retry_count", 0)
    
    # 1. Se è OK -> Vai alla fine
    if is_ok:
        return "generator"
    
    # 2. Se NON è OK ma abbiamo ancora tentativi (Max 2 retry) -> Riscrivi
    if retries < 2:
        return "transform_query"
    
    # 3. Se abbiamo finito i tentativi -> Arrenditi e vai alla fine
    return "generator"

workflow.add_conditional_edges(
    "grader",
    check_grade,
    {
        "generator": "generator",
        "transform_query": "transform_query" # <--- ARCO DI RITORNO
    }
)

# Dal trasformatore torniamo al Router (perché la nuova query potrebbe richiedere DB diversi!)
workflow.add_edge("transform_query", "router")

workflow.add_edge("generator", END)

app_graph = workflow.compile()