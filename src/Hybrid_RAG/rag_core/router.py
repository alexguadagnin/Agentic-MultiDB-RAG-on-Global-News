from typing import Literal, List
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
import os

# --- 1. DEFINIZIONE DELLA STRUTTURA DI USCITA ---
# Usiamo Pydantic per costringere l'LLM a rispondere SOLO con questo JSON.
# Questo elimina il rischio che l'LLM risponda con "Certo! Ecco cosa penso..."
class RouteQuery(BaseModel):
    """
    Modello per instradare la query dell'utente verso i database più appropriati.
    Puoi scegliere più di una destinazione se necessario.
    """
    datasources: List[Literal["sql_db", "vector_db", "graph_db"]] = Field(
        ..., 
        description="La lista dei database da interrogare per risolvere la domanda."
    )
    reasoning: str = Field(
        ...,
        description="Spiegazione breve del perché hai scelto questi datasource. Utile per il tracing."
    )

# --- 2. IL SISTEMA DI ROUTING ---
class SemanticRouter:
    def __init__(self, model_name="gpt-5-nano"): 
        # Per il routing basta un modello veloce 
        self.llm = ChatOpenAI(
            model=model_name, 
            temperature=1,
            api_key=os.getenv("OPENAI_API_KEY")
        )
        self.structured_llm = self.llm.with_structured_output(RouteQuery)

    def route_question(self, question: str) -> RouteQuery:
        """
        Analizza la domanda e restituisce le destinazioni.
        """
        
        # --- PROMPT ENGINEERING PER LA TESI ---
        # Qui definiamo le 'Expertise' di ogni DB. 
        # Più preciso sei qui, più intelligente sembrerà il tuo RAG.
        system_prompt = """
        Sei un esperto analista di dati GDELT. Il tuo compito è scegliere ESCLUSIVAMENTE i database necessari.
        
        DATABASE DISPONIBILI:
        
        1. **SQL_DB (PostgreSQL)**:
           - SOLO per domande che chiedono NUMERI, CONTEGGI, STATISTICHE o LISTE SECCHE.
           - Trigger words: "Quanti", "Numero di", "Statistiche", "Frequenza", "Elenco degli eventi".
           - NON USARE se l'utente chiede "Quali sono", "Descrivi", "Racconta", "Perché".
           
        2. **GRAPH_DB (Neo4j)**:
           - PERFETTO per RELAZIONI tra entità specifiche e CONNESSIONI.
           - Trigger words: "Chi supporta", "Chi critica", "Relazioni tra", "Connessioni", "Coinvolgimento".
           - Se la domanda cita due nazioni o leader (es. "Francia e Germania"), USA IL GRAFO.
           
        3. **VECTOR_DB (Qdrant/Elastic)**:
           - FONDAMENTALE per CONTESTO, DESCRIZIONI, MOTIVAZIONI e NARRAZIONE.
           - Trigger words: "Quali tensioni", "Motivi", "Cosa è successo", "Descrivi la situazione".
           - Se la domanda è vaga o chiede "Quali...", includi SEMPRE il Vector DB.

        REGOLA D'ORO:
        - Se l'utente chiede "Quali tensioni..." -> Usa ["graph_db", "vector_db"].
        - Se l'utente chiede "Quanti eventi..." -> Usa ["sql_db"].
        
        Sii aggressivo nel selezionare VECTOR_DB per domande descrittive.
        """

        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("human", "{question}"),
        ])

        # Creiamo la catena: Prompt -> LLM -> JSON Parser
        router_chain = prompt | self.structured_llm
        
        # Eseguiamo
        return router_chain.invoke({"question": question})

# --- ESEMPIO DI UTILIZZO (SOLO PER TEST LOCALE) ---
if __name__ == "__main__":
    # Questo blocco viene eseguito solo se lanci il file direttamente
    router = SemanticRouter()
    
    test_q = "Quante proteste ci sono state a Roma e chi le ha organizzate?"
    decision = router.route_question(test_q)
    
    print(f"Domanda: {test_q}")
    print(f"Decisione: {decision.datasources}")
    print(f"Ragionamento: {decision.reasoning}")