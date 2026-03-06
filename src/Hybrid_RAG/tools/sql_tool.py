import os
import logging
from langchain_community.utilities import SQLDatabase
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

# Configura Logger
logger = logging.getLogger(__name__)

class GDELTSQLTool:
    def __init__(self):

        # 1. Configurazione DB
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        host = os.getenv("POSTGRES_HOST", "postgres-gdelt")
        port = os.getenv("POSTGRES_PORT", "5432")
        #db_name = os.getenv("POSTGRES_DB", "gdelt_rag_db")
        
        if not user or not password:
             raise ValueError("Credenziali DB mancanti nelle variabili d'ambiente.")
        
        # ***** TEST *****
        # --- LOGICA DINAMICA PER I TEST ---
        test_size = os.getenv("GDELT_TEST_SIZE", "").lower()
        
        if test_size == "full":
            # ECCEZIONE PER IL FULL DATASET
            db_name = os.getenv("POSTGRES_DB", "gdelt_rag_db")
            print(f"🧪 SQL TOOL: Using PRODUCTION DB -> {db_name}")
        elif test_size:
            # LOGICA PER I SUBSET
            db_name = f"gdelt_{test_size}"
            print(f"🧪 SQL TOOL: Switching to TEST DB -> {db_name}")
        else:
            # Default
            db_name = os.getenv("POSTGRES_DB", "gdelt_rag_db")
        # ***** FINE TEST *****

        uri = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
        
        try:
            self.db = SQLDatabase.from_uri(
                uri,
                include_tables=['event', 'article', 'mention'], 
                sample_rows_in_table_info=3 
            )
            print(f"✅ Connessione SQL stabilita su {db_name}.")
        except Exception as e:
            logger.error(f"❌ Errore critico connessione SQL: {e}")
            raise e

        # 2. LLM
        # NOTA: Se usi modelli 'o1' o beta che non supportano 'stop', questa configurazione manuale funzionerà.
        # temperature=0 è consigliato, ma i modelli o1 a volte forzano temperature=1. 
        # Se ti da errore sulla temperature, rimuovi il parametro temperature=0.
        self.llm = ChatOpenAI(model="gpt-5", temperature=1) 
        
        # 3. Setup della Chain Manuale (Bypassa create_sql_query_chain)
        self.query_chain = self._build_chain()
        self.execute_tool = QuerySQLDataBaseTool(db=self.db)

    def _get_system_rules(self):
        """
        Definisce la struttura ESATTA del DB personalizzato per evitare allucinazioni su schema GDELT standard.
        """
        return """
        Sei un esperto Data Scientist che interroga un database PostgreSQL contenente dati GDELT *personalizzati*.
        Il tuo compito è generare SOLO una query SQL valida, senza commenti o markdown.

        *** REGOLA DI SICUREZZA SUPREMA (DA SEGUIRE SEMPRE) ***
        - 🚫 VIETATO USARE BIND PARAMETERS (es. :param, %s, ?).
        - ✅ INSERISCI I VALORI DIRETTAMENTE NELLA STRINGA SQL (Hardcoding).
        - Esempio CORRETTO: WHERE name = 'Mario'
        - Esempio ERRATO: WHERE name = %s

        *** 1. STRUTTURA DEL DATABASE (SCHEMA SEMPLIFICATO) ***
        Il database ha 3 tabelle collegate:
        
        A) TABELLA `event` (Fatti e Azioni)
           - `globaleventid` (PK): ID univoco evento.
           - `day` (DATE): Data evento (YYYY-MM-DD).
           - `eventrootcode` (TEXT): TIPO evento. NON è numerico. È una descrizione (vedi lista sotto).
           - `actor1name`, `actor2name` (TEXT): Nomi attori (es. "United States", "Rebels"). Usa ILIKE.
           - `actiongeo_fullname` (TEXT): Luogo (es. "Paris, France"). Usa ILIKE.
           - `nummentions` (INT): Importanza evento.
           - `avgtone` (REAL): Tono medio (-10 a +10).

        B) TABELLA `article` (Il Contesto Tematico - QUI TROVI I CONCETTI)
           - `themes_human`: **LA COLONNA PIÙ IMPORTANTE PER I TEMI**. Contiene keyword separate da ';' (es. "guerrilla; corruption; crisis").
           - `themes_category`: Solo macro-categorie generiche (vedi lista sotto).
           - `allnames`: Tutte le entità citate (persone, organizzazioni).
           
        C) TABELLA `mention` (Ponte N-N)
           - Collega Eventi e Articoli.
           - JOIN: `event.globaleventid = mention.globaleventid`
           - JOIN: `mention.mentionidentifier_normalized = article.docidentifier_normalized`

        *** 2. VALORI VALIDI (IMPORTANTE) ***
        
        VALORI per `eventrootcode` (Usa SOLO questi, NO codici CAMEO numerici):
        - appeal, assault, coerce, consult, demand, disapprove
        - engage in diplomatic cooperation, engage in material cooperation
        - exhibit force posture, express intent to cooperate, fight, investigate
        - make public statement, protest, provide aid, reduce relations
        - reject, threaten, use unconventional mass violence, yield

        VALORI per `themes_category` (Macro-temi):
        - conflict_security_defense, economy_trade_industry, education_culture
        - health_society_welfare, humanitarian_aid_organizations, international_relations
        - justice_law, media_communication, politics_governance
        - resources_energy_environment, tech_infrastructure

        *** 3. STRATEGIA DI RICERCA INTELLIGENTE ***
        
        CASO 1: Domanda su ATTORI ("Chi ha attaccato?", "Cosa ha fatto la Polizia?")
        -> Cerca in `event.actor1name` O `event.actor2name`.
        
        CASO 2: Domanda su ARGOMENTI/TEMI ("Eventi riguardanti la guerriglia", "News sulla corruzione")
        -> Cerca in `article.themes_human`.
        -> Esempio: `WHERE a.themes_human ILIKE '%guerrilla%'`
        
        CASO 3: Domanda su TIPI DI EVENTO ("Quante proteste?", "Quanti aiuti?")
        -> Cerca in `event.eventrootcode`.
        -> Usa SOLO questi valori stringa: 'provide aid', 'engage in material cooperation', 'protest', 'fight', 'assault'.

        *** 4. OUTPUT ***
        Genera SOLO la stringa SQL (senza markdown).
        """

    def _build_chain(self):
        # Costruiamo la chain manualmente per evitare che LangChain inietti il parametro 'stop'
        # che il tuo modello non supporta.
        
        template = """{system_rules}

        SCHEMA DEL DATABASE:
        {table_info}

        DOMANDA UTENTE: {input}

        SQL Query:"""
        
        prompt = PromptTemplate(
            template=template,
            input_variables=["system_rules", "table_info", "input"]
        )
        
        # PIPELINE LCEL PURA: Prompt -> LLM -> Stringa
        # Nessun parametro nascosto.
        chain = prompt | self.llm | StrOutputParser()
        
        return chain

    def run_query(self, question: str) -> dict:
        if not self.db:
            return {"error": "DB Disconnected", "status": "error"}

        try:
            # 1. Recuperiamo lo schema manualmente
            table_info = self.db.get_table_info()
            
            # 2. Prepariamo gli input
            # Manteniamo la doppia chiave per sicurezza, ma qui usiamo "input" nel prompt
            inputs = {
                "input": question,     
                "question": question,
                "system_rules": self._get_system_rules(),
                "table_info": table_info
            }
            
            # 3. Generazione SQL
            sql_query = self.query_chain.invoke(inputs)
            
            # 4. Pulizia aggressiva (Il modello potrebbe mettere ```sql ... ```)
            sql_query = sql_query.strip()
            if sql_query.startswith("```sql"):
                sql_query = sql_query[6:]
            if sql_query.startswith("```"):
                sql_query = sql_query[3:]
            if sql_query.endswith("```"):
                sql_query = sql_query[:-3]
            sql_query = sql_query.strip()
            
            logger.info(f"🐛 SQL GENERATO: {sql_query}")
            
            # 5. Esecuzione
            result = self.execute_tool.invoke(sql_query)
            
            result_str = str(result)
            if not result_str or result_str == "[]" or "[(0,)]" in result_str:
                result_str = "Nessun evento trovato con i criteri specificati."

            return {
                "generated_query": sql_query,
                "result": result_str,
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"❌ SQL ERROR: {e}")
            return {
                "error": str(e), 
                "generated_query": sql_query if 'sql_query' in locals() else "N/A", 
                "status": "error"
            }