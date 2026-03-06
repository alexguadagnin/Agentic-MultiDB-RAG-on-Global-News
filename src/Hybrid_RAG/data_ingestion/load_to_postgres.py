import pandas as pd
import psycopg2
from psycopg2 import extras
from pathlib import Path
import glob
import os
import io
import time
import sys

# --- 1. CONFIGURAZIONE ---

# Percorsi (basati sulla tua struttura e constants.py)
try:
    # Prova a importare dal tuo file, se è nel PYTHONPATH
    from Hybrid_RAG.constants import PROJECT_ROOT
except ImportError:
    # Altrimenti, definisci la radice partendo da questo script
    PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.resolve()

# Percorsi aggiornati basati sulla tua struttura
PARQUET_ROOT = PROJECT_ROOT / "data" / "gdelt_event" / "processed_parquet"
EXPORT_DIR = PARQUET_ROOT / "export"
GKG_DIR = PARQUET_ROOT / "gkg"
MENTIONS_DIR = PARQUET_ROOT / "mentions"
SCHEMA_FILE_PATH = PROJECT_ROOT / "src" / "Hybrid_RAG" / "db" / "schema.sql" # <-- NUOVO
#SCHEMA_FILE_PATH = PROJECT_ROOT / "Hybrid_RAG" / "db" / "schema.sql" # <-- NUOVO


# Dettagli Connessione DB (da docker-compose.yml)
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "gdelt_rag_db"
DB_USER = "gdelt_admin"
DB_PASS = "strong_password_123" # La password che hai scelto

# Colonne da caricare (basate sulla tua analisi, con Extras)
EXPORT_COLS = ['GlobalEventID', 'Day', 'Actor1Name', 'Actor2Name', 
               'IsRootEvent', 'EventRootCode', 'GoldsteinScale', 
               'NumMentions', 'AvgTone', 'ActionGeo_Fullname', 'SOURCEURL']

GKG_COLS = ['GKGRECORDID', 'Date', 'Source', 'DocumentURL', 'GCAM', 'Tone', 
            'Extras', 'AllNames', 'EnhancedThemes', 'Locations'] # <-- 'Extras' INCLUSO

MENTIONS_COLS = ['GlobalEventID', 'MentionTimeDate', 'MentionSourceName', 
                 'MentionIdentifier', 'Confidence']


# --- 2. FUNZIONI HELPER ---

def normalize_url_for_key(url):
    """
    Crea la chiave di join normalizzata dall'URL.
    Rimuove http/https e lo slash finale.
    """
    if not isinstance(url, str):
        return None
    url = url.lower().strip() 
    
    if url.startswith('https://'):
        url = url[8:]
    elif url.startswith('http://'):
        url = url[7:]
        
    if url.endswith('/'):
        url = url[:-1]
        
    return url

def get_db_connection():
    """Tenta di connettersi al DB Postgres in Docker."""
    print("Tentativo di connessione a PostgreSQL...")
    retries = 10
    while retries > 0:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            print("✅ Connessione a PostgreSQL stabilita.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Connessione fallita (il container sta partendo?): {e}")
            retries -= 1
            time.sleep(3)
    
    print("❌ Impossibile connettersi al database.")
    sys.exit(1)

def create_schema(conn):
    """
    MODIFICATO: Esegue lo schema SQL leggendolo dal file db/schema.sql
    """
    print(f"Creazione schema database da {SCHEMA_FILE_PATH}...")
    
    try:
        with open(SCHEMA_FILE_PATH, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
    except FileNotFoundError:
        print(f"❌ ERRORE CRITICO: File schema.sql non trovato in {SCHEMA_FILE_PATH}")
        sys.exit(1)
            
    try:
        with conn.cursor() as cursor:
            cursor.execute(schema_sql) # Esegue lo schema dal file
        conn.commit()
        print("✅ Schema creato con successo.")
    except Exception as e:
        print(f"❌ Errore creazione schema: {e}")
        conn.rollback()
        raise

def bulk_load_to_postgres(df, table_name, cursor):
    """
    Metodo scalabile per caricare un DataFrame in Postgres usando COPY.
    Questa funzione NON committa o fa rollback. Il chiamante è responsabile.
    """
    buffer = io.StringIO()
    # Scrive il DF in un formato CSV (in memoria)
    df.to_csv(buffer, index=False, header=False, sep='\t', quotechar='"', na_rep='\\N')
    buffer.seek(0) 
    
    # Il blocco try...except è ancora qui per stampare l'errore, ma poi lo ri-solleva
    try:
        cursor.copy_expert(
            f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', QUOTE '\"', NULL '\\N')",
            buffer
        )
        return len(df)
    except Exception as e:
        # Stampa l'errore (es. 'date/time') e poi lo solleva per 
        # essere gestito dal blocco 'main'
        print(f"❌ Errore (interno bulk_load): {e}") 
        raise e

# --- 3. PROCESSO ETL IN 2 FASI ---

# --- 3. PROCESSO ETL IN 2 FASI ---

def main():
    conn = get_db_connection()
    create_schema(conn)

    # --- FASE 1: Caricamento Tabelle Genitore ---
    
    # 1a. EVENT (da export) - QUESTA FASE ERA GIA' CORRETTA
    print("\n--- FASE 1a: Caricamento EVENTI (export) ---")
    export_files = glob.glob(str(EXPORT_DIR / "*.parquet"))
    print(f"Trovati {len(export_files)} file 'export'.")
    total_rows = 0
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE TEMPORARY TABLE temp_event (LIKE EVENT);")

            for f in export_files:
                df = pd.read_parquet(f, columns=EXPORT_COLS)
                df = df[['GlobalEventID', 'Day', 'IsRootEvent', 'EventRootCode', 
                        'GoldsteinScale', 'NumMentions', 'AvgTone', 
                        'ActionGeo_Fullname', 'Actor1Name', 'Actor2Name', 'SOURCEURL']]
                
                rows_loaded = bulk_load_to_postgres(df, "temp_event", cursor)
                
                cursor.execute("""
                    INSERT INTO EVENT SELECT * FROM temp_event
                    ON CONFLICT (GlobalEventID) DO NOTHING;
                """)
                conn.commit()
                
                cursor.execute("TRUNCATE TABLE temp_event;")
                
                total_rows += rows_loaded
                print(f"  ... processato {Path(f).name}, caricate {rows_loaded} righe.")
    except Exception as e:
        print(f"Errore FASE 1a: {e}")
        conn.rollback()
    
    print(f"✅ Fase 1a (EVENT) completata. Totale righe: {total_rows}")


    # 1b. ARTICLE (da gkg)
    print("\n--- FASE 1b: Caricamento ARTICOLI (gkg) ---")
    gkg_files = glob.glob(str(GKG_DIR / "*.parquet"))
    print(f"Trovati {len(gkg_files)} file 'gkg'.")
    total_rows = 0
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE TEMPORARY TABLE temp_article (LIKE ARTICLE);")

            for f in gkg_files:
                df = pd.read_parquet(f, columns=GKG_COLS)
                
                # --- MODIFICA QUI (1 di 2): Converti la data GKG in un timestamp ---
                df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d%H%M%S', errors='coerce')
                
                df['DocIdentifier_Original'] = df['DocumentURL']
                df['DocIdentifier_Normalized'] = df['DocumentURL'].apply(normalize_url_for_key)
                
                df = df[['DocIdentifier_Normalized', 'DocIdentifier_Original', 'Date', 
                         'Source', 'Tone', 'GCAM', 'Extras', 'AllNames', 
                         'EnhancedThemes', 'Locations']]
                df = df.dropna(subset=['DocIdentifier_Normalized'])

                rows_loaded = bulk_load_to_postgres(df, "temp_article", cursor)
                
                cursor.execute("""
                    INSERT INTO ARTICLE SELECT * FROM temp_article
                    ON CONFLICT (DocIdentifier_Normalized) DO NOTHING;
                """)
                conn.commit()
                
                cursor.execute("TRUNCATE TABLE temp_article;")

                total_rows += rows_loaded
                print(f"  ... processato {Path(f).name}, caricate {rows_loaded} righe.")
    except Exception as e:
        print(f"Errore FASE 1b: {e}")
        conn.rollback()

    print(f"✅ Fase 1b (ARTICLE) completata. Totale righe: {total_rows}")

    # --- FASE 2: Caricamento Tabella Ponte (con controllo FK) ---

    print("\n--- FASE 2: Caricamento MENZIONI (mentions) ---")
    mention_files = glob.glob(str(MENTIONS_DIR / "*.parquet"))
    print(f"Trovati {len(mention_files)} file 'mentions'.")
    total_rows = 0
    total_orphans = 0
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TEMPORARY TABLE temp_mention (
                    GlobalEventID BIGINT,
                    MentionIdentifier_Normalized TEXT,
                    MentionTimeDate TIMESTAMPTZ,
                    MentionSourceName TEXT,
                    Confidence REAL
                );
            """)
            
            for f in mention_files:
                df = pd.read_parquet(f, columns=MENTIONS_COLS)

                # --- MODIFICA QUI (2 di 2): Converti la data GKG in un timestamp ---
                df['MentionTimeDate'] = pd.to_datetime(df['MentionTimeDate'], format='%Y%m%d%H%M%S', errors='coerce')
                
                df['MentionIdentifier_Normalized'] = df['MentionIdentifier'].apply(normalize_url_for_key)
                
                df = df[['GlobalEventID', 'MentionIdentifier_Normalized', 
                         'MentionTimeDate', 'MentionSourceName', 'Confidence']]
                df = df.dropna(subset=['GlobalEventID', 'MentionIdentifier_Normalized'])

                rows_loaded = bulk_load_to_postgres(df, "temp_mention", cursor)
                
                cursor.execute("""
                    INSERT INTO MENTION (GlobalEventID, MentionIdentifier_Normalized, MentionTimeDate, MentionSourceName, Confidence)
                    SELECT
                        t.GlobalEventID,
                        t.MentionIdentifier_Normalized,
                        t.MentionTimeDate,
                        t.MentionSourceName,
                        t.Confidence
                    FROM temp_mention t
                    WHERE
                        EXISTS (SELECT 1 FROM EVENT e WHERE e.GlobalEventID = t.GlobalEventID)
                    AND
                        EXISTS (SELECT 1 FROM ARTICLE a WHERE a.DocIdentifier_Normalized = t.MentionIdentifier_Normalized);
                """)
                
                rows_inserted = cursor.rowcount
                orphans = rows_loaded - rows_inserted
                total_rows += rows_inserted
                total_orphans += orphans
                conn.commit()
                
                cursor.execute("TRUNCATE TABLE temp_mention;")
                
                print(f"  ... processato {Path(f).name}, caricate {rows_inserted} righe ({orphans} orfani scartati).")
    except Exception as e:
        print(f"Errore FASE 2: {e}")
        conn.rollback()

    print(f"✅ Fase 2 (MENTION) completata. Totale righe inserite: {total_rows}")
    print(f"ℹ️ Totale righe orfane (scartate per integrità): {total_orphans}")
    
    conn.close()
    print("\n🎉 Processo ETL completato. Il tuo database RAG è pronto!")


if __name__ == "__main__":
    main()