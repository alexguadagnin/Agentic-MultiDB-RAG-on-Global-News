import psycopg2
from psycopg2.extras import execute_values
import sys
import time

# --- 1. CONFIGURAZIONE DI SICUREZZA ---
SOURCE_DB_CONFIG = {
    'dbname': 'gdelt_rag_db',     
    'user': 'gdelt_admin',
    'password': 'strong_password_123',
    'host': 'localhost',
    'port': '5432'
}

TARGETS = {
    "xs": {"dbname": "gdelt_xs", "limit": 10000},
    "s":  {"dbname": "gdelt_s",  "limit": 50000},
    "m":  {"dbname": "gdelt_m",  "limit": 200000},
    "l":  {"dbname": "gdelt_l",  "limit": 500000},
    "xl": {"dbname": "gdelt_xl", "limit": 1500000},
}

def get_conn(config):
    try:
        return psycopg2.connect(**config)
    except Exception as e:
        print(f"❌ Errore connessione a {config['dbname']}: {e}")
        sys.exit(1)

def create_subset(tag, config):
    target_db_name = config['dbname']
    limit = config['limit']
    source_db_name = SOURCE_DB_CONFIG['dbname']

    # --- 🛡️ SAFETY SWITCH 🛡️ ---
    if target_db_name == source_db_name:
        print(f"❌ ERRORE CRITICO: Stop scrittura su '{source_db_name}'!")
        sys.exit(1)

    start_t = time.time()
    print(f"\n🚀 [Target: {tag.upper()}] Inizio procedura su '{target_db_name}' (Target: {limit} eventi)...")
    
    try:
        print("🔌 Connessione ai database...")
        src_conn = get_conn(SOURCE_DB_CONFIG)
        tgt_conn = get_conn({**SOURCE_DB_CONFIG, 'dbname': target_db_name})
        
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        # 1. TRUNCATE (Pulizia)
        print(f"🧹 Svuoto tabelle su '{target_db_name}'...")
        tgt_cur.execute("TRUNCATE TABLE MENTION, EVENT, ARTICLE CASCADE;")
        tgt_conn.commit()

        # 2. EVENTI (Estrai e Inserisci)
        print(f"📥 Estraggo {limit} Eventi casuali...")
        src_cur.execute(f"SELECT * FROM EVENT ORDER BY RANDOM() LIMIT {limit}")
        events = src_cur.fetchall()
        
        if not events:
            print("❌ Nessun evento trovato! Esco.")
            return

        event_ids = [row[0] for row in events]
        cols_evt = [desc[0] for desc in src_cur.description]
        
        print(f"📤 Inserisco {len(events)} Eventi...")
        execute_values(tgt_cur, f"INSERT INTO EVENT ({','.join(cols_evt)}) VALUES %s", events)
        tgt_conn.commit()

        # 3. MENZIONI (Estrai ma NON inserire ancora)
        print("📥 Estraggo Menzioni collegate (In Memory)...")
        src_cur.execute("SELECT * FROM MENTION LIMIT 0")
        col_names_ment = [d[0] for d in src_cur.description]
        
        # Recupera Menzioni
        ids_tuple = tuple(event_ids)
        src_cur.execute(f"SELECT * FROM MENTION WHERE GlobalEventID IN %s", (ids_tuple,))
        mentions = src_cur.fetchall()
        print(f"   -> Trovate {len(mentions)} menzioni in memoria.")

        # 4. ARTICOLI (Estrai ID dalle menzioni -> Scarica -> Inserisci)
        # Cerchiamo la colonna dell'ID Articolo
        idx_doc = -1
        target_col_name = 'mentionidentifier_normalized'
        for i, name in enumerate(col_names_ment):
            if name.lower() == target_col_name:
                idx_doc = i
                break
        
        if idx_doc == -1:
            print(f"❌ ERRORE SCHEMA: Colonna '{target_col_name}' non trovata.")
            sys.exit(1)

        mention_doc_ids = set()
        for row in mentions:
            val = row[idx_doc]
            if val: mention_doc_ids.add(val)

        print(f"📥 Estraggo {len(mention_doc_ids)} Articoli collegati...")
        if mention_doc_ids:
            doc_ids_list = list(mention_doc_ids)
            src_cur.execute("SELECT * FROM ARTICLE LIMIT 0")
            cols_art = [desc[0] for desc in src_cur.description]
            insert_query_art = f"INSERT INTO ARTICLE ({','.join(cols_art)}) VALUES %s"

            # Batch Insert Articoli
            batch_size = 5000
            total_articles = 0
            for i in range(0, len(doc_ids_list), batch_size):
                batch_ids = tuple(doc_ids_list[i:i+batch_size])
                src_cur.execute(f"SELECT * FROM ARTICLE WHERE DocIdentifier_Normalized IN %s", (batch_ids,))
                articles = src_cur.fetchall()
                if articles:
                    execute_values(tgt_cur, insert_query_art, articles)
                    total_articles += len(articles)
                    sys.stdout.write(f"\r   ...scritti {total_articles} articoli")
                    sys.stdout.flush()
            tgt_conn.commit()
            print(f"\n✅ Articoli inseriti.")
        
        # 5. MENZIONI (ORA le inseriamo, perché gli articoli esistono!)
        print(f"📤 Inserisco {len(mentions)} Menzioni...")
        if mentions:
            insert_query_ment = f"INSERT INTO MENTION ({','.join(col_names_ment)}) VALUES %s"
            execute_values(tgt_cur, insert_query_ment, mentions)
            tgt_conn.commit()

        src_conn.close()
        tgt_conn.close()
        elapsed = time.time() - start_t
        print(f"\n✅ SUCCESS: Subset {tag.upper()} creato in {elapsed:.2f}s!\n")

    except psycopg2.Error as e:
        print(f"\n❌ ERRORE SQL: {e}")
        if tgt_conn: tgt_conn.rollback()
    except Exception as e:
        print(f"\n❌ ERRORE GENERICO: {e}")

if __name__ == "__main__":
    print("--- GDELT SCALABILITY CREATOR (SAFE MODE V2 - ORDERED) ---")
    
    create_subset("xs", TARGETS["xs"])
    create_subset("s", TARGETS["s"])
    create_subset("m", TARGETS["m"])
    create_subset("l", TARGETS["l"])
    create_subset("xl", TARGETS["xl"])