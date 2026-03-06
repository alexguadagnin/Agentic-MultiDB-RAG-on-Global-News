import os
import sys
import re
import json
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from neo4j import GraphDatabase
from tqdm import tqdm

try:
    import spacy
except Exception:
    spacy = None

# =============================================================================
# CONFIG
# =============================================================================

BASE_PATH = Path(r"...") 
KG_DIR = BASE_PATH / "knowledge_graph"

# Neo4j TEST (porta 7688)
NEO4J_URI = "bolt://localhost:7688"
NEO4J_AUTH = ("neo4j", "strong_password_test")

# Postgres subset DB
DB_CONFIG_BASE = {
    "user": "gdelt_admin",
    "password": "strong_password_123",
    "host": "localhost",
    "port": "5432",
}

SUBSETS = {
    "xs": "gdelt_xs",
    "s":  "gdelt_s",
    "m":  "gdelt_m",
    "l":  "gdelt_l",
    "xl": "gdelt_xl",
}

# Master files (full KG)
MASTER_ENTITY_MAP_FILE = KG_DIR / "entity_map.json"  # prodotto dal full (fase 1)
MASTER_SEMANTIC_RELATIONS_CSV = KG_DIR / "FINAL_RELATIONS_IMPORT.csv"  # full (fase 3)

# Regex identica alla fase 1 del full (quella che mi hai mostrato)
CLEAN_RE = re.compile(r"[,0-9\(\):]+.*$")


# =============================================================================
# Helpers: schema / safe introspection
# =============================================================================

def _pg_columns(cur, table: str) -> set:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        """,
        (table.lower(),),
    )
    return {r[0].lower() for r in cur.fetchall()}

def normalize_string(s: str) -> str:
    s = str(s).lower().strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^\w_]", "", s)
    return s or "unknown"

def safe_strip(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    x = str(x).strip()
    return x if x else None


# =============================================================================
# Fallback: build an entity_map "full-like" (if master missing)
# =============================================================================

def setup_entity_linker():
    if spacy is None:
        raise RuntimeError("spaCy non disponibile. Installa spacy + modello + spacy-entity-linker.")
    try:
        nlp = spacy.load("en_core_web_lg")
    except OSError as e:
        raise RuntimeError("Modello spaCy 'en_core_web_lg' mancante. Esegui: python -m spacy download en_core_web_lg") from e

    if "sentencizer" not in nlp.pipe_names:
        nlp.add_pipe("sentencizer", first=True)

    # spacy-entity-linker usa 'entityLinker'
    if "entityLinker" in nlp.pipe_names:
        nlp.remove_pipe("entityLinker")
    nlp.add_pipe("entityLinker", last=True)
    return nlp

def extract_dirty_entities_full_like(pg_conn) -> set:
    """
    Replica la logica del full:
    - EVENT: Actor1Name, Actor2Name, ActionGeo_Fullname
    - ARTICLE: AllNames, Locations, EnhancedThemes
    """
    dirty = set()
    cur = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # EVENT
    cur.execute(
        """
        SELECT DISTINCT Actor1Name FROM event WHERE Actor1Name IS NOT NULL
        UNION
        SELECT DISTINCT Actor2Name FROM event WHERE Actor2Name IS NOT NULL
        UNION
        SELECT DISTINCT ActionGeo_Fullname FROM event WHERE ActionGeo_Fullname IS NOT NULL;
        """
    )
    for row in cur.fetchall():
        if row[0]:
            dirty.add((row[0], "UNKNOWN"))

    # ARTICLE streaming
    cur.close()
    ssc = pg_conn.cursor("article_stream", cursor_factory=psycopg2.extras.DictCursor)
    ssc.execute("SELECT AllNames, Locations, EnhancedThemes FROM article")

    for row in tqdm(ssc, desc="Extract ARTICLE entities"):
        # AllNames
        if row["allnames"]:
            for name in row["allnames"].split(";"):
                cleaned = CLEAN_RE.sub("", name).strip()
                if cleaned and len(cleaned) > 2:
                    dirty.add((cleaned, "ACTOR_OR_LOCATION"))
        # Locations
        if row["locations"]:
            for loc in row["locations"].split(";"):
                cleaned = CLEAN_RE.sub("", loc).strip()
                if cleaned and len(cleaned) > 2:
                    dirty.add((cleaned, "LOCATION"))
        # Themes
        if row["enhancedthemes"]:
            for theme in row["enhancedthemes"].split(";"):
                theme = theme.strip()
                if theme and len(theme) > 2:
                    dirty.add((theme, "THEME"))

    ssc.close()
    return dirty

def create_entity_map_full_like(dirty_entities: set, nlp) -> Dict[str, Dict[str, Any]]:
    """
    Replica (in modo compatibile) la logica del full:
    - Theme: id = theme:<normalized>
    - Actor/Location: prefer Wikidata numeric id (string), else custom:<type>:<normalized>
    """
    entity_map: Dict[str, Dict[str, Any]] = {}

    # Themes (no linker)
    themes = {name for (name, t) in dirty_entities if t == "THEME"}
    for theme_name in tqdm(themes, desc="Themes"):
        entity_map[theme_name] = {
            "id": "theme:" + normalize_string(theme_name),
            "name": theme_name,
            "type": "Theme",
            "source": "custom",
        }

    other = [name for (name, t) in dirty_entities if t != "THEME"]
    # tipo grezzo da GDELT extraction
    rough_type = {name: t for (name, t) in dirty_entities if t != "THEME"}

    docs = nlp.pipe(other, batch_size=20)
    for doc in tqdm(docs, total=len(other), desc="EntityLinking"):
        original = doc.text
        canonical = original
        etype = "Unknown"
        wikidata_id = None

        if hasattr(doc._, "linkedEntities") and doc._.linkedEntities:
            ent = doc._.linkedEntities[0]
            wikidata_id = ent.get_id()  # spesso "76"
            canonical = ent.get_label()

        if not wikidata_id and doc.ents:
            ent0 = doc.ents[0]
            canonical = ent0.text
            if ent0.label_ in ["PERSON", "ORG", "NORP"]:
                etype = "Actor"
            elif ent0.label_ in ["GPE", "LOC", "FAC"]:
                etype = "Location"

        # fallback tipo da GDELT
        if etype == "Unknown":
            guess = rough_type.get(original, "UNKNOWN")
            etype = "Location" if guess == "LOCATION" else "Actor"

        if wikidata_id:
            eid = str(wikidata_id)
            source = "wikidata"
        else:
            eid = f"custom:{etype.lower()}:{normalize_string(original)}"
            source = "custom"

        entity_map[original] = {"id": eid, "name": canonical, "type": etype, "source": source}

    return entity_map


# =============================================================================
# Pipeline (subset -> full KG schema)
# =============================================================================

class ScalableGraphPipeline:
    def __init__(self, subset_tag: str, db_name: str):
        self.tag = subset_tag
        self.db_name = db_name

        self.entity_map_file = KG_DIR / f"entity_map_{subset_tag}.json"
        self.semantic_rel_csv = KG_DIR / f"FINAL_RELATIONS_IMPORT_{subset_tag}.csv"

        self.driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
        try:
            self.pg_conn = psycopg2.connect(dbname=db_name, **DB_CONFIG_BASE)
            self.pg_conn.set_session(autocommit=False, readonly=True)
            self.pg_cur = self.pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except Exception as e:
            raise RuntimeError(f"Errore connessione Postgres '{db_name}': {e}")

        # cache columns
        self.article_cols = _pg_columns(self.pg_cur, "article")
        self.event_cols = _pg_columns(self.pg_cur, "event")

    def close(self):
        self.driver.close()
        self.pg_cur.close()
        self.pg_conn.close()

    # -------------------------------------------------------------------------
    # Neo4j reset + indexes (identici al full)
    # -------------------------------------------------------------------------
    def wipe_neo4j(self):
        print("🧹 [INIT] Pulizia Neo4j TEST (7688)...")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    def create_indexes_full_schema(self):
        print("🧱 [INIT] Creazione indici/vincoli (full schema)...")
        queries = [
            "CREATE CONSTRAINT event_id IF NOT EXISTS FOR (e:Event) REQUIRE e.GlobalEventID IS UNIQUE",
            "CREATE CONSTRAINT article_id IF NOT EXISTS FOR (a:Article) REQUIRE a.DocIdentifier_Normalized IS UNIQUE",
            "CREATE CONSTRAINT actor_id IF NOT EXISTS FOR (a:Actor) REQUIRE a.entityID IS UNIQUE",
            "CREATE CONSTRAINT loc_id IF NOT EXISTS FOR (l:Location) REQUIRE l.entityID IS UNIQUE",
            "CREATE CONSTRAINT theme_name IF NOT EXISTS FOR (t:Theme) REQUIRE t.name IS UNIQUE",
            "CREATE CONSTRAINT cat_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE",
            "CREATE INDEX entity_lookup IF NOT EXISTS FOR (n:Entity) ON (n.entityID)",
        ]
        with self.driver.session() as session:
            for q in queries:
                session.run(q)

    # -------------------------------------------------------------------------
    # Phase 1: entity_map (preferisci master full)
    # -------------------------------------------------------------------------
    def phase_1_entity_map(self) -> Dict[str, Dict[str, Any]]:
        print(f"\n🏗️ [PHASE 1] Entity Map per subset {self.tag.upper()} (full-like)...")

        # 1) Estrai dirty entities del subset (serve per filtrare master, se esiste)
        dirty = extract_dirty_entities_full_like(self.pg_conn)
        dirty_names = {name for (name, _) in dirty}

        # 2) Se esiste la mappa master del full: riusala e filtra
        if MASTER_ENTITY_MAP_FILE.exists():
            print(f"   ✅ Uso master entity_map del full: {MASTER_ENTITY_MAP_FILE}")
            with open(MASTER_ENTITY_MAP_FILE, "r", encoding="utf-8") as f:
                master = json.load(f)
            # master: dict original_name -> {id,name,type,source}
            filtered = {k: v for k, v in master.items() if k in dirty_names or (v.get("type") == "Theme" and k in dirty_names)}
            # fallback: se alcuni dirty non sono nel master, li aggiungiamo come custom
            missing = [n for n in dirty_names if n not in filtered]
            if missing:
                print(f"   ⚠️  {len(missing)} entità non presenti nel master. Le creo come custom (per non perdere coverage).")
                for n in missing:
                    # euristica: se nel dirty set era THEME -> Theme, altrimenti Actor/Location custom
                    t = next((tt for (name, tt) in dirty if name == n), "UNKNOWN")
                    if t == "THEME":
                        filtered[n] = {"id": "theme:" + normalize_string(n), "name": n, "type": "Theme", "source": "custom"}
                    else:
                        # non sappiamo se Actor/Location -> euristica
                        etype = "Location" if t == "LOCATION" else "Actor"
                        filtered[n] = {"id": f"custom:{etype.lower()}:{normalize_string(n)}", "name": n, "type": etype, "source": "custom"}

            with open(self.entity_map_file, "w", encoding="utf-8") as f:
                json.dump(filtered, f, ensure_ascii=False, indent=2)
            print(f"   💾 Salvata entity_map subset: {self.entity_map_file}")
            return filtered

        # 3) Altrimenti: genera davvero "full-like" con spaCy entityLinker
        print("   ⚠️ Master entity_map.json non trovato. Genero entity_map full-like con spaCy (più lento).")
        nlp = setup_entity_linker()
        subset_map = create_entity_map_full_like(dirty, nlp)

        with open(self.entity_map_file, "w", encoding="utf-8") as f:
            json.dump(subset_map, f, ensure_ascii=False, indent=2)
        print(f"   💾 Salvata entity_map subset: {self.entity_map_file}")
        return subset_map

    # -------------------------------------------------------------------------
    # Phase 2: load nodes (Entities, Events, Articles) full schema
    # -------------------------------------------------------------------------
    def phase_2_load_nodes(self, entity_map: Dict[str, Dict[str, Any]]):
        print(f"\n🏗️ [PHASE 2] Caricamento nodi (full schema)...")

        # A) Entities: Actor/Location con entityID + name e label [Actor|Location, Entity]
        ents = []
        for k, v in entity_map.items():
            t = v.get("type")
            if t in ("Actor", "Location") and v.get("id"):
                ents.append({"entityID": str(v["id"]), "name": v.get("name", k), "type": t})

        print(f"   -> Entities (Actor/Location): {len(ents):,}")
        self._push_entities(ents)

        # B) Events
        print("   -> Events...")
        self.pg_cur.execute("SELECT GlobalEventID, Day, AvgTone FROM event")
        batch = []
        for row in self.pg_cur:
            batch.append({"gid": str(row["globaleventid"]), "day": str(row["day"]), "tone": float(row["avgtone"]) if row["avgtone"] is not None else None})
            if len(batch) >= 5000:
                self._push_events(batch)
                batch = []
        if batch:
            self._push_events(batch)

        # C) Articles
        print("   -> Articles...")
        # scegliamo una colonna per URL se esiste (url) altrimenti usiamo source
        url_col = "url" if "url" in self.article_cols else ("source" if "source" in self.article_cols else None)

        if url_col:
            self.pg_cur.execute(f"SELECT DocIdentifier_Normalized, Date, {url_col} AS url FROM article")
        else:
            self.pg_cur.execute("SELECT DocIdentifier_Normalized, Date FROM article")

        batch = []
        for row in self.pg_cur:
            batch.append({
                "doc_id": row["docidentifier_normalized"],
                "date": str(row["date"]) if row["date"] is not None else None,
                "url": row["url"] if url_col else None,
            })
            if len(batch) >= 5000:
                self._push_articles(batch)
                batch = []
        if batch:
            self._push_articles(batch)

    def _push_entities(self, rows: List[Dict[str, Any]]):
        """
        Prefer APOC apoc.merge.node([type,'Entity'], {entityID}, {name})
        Fallback senza APOC: MERGE separato per Actor/Location.
        """
        if not rows:
            return

        try:
            q_apoc = """
            UNWIND $rows AS r
            CALL {
              WITH r
              CALL apoc.merge.node([r.type, 'Entity'], {entityID: r.entityID}, {name: r.name})
              YIELD node
              RETURN node
            }
            RETURN count(*) AS c
            """
            with self.driver.session() as s:
                for i in range(0, len(rows), 10000):
                    s.run(q_apoc, rows=rows[i:i+10000])
            return
        except Exception as e:
            print(f"   ⚠️ APOC merge.node non disponibile o errore ({e}). Uso fallback MERGE per type.")

        q_actor = "UNWIND $rows AS r MERGE (n:Actor:Entity {entityID:r.entityID}) SET n.name=r.name"
        q_loc = "UNWIND $rows AS r MERGE (n:Location:Entity {entityID:r.entityID}) SET n.name=r.name"

        actors = [r for r in rows if r["type"] == "Actor"]
        locs = [r for r in rows if r["type"] == "Location"]

        with self.driver.session() as s:
            for i in range(0, len(actors), 10000):
                s.run(q_actor, rows=actors[i:i+10000])
            for i in range(0, len(locs), 10000):
                s.run(q_loc, rows=locs[i:i+10000])

    def _push_events(self, batch: List[Dict[str, Any]]):
        q = """
        UNWIND $batch AS row
        MERGE (e:Event {GlobalEventID: row.gid})
        SET e.Day = row.day,
            e.AvgTone = row.tone
        """
        with self.driver.session() as s:
            s.run(q, batch=batch)

    def _push_articles(self, batch: List[Dict[str, Any]]):
        q = """
        UNWIND $batch AS row
        MERGE (a:Article {DocIdentifier_Normalized: row.doc_id})
        SET a.Date = row.date,
            a.url = row.url
        """
        with self.driver.session() as s:
            s.run(q, batch=batch)

    # -------------------------------------------------------------------------
    # Phase 3: load relationships (full schema)
    # -------------------------------------------------------------------------
    def phase_3_load_relationships(self, entity_map: Dict[str, Dict[str, Any]]):
        print(f"\n🏗️ [PHASE 3] Caricamento relazioni (full schema)...")

        # lookup: original_name -> entityID (solo Actor/Location)
        name_to_eid = {
            k: str(v["id"])
            for k, v in entity_map.items()
            if v.get("type") in ("Actor", "Location") and v.get("id")
        }

        # A) Entity -> Event  (INVOLVED_IN)  usando Actor1/Actor2/ActionGeo
        print("   -> Relazioni INVOLVED_IN (Entity -> Event)...")
        self.pg_cur.execute("SELECT GlobalEventID, Actor1Name, Actor2Name, ActionGeo_Fullname FROM event")
        edges = []
        for row in self.pg_cur:
            gid = str(row["globaleventid"])
            for field in ("actor1name", "actor2name", "actiongeo_fullname"):
                n = safe_strip(row[field])
                if not n:
                    continue
                eid = name_to_eid.get(n)
                if eid:
                    edges.append({"gid": gid, "eid": eid})
            if len(edges) >= 20000:
                self._push_involved_in(edges)
                edges = []
        if edges:
            self._push_involved_in(edges)

        # B) Article -> Entity (MENTIONS)  da ARTICLE.AllNames + ARTICLE.Locations
        print("   -> Relazioni MENTIONS (Article -> Entity) da AllNames/Locations...")
        if "allnames" in self.article_cols or "locations" in self.article_cols:
            cols = []
            if "allnames" in self.article_cols:
                cols.append("AllNames")
            if "locations" in self.article_cols:
                cols.append("Locations")
            col_sql = ", ".join(cols)
            ssc = self.pg_conn.cursor("article_mentions_stream", cursor_factory=psycopg2.extras.DictCursor)
            ssc.execute(f"SELECT DocIdentifier_Normalized, {col_sql} FROM article")

            batch = []
            for row in tqdm(ssc, desc="Parse mentions"):
                doc_id = row["docidentifier_normalized"]

                # AllNames
                if "allnames" in row and row["allnames"]:
                    for name in row["allnames"].split(";"):
                        cleaned = CLEAN_RE.sub("", name).strip()
                        if cleaned and len(cleaned) > 2:
                            eid = name_to_eid.get(cleaned)
                            if eid:
                                batch.append({"doc_id": doc_id, "eid": eid})

                # Locations
                if "locations" in row and row["locations"]:
                    for loc in row["locations"].split(";"):
                        cleaned = CLEAN_RE.sub("", loc).strip()
                        if cleaned and len(cleaned) > 2:
                            eid = name_to_eid.get(cleaned)
                            if eid:
                                batch.append({"doc_id": doc_id, "eid": eid})

                if len(batch) >= 20000:
                    self._push_mentions(batch)
                    batch = []

            if batch:
                self._push_mentions(batch)
            ssc.close()
        else:
            print("   ⚠️ ARTICLE.AllNames/Locations non disponibili: salto MENTIONS (Article->Entity).")

        # C) Article -> Theme (HAS_THEME) da EnhancedThemes (crea nodi Theme separati)
        print("   -> Relazioni HAS_THEME (Article -> Theme) da EnhancedThemes...")
        if "enhancedthemes" in self.article_cols:
            ssc = self.pg_conn.cursor("article_theme_stream", cursor_factory=psycopg2.extras.DictCursor)
            ssc.execute("SELECT DocIdentifier_Normalized, EnhancedThemes FROM article")

            batch = []
            for row in tqdm(ssc, desc="Parse themes"):
                if not row["enhancedthemes"]:
                    continue
                doc_id = row["docidentifier_normalized"]
                for t in row["enhancedthemes"].split(";"):
                    t = t.strip()
                    if t:
                        batch.append({"doc_id": doc_id, "theme": t})
                if len(batch) >= 20000:
                    self._push_has_theme(batch)
                    batch = []
            if batch:
                self._push_has_theme(batch)
            ssc.close()
        else:
            print("   ⚠️ ARTICLE.EnhancedThemes non disponibile: salto HAS_THEME.")

    def _push_involved_in(self, batch: List[Dict[str, Any]]):
        q = """
        UNWIND $batch AS row
        MATCH (e:Event {GlobalEventID: row.gid})
        MATCH (ent:Entity {entityID: row.eid})
        MERGE (ent)-[:INVOLVED_IN]->(e)
        """
        with self.driver.session() as s:
            s.run(q, batch=batch)

    def _push_mentions(self, batch: List[Dict[str, Any]]):
        q = """
        UNWIND $batch AS row
        MATCH (a:Article {DocIdentifier_Normalized: row.doc_id})
        MATCH (ent:Entity {entityID: row.eid})
        MERGE (a)-[:MENTIONS]->(ent)
        """
        with self.driver.session() as s:
            s.run(q, batch=batch)

    def _push_has_theme(self, batch: List[Dict[str, Any]]):
        q = """
        UNWIND $batch AS row
        MATCH (a:Article {DocIdentifier_Normalized: row.doc_id})
        MERGE (t:Theme {name: row.theme})
        MERGE (a)-[:HAS_THEME]->(t)
        """
        with self.driver.session() as s:
            s.run(q, batch=batch)

    # -------------------------------------------------------------------------
    # Phase 4 (optional): semantic Entity->Entity relations (full phase 3)
    # -------------------------------------------------------------------------
    def phase_4_load_semantic_relations(self):
        """
        Se hai già un CSV relazionale aggregato, lo carica come nel full.
        Cerca prima FINAL_RELATIONS_IMPORT_<tag>.csv, altrimenti usa quello master.
        """
        csv_path = self.semantic_rel_csv if self.semantic_rel_csv.exists() else MASTER_SEMANTIC_RELATIONS_CSV
        if not csv_path.exists():
            print("   ⚠️ [PHASE 4] CSV relazioni semantiche non trovato. Skip.")
            return

        file_url = f"file:///knowledge_graph/{csv_path.name}"
        print(f"\n🏗️ [PHASE 4] Caricamento relazioni semantiche Entity->Entity da {csv_path.name}")

        query = """
        CALL apoc.periodic.iterate(
          "LOAD CSV WITH HEADERS FROM $url AS row RETURN row",
          "
          MATCH (s:Entity {entityID: row.source_id})
          MATCH (t:Entity {entityID: row.target_id})

          CALL apoc.create.relationship(s, row.relation, {
            score: toFloat(row.max_score),
            weight: toInteger(row.weight),
            chunk_ids: split(row.chunk_ids, '|'),
            source: 'LLM-Qwen-Aggregation',
            last_updated: datetime()
          }, t) YIELD rel

          RETURN count(rel)
          ",
          {batchSize: 5000, parallel: true, params: {url: $url}}
        )
        YIELD batches, total, errorMessages
        RETURN batches, total, errorMessages
        """

        try:
            with self.driver.session() as session:
                rec = session.run(query, url=file_url).single()
                print(f"   ✅ Relazioni create/aggiornate: {rec['total']:,}")
                if rec["errorMessages"]:
                    print(f"   ⚠️ Errori: {rec['errorMessages']}")
        except Exception as e:
            print(f"   ⚠️ [PHASE 4] Impossibile caricare relazioni semantiche (APOC?): {e}")

    # -------------------------------------------------------------------------
    def run_pipeline(self):
        t0 = time.time()
        self.wipe_neo4j()
        self.create_indexes_full_schema()

        entity_map = self.phase_1_entity_map()
        self.phase_2_load_nodes(entity_map)
        self.phase_3_load_relationships(entity_map)

        # opzionale ma “identico al full” se hai il CSV disponibile
        self.phase_4_load_semantic_relations()

        print(f"\n🎉 PIPELINE COMPLETATA PER {self.tag.upper()} in {time.time()-t0:.1f}s")


# =============================================================================
# ESECUZIONE
# =============================================================================
if __name__ == "__main__":
    tag = "xs"
    pipeline = ScalableGraphPipeline(tag, SUBSETS[tag])
    pipeline.run_pipeline()
    pipeline.close()
