-- Abilita l'estensione per gli indici GIN su testo (ci serve per la ricerca RAG)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- --- FASE 1: TABELLE GENITORE ---

CREATE TABLE IF NOT EXISTS EVENT (
    GlobalEventID BIGINT PRIMARY KEY,
    Day DATE,
    IsRootEvent SMALLINT,
    EventRootCode CHAR(4),
    GoldsteinScale REAL,
    NumMentions INTEGER,
    AvgTone REAL,
    Actor1Name TEXT, 
    Actor2Name TEXT, 
    ActionGeo_Fullname TEXT,
    SOURCEURL TEXT
);

-- Indici per i filtri RAG comuni
CREATE INDEX IF NOT EXISTS idx_event_day ON EVENT(Day);
CREATE INDEX IF NOT EXISTS idx_event_root_code ON EVENT(EventRootCode);
-- Indice GIN per la ricerca testuale 'fuzzy' su nomi e luoghi
CREATE INDEX IF NOT EXISTS idx_gin_event_geo ON EVENT USING gin (ActionGeo_Fullname gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_gin_event_actor1 ON EVENT USING gin (Actor1Name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_gin_event_actor2 ON EVENT USING gin (Actor2Name gin_trgm_ops);


-- 1b. Tabella ARTICLE (da gkg)
CREATE TABLE IF NOT EXISTS ARTICLE (
    DocIdentifier_Normalized TEXT PRIMARY KEY,
    DocIdentifier_Original TEXT, -- L'URL originale 'minuscolo'
    Date TIMESTAMPTZ,
    Source TEXT,
    Tone REAL,
    GCAM TEXT,
    Extras TEXT,
    AllNames TEXT,
    EnhancedThemes TEXT,
    Locations TEXT
);

-- Indici per i filtri RAG comuni
CREATE INDEX IF NOT EXISTS idx_article_date ON ARTICLE(Date);
CREATE INDEX IF NOT EXISTS idx_article_source ON ARTICLE(Source);
-- Indici GIN per la ricerca testuale RAG (IL CUORE DEL RAG!)
CREATE INDEX IF NOT EXISTS idx_gin_article_themes ON ARTICLE USING gin (EnhancedThemes gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_gin_article_names ON ARTICLE USING gin (AllNames gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_gin_article_locations ON ARTICLE USING gin (Locations gin_trgm_ops);


-- --- FASE 2: TABELLA PONTE ---

CREATE TABLE IF NOT EXISTS MENTION (
    MentionID SERIAL PRIMARY KEY,
    GlobalEventID BIGINT NOT NULL,
    MentionIdentifier_Normalized TEXT NOT NULL,
    MentionTimeDate TIMESTAMPTZ,
    MentionSourceName TEXT,
    Confidence REAL,
    
    CONSTRAINT fk_event
        FOREIGN KEY(GlobalEventID) 
        REFERENCES EVENT(GlobalEventID) ON DELETE CASCADE,
    CONSTRAINT fk_article
        FOREIGN KEY(MentionIdentifier_Normalized) 
        REFERENCES ARTICLE(DocIdentifier_Normalized) ON DELETE CASCADE
);

-- Indici per JOIN veloci (FONDAMENTALI)
CREATE INDEX IF NOT EXISTS idx_mention_event_id ON MENTION(GlobalEventID);
CREATE INDEX IF NOT EXISTS idx_mention_article_id ON MENTION(MentionIdentifier_Normalized);