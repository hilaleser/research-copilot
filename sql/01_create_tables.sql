-- Research Copilot - Lakebase schema
-- Run this ONCE in the Lakebase SQL editor before deploying the MCP server.
--
-- Four tables. The spec lists nine; these four carry the whole workflow:
--   learning_goals    what the student wants to learn
--   papers            papers discovered from OpenAlex
--   paper_embeddings  abstract chunks as vectors, for evidence retrieval
--   collection_papers which papers are saved, and whether they've been read
--
-- Author details live in a JSONB column on `papers` rather than in separate
-- `authors` / `paper_authors` tables. We only ever read author names for
-- display and citations, never query across them, so two joins would buy
-- nothing.

CREATE EXTENSION IF NOT EXISTS vector;

-- ---------------------------------------------------------------------------
-- 1. Learning goals
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS learning_goals (
    id          BIGSERIAL PRIMARY KEY,
    user_email  TEXT        NOT NULL,
    goal        TEXT        NOT NULL,   -- e.g. "understand attention in transformers"
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- 2. Papers
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS papers (
    openalex_id      TEXT PRIMARY KEY,      -- e.g. "W3177318507"
    title            TEXT NOT NULL,
    abstract         TEXT,                  -- reconstructed from OpenAlex
    authors          JSONB,                 -- ["Ashish Vaswani", "Noam Shazeer", ...]
    publication_year INTEGER,
    doi              TEXT,
    url              TEXT,
    cited_by_count   INTEGER DEFAULT 0,     -- drives the reading-plan ordering
    is_open_access   BOOLEAN DEFAULT FALSE,
    added_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- 3. Paper embeddings
-- ---------------------------------------------------------------------------
-- 384 dimensions because we use sentence-transformers/all-MiniLM-L6-v2, the
-- same model as Day 2. Change the model and this number must change with it -
-- vectors of different lengths cannot be compared.
CREATE TABLE IF NOT EXISTS paper_embeddings (
    id          BIGSERIAL PRIMARY KEY,
    openalex_id TEXT NOT NULL REFERENCES papers(openalex_id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    chunk_text  TEXT   NOT NULL,
    embedding   VECTOR(384) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (openalex_id, chunk_index)
);

-- HNSW index for fast similarity search. vector_cosine_ops pairs with the
-- <=> operator - mismatch them and Postgres silently ignores the index.
CREATE INDEX IF NOT EXISTS idx_paper_embeddings_vec
    ON paper_embeddings USING hnsw (embedding vector_cosine_ops);

-- ---------------------------------------------------------------------------
-- 4. Collections and reading status
-- ---------------------------------------------------------------------------
-- The collection is just a name on the row. A separate `collections` table
-- would only store that same name plus an id, so it is folded in here.
-- `status` gives lightweight reading-progress tracking without a fifth table.
CREATE TABLE IF NOT EXISTS collection_papers (
    id              BIGSERIAL PRIMARY KEY,
    user_email      TEXT NOT NULL,
    collection_name TEXT NOT NULL DEFAULT 'default',
    openalex_id     TEXT NOT NULL REFERENCES papers(openalex_id) ON DELETE CASCADE,
    status          TEXT NOT NULL DEFAULT 'to_read',  -- to_read | reading | read
    notes           TEXT,
    added_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_email, collection_name, openalex_id)
);

CREATE INDEX IF NOT EXISTS idx_collection_lookup
    ON collection_papers (user_email, collection_name, status);

-- ---------------------------------------------------------------------------
-- PERMISSIONS - do not skip
-- ---------------------------------------------------------------------------
-- You create these tables as your own identity in the SQL editor, but the app
-- connects using the role in the Lakebase connection string. Without these
-- grants the app fails with "permission denied for table ...".
--
-- The SEQUENCE grants matter as much as the table grants: BIGSERIAL columns
-- call nextval() on every INSERT.

GRANT SELECT, INSERT, UPDATE, DELETE ON learning_goals    TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON papers            TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON paper_embeddings  TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON collection_papers TO PUBLIC;

GRANT USAGE, SELECT ON SEQUENCE learning_goals_id_seq    TO PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE paper_embeddings_id_seq  TO PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE collection_papers_id_seq TO PUBLIC;
