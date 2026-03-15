SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS narrative_evidence_spans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    unit_id TEXT NOT NULL,
    document_id TEXT NOT NULL,
    page_number INTEGER,
    source_tag TEXT,
    char_start INTEGER DEFAULT 0,
    char_end INTEGER DEFAULT 0,
    text TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_nie_evidence_doc ON narrative_evidence_spans(document_id, page_number);

CREATE TABLE IF NOT EXISTS narrative_claims (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_span_id INTEGER NOT NULL,
    subject TEXT,
    predicate TEXT,
    object TEXT,
    speaker TEXT,
    target TEXT,
    claim_type TEXT,
    polarity TEXT,
    certainty TEXT,
    event_time_text TEXT,
    normalized_date TEXT,
    location_text TEXT,
    summary TEXT,
    quote TEXT,
    source_method TEXT DEFAULT 'ollama',
    extractor_confidence REAL DEFAULT 0.5,
    normalized_subject TEXT,
    normalized_predicate TEXT,
    normalized_object TEXT,
    raw_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(evidence_span_id) REFERENCES narrative_evidence_spans(id)
);
CREATE INDEX IF NOT EXISTS idx_nie_claims_subject ON narrative_claims(normalized_subject);
CREATE INDEX IF NOT EXISTS idx_nie_claims_predicate ON narrative_claims(normalized_predicate);
CREATE INDEX IF NOT EXISTS idx_nie_claims_object ON narrative_claims(normalized_object);

CREATE TABLE IF NOT EXISTS narrative_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key TEXT UNIQUE NOT NULL,
    event_type TEXT,
    primary_actor TEXT,
    secondary_actor TEXT,
    object TEXT,
    location_text TEXT,
    normalized_date TEXT,
    summary TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_nie_events_key ON narrative_events(event_key);
CREATE INDEX IF NOT EXISTS idx_nie_events_actor ON narrative_events(primary_actor);
CREATE INDEX IF NOT EXISTS idx_nie_events_date ON narrative_events(normalized_date);

CREATE TABLE IF NOT EXISTS narrative_claim_event_map (
    claim_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    PRIMARY KEY(claim_id, event_id),
    FOREIGN KEY(claim_id) REFERENCES narrative_claims(id),
    FOREIGN KEY(event_id) REFERENCES narrative_events(id)
);

CREATE TABLE IF NOT EXISTS narrative_edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    src_claim_id INTEGER NOT NULL,
    dst_claim_id INTEGER NOT NULL,
    edge_type TEXT NOT NULL,
    score REAL DEFAULT 0.0,
    rationale TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(src_claim_id) REFERENCES narrative_claims(id),
    FOREIGN KEY(dst_claim_id) REFERENCES narrative_claims(id)
);
CREATE INDEX IF NOT EXISTS idx_nie_edges_src ON narrative_edges(src_claim_id);
CREATE INDEX IF NOT EXISTS idx_nie_edges_dst ON narrative_edges(dst_claim_id);
CREATE INDEX IF NOT EXISTS idx_nie_edges_type ON narrative_edges(edge_type);

CREATE TABLE IF NOT EXISTS narrative_entity_scores (
    entity_name TEXT PRIMARY KEY,
    influence_score REAL DEFAULT 0.0,
    witness_score REAL DEFAULT 0.0,
    contradiction_score REAL DEFAULT 0.0,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS narrative_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type TEXT NOT NULL,
    source_tag TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    total_units INTEGER DEFAULT 0,
    processed_units INTEGER DEFAULT 0,
    error_text TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE VIEW IF NOT EXISTS opensight_text_units AS
SELECT
    CAST(p.id AS TEXT) AS unit_id,
    CAST(p.document_id AS TEXT) AS document_id,
    p.page_number AS page_number,
    d.source_tag AS source_tag,
    COALESCE(
        NULLIF(trim(p.ocr_text), ''),
        NULLIF(trim(p.final_text), ''),
        NULLIF(trim(p.raw_text), '')
    ) AS text
FROM document_pages p
JOIN documents d ON d.id = p.document_id
WHERE COALESCE(
    NULLIF(trim(p.ocr_text), ''),
    NULLIF(trim(p.final_text), ''),
    NULLIF(trim(p.raw_text), '')
) IS NOT NULL
  AND d.status = 'done';
"""
