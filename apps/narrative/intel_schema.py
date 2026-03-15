from __future__ import annotations

INTEL_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS narrative_canonical_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key TEXT NOT NULL UNIQUE,
    event_type TEXT,
    normalized_date TEXT,
    end_date TEXT,
    location_text TEXT,
    primary_actor TEXT,
    secondary_actor TEXT,
    object_text TEXT,
    canonical_summary TEXT,
    support_count INTEGER DEFAULT 0,
    contradiction_count INTEGER DEFAULT 0,
    confidence REAL DEFAULT 0.0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_narrative_canonical_events_date
ON narrative_canonical_events(normalized_date);

CREATE INDEX IF NOT EXISTS idx_narrative_canonical_events_actor
ON narrative_canonical_events(primary_actor);

CREATE TABLE IF NOT EXISTS narrative_event_claim_map (
    event_id INTEGER NOT NULL,
    claim_id INTEGER NOT NULL,
    role TEXT DEFAULT 'support',
    PRIMARY KEY (event_id, claim_id),
    FOREIGN KEY(event_id) REFERENCES narrative_canonical_events(id),
    FOREIGN KEY(claim_id) REFERENCES narrative_claims(id)
);

CREATE TABLE IF NOT EXISTS narrative_claim_lineage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    src_claim_id INTEGER NOT NULL,
    dst_claim_id INTEGER NOT NULL,
    relation_type TEXT NOT NULL,   -- repeats | derives_from | supports | contradicts
    score REAL DEFAULT 0.0,
    rationale TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(src_claim_id) REFERENCES narrative_claims(id),
    FOREIGN KEY(dst_claim_id) REFERENCES narrative_claims(id)
);

CREATE INDEX IF NOT EXISTS idx_narrative_claim_lineage_src
ON narrative_claim_lineage(src_claim_id);

CREATE INDEX IF NOT EXISTS idx_narrative_claim_lineage_dst
ON narrative_claim_lineage(dst_claim_id);

CREATE TABLE IF NOT EXISTS narrative_temporal_anomalies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_name TEXT NOT NULL,
    event_a_id INTEGER NOT NULL,
    event_b_id INTEGER NOT NULL,
    anomaly_type TEXT NOT NULL,    -- same_date_different_locations | polarity_conflict | impossible_overlap
    score REAL DEFAULT 0.0,
    rationale TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(event_a_id) REFERENCES narrative_canonical_events(id),
    FOREIGN KEY(event_b_id) REFERENCES narrative_canonical_events(id)
);

CREATE INDEX IF NOT EXISTS idx_narrative_temporal_anomalies_actor
ON narrative_temporal_anomalies(actor_name);
"""
