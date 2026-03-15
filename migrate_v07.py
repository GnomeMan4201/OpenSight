"""
migrate_v07.py — Run once to add v0.7 tables and columns to existing opensight.db
"""
import sqlite3
from pathlib import Path

db_path = Path(__file__).parent / "opensight.db"
if not db_path.exists():
    print("No database found — will be created on first server start")
    exit(0)

conn = sqlite3.connect(str(db_path))

# Add doc_type to documents
cols = [r[1] for r in conn.execute("PRAGMA table_info(documents)")]
if "doc_type" not in cols:
    conn.execute("ALTER TABLE documents ADD COLUMN doc_type TEXT DEFAULT 'other'")
    print("Added doc_type to documents")

# Create claims table
tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
if "claims" not in tables:
    conn.execute("""
        CREATE TABLE claims (
            id TEXT PRIMARY KEY,
            document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
            page_number INTEGER NOT NULL,
            speaker_entity_id TEXT REFERENCES entities(id) ON DELETE SET NULL,
            subject_entity_id TEXT REFERENCES entities(id) ON DELETE SET NULL,
            claim_text TEXT NOT NULL,
            claim_type TEXT DEFAULT 'observation',
            sentiment TEXT DEFAULT 'neutral',
            confidence REAL DEFAULT 0.7,
            extraction_method TEXT DEFAULT 'heuristic',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX ix_claims_document_id ON claims(document_id)")
    conn.execute("CREATE INDEX ix_claims_speaker_entity ON claims(speaker_entity_id)")
    conn.execute("CREATE INDEX ix_claims_subject_entity ON claims(subject_entity_id)")
    print("Created claims table")

conn.commit()
conn.close()
print("Migration complete")
