"""migrate_v08.py — Run once for v0.8 schema additions"""
import sqlite3
from pathlib import Path

conn = sqlite3.connect(str(Path(__file__).parent / "opensight.db"))

# semantic_weight on entity_relationships
cols = [r[1] for r in conn.execute("PRAGMA table_info(entity_relationships)")]
if "semantic_weight" not in cols:
    conn.execute("ALTER TABLE entity_relationships ADD COLUMN semantic_weight REAL DEFAULT 0.0")
    print("Added semantic_weight to entity_relationships")

# embedding_indexed flag on document_pages
cols = [r[1] for r in conn.execute("PRAGMA table_info(document_pages)")]
if "embedding_indexed" not in cols:
    conn.execute("ALTER TABLE document_pages ADD COLUMN embedding_indexed INTEGER DEFAULT 0")
    print("Added embedding_indexed to document_pages")

conn.commit()
conn.close()
print("v0.8 migration complete")
