#!/bin/bash
# scripts/entity_audit.sh — print current entity quality summary
cd "$(dirname "$0")/.."
python3 - <<'PY'
import sqlite3
from collections import defaultdict
conn = sqlite3.connect("opensight.db")

rows = conn.execute("""
    SELECT entity_type, canonical_name, mention_count, confidence
    FROM entities ORDER BY entity_type, mention_count DESC
""").fetchall()

rel_count = conn.execute("SELECT COUNT(*) FROM entity_relationships").fetchone()[0]
doc_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]

by_type = defaultdict(list)
for r in rows:
    by_type[r[0]].append(r)

print(f"\n{'='*60}")
print(f"OpenSight Entity Audit — {len(rows)} entities, {rel_count} relationships, {doc_count} documents")
print(f"{'='*60}")

for t in ["Organization", "Person", "Location"]:
    if t not in by_type: continue
    entries = by_type[t]
    print(f"\n{t} ({len(entries)}):")
    for r in entries:
        bar = "█" * min(20, r[2] // 5)
        print(f"  {r[1]:40} m={r[2]:3d} conf={r[3]:.2f} {bar}")

conn.close()
PY
