#!/bin/bash
# scripts/reingest.sh <source_tag>
# Re-runs entity extraction on all documents with the given source_tag.
# Usage: scripts/reingest.sh opensight-legal-demo
set -e
cd "$(dirname "$0")/.."

SOURCE_TAG="${1:-}"
if [ -z "$SOURCE_TAG" ]; then
  echo "Usage: scripts/reingest.sh <source_tag>"
  echo "Available source tags:"
  sqlite3 opensight.db "SELECT DISTINCT source_tag FROM documents ORDER BY source_tag"
  exit 1
fi

echo "Re-ingesting documents with source_tag='$SOURCE_TAG'..."
DOC_IDS=$(sqlite3 opensight.db "SELECT id FROM documents WHERE source_tag='$SOURCE_TAG'")
COUNT=$(echo "$DOC_IDS" | grep -c . || true)
echo "Found $COUNT documents"

for ID in $DOC_IDS; do
  echo -n "  Reingesting $ID ... "
  STATUS=$(curl -s -X POST "http://localhost:8000/api/v1/documents/$ID/reingest" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status','?'))" 2>/dev/null)
  echo "$STATUS"
done

echo ""
echo "Rebuilding co-occurrence relationships..."
python3 - << 'PYEOF'
import sqlite3, uuid
from collections import defaultdict
conn = sqlite3.connect("opensight.db")
conn.execute("DELETE FROM entity_relationships")
doc_mentions = defaultdict(list)
for eid, did in conn.execute("SELECT entity_id, document_id FROM mentions").fetchall():
    doc_mentions[did].append(eid)
pairs = defaultdict(lambda: [0, 0])
for did, eids in doc_mentions.items():
    unique = list(set(eids))
    for i in range(len(unique)):
        for j in range(i+1, len(unique)):
            k = (min(unique[i], unique[j]), max(unique[i], unique[j]))
            pairs[k][0] += 1; pairs[k][1] += 1
n = 0
for (a, b), (w, dc) in pairs.items():
    try:
        conn.execute("INSERT INTO entity_relationships (id,entity_a_id,entity_b_id,weight,doc_count,created_at,updated_at) VALUES (?,?,?,?,?,datetime('now'),datetime('now'))", (str(uuid.uuid4()), a, b, w, dc))
        n += 1
    except: pass
conn.commit()
print(f"✓ {conn.execute('SELECT COUNT(*) FROM entities').fetchone()[0]} entities, {n} relationships rebuilt")
conn.close()
PYEOF
echo ""
echo "Running canonicalization pass..."
python3 - << PYEOF
import sys
sys.path.insert(0, '.')
from apps.api.database import SessionLocal
from apps.api.services.canonicalize import run_canonicalization
db = SessionLocal()
try:
    result = run_canonicalization(db, commit=True)
    print(f"  noise_deleted={result['noise_deleted']} merged={result['cross_type_merged']+result['judge_prefix_resolved']} type_fixes={result['type_fixes']} entities={result['entities_remaining']}")
finally:
    db.close()
PYEOF

echo "Rebuilding relationships from canonical mentions..."
python3 - << PYEOF
import sqlite3, uuid
from collections import defaultdict
from itertools import combinations
conn = sqlite3.connect("opensight.db")
conn.execute("DELETE FROM entity_relationships")
doc_to_entities = defaultdict(list)
for eid, did in conn.execute("SELECT entity_id, document_id FROM mentions").fetchall():
    doc_to_entities[did].append(eid)
pairs = defaultdict(lambda: [0, 0])
for did, eids in doc_to_entities.items():
    for a, b in combinations(sorted(set(eids)), 2):
        pairs[(a,b)][0] += 1; pairs[(a,b)][1] += 1
n = 0
for (a, b), (w, dc) in pairs.items():
    conn.execute("INSERT INTO entity_relationships (id,entity_a_id,entity_b_id,weight,doc_count,relationship_type,confidence,created_at,updated_at) VALUES (?,?,?,?,?,'co_occurrence',0.5,datetime('now'),datetime('now'))", (str(uuid.uuid4()), a, b, w, dc))
    n += 1
conn.commit()
e = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
print(f"  {e} entities, {n} relationships")
conn.close()
PYEOF

echo "Done. Run scripts/entity_audit.sh to see updated entities."
