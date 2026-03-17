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
echo "Done. Run scripts/entity_audit.sh to see updated entities."
