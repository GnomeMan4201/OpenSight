#!/bin/bash
# scripts/backup.sh — timestamped backup of DB + key source files
set -e
cd "$(dirname "$0")/.."

STAMP=$(date +%Y%m%d_%H%M%S)
DIR="backups/$STAMP"
mkdir -p "$DIR"

# DB
cp opensight.db "$DIR/opensight.db"
echo "✓ DB backed up"

# Key source files
for f in \
  apps/api/services/entity_extraction.py \
  apps/api/routes/graph_metrics.py \
  apps/api/routes/graph_insights.py \
  apps/api/routes/entities.py \
  apps/api/routes/graph.py \
  apps/api/models.py; do
  [ -f "$f" ] && cp "$f" "$DIR/$(echo $f | tr '/' '_')" && echo "✓ $f"
done

# Git snapshot
git add -A 2>/dev/null && git commit -m "backup: $STAMP" 2>/dev/null && echo "✓ git commit" || true

echo ""
echo "Backup complete → $DIR"
