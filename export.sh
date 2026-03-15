#!/usr/bin/env bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUT="$DIR/exports/$TIMESTAMP"
mkdir -p "$OUT"
echo "[export] writing to $OUT"

for TAG in "epstein-fbi" "epstein-fbi-imported-20260312040226" "pascal"; do
  SLUG="${TAG//\//_}"
  curl -sf -X POST http://localhost:8000/api/v1/bundles/export \
    -d "source_tag=$TAG&title=OpenSight Export&author=bad_banana&include_documents=true" \
    -o "$OUT/bundle_${SLUG}.osight" && echo "  -> bundle_${SLUG}.osight" || echo "  FAILED: $TAG"
done

declare -A ENDPOINTS=(
  [documents]="http://localhost:8000/api/v1/documents"
  [entities]="http://localhost:8000/api/v1/entities?limit=1000"
  [claims]="http://localhost:8000/api/v1/claims"
  [contradictions]="http://localhost:8000/api/v1/claims/contradictions"
  [relationships]="http://localhost:8000/api/v1/graph/relationships?limit=500"
  [timeline]="http://localhost:8000/api/v1/analysis/timeline"
  [clusters]="http://localhost:8000/api/v1/analysis/clusters"
  [leads]="http://localhost:8000/api/v1/analysis/leads"
  [nie_entities]="http://localhost:8000/api/v1/nie/entities"
  [nie_claims]="http://localhost:8000/api/v1/nie/claims"
  [nie_timeline]="http://localhost:8000/api/v1/nie/timeline"
  [duplicates]="http://localhost:8000/api/v1/entities/duplicates"
)

for LABEL in "${!ENDPOINTS[@]}"; do
  curl -sf "${ENDPOINTS[$LABEL]}" -o "$OUT/${LABEL}.json" && echo "  -> ${LABEL}.json" || echo "  FAILED: $LABEL"
done

echo "[export] done -> $OUT"
ls -lh "$OUT"
