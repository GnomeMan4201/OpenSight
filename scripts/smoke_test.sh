#!/bin/bash
# scripts/smoke_test.sh — quick endpoint verification
set -e
cd "$(dirname "$0")/.."

BASE="http://localhost:8000/api/v1"
PASS=0; FAIL=0

check() {
  local name="$1"
  local url="$2"
  local expect="$3"
  local result
  result=$(curl -s "$url" 2>/dev/null)
  if echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); assert $expect" 2>/dev/null; then
    echo "  ✓ $name"
    PASS=$((PASS+1))
  else
    echo "  ✗ $name  →  $(echo "$result" | head -c 120)"
    FAIL=$((FAIL+1))
  fi
}

echo "=== OpenSight Smoke Tests ==="
echo ""
echo "Services:"
check "API health"           "http://localhost:8000/health"                          "d['status']=='ok'"
check "Semantic health"      "http://localhost:8010/health"                          "d['status']=='ok'"

echo ""
echo "Entities:"
check "Entity list"          "$BASE/entities?page_size=5"                            "d.get('total',0) > 0"
check "Entity min_mentions"  "$BASE/entities?min_mentions=5"                         "d.get('total',0) > 0"

echo ""
echo "Graph:"
check "Graph metrics"        "$BASE/graph/metrics"                                   "d['node_count'] > 0"
check "Graph metrics map"    "$BASE/graph/metrics/map?metric=betweenness"            "'scores' in d"
check "Graph metrics top"    "$BASE/graph/metrics/top?metric=influence&top_n=5"      "len(d['entities']) > 0"
check "Graph communities"    "$BASE/graph/communities"                               "d.get('community_count',0) >= 0"

echo ""
echo "Documents:"
check "Documents list"       "$BASE/documents"                                       "isinstance(d,dict) or isinstance(d,list)"

echo ""
echo "Analytics:"
check "Broker paths"

echo ""
echo "Dossier:"
DOSSIER_EID=$(sqlite3 opensight.db "SELECT id FROM entities ORDER BY mention_count DESC LIMIT 1" 2>/dev/null || echo "")
if [ -n "$DOSSIER_EID" ]; then
  check "Entity dossier"        "$BASE/entities/$DOSSIER_EID/dossier"                   "'entity' in d and 'metrics' in d and 'relationships' in d"
  check "Dossier 404"           "$BASE/entities/00000000-0000-0000-0000-000000000000/dossier" "d.get('detail') is not None"
fi         "$BASE/graph/broker-paths/$(sqlite3 opensight.db "SELECT id FROM entities ORDER BY mention_count DESC LIMIT 1")" \
                                                                                      "isinstance(d,dict) or isinstance(d,list)"

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
