#!/bin/bash
# scripts/start.sh — start all OpenSight services
set -e
cd "$(dirname "$0")/.."

echo "Starting OpenSight..."

# Kill any existing instances
pkill -f "uvicorn apps.api.main" 2>/dev/null && sleep 1 || true
pkill -f "semantic_service.py" 2>/dev/null && sleep 1 || true

# Activate venv
source .venv/bin/activate

# Start semantic service (port 8010)
echo "→ Starting semantic service (port 8010)..."
nohup python3 semantic_service.py > /tmp/opensight_semantic.log 2>&1 &
SEMANTIC_PID=$!

# Start API (port 8000)
echo "→ Starting API (port 8000)..."
nohup uvicorn apps.api.main:app --host 0.0.0.0 --port 8000 --reload \
  > /tmp/opensight_api.log 2>&1 &
API_PID=$!

# Wait for startup
sleep 3

# Health checks
API_STATUS=$(curl -s http://localhost:8000/health | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status','?'))" 2>/dev/null)
SEM_STATUS=$(curl -s http://localhost:8010/health | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status','?'))" 2>/dev/null)

echo ""
echo "  API (8000):      $API_STATUS  [pid $API_PID]"
echo "  Semantic (8010): $SEM_STATUS  [pid $SEMANTIC_PID]"
echo ""
echo "Logs: tail -f /tmp/opensight_api.log /tmp/opensight_semantic.log"
echo "Stop: scripts/stop.sh"
