#!/usr/bin/env bash
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo "[opensight] starting semantic service..."
fuser -k 8010/tcp >/dev/null 2>&1 || true
.venv/bin/python semantic_service.py > /tmp/sem.log 2>&1 &
SEM_PID=$!

# Wait for semantic health
for i in {1..45}; do
  sleep 1
  if curl -sf http://localhost:8010/health > /dev/null 2>&1; then
    echo "[opensight] semantic ready (pid $SEM_PID)"
    break
  fi
  if [ $i -eq 45 ]; then echo "[opensight] semantic failed to start"; exit 1; fi
done

echo "[opensight] starting api..."
fuser -k 8000/tcp >/dev/null 2>&1 || true
.venv/bin/python -m uvicorn apps.api.main:app --host 0.0.0.0 --port 8000 > /tmp/uv.log 2>&1 &
API_PID=$!

# Wait for api health
for i in {1..15}; do
  sleep 1
  if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
    echo "[opensight] api ready (pid $API_PID)"
    break
  fi
  if [ $i -eq 15 ]; then echo "[opensight] api failed to start"; exit 1; fi
done

echo ""
echo "  semantic → http://localhost:8010/docs"
echo "  api      → http://localhost:8000/docs"
echo "  logs     → tail -f /tmp/uv.log /tmp/sem.log"
echo ""
echo "[opensight] all systems up"
echo ""
echo "  UI       → http://$(hostname -I | awk '{print $1}'):8000/ui/opensight_ui.html"
echo "  UI local → http://localhost:8000/ui/opensight_ui.html"
xdg-open http://localhost:8000/ui/opensight_ui.html 2>/dev/null &
