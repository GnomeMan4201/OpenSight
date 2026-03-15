#!/usr/bin/env bash
echo "[opensight] stopping..."
fuser -k 8000/tcp 2>/dev/null && echo "  api stopped" || echo "  api was not running"
fuser -k 8010/tcp 2>/dev/null && echo "  semantic stopped" || echo "  semantic was not running"
echo "[opensight] done"
