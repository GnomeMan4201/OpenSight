#!/usr/bin/env bash
set -e

UI="opensight_ui.html"

echo "[1/3] Updating API port references..."

# replace common API base patterns
sed -i 's|127.0.0.1:8000|127.0.0.1:8010|g' $UI
sed -i 's|localhost:8000|127.0.0.1:8010|g' $UI

echo "[2/3] Ensure explicit API base constant exists..."

python3 <<'PY'
from pathlib import Path
p = Path("opensight_ui.html")
text = p.read_text()

if "const API_BASE" not in text:
    text = text.replace(
        "<script>",
        '<script>\nconst API_BASE = "http://127.0.0.1:8010";\n',
        1
    )

p.write_text(text)
print("API base constant ensured.")
PY

echo "[3/3] Verify patch..."
grep -n "8010" opensight_ui.html || true

echo "Patch complete."
