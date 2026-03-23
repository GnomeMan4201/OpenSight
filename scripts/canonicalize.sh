#!/bin/bash
# scripts/canonicalize.sh — run full canonicalization pass on demand
# Usage: bash scripts/canonicalize.sh
set -e
cd "$(dirname "$0")/.."
source .venv/bin/activate

python3 - << 'PY'
import sys
sys.path.insert(0, '.')
from apps.api.database import SessionLocal
from apps.api.services.canonicalize import run_canonicalization

db = SessionLocal()
try:
    print("Running canonicalization pass...")
    summary = run_canonicalization(db, commit=True)
    print(f"  noise_deleted:         {summary['noise_deleted']}")
    print(f"  cross_type_merged:     {summary['cross_type_merged']}")
    print(f"  judge_prefix_resolved: {summary['judge_prefix_resolved']}")
    print(f"  explicit_merged:       {summary['explicit_merged']}")
    print(f"  type_fixes:            {summary['type_fixes']}")
    print(f"  entities_remaining:    {summary['entities_remaining']}")
    print("Done.")
finally:
    db.close()
PY
