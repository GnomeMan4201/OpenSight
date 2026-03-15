from __future__ import annotations
from pathlib import Path
from datetime import datetime
import uuid

from modules.entity_extraction.extract_entities import extract_entities

def ingest_text_file(path: str) -> dict:
    p = Path(path)
    text = p.read_text(encoding="utf-8", errors="ignore")
    return {
        "id": str(uuid.uuid4()),
        "title": p.name,
        "path": str(p.resolve()),
        "ingested_at": datetime.utcnow().isoformat() + "Z",
        "text": text,
        "entities": extract_entities(text),
    }
