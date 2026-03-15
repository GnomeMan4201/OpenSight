"""
apps/api/services/semantic_bridge.py

Semantic Bridge — connects the semantic microservice (:8010) into the
OpenSight pipeline.

Responsibilities:
  1. Index document pages into semantic service at ingestion time
  2. Index entity contexts (what text surrounds each entity)
  3. Index claim texts
  4. Provide hybrid search: keyword FTS5 + semantic similarity + graph boost
  5. Semantic similarity between two arbitrary texts (for claim correlation)
  6. Entity context disambiguation (is this "John Smith" the same person?)

The semantic service API (on :8010) must support:
  POST /api/v1/semantic/index   {"text": str, "metadata": dict}
  POST /api/v1/semantic/search  {"text": str, "k": int}

All calls are fire-and-forget during ingestion (failures are logged, not fatal).
"""

from __future__ import annotations

import json
import logging
import urllib.request
import urllib.error
from typing import Optional

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

SEMANTIC_BASE = "http://127.0.0.1:8010"
TIMEOUT = 10  # seconds per call during ingestion


# ── Low-level HTTP helpers ────────────────────────────────────────────────────

def _post(path: str, payload: dict, timeout: int = TIMEOUT) -> Optional[dict]:
    """POST to semantic service. Returns parsed JSON or None on failure."""
    try:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"{SEMANTIC_BASE}{path}",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.URLError as e:
        log.debug("[semantic] service unavailable: %s", e)
        return None
    except Exception as e:
        log.warning("[semantic] call failed (%s): %s", path, e)
        return None


def is_available() -> bool:
    """Quick health check — returns True if semantic service is reachable."""
    result = _post("/search", {"text": "test", "k": 1})
    return result is not None


# ── Indexing ──────────────────────────────────────────────────────────────────

def index_page(
    document_id: str,
    page_number: int,
    text: str,
    doc_type: str = "other",
    source_tag: str = "",
    filename: str = "",
) -> bool:
    """Index a single document page. Called during Stage 5 of ingestion."""
    if not text or len(text.strip()) < 20:
        return False

    # Chunk long pages into overlapping 512-char segments
    chunks = _chunk_text(text, size=512, overlap=64)
    success = 0
    for i, chunk in enumerate(chunks):
        result = _post("/index", {
            "text": chunk,
            "metadata": {
                "type":        "page",
                "document_id": document_id,
                "page_number": page_number,
                "chunk":       i,
                "doc_type":    doc_type,
                "source_tag":  source_tag,
                "filename":    filename,
            }
        })
        if result:
            success += 1

    log.debug("[semantic] indexed doc=%s page=%d chunks=%d/%d",
              document_id[:8], page_number, success, len(chunks))
    return success > 0


def index_entity(
    entity_id: str,
    entity_name: str,
    entity_type: str,
    context_texts: list[str],
    document_ids: list[str],
) -> bool:
    """
    Index an entity's combined context — all the text surrounding its mentions.
    This enables semantic entity similarity: 'is this Maxwell the same as that Maxwell?'
    """
    combined = " | ".join(ctx[:200] for ctx in context_texts[:5] if ctx)
    if not combined.strip():
        return False

    result = _post("/index", {
        "text": f"{entity_name}: {combined}",
        "metadata": {
            "type":        "entity",
            "entity_id":   entity_id,
            "entity_name": entity_name,
            "entity_type": entity_type,
            "document_ids": document_ids[:10],
        }
    })
    return result is not None


def index_claim(
    claim_id: str,
    claim_text: str,
    claim_type: str,
    speaker_name: Optional[str],
    subject_name: Optional[str],
    document_id: str,
    doc_type: str = "other",
) -> bool:
    """Index a claim for semantic similarity search and contradiction detection."""
    if not claim_text.strip():
        return False

    enriched = claim_text
    if speaker_name:
        enriched = f"{speaker_name} states: {claim_text}"
    if subject_name:
        enriched = f"{enriched} [about: {subject_name}]"

    result = _post("/index", {
        "text": enriched,
        "metadata": {
            "type":         "claim",
            "claim_id":     claim_id,
            "claim_type":   claim_type,
            "speaker_name": speaker_name,
            "subject_name": subject_name,
            "document_id":  document_id,
            "doc_type":     doc_type,
        }
    })
    return result is not None


# ── Search ────────────────────────────────────────────────────────────────────

def semantic_search(
    query: str,
    k: int = 10,
    filter_type: Optional[str] = None,
) -> list[dict]:
    """
    Search the semantic index. Returns list of results with score + metadata.
    filter_type: 'page' | 'entity' | 'claim' | None (all)
    """
    result = _post("/search", {"text": query, "k": k * 2}, timeout=15)
    if not result:
        return []

    results = result.get("results", [])

    if filter_type:
        results = [r for r in results
                   if r.get("metadata", {}).get("type") == filter_type]

    return results[:k]


def find_similar_claims(
    claim_text: str,
    k: int = 5,
    min_score: float = 0.6,
) -> list[dict]:
    """
    Find semantically similar claims to the given text.
    Used for claim propagation and contradiction detection.
    """
    results = semantic_search(claim_text, k=k * 2, filter_type="claim")
    return [r for r in results if r.get("score", 0) >= min_score][:k]


def find_similar_entities(
    entity_name: str,
    entity_context: str,
    k: int = 5,
    min_score: float = 0.65,
) -> list[dict]:
    """
    Find semantically similar entities — for disambiguation.
    Two entities with different names but similar contexts may be the same person.
    """
    query = f"{entity_name}: {entity_context[:200]}"
    results = semantic_search(query, k=k * 2, filter_type="entity")
    # Exclude exact name match (that's just the entity itself)
    filtered = [r for r in results
                if r.get("metadata", {}).get("entity_name", "").lower() != entity_name.lower()
                and r.get("score", 0) >= min_score]
    return filtered[:k]


def compute_similarity(text_a: str, text_b: str) -> float:
    """
    Compute semantic similarity between two texts.
    Uses the search service as a proxy: index A, search for B, get score.
    Returns 0.0 on failure.
    """
    # Index text_a temporarily
    _post("/index", {"text": text_a, "metadata": {"type": "temp_similarity"}})
    # Search for text_b
    result = _post("/search", {"text": text_b, "k": 1}, timeout=15)
    if not result or not result.get("results"):
        return 0.0
    return float(result["results"][0].get("score", 0.0))


# ── Text chunking ─────────────────────────────────────────────────────────────

def _chunk_text(text: str, size: int = 512, overlap: int = 64) -> list[str]:
    """
    Split text into overlapping chunks for indexing.
    Tries to break on sentence boundaries.
    """
    if len(text) <= size:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + size
        if end >= len(text):
            chunks.append(text[start:])
            break

        # Try to break at a sentence boundary
        boundary = text.rfind('. ', start, end)
        if boundary > start + size // 2:
            end = boundary + 1
        chunks.append(text[start:end])
        start = end - overlap

    return [c.strip() for c in chunks if c.strip()]
