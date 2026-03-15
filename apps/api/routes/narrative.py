"""
apps/api/routes/narrative.py
Narrative Intelligence Engine API — v0.8
"""
from __future__ import annotations
import os
from fastapi import APIRouter, BackgroundTasks, Query
from apps.narrative.engine import NarrativeEngine
from apps.narrative.models import NarrativeRebuildRequest

router = APIRouter(tags=["narrative_nie"])

def _engine() -> NarrativeEngine:
    return NarrativeEngine(
        db_path=os.getenv("OPENSIGHT_DB_PATH", "./opensight.db"),
        ollama_url=os.getenv("OLLAMA_URL", "http://127.0.0.1:11434/api/generate"),
        ollama_model=os.getenv("OLLAMA_MODEL", "phi3"),
    )

@router.get("/nie/status")
def nie_status():
    return _engine().status()

@router.post("/nie/rebuild")
def nie_rebuild(request: NarrativeRebuildRequest, background_tasks: BackgroundTasks):
    """Trigger NIE rebuild. Runs in background for large corpora."""
    engine = _engine()
    status = engine.status()
    # Run synchronously for small corpora, background for large
    if (status.get("text_units", 0) or 0) > 100:
        background_tasks.add_task(engine.rebuild,
            request.source_tag, request.max_units, request.rebuild_edges)
        return {"status": "queued", "note": "Large corpus — running in background"}
    return engine.rebuild(request.source_tag, request.max_units, request.rebuild_edges)

@router.get("/nie/dossier/{entity_name}")
def nie_dossier(entity_name: str):
    return _engine().dossier(entity_name)

@router.get("/nie/contradictions")
def nie_contradictions(
    min_score: float = Query(0.6, ge=0.0, le=1.0),
    limit: int = Query(100, ge=1, le=500),
):
    return _engine().contradictions(min_score=min_score, limit=limit)

@router.get("/nie/timeline")
def nie_timeline(
    entity: str | None = None,
    limit: int = Query(200, ge=1, le=2000),
):
    return _engine().merged_timeline(entity=entity, limit=limit)

@router.get("/nie/entities")
def nie_entities(limit: int = Query(25, ge=1, le=100)):
    return {"entities": _engine().top_entities(limit=limit)}

@router.get("/nie/claims")
def nie_claims(
    subject: str | None = None,
    claim_type: str | None = None,
    limit: int = Query(50, ge=1, le=500),
):
    """List narrative claims with optional filtering."""
    engine = _engine()
    conn = engine.connect()
    try:
        sql = """
            SELECT c.*,e.document_id,e.page_number,e.source_tag
            FROM narrative_claims c
            JOIN narrative_evidence_spans e ON e.id=c.evidence_span_id
            WHERE 1=1
        """
        params = []
        if subject:
            sql += " AND (lower(c.subject)=lower(?) OR lower(c.object)=lower(?))"
            params += [subject, subject]
        if claim_type:
            sql += " AND c.claim_type=?"
            params.append(claim_type)
        sql += " ORDER BY c.extractor_confidence DESC, c.id DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        return {"total": len(rows), "claims": [dict(r) for r in rows]}
    finally:
        conn.close()
