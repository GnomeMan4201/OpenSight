"""
OpenSight — Provenance API Routes
apps/api/routes/provenance.py

Mount in apps/api/main.py:
    from apps.api.routes.provenance import router as provenance_router
    app.include_router(provenance_router, prefix="/api/v1")
"""

from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from apps.narrative.provenance_engine import NarrativeProvenanceEngine

router = APIRouter(tags=["narrative-provenance"])
_engine = NarrativeProvenanceEngine()


# ── Build ────────────────────────────────────────────────────

@router.post("/narrative-provenance/build")
def build_provenance(
    source_tag: Optional[str] = Query(default=None, description="Filter to a specific corpus")
):
    """
    Run the full provenance pipeline for a source_tag.
    Detects seeds, builds thread DAGs, scores mutation, finds brokers,
    classifies contradictions.
    """
    result = _engine.build(source_tag=source_tag)
    return result


# ── Threads ──────────────────────────────────────────────────

@router.get("/narrative-provenance/threads")
def list_threads(
    source_tag: Optional[str] = Query(default=None),
):
    """
    Return all narrative threads, ordered by coherence score (desc).
    Each thread represents one coherent propagation chain.
    """
    return _engine.get_threads(source_tag=source_tag)


@router.get("/narrative-provenance/threads/{thread_id}")
def get_thread(thread_id: int):
    """
    Full provenance graph for a single thread:
    thread metadata + nodes (with generation, mutation) + directed edges.
    """
    data = _engine.get_thread_provenance(thread_id)
    if not data.get("thread"):
        raise HTTPException(status_code=404, detail="Thread not found")
    return data


# ── Brokers ──────────────────────────────────────────────────

@router.get("/narrative-provenance/brokers")
def list_brokers(
    source_tag: Optional[str] = Query(default=None),
    broker_type: Optional[str] = Query(
        default=None,
        description="Filter by type: amplifier | originator | bridge | suppressor | relay"
    ),
):
    """
    Narrative brokers ranked by betweenness centrality.
    Brokers are actors appearing at the junctions of propagation paths.

    Types:
    - originator   — high out-degree, no in-degree, earliest appearance
    - amplifier    — re-broadcasts many more claims than it originates
    - bridge       — connects otherwise separate narrative threads
    - suppressor   — claims terminate here (no downstream propagation)
    - relay        — general conduit actor
    """
    brokers = _engine.get_brokers(source_tag=source_tag)
    if broker_type:
        brokers = [b for b in brokers if b.get("broker_type") == broker_type]
    return brokers


# ── Contradictions ───────────────────────────────────────────

@router.get("/narrative-provenance/contradictions")
def list_contradictions(
    source_tag: Optional[str] = Query(default=None),
    contradiction_type: Optional[str] = Query(
        default=None,
        description="Filter by type: factual | temporal | identity | causal | presence"
    ),
    min_severity: float = Query(default=0.0, ge=0.0, le=1.0),
):
    """
    Typed contradictions between claims, ranked by severity.

    Types:
    - factual     — same actor, same topic, opposing assertions
    - temporal    — same actor/topic, significant date gap
    - identity    — very similar claim attributed to different actors
    - presence    — presence/location conflict between claims
    - causal      — same event, conflicting cause attribution
    """
    rows = _engine.get_contradictions(source_tag=source_tag)
    if contradiction_type:
        rows = [r for r in rows if r.get("contradiction_type") == contradiction_type]
    if min_severity > 0:
        rows = [r for r in rows if r.get("severity", 0) >= min_severity]
    return rows


# ── Manipulation flags ───────────────────────────────────────

@router.get("/narrative-provenance/manipulation-flags")
def manipulation_flags(
    source_tag: Optional[str] = Query(default=None),
):
    """
    Threads flagged for possible narrative manipulation:
    high mutation rate + multi-actor amplification + low coherence.

    This is the highest-signal investigative endpoint.
    """
    return _engine.get_manipulation_flags(source_tag=source_tag)


# ── Overview ─────────────────────────────────────────────────

@router.get("/narrative-provenance/overview")
def provenance_overview(
    source_tag: Optional[str] = Query(default=None),
):
    """
    System-level provenance summary.
    """
    threads       = _engine.get_threads(source_tag)
    brokers       = _engine.get_brokers(source_tag)
    contradictions = _engine.get_contradictions(source_tag)
    flags         = _engine.get_manipulation_flags(source_tag)

    tight_threads = [t for t in threads if t.get("coherence_score", 0) >= 0.75]
    top_broker    = brokers[0] if brokers else None
    top_contra    = contradictions[0] if contradictions else None

    return {
        "source_tag":           source_tag,
        "total_threads":        len(threads),
        "tight_threads":        len(tight_threads),
        "manipulation_flags":   len(flags),
        "total_brokers":        len(brokers),
        "total_contradictions": len(contradictions),
        "top_broker":           top_broker,
        "top_contradiction":    top_contra,
        "flagged_threads":      flags[:5],
    }
