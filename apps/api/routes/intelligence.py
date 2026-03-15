"""
apps/api/routes/intelligence.py

Intelligence API endpoints — v0.8

GET  /api/v1/intelligence/search?q=...        hybrid semantic+keyword+graph search
GET  /api/v1/intelligence/influence           actor influence map
GET  /api/v1/intelligence/correlate-claims    cross-document claim correlation
GET  /api/v1/intelligence/disambiguate        semantic entity disambiguation candidates
POST /api/v1/intelligence/enrich/{doc_id}     trigger semantic graph enrichment
GET  /api/v1/intelligence/status              semantic service health + index stats
"""

from fastapi import APIRouter, Depends, Query, Path, BackgroundTasks
from sqlalchemy.orm import Session

from apps.api.database import get_db
from apps.api.services.semantic_bridge import is_available, semantic_search, index_page
from apps.api.services.intelligence import (
    hybrid_search,
    enrich_graph_semantically,
    correlate_claims,
    find_disambiguation_candidates,
    compute_influence_map,
)
from apps.api.models import Document, DocumentPage

router = APIRouter(tags=["intelligence"])


@router.get("/intelligence/status")
def intelligence_status(db: Session = Depends(get_db)):
    """Health check for the semantic service + index statistics."""
    available = is_available()

    # Quick semantic search to get index size estimate
    index_size = 0
    if available:
        result = semantic_search("the", k=1000)
        index_size = len(result)

    return {
        "semantic_service": "online" if available else "offline",
        "semantic_url": "http://127.0.0.1:8010",
        "index_size_estimate": index_size,
        "features": {
            "hybrid_search":    available,
            "claim_correlation": True,
            "influence_map":    True,
            "disambiguation":   available,
            "graph_enrichment": available,
        }
    }


@router.get("/intelligence/search")
def intelligence_search(
    q:              str   = Query(..., min_length=2),
    k:              int   = Query(20, ge=1, le=100),
    semantic_w:     float = Query(0.6, ge=0.0, le=1.0),
    keyword_w:      float = Query(0.3, ge=0.0, le=1.0),
    graph_w:        float = Query(0.1, ge=0.0, le=1.0),
    db: Session = Depends(get_db),
):
    """
    Hybrid search: semantic vector + FTS5 keyword + graph centrality boost.
    Weights must sum to 1.0 (will be normalized if not).
    """
    # Normalize weights
    total = semantic_w + keyword_w + graph_w
    if total > 0:
        semantic_w /= total
        keyword_w  /= total
        graph_w    /= total

    results = hybrid_search(
        query=q, db=db, k=k,
        semantic_weight=semantic_w,
        keyword_weight=keyword_w,
        graph_weight=graph_w,
    )
    return {
        "query": q,
        "total": len(results),
        "weights": {"semantic": round(semantic_w,3), "keyword": round(keyword_w,3), "graph": round(graph_w,3)},
        "results": results,
    }


@router.get("/intelligence/influence")
def influence_map(
    top_n: int = Query(25, ge=5, le=100),
    db: Session = Depends(get_db),
):
    """
    Multi-signal actor influence scoring.
    Combines graph degree, bridge position, claim authority, document spread.
    """
    scores = compute_influence_map(db, top_n=top_n)
    return {
        "total": len(scores),
        "signals": ["degree", "doc_spread", "claim_target", "claim_authority", "bridge"],
        "actors": scores,
    }


@router.get("/intelligence/correlate-claims")
def correlate(
    min_similarity: float = Query(0.65, ge=0.3, le=0.99),
    db: Session = Depends(get_db),
):
    """
    Cross-document claim correlation, contradiction detection, propagation chains.
    Uses semantic similarity when service is available, structural fallback otherwise.
    """
    return correlate_claims(db, min_similarity=min_similarity)


@router.get("/intelligence/disambiguate")
def disambiguate(
    min_score: float = Query(0.72, ge=0.5, le=0.99),
    db: Session = Depends(get_db),
):
    """
    Find entity pairs that are string-different but semantically similar.
    These may be the same real-world entity described differently across documents.
    """
    candidates = find_disambiguation_candidates(db, min_semantic_score=min_score)
    return {
        "total": len(candidates),
        "min_score": min_score,
        "candidates": candidates,
        "note": "Candidates with high scores likely refer to the same entity. Use /api/v1/entities/merge to consolidate."
    }


@router.post("/intelligence/enrich/{document_id}")
def enrich_document(
    document_id: str = Path(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
):
    """
    Trigger semantic graph enrichment for a document.
    Finds cross-document semantic edges not captured by co-occurrence alone.
    Runs in background.
    """
    doc = db.query(Document).filter_by(id=document_id).first()
    if not doc:
        return {"error": "Document not found"}

    if not is_available():
        return {"error": "Semantic service offline", "url": "http://127.0.0.1:8010"}

    background_tasks.add_task(enrich_graph_semantically, db, document_id)
    return {"status": "enriching", "document_id": document_id, "filename": doc.filename}


@router.post("/intelligence/index-corpus")
def index_corpus(
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(get_db),
):
    """
    Index all done documents into the semantic service.
    Use this after first connecting the semantic service to an existing corpus.
    """
    if not is_available():
        return {"error": "Semantic service offline"}

    docs = db.query(Document).filter_by(status="done").all()

    def _run():
        total = 0
        for doc in docs:
            pages = db.query(DocumentPage).filter_by(document_id=doc.id).all()
            for page in pages:
                if page.final_text:
                    ok = index_page(
                        document_id=doc.id,
                        page_number=page.page_number,
                        text=page.final_text,
                        doc_type=doc.doc_type,
                        source_tag=doc.source_tag,
                        filename=doc.filename,
                    )
                    if ok:
                        total += 1
        import logging
        logging.getLogger(__name__).info("[index_corpus] indexed %d pages", total)

    background_tasks.add_task(_run)
    total_pages = sum(
        db.query(DocumentPage).filter_by(document_id=d.id).count()
        for d in docs
    )
    return {
        "status": "indexing",
        "documents": len(docs),
        "estimated_pages": total_pages,
    }
