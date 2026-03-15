"""
apps/api/routes/claims.py

Claim/statement endpoints for narrative intelligence.

GET  /api/v1/claims                       — list claims, filterable
GET  /api/v1/claims/{id}                  — single claim
GET  /api/v1/claims/propagation           — claim chains across documents
GET  /api/v1/claims/contradictions        — conflicting claims about same subject
"""

from __future__ import annotations
from collections import defaultdict
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from apps.api.database import get_db
from apps.api.models import Claim, Entity, Document

router = APIRouter(tags=["claims"])


def _claim_out(c: Claim, db: Session) -> dict:
    speaker = db.query(Entity).filter_by(id=c.speaker_entity_id).first() if c.speaker_entity_id else None
    subject = db.query(Entity).filter_by(id=c.subject_entity_id).first() if c.subject_entity_id else None
    doc     = db.query(Document).filter_by(id=c.document_id).first()
    return {
        "id":           c.id,
        "document_id":  c.document_id,
        "document_name": doc.filename if doc else None,
        "doc_type":     doc.doc_type if doc else None,
        "page_number":  c.page_number,
        "speaker":      {"id": speaker.id, "name": speaker.canonical_name, "type": speaker.entity_type} if speaker else None,
        "subject":      {"id": subject.id, "name": subject.canonical_name, "type": subject.entity_type} if subject else None,
        "claim_text":   c.claim_text,
        "claim_type":   c.claim_type,
        "sentiment":    c.sentiment,
        "confidence":   c.confidence,
        "method":       c.extraction_method,
        "created_at":   c.created_at.isoformat() if c.created_at else None,
    }


@router.get("/claims")
def list_claims(
    speaker_id:  str = Query(None),
    subject_id:  str = Query(None),
    document_id: str = Query(None),
    claim_type:  str = Query(None),
    sentiment:   str = Query(None),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0),
    limit:       int = Query(100, ge=1, le=500),
    offset:      int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    q = db.query(Claim).filter(Claim.confidence >= min_confidence)
    if speaker_id:
        q = q.filter(Claim.speaker_entity_id == speaker_id)
    if subject_id:
        q = q.filter(Claim.subject_entity_id == subject_id)
    if document_id:
        q = q.filter(Claim.document_id == document_id)
    if claim_type:
        q = q.filter(Claim.claim_type == claim_type)
    if sentiment:
        q = q.filter(Claim.sentiment == sentiment)

    total = q.count()
    claims = q.order_by(Claim.confidence.desc()).offset(offset).limit(limit).all()
    return {
        "total": total,
        "items": [_claim_out(c, db) for c in claims],
    }


@router.get("/claims/propagation")
def claim_propagation(
    subject_id: str = Query(None, description="Filter to claims about this entity"),
    min_docs:   int = Query(2, ge=2, description="Minimum documents a claim appears in"),
    db: Session = Depends(get_db),
):
    """
    Find claims that propagate across multiple documents.
    Groups by subject entity + rough semantic similarity (same subject, same claim_type).
    Returns chains showing how a claim spreads through the document corpus.
    """
    q = db.query(Claim)
    if subject_id:
        q = q.filter(Claim.subject_entity_id == subject_id)

    all_claims = q.all()

    # Group by (subject_entity_id, claim_type) — same subject, same type of claim
    groups: dict[tuple, list[Claim]] = defaultdict(list)
    for c in all_claims:
        key = (c.subject_entity_id, c.claim_type)
        groups[key].append(c)

    chains = []
    for (subject_id_key, claim_type), group in groups.items():
        doc_ids = {c.document_id for c in group}
        if len(doc_ids) < min_docs:
            continue

        subject = db.query(Entity).filter_by(id=subject_id_key).first() if subject_id_key else None

        # Sort by document created_at to get propagation order
        docs_info = {}
        for c in group:
            doc = db.query(Document).filter_by(id=c.document_id).first()
            if doc:
                docs_info[doc.id] = {
                    "document_id": doc.id,
                    "filename": doc.filename,
                    "doc_type": doc.doc_type,
                    "created_at": doc.created_at.isoformat() if doc.created_at else None,
                }

        speakers = []
        for c in group:
            if c.speaker_entity_id:
                sp = db.query(Entity).filter_by(id=c.speaker_entity_id).first()
                if sp and sp.canonical_name not in speakers:
                    speakers.append(sp.canonical_name)

        chains.append({
            "subject": {"id": subject_id_key, "name": subject.canonical_name if subject else None},
            "claim_type": claim_type,
            "document_count": len(doc_ids),
            "speaker_count": len(speakers),
            "speakers": speakers[:5],
            "documents": list(docs_info.values()),
            "sample_claim": group[0].claim_text[:200],
            "propagation_depth": len(doc_ids),
            "unchallenged": not any(c.claim_type == "denial" for c in group),
        })

    chains.sort(key=lambda x: x["propagation_depth"], reverse=True)
    return {"total": len(chains), "chains": chains}


@router.get("/claims/contradictions")
def find_contradictions(
    subject_id: str = Query(None),
    min_confidence: float = Query(0.5),
    db: Session = Depends(get_db),
):
    """
    Find contradicting claims — same subject entity, opposing claim_types
    (allegation vs denial) or opposing sentiments.
    """
    q = db.query(Claim).filter(Claim.confidence >= min_confidence)
    if subject_id:
        q = q.filter(Claim.subject_entity_id == subject_id)
    all_claims = q.all()

    # Group by subject
    by_subject: dict[str, list[Claim]] = defaultdict(list)
    for c in all_claims:
        if c.subject_entity_id:
            by_subject[c.subject_entity_id].append(c)

    contradictions = []
    for subj_id, claims in by_subject.items():
        allegations = [c for c in claims if c.claim_type == "allegation"]
        denials     = [c for c in claims if c.claim_type == "denial"]
        pos_claims  = [c for c in claims if c.sentiment == "positive"]
        neg_claims  = [c for c in claims if c.sentiment == "negative"]

        pairs = []
        for a in allegations[:3]:
            for d in denials[:3]:
                if a.document_id != d.document_id:
                    pairs.append({
                        "claim_a": {"text": a.claim_text[:200], "doc": a.document_id, "type": "allegation"},
                        "claim_b": {"text": d.claim_text[:200], "doc": d.document_id, "type": "denial"},
                        "contradiction_type": "allegation_vs_denial",
                    })

        for p in pos_claims[:2]:
            for n in neg_claims[:2]:
                if p.document_id != n.document_id:
                    pairs.append({
                        "claim_a": {"text": p.claim_text[:200], "doc": p.document_id, "type": "positive"},
                        "claim_b": {"text": n.claim_text[:200], "doc": n.document_id, "type": "negative"},
                        "contradiction_type": "sentiment_conflict",
                    })

        if pairs:
            subject = db.query(Entity).filter_by(id=subj_id).first()
            contradictions.append({
                "subject": {"id": subj_id, "name": subject.canonical_name if subject else None},
                "contradiction_count": len(pairs),
                "pairs": pairs[:5],
            })

    contradictions.sort(key=lambda x: x["contradiction_count"], reverse=True)
    return {"total": len(contradictions), "contradictions": contradictions}
