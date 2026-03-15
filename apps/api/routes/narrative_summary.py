from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, or_

from apps.api.database import get_db
from apps.api.models import Entity, Mention

try:
    from apps.api.models import Claim
except Exception:
    Claim = None

router = APIRouter(tags=["narrative_summary"])


@router.get("/analysis/narrative/summary")
def narrative_summary(
    entity_id: str = Query(...),
    db: Session = Depends(get_db),
):
    e = db.query(Entity).filter(Entity.id == entity_id).first()
    if not e:
        return {"summary": "Entity not found", "bullets": []}

    mention_count = db.query(func.count(Mention.id)).filter(Mention.entity_id == entity_id).scalar() or 0

    bullets = []
    if Claim is not None:
        try:
            claims = db.query(Claim).filter(
                or_(
                    Claim.speaker_entity_id == entity_id,
                    Claim.subject_entity_id == entity_id,
                )
            ).limit(25).all()
            for c in claims[:8]:
                txt = (getattr(c, "claim_text", "") or "").strip()
                if txt:
                    bullets.append(txt[:220])
        except Exception:
            bullets = []

    summary = (
        f"{e.canonical_name} is a {e.entity_type} with {mention_count} mentions in the corpus. "
        f"The entity appears in multiple co-occurrence relationships and narrative claims, "
        f"indicating relevance to the current investigative cluster."
    )

    return {
        "entity_id": entity_id,
        "canonical_name": e.canonical_name,
        "entity_type": e.entity_type,
        "mention_count": mention_count,
        "summary": summary,
        "bullets": bullets,
    }
