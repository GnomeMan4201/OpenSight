"""
apps/api/routes/entities.py
Entity listing, detail, mention history, and review status.
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from apps.api.database import get_db
from apps.api.models import Entity, Mention, Document
from apps.api.schemas import EntityListOut, EntityOut

router = APIRouter()


@router.get("", response_model=EntityListOut)
def list_entities(
    entity_type: Optional[str] = Query(
        None,
        description="Filter by type: Person, Organization, Location, Aircraft, Phone, Email, Date, Airport",
    ),
    q: Optional[str] = Query(None, description="Case-insensitive substring search on canonical_name"),
    min_mentions: int = Query(1, ge=1, description="Only return entities with at least this many mentions"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    List canonical entities sorted by mention count descending.

    Bug fixed: previous version used HAVING on a non-aggregate query, which
    SQLite rejects with OperationalError. Fix: use a pre-aggregated subquery
    and apply min_mentions as a WHERE filter on the outer query.
    """
    # Step 1: build a subquery with mention counts already aggregated.
    mention_counts_sq = (
        select(
            Mention.entity_id,
            func.count(Mention.id).label("cnt"),
        )
        .group_by(Mention.entity_id)
        .subquery("mc")
    )

    # Step 2: join entities to the pre-aggregated subquery.
    # Filtering with .filter() generates WHERE (not HAVING), which is correct
    # because the aggregation already happened inside the subquery.
    base = (
        db.query(
            Entity,
            func.coalesce(mention_counts_sq.c.cnt, 0).label("mention_count"),
        )
        .outerjoin(mention_counts_sq, mention_counts_sq.c.entity_id == Entity.id)
        .filter(func.coalesce(mention_counts_sq.c.cnt, 0) >= min_mentions)
    )

    if entity_type:
        base = base.filter(Entity.entity_type == entity_type)
    if q:
        base = base.filter(Entity.canonical_name.ilike(f"%{q}%"))

    total = base.count()

    rows = (
        base.order_by(func.coalesce(mention_counts_sq.c.cnt, 0).desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    items = []
    for entity, mention_count in rows:
        doc_count = (
            db.query(func.count(func.distinct(Mention.document_id)))
            .filter(Mention.entity_id == entity.id)
            .scalar()
        ) or 0
        items.append(EntityOut(
            id=entity.id,
            entity_type=entity.entity_type,
            canonical_name=entity.canonical_name,
            aliases=entity.aliases or [],
            confidence=entity.confidence,
            review_status=entity.review_status,
            created_at=entity.created_at,
            mention_count=mention_count,
            document_count=doc_count,
        ))

    return EntityListOut(total=total, items=items)


@router.get("/{entity_id}", response_model=EntityOut)
def get_entity(entity_id: str, db: Session = Depends(get_db)):
    entity = db.query(Entity).filter_by(id=entity_id).first()
    if not entity:
        raise HTTPException(404, "Entity not found")

    mention_count = (
        db.query(func.count(Mention.id)).filter_by(entity_id=entity_id).scalar()
    ) or 0
    doc_count = (
        db.query(func.count(func.distinct(Mention.document_id)))
        .filter(Mention.entity_id == entity_id)
        .scalar()
    ) or 0

    return EntityOut(
        id=entity.id,
        entity_type=entity.entity_type,
        canonical_name=entity.canonical_name,
        aliases=entity.aliases or [],
        confidence=entity.confidence,
        review_status=entity.review_status,
        created_at=entity.created_at,
        mention_count=mention_count,
        document_count=doc_count,
    )


@router.get("/{entity_id}/mentions")
def get_entity_mentions(
    entity_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """All occurrences of this entity across documents, with context snippets."""
    entity = db.query(Entity).filter_by(id=entity_id).first()
    if not entity:
        raise HTTPException(404, "Entity not found")

    total = db.query(Mention).filter_by(entity_id=entity_id).count()
    rows = (
        db.query(Mention, Document)
        .join(Document, Document.id == Mention.document_id)
        .filter(Mention.entity_id == entity_id)
        .order_by(Document.created_at, Mention.page_number, Mention.char_start)
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return {
        "entity_id": entity_id,
        "canonical_name": entity.canonical_name,
        "total": total,
        "page": page,
        "page_size": page_size,
        "mentions": [
            {
                "document_id": doc.id,
                "filename": doc.filename,
                "source_tag": doc.source_tag,
                "page_number": mention.page_number,
                "char_start": mention.char_start,
                "char_end": mention.char_end,
                "context": mention.context_text,
                "extraction_method": mention.extraction_method,
                "confidence": mention.confidence,
            }
            for mention, doc in rows
        ],
    }


@router.patch("/{entity_id}/review")
def update_review_status(
    entity_id: str,
    status: str = Query(..., pattern="^(confirmed|disputed|auto)$"),
    db: Session = Depends(get_db),
):
    entity = db.query(Entity).filter_by(id=entity_id).first()
    if not entity:
        raise HTTPException(404, "Entity not found")
    entity.review_status = status
    db.commit()
    return {"entity_id": entity_id, "review_status": status}
