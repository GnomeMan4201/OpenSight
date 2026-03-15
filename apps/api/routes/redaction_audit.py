"""
apps/api/routes/redaction_audit.py

Redaction audit endpoints for the Epstein files research workflow.

GET /api/v1/redaction/audit
    — corpus-wide redaction summary: which docs have redactions, counts by type

GET /api/v1/redaction/audit/{document_id}
    — full redaction report for one document:
      - all redaction flags with page context
      - entities appearing on the same page as each redaction
      - hidden text recovered (if any)
      - surrounding text context (50 chars before/after position estimate)

GET /api/v1/redaction/candidates
    — pages with redactions + nearby named entities = "redaction candidates"
      ranked by entity density near the redaction
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from apps.api.database import get_db
from apps.api.models import (
    Document, DocumentPage, RedactionFlag, Entity, Mention
)

router = APIRouter(tags=["redaction"])


# ── Corpus-wide summary ───────────────────────────────────────────────────────

@router.get("/audit")
def redaction_audit_summary(db: Session = Depends(get_db)):
    """
    High-level redaction summary across all documents.
    Shows which documents have redactions, counts, and types.
    """
    docs_with_flags = (
        db.query(Document)
        .filter(Document.has_redactions == True)
        .all()
    )

    result = []
    for doc in docs_with_flags:
        flags = db.query(RedactionFlag).filter_by(document_id=doc.id).all()

        black_boxes   = [f for f in flags if f.flag_type == "black_box"]
        hidden_texts  = [f for f in flags if f.flag_type == "hidden_text"]
        reviewed      = [f for f in flags if f.reviewed]

        # Pages with redactions
        redacted_pages = sorted({f.page_number for f in flags})

        # Entities on redacted pages
        entity_count = (
            db.query(func.count(func.distinct(Mention.entity_id)))
            .filter(
                Mention.document_id == doc.id,
                Mention.page_number.in_(redacted_pages),
            )
            .scalar() or 0
        )

        result.append({
            "document_id":    doc.id,
            "filename":       doc.filename,
            "original_name":  doc.original_name,
            "page_count":     doc.page_count,
            "total_flags":    len(flags),
            "black_boxes":    len(black_boxes),
            "hidden_texts":   len(hidden_texts),
            "reviewed":       len(reviewed),
            "redacted_pages": redacted_pages,
            "entities_on_redacted_pages": entity_count,
        })

    result.sort(key=lambda x: x["total_flags"], reverse=True)

    return {
        "total_documents_with_redactions": len(result),
        "total_flags": sum(r["total_flags"] for r in result),
        "total_black_boxes": sum(r["black_boxes"] for r in result),
        "total_hidden_texts": sum(r["hidden_texts"] for r in result),
        "documents": result,
    }


# ── Per-document redaction report ─────────────────────────────────────────────

@router.get("/audit/{document_id}")
def redaction_audit_document(document_id: str, db: Session = Depends(get_db)):
    """
    Full redaction audit for one document.
    For each redaction flag: page context, nearby entities, recovered text.
    """
    from fastapi import HTTPException
    doc = db.query(Document).filter_by(id=document_id).first()
    if not doc:
        raise HTTPException(404, "Document not found")

    flags = (
        db.query(RedactionFlag)
        .filter_by(document_id=document_id)
        .order_by(RedactionFlag.page_number)
        .all()
    )

    # Build page text index
    pages = db.query(DocumentPage).filter_by(document_id=document_id).all()
    page_text = {p.page_number: p.final_text or p.raw_text or "" for p in pages}

    # Build entity index by page
    mentions = (
        db.query(Mention, Entity)
        .join(Entity, Entity.id == Mention.entity_id)
        .filter(Mention.document_id == document_id)
        .all()
    )
    entities_by_page: dict[int, list[dict]] = {}
    for mention, entity in mentions:
        pg = mention.page_number
        if pg not in entities_by_page:
            entities_by_page[pg] = []
        # Avoid duplicates per page
        if not any(e["name"] == entity.canonical_name for e in entities_by_page[pg]):
            entities_by_page[pg].append({
                "id":   entity.id,
                "name": entity.canonical_name,
                "type": entity.entity_type,
            })

    audit_flags = []
    for flag in flags:
        pg_text = page_text.get(flag.page_number, "")

        # Extract context window around estimated redaction position
        context_before = ""
        context_after  = ""
        if pg_text and flag.bounding_box:
            # Estimate character position from y-position ratio
            total_chars = len(pg_text)
            y_ratio = flag.bounding_box.get("y", 0) / 792  # standard page height
            estimated_pos = int(y_ratio * total_chars)
            window = 120
            start = max(0, estimated_pos - window)
            end   = min(total_chars, estimated_pos + window)
            context_before = pg_text[start:estimated_pos].strip()[-80:]
            context_after  = pg_text[estimated_pos:end].strip()[:80]

        audit_flags.append({
            "flag_id":        flag.id,
            "page_number":    flag.page_number,
            "flag_type":      flag.flag_type,
            "bounding_box":   flag.bounding_box,
            "confidence":     flag.confidence,
            "reviewed":       flag.reviewed,
            "hidden_text":    flag.hidden_text,
            "context_before": context_before,
            "context_after":  context_after,
            "entities_on_page": entities_by_page.get(flag.page_number, []),
        })

    # Group by page for summary
    pages_summary = {}
    for f in audit_flags:
        pg = f["page_number"]
        if pg not in pages_summary:
            pages_summary[pg] = {
                "page_number": pg,
                "flags": [],
                "entities": f["entities_on_page"],
                "page_text_preview": page_text.get(pg, "")[:300],
            }
        pages_summary[pg]["flags"].append(f)

    return {
        "document_id":   document_id,
        "filename":      doc.filename,
        "original_name": doc.original_name,
        "total_flags":   len(flags),
        "pages":         sorted(pages_summary.values(), key=lambda x: x["page_number"]),
        "all_flags":     audit_flags,
    }


# ── Redaction candidates ──────────────────────────────────────────────────────

@router.get("/candidates")
def redaction_candidates(
    min_entities: int = Query(1, ge=1, description="Min entities on same page as redaction"),
    flag_type:    str = Query(None, description="Filter: black_box or hidden_text"),
    db:           Session = Depends(get_db),
):
    """
    Pages where redactions occur alongside named entities.
    Higher entity density = more investigatively interesting.
    Ranked by entity count descending.
    """
    q = db.query(RedactionFlag).filter(RedactionFlag.reviewed == False)
    if flag_type:
        q = q.filter(RedactionFlag.flag_type == flag_type)
    flags = q.all()

    candidates = []
    seen = set()

    for flag in flags:
        key = (flag.document_id, flag.page_number)
        if key in seen:
            continue
        seen.add(key)

        # Entities on this page
        page_mentions = (
            db.query(Mention, Entity)
            .join(Entity, Entity.id == Mention.entity_id)
            .filter(
                Mention.document_id == flag.document_id,
                Mention.page_number  == flag.page_number,
            )
            .all()
        )

        entities = []
        seen_ent = set()
        for m, e in page_mentions:
            if e.id not in seen_ent:
                seen_ent.add(e.id)
                entities.append({
                    "id":   e.id,
                    "name": e.canonical_name,
                    "type": e.entity_type,
                })

        if len(entities) < min_entities:
            continue

        doc = db.query(Document).filter_by(id=flag.document_id).first()
        page = db.query(DocumentPage).filter_by(
            document_id=flag.document_id,
            page_number=flag.page_number,
        ).first()

        # Count redactions on this page
        page_flags = [f for f in flags
                      if f.document_id == flag.document_id
                      and f.page_number == flag.page_number]

        hidden_texts = [f.hidden_text for f in page_flags if f.hidden_text]

        candidates.append({
            "document_id":   flag.document_id,
            "filename":      doc.filename if doc else "unknown",
            "original_name": doc.original_name if doc else "unknown",
            "page_number":   flag.page_number,
            "redaction_count": len(page_flags),
            "flag_types":    list({f.flag_type for f in page_flags}),
            "entity_count":  len(entities),
            "entities":      entities,
            "hidden_texts":  hidden_texts,
            "page_preview":  (page.final_text or page.raw_text or "")[:400] if page else "",
        })

    candidates.sort(key=lambda x: x["entity_count"], reverse=True)

    return {
        "total_candidates": len(candidates),
        "candidates": candidates,
    }
