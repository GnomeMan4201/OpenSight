"""
apps/api/services/search.py

Full-text search over ingested document pages.

Primary: SQLite FTS5 with Porter stemmer, sentinel-based snippet escaping,
         and a CTE-based architecture that keeps COUNT(*) and result rows
         on the exact same filtered set (correct pagination totals).

Fallback: PostgreSQL ILIKE scan for when FTS5 is unavailable.

Relevance ordering:
  FTS5 returns a negative rank (BM25-inspired). We convert it to a 0–1 score
  as: score = clamp(1 + rank/20, 0, 1). Higher is more relevant.

Snippet safety:
  FTS5 snippet() is called with sentinel tokens (not angle brackets) so that
  the raw snippet can be fully html.escape()'d before <mark> tags are inserted.
  Document text can never inject arbitrary HTML into the snippet.
"""

import html
import logging
import math
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.config import settings
from apps.api.schemas import SearchResponse, SearchResultItem

log = logging.getLogger(__name__)

_MARK_OPEN  = "|||MOPEN|||"
_MARK_CLOSE = "|||MCLOSE|||"


def search_documents(
    db: Session,
    query: str,
    source_tags: Optional[list[str]] = None,
    entity_names: Optional[list[str]] = None,
    has_redactions: Optional[bool] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    page: int = 1,
    page_size: int = 20,
) -> SearchResponse:
    """
    Execute a full-text search with optional filters.

    All filters are applied inside SQL before COUNT(*), so the returned
    `total` always reflects the actual filtered result count.
    """
    if settings.database_url.startswith("sqlite"):
        return _fts5_search(
            db, query, source_tags, entity_names,
            has_redactions, date_from, date_to, page, page_size,
        )
    return _ilike_search(
        db, query, source_tags, entity_names,
        has_redactions, date_from, date_to, page, page_size,
    )


# ── SQLite FTS5 ────────────────────────────────────────────────────────────────

def _fts5_search(
    db: Session,
    query: str,
    source_tags: Optional[list[str]],
    entity_names: Optional[list[str]],
    has_redactions: Optional[bool],
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    page: int,
    page_size: int,
) -> SearchResponse:
    """
    CTE-based FTS5 search.

    CTE hits     — raw FTS5 match with snippet sentinels and rank
    CTE filtered — applies document-level WHERE conditions (source_tag,
                   has_redactions, date range, entity existence)
    COUNT and LIMIT/OFFSET both run against `filtered`.
    """
    safe_q = _fts_escape(query)
    params: dict = {
        "query":      safe_q,
        "mark_open":  _MARK_OPEN,
        "mark_close": _MARK_CLOSE,
        "limit":      page_size,
        "offset":     (page - 1) * page_size,
    }

    where: list[str] = []

    if source_tags:
        ph = ", ".join(f":st_{i}" for i in range(len(source_tags)))
        where.append(f"h.source_tag IN ({ph})")
        params.update({f"st_{i}": v for i, v in enumerate(source_tags)})

    if has_redactions is not None:
        where.append("d.has_redactions = :has_redactions")
        params["has_redactions"] = has_redactions

    if date_from:
        where.append("d.created_at >= :date_from")
        params["date_from"] = date_from.isoformat()

    if date_to:
        where.append("d.created_at <= :date_to")
        params["date_to"] = date_to.isoformat()

    if entity_names:
        en_ph = ", ".join(f":en_{i}" for i in range(len(entity_names)))
        where.append(f"""
            EXISTS (
                SELECT 1 FROM mentions m
                JOIN entities e ON e.id = m.entity_id
                WHERE m.document_id = h.document_id
                  AND m.page_number = h.page_number
                  AND e.canonical_name IN ({en_ph})
            )
        """)
        params.update({f"en_{i}": v for i, v in enumerate(entity_names)})

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    cte = f"""
        WITH hits AS (
            SELECT
                document_id,
                page_number,
                source_tag,
                filename,
                snippet(fts_pages, 4, :mark_open, :mark_close, '…', 24) AS snip,
                rank
            FROM fts_pages
            WHERE fts_pages MATCH :query
        ),
        filtered AS (
            SELECT h.*
            FROM hits h
            JOIN documents d ON d.id = h.document_id
            {where_sql}
        )
    """

    try:
        total = db.execute(text(cte + "SELECT COUNT(*) FROM filtered"), params).scalar() or 0
    except Exception as exc:
        log.warning("[search] FTS5 count failed (%s) — falling back to ILIKE", exc)
        return _ilike_search(
            db, query, source_tags, entity_names,
            has_redactions, date_from, date_to, page, page_size,
        )

    try:
        rows = db.execute(
            text(cte + """
                SELECT document_id, page_number, source_tag, filename, snip, rank
                FROM filtered
                ORDER BY rank
                LIMIT :limit OFFSET :offset
            """),
            params,
        ).fetchall()
    except Exception as exc:
        log.error("[search] FTS5 result query failed: %s", exc)
        rows = []

    return SearchResponse(
        total=total,
        page=page,
        page_size=page_size,
        total_pages=math.ceil(total / page_size) if total else 0,
        query=query,
        results=_build_items(db, rows),
    )


def _build_items(db: Session, rows) -> list[SearchResultItem]:
    from apps.api.models import Document, Mention, Entity

    items: list[SearchResultItem] = []
    for row in rows:
        document_id  = row[0]
        page_number  = row[1]
        source_tag   = row[2]
        filename     = row[3]
        raw_snip     = row[4] or ""
        rank         = float(row[5]) if row[5] is not None else 0.0

        enames = [
            r[0] for r in (
                db.query(Entity.canonical_name)
                .join(Mention, Mention.entity_id == Entity.id)
                .filter(
                    Mention.document_id == document_id,
                    Mention.page_number == page_number,
                )
                .limit(20)
                .all()
            )
        ]

        doc = db.query(Document).filter_by(id=document_id).first()
        score = max(0.0, min(1.0, 1.0 + rank / 20.0))

        items.append(SearchResultItem(
            document_id=document_id,
            filename=filename,
            source_tag=source_tag,
            page_number=page_number,
            score=round(score, 4),
            snippet=_safe_snippet(raw_snip),
            entity_names=enames,
            has_redactions=bool(doc and doc.has_redactions),
        ))

    return items


def _safe_snippet(raw: str) -> str:
    """
    Convert sentinel-marked FTS5 snippet to safe HTML.
    Escape all document text, then re-insert <mark> tags from known sentinels.
    """
    escaped = html.escape(raw)
    return (
        escaped
        .replace(html.escape(_MARK_OPEN),  "<mark>")
        .replace(html.escape(_MARK_CLOSE), "</mark>")
        .replace(_MARK_OPEN,  "<mark>")
        .replace(_MARK_CLOSE, "</mark>")
    )


def _fts_escape(q: str) -> str:
    """Wrap user query in double quotes for FTS5 MATCH safety."""
    return '"' + q.replace('"', '""') + '"'


# ── PostgreSQL / ILIKE fallback ────────────────────────────────────────────────

def _ilike_search(
    db: Session,
    query: str,
    source_tags: Optional[list[str]],
    entity_names: Optional[list[str]],
    has_redactions: Optional[bool],
    date_from: Optional[datetime],
    date_to: Optional[datetime],
    page: int,
    page_size: int,
) -> SearchResponse:
    import re as _re
    from apps.api.models import Document, DocumentPage, Mention, Entity

    like_term = f"%{query}%"
    offset    = (page - 1) * page_size

    q = (
        db.query(DocumentPage, Document)
        .join(Document, Document.id == DocumentPage.document_id)
        .filter(DocumentPage.final_text.ilike(like_term))
    )
    if source_tags:
        q = q.filter(Document.source_tag.in_(source_tags))
    if has_redactions is not None:
        q = q.filter(Document.has_redactions == has_redactions)
    if date_from:
        q = q.filter(Document.created_at >= date_from)
    if date_to:
        q = q.filter(Document.created_at <= date_to)
    if entity_names:
        for en in entity_names:
            sub = (
                db.query(Mention.document_id)
                .join(Entity, Entity.id == Mention.entity_id)
                .filter(Entity.canonical_name == en)
                .subquery()
            )
            q = q.filter(Document.id.in_(sub))

    total = q.count()
    rows  = q.order_by(Document.created_at.desc()).offset(offset).limit(page_size).all()

    items: list[SearchResultItem] = []
    for page_row, doc_row in rows:
        idx = page_row.final_text.lower().find(query.lower())
        if idx >= 0:
            start   = max(0, idx - 100)
            end     = min(len(page_row.final_text), idx + len(query) + 100)
            raw_snip = page_row.final_text[start:end]
            snippet = _re.sub(
                _re.escape(query),
                lambda m: f"<mark>{html.escape(m.group())}</mark>",
                html.escape(raw_snip),
                count=3,
                flags=_re.IGNORECASE,
            )
        else:
            snippet = html.escape(page_row.final_text[:200])

        enames = [
            r[0] for r in (
                db.query(Entity.canonical_name)
                .join(Mention, Mention.entity_id == Entity.id)
                .filter(
                    Mention.document_id == doc_row.id,
                    Mention.page_number == page_row.page_number,
                )
                .limit(20)
                .all()
            )
        ]

        items.append(SearchResultItem(
            document_id=doc_row.id,
            filename=doc_row.filename,
            source_tag=doc_row.source_tag,
            page_number=page_row.page_number,
            score=1.0,
            snippet=snippet,
            entity_names=enames,
            has_redactions=doc_row.has_redactions,
        ))

    return SearchResponse(
        total=total,
        page=page,
        page_size=page_size,
        total_pages=math.ceil(total / page_size) if total else 0,
        query=query,
        results=items,
    )
