"""
apps/api/routes/search.py
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime

from apps.api.database import get_db
from apps.api.schemas import SearchRequest, SearchResponse
from apps.api.services.search import search_documents

router = APIRouter()


@router.post("", response_model=SearchResponse)
def search_post(request: SearchRequest, db: Session = Depends(get_db)):
    """
    Full-text search with filters. Returns HTML-safe snippets.
    `total` reflects all active filters and is correct for pagination.
    """
    f = request.filters
    return search_documents(
        db=db,
        query=request.query,
        source_tags=f.source_tags,
        entity_names=f.entity_names,
        has_redactions=f.has_redactions,
        date_from=f.date_from,
        date_to=f.date_to,
        page=request.page,
        page_size=request.page_size,
    )


@router.get("", response_model=SearchResponse)
def search_get(
    q: str = Query(..., min_length=1, max_length=1000),
    source_tag: Optional[str] = Query(None),
    has_redactions: Optional[bool] = Query(None),
    date_from: Optional[datetime] = Query(None),
    date_to: Optional[datetime] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Convenience GET endpoint. Use POST for entity_names and source_tags list filtering."""
    return search_documents(
        db=db,
        query=q,
        source_tags=[source_tag] if source_tag else None,
        has_redactions=has_redactions,
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size,
    )
