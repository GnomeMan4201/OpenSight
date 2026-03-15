from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter
from pydantic import BaseModel, Field

from ..services.semantic_search import semantic_engine
from ..services.timeline import extract_timeline

router = APIRouter(prefix="/api/v1", tags=["semantic"])


class SemanticIndexItem(BaseModel):
    text: str
    metadata: Optional[Dict[str, Any]] = None


class SemanticIndexRequest(BaseModel):
    text: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    items: Optional[List[SemanticIndexItem]] = None


class SemanticSearchRequest(BaseModel):
    text: str
    k: int = Field(default=5, ge=1, le=50)


class TimelineRequest(BaseModel):
    text: str


@router.post("/semantic/index")
def semantic_index(payload: SemanticIndexRequest):
    if payload.items:
        return semantic_engine.bulk_index([item.model_dump() for item in payload.items])
    return semantic_engine.index(payload.text or "", payload.metadata)


@router.post("/semantic/search")
def semantic_search(payload: SemanticSearchRequest):
    return {
        "results": semantic_engine.search(payload.text, payload.k),
        "count": min(payload.k, len(semantic_engine.texts)),
    }


@router.post("/timeline/extract")
def timeline_extract(payload: TimelineRequest):
    events = extract_timeline(payload.text)
    return {"events": events, "count": len(events)}
