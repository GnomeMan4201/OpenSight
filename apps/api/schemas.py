"""
apps/api/schemas.py
Pydantic v2 schemas for all request/response shapes.

Snippet safety contract:
  SearchResultItem.snippet contains text with <mark>…</mark> highlight tags.
  The text content is HTML-escaped server-side before <mark> tags are inserted.
  Callers may render snippet as innerHTML safely; only <mark> is injected.
"""

from datetime import datetime
from typing import Optional, Any, Literal
from pydantic import BaseModel, Field, ConfigDict


# ── Document ──────────────────────────────────────────────────────────────────

class DocumentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    filename: str
    original_name: str
    sha256_hash: str
    mime_type: Optional[str] = None
    file_size_bytes: Optional[int] = None
    page_count: int
    source_tag: str
    doc_type: str = "other"
    status: str
    has_redactions: Optional[bool] = False
    error_message: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime] = None


class DocumentListOut(BaseModel):
    total: int
    items: list[DocumentOut]


class DocumentUploadResponse(BaseModel):
    document_id: str
    filename: str
    status: str      # queued | duplicate | rejected
    message: str


# ── Pages ─────────────────────────────────────────────────────────────────────

class PageEntitySpan(BaseModel):
    id: str
    entity_type: str
    canonical_name: str
    char_start: Optional[int]
    char_end: Optional[int]
    confidence: float
    extraction_method: str


class PageOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    document_id: str
    page_number: int
    final_text: str
    word_count: int
    ocr_used: bool
    has_redactions: Optional[bool] = False
    entities: list[PageEntitySpan] = []


# ── Entity ────────────────────────────────────────────────────────────────────

class EntityOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    entity_type: str
    canonical_name: str
    aliases: list[str]
    confidence: float
    review_status: str
    created_at: datetime
    mention_count: int = 0
    document_count: int = 0


class EntityListOut(BaseModel):
    total: int
    items: list[EntityOut]


# ── Search ────────────────────────────────────────────────────────────────────

class SearchFilters(BaseModel):
    # ISO 8601 datetimes; Pydantic coerces str → datetime automatically.
    date_from: Optional[datetime] = Field(
        None, description="Only return pages from documents ingested on/after this datetime"
    )
    date_to: Optional[datetime] = Field(
        None, description="Only return pages from documents ingested on/before this datetime"
    )
    entity_names: Optional[list[str]] = Field(
        None, description="Only return pages that contain all of these entity canonical names"
    )
    source_tags: Optional[list[str]] = Field(
        None, description="Restrict results to these source tags"
    )
    has_redactions: Optional[bool] = Field(
        None, description="If true, only pages from documents with detected redactions"
    )


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=1000)
    filters: SearchFilters = Field(default_factory=SearchFilters)
    page: int = Field(1, ge=1)
    page_size: int = Field(20, ge=1, le=100)


class SearchResultItem(BaseModel):
    document_id: str
    filename: str
    source_tag: str
    page_number: int
    score: float
    # HTML-escaped text with <mark>…</mark> highlights. Safe for innerHTML.
    snippet: str
    entity_names: list[str] = []
    has_redactions: bool = False


class SearchResponse(BaseModel):
    total: int           # Filtered total — use for pagination math
    page: int
    page_size: int
    total_pages: int     # ceil(total / page_size)
    query: str
    results: list[SearchResultItem]


# ── Annotation ────────────────────────────────────────────────────────────────

class AnnotationCreate(BaseModel):
    document_id: str
    page_number: Optional[int] = None
    annotation_type: Literal["note", "highlight", "flag", "redaction_note"] = "note"
    char_start: Optional[int] = None
    char_end: Optional[int] = None
    highlighted_text: str = ""
    note_text: str = ""
    color: str = "#FFEB3B"
    author: str = "anonymous"


class AnnotationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    document_id: str
    page_number: Optional[int]
    annotation_type: str
    char_start: Optional[int]
    char_end: Optional[int]
    highlighted_text: str
    note_text: str
    color: str
    author: str
    created_at: datetime


class AnnotationListOut(BaseModel):
    total: int
    items: list[AnnotationOut]


# ── Redaction flags ───────────────────────────────────────────────────────────

class RedactionFlagOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    document_id: str
    page_number: int
    flag_type: str
    bounding_box: Optional[Any]
    hidden_text: Optional[str]
    confidence: float
    reviewed: bool
    created_at: datetime
