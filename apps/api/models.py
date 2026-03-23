"""
apps/api/models.py

SQLAlchemy ORM models.

Model overview:
  Document       — one row per uploaded file; tracks ingestion status
  DocumentPage   — one row per logical page; stores extracted/OCR text
  Entity         — one canonical entity (PERSON, ORG, etc.); deduplicated by (type, name)
  Mention        — one row per entity occurrence at a specific offset in a page
  Annotation     — human-added notes/highlights on pages
  RedactionFlag  — detected redaction regions inside PDFs

All tables are created by init_db() on startup (idempotent).
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Index, Integer, JSON, String, Text, UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, relationship


def _uuid() -> str:
    return str(uuid.uuid4())


class Base(DeclarativeBase):
    pass


class Document(Base):
    __tablename__ = "documents"

    id              = Column(String, primary_key=True, default=_uuid)
    filename        = Column(String, nullable=False)
    original_name   = Column(String, nullable=False)
    sha256_hash     = Column(String, unique=True, nullable=False)
    mime_type       = Column(String, default="application/octet-stream")
    file_size_bytes = Column(Integer, default=0)
    page_count      = Column(Integer, default=0)
    source_tag      = Column(String, default="", index=True)
    doc_type        = Column(String, default="other", index=True)  # witness_statement|psych_report|court_decision|email|legal_filing|other
    status          = Column(String, default="pending", index=True)  # pending|processing|done|error
    has_redactions  = Column(Boolean, default=False)
    storage_path    = Column(String, nullable=False)
    error_message   = Column(Text, nullable=True)
    created_at      = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at      = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    pages           = relationship(
        "DocumentPage", back_populates="document",
        cascade="all, delete-orphan", order_by="DocumentPage.page_number",
    )
    mentions        = relationship("Mention",       back_populates="document", cascade="all, delete-orphan")
    annotations     = relationship("Annotation",    back_populates="document", cascade="all, delete-orphan")
    redaction_flags = relationship("RedactionFlag", back_populates="document", cascade="all, delete-orphan")
    claims          = relationship("Claim",         back_populates="document", cascade="all, delete-orphan")
    claims          = relationship("Claim",         back_populates="document", cascade="all, delete-orphan")
    claims          = relationship("Claim",         back_populates="document", cascade="all, delete-orphan")


class DocumentPage(Base):
    __tablename__ = "document_pages"
    __table_args__ = (UniqueConstraint("document_id", "page_number"),)

    id             = Column(String, primary_key=True, default=_uuid)
    document_id    = Column(String, ForeignKey("documents.id", ondelete="CASCADE"), nullable=False)
    page_number    = Column(Integer, nullable=False)
    raw_text       = Column(Text,    default="")   # native PDF text
    ocr_text       = Column(Text,    default="")   # Tesseract/ocrmypdf output
    final_text     = Column(Text,    default="")   # text used for search and NER
    word_count     = Column(Integer, default=0)
    ocr_used       = Column(Boolean, default=False)  # True when OCR was the text source
    has_redactions = Column(Boolean, default=False)

    document = relationship("Document", back_populates="pages")


class Entity(Base):
    """
    One canonical entity per (entity_type, canonical_name) pair.
    Multiple documents / pages that reference the same person/org share one Entity row;
    individual occurrences are stored as Mention rows.
    """
    __tablename__ = "entities"
    __table_args__ = (
        UniqueConstraint("entity_type", "canonical_name"),
        Index("ix_entities_type", "entity_type"),
        Index("ix_entities_name", "canonical_name"),
    )

    id             = Column(String,  primary_key=True, default=_uuid)
    entity_type    = Column(String,  nullable=False)  # Person|Organization|Location|Aircraft|Phone|Date|Email
    canonical_name = Column(String,  nullable=False)
    aliases        = Column(JSON,    default=list)
    confidence     = Column(Float,   default=1.0)
    review_status  = Column(String,  default="auto")  # auto|confirmed|disputed
    review_notes   = Column(String,  nullable=True)
    mention_count  = Column(Integer, default=0)
    created_at     = Column(DateTime, default=datetime.utcnow)
    updated_at     = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    mentions = relationship("Mention", back_populates="entity", cascade="all, delete-orphan")


class Mention(Base):
    """One row per entity occurrence — preserves exact position within a page."""
    __tablename__ = "mentions"
    __table_args__ = (
        Index("ix_mentions_entity_id",   "entity_id"),
        Index("ix_mentions_document_id", "document_id"),
    )

    id                = Column(String,  primary_key=True, default=_uuid)
    entity_id         = Column(String,  ForeignKey("entities.id",  ondelete="CASCADE"), nullable=False)
    document_id       = Column(String,  ForeignKey("documents.id", ondelete="CASCADE"), nullable=False)
    page_number       = Column(Integer, nullable=False)
    char_start        = Column(Integer, nullable=True)
    char_end          = Column(Integer, nullable=True)
    context_text      = Column(Text,    default="")
    extraction_method = Column(String,  default="regex")  # regex|spacy|manual
    confidence        = Column(Float,   default=1.0)
    created_at        = Column(DateTime, default=datetime.utcnow)

    entity   = relationship("Entity",   back_populates="mentions")
    document = relationship("Document", back_populates="mentions")


class Annotation(Base):
    """Human-added notes, highlights, or flags on a page region."""
    __tablename__ = "annotations"

    id               = Column(String,  primary_key=True, default=_uuid)
    document_id      = Column(String,  ForeignKey("documents.id", ondelete="CASCADE"), nullable=False)
    page_number      = Column(Integer, nullable=True)
    annotation_type  = Column(String,  default="note")   # note|highlight|flag|redaction_note
    char_start       = Column(Integer, nullable=True)
    char_end         = Column(Integer, nullable=True)
    highlighted_text = Column(Text,    default="")
    note_text        = Column(Text,    default="")
    color            = Column(String,  default="#FFEB3B")
    author           = Column(String,  default="anonymous")
    is_deleted       = Column(Boolean, default=False)
    created_at       = Column(DateTime, default=datetime.utcnow)
    updated_at       = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    document = relationship("Document", back_populates="annotations")


class EntityRelationship(Base):
    """
    Co-occurrence relationship between two entities.
    Created when two entities appear in the same document.
    Weight increments each time they co-occur; higher weight = stronger connection.
    entity_a_id is always the lexicographically smaller ID to avoid duplicates.
    """
    __tablename__ = "entity_relationships"
    __table_args__ = (
        UniqueConstraint("entity_a_id", "entity_b_id"),
        Index("ix_rel_entity_a", "entity_a_id"),
        Index("ix_rel_entity_b", "entity_b_id"),
    )

    id           = Column(String,  primary_key=True, default=_uuid)
    entity_a_id  = Column(String,  ForeignKey("entities.id", ondelete="CASCADE"), nullable=False)
    entity_b_id  = Column(String,  ForeignKey("entities.id", ondelete="CASCADE"), nullable=False)
    weight       = Column(Integer, default=1)       # incremented on each co-occurrence
    doc_count    = Column(Integer, default=1)       # number of distinct documents
    relationship_type = Column(String, default="co_occurrence")
    confidence        = Column(Float, default=0.5)
    sentence_span     = Column(Text, nullable=True)
    created_at   = Column(DateTime, default=datetime.utcnow)
    updated_at   = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    entity_a = relationship("Entity", foreign_keys=[entity_a_id])
    entity_b = relationship("Entity", foreign_keys=[entity_b_id])


class RedactionFlag(Base):
    """A detected redaction region within a PDF page."""
    __tablename__ = "redaction_flags"

    id           = Column(String,  primary_key=True, default=_uuid)
    document_id  = Column(String,  ForeignKey("documents.id", ondelete="CASCADE"), nullable=False)
    page_number  = Column(Integer, nullable=False)
    flag_type    = Column(String,  nullable=False)  # hidden_text|black_box|incremental_save
    bounding_box = Column(JSON,    nullable=True)   # {x, y, w, h} in PDF points
    hidden_text  = Column(Text,    nullable=True)   # recovered hidden text if any
    confidence   = Column(Float,   default=0.5)
    reviewed     = Column(Boolean, default=False)
    created_at   = Column(DateTime, default=datetime.utcnow)

    document = relationship("Document", back_populates="redaction_flags")


# ── v0.7: Claim model ─────────────────────────────────────────────────────────

class Claim(Base):
    """
    An attributed statement extracted from a document.
    speaker_entity_id → subject_entity_id via claim_text.
    """
    __tablename__ = "claims"
    __table_args__ = (
        Index("ix_claims_document_id",     "document_id"),
        Index("ix_claims_speaker_entity",  "speaker_entity_id"),
        Index("ix_claims_subject_entity",  "subject_entity_id"),
    )

    id                = Column(String,  primary_key=True, default=_uuid)
    document_id       = Column(String,  ForeignKey("documents.id", ondelete="CASCADE"), nullable=False)
    page_number       = Column(Integer, nullable=False)
    speaker_entity_id = Column(String,  ForeignKey("entities.id", ondelete="SET NULL"), nullable=True)
    subject_entity_id = Column(String,  ForeignKey("entities.id", ondelete="SET NULL"), nullable=True)
    claim_text        = Column(Text,    nullable=False)
    claim_type        = Column(String,  default="observation")  # allegation|denial|observation|testimony|ruling|other
    sentiment         = Column(String,  default="neutral")      # positive|negative|neutral
    confidence        = Column(Float,   default=0.7)
    extraction_method = Column(String,  default="ollama")       # ollama|spacy|regex
    created_at        = Column(DateTime, default=datetime.utcnow)

    document = relationship("Document", back_populates="claims")

class SemanticChunk(Base):
    __tablename__ = "semantic_chunks"

    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, index=True)
    chunk_index = Column(Integer)
    text = Column(Text)

    created_at = Column(DateTime, default=datetime.utcnow)

