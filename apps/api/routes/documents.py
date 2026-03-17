"""
Document upload, listing, status polling, page retrieval, deletion, and re-ingest.
"""

import hashlib
import logging
import mimetypes
import os
import time
import uuid
from pathlib import Path
from typing import Optional

from fastapi import (
    APIRouter, BackgroundTasks, Depends, File,
    Form, HTTPException, Query, UploadFile,
)
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from apps.api.config import settings
from apps.api.database import get_db
from apps.api.models import (
    Document,
    DocumentPage,
    Mention,
    Entity,
    RedactionFlag,
    EntityRelationship,
)
from apps.api.schemas import (
    DocumentListOut,
    DocumentOut,
    DocumentUploadResponse,
    PageOut,
    PageEntitySpan,
    RedactionFlagOut,
)
from apps.api.services.storage import storage

log = logging.getLogger(__name__)
router = APIRouter()

SUPPORTED_EXTENSIONS: frozenset[str] = frozenset({
    ".pdf",
    ".txt", ".md",
    ".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp",
})


@router.post("/upload", response_model=list[DocumentUploadResponse], status_code=202)
def upload_documents(
    background_tasks: BackgroundTasks,
    files: list[UploadFile] = File(...),
    source_tag: str = Form(""),
    db: Session = Depends(get_db),
):
    if not files:
        raise HTTPException(400, "No files provided")

    for upload in files:
        ext = Path(_safe_filename(upload.filename or "")).suffix.lower()
        if ext not in SUPPORTED_EXTENSIONS:
            raise HTTPException(
                415,
                detail={
                    "error": "unsupported_file_type",
                    "filename": upload.filename,
                    "extension": ext or "(none)",
                    "supported": sorted(SUPPORTED_EXTENSIONS),
                },
            )

    out: list[DocumentUploadResponse] = []
    for upload in files:
        out.append(
            _process_upload(
                upload,
                _safe_filename(upload.filename or "upload.bin"),
                source_tag,
                background_tasks,
                db,
            )
        )
    return out


def _process_upload(
    upload: UploadFile,
    filename: str,
    source_tag: str,
    background_tasks: BackgroundTasks,
    db: Session,
) -> DocumentUploadResponse:
    tmp_path, sha256, size = _stream_to_temp(upload.file, settings.max_upload_bytes, filename)

    existing = db.query(Document).filter_by(sha256_hash=sha256).first()
    if existing:
        storage.discard_temp(tmp_path)
        return DocumentUploadResponse(
            document_id=existing.id,
            filename=filename,
            status="duplicate",
            message=f"Content already ingested as document {existing.id} (status: {existing.status})",
        )

    mime, _ = mimetypes.guess_type(filename)
    mime = mime or "application/octet-stream"
    storage_path = storage.finalize_temp(tmp_path, filename, source_tag)

    doc_id = str(uuid.uuid4())

    last_exc = None
    for attempt in range(5):
        try:
            doc = Document(
                id=doc_id,
                filename=filename,
                original_name=upload.filename or filename,
                sha256_hash=sha256,
                mime_type=mime,
                file_size_bytes=size,
                source_tag=source_tag,
                status="pending",
                storage_path=storage_path,
            )
            db.add(doc)
            db.commit()
            last_exc = None
            break
        except Exception as exc:
            db.rollback()
            last_exc = exc
            if "database is locked" not in str(exc).lower():
                raise
            time.sleep(0.75 * (attempt + 1))

    if last_exc is not None:
        raise HTTPException(503, "Database busy during upload. Retry in a moment.")

    background_tasks.add_task(_run_ingestion_bg, doc_id, settings.database_url)
    return DocumentUploadResponse(
        document_id=doc_id,
        filename=filename,
        status="queued",
        message="Ingestion started. Poll /status for completion.",
    )


def _stream_to_temp(file_obj, max_bytes: int, filename: str):
    hasher = hashlib.sha256()
    size = 0
    tmp_path, tmp_fh = storage.stream_to_temp()

    try:
        while True:
            chunk = file_obj.read(65_536)
            if not chunk:
                break
            size += len(chunk)
            if size > max_bytes:
                tmp_fh.close()
                storage.discard_temp(tmp_path)
                raise HTTPException(
                    413,
                    detail={
                        "error": "file_too_large",
                        "filename": filename,
                        "limit_mb": settings.max_upload_size_mb,
                    },
                )
            tmp_fh.write(chunk)
            hasher.update(chunk)
    except HTTPException:
        raise
    except Exception as exc:
        tmp_fh.close()
        storage.discard_temp(tmp_path)
        raise HTTPException(500, f"Upload stream error: {exc}") from exc
    finally:
        tmp_fh.close()

    return tmp_path, hasher.hexdigest(), size


def _run_ingestion_bg(document_id: str, db_url: str) -> None:
    from apps.api.services.ingestion import run_ingestion
    run_ingestion(document_id, db_url)


def _safe_filename(name: str) -> str:
    return os.path.basename(name.replace("\x00", "").strip())


def _get_or_404(db: Session, document_id: str) -> Document:
    doc = db.query(Document).filter_by(id=document_id).first()
    if not doc:
        raise HTTPException(404, "Document not found")
    return doc


@router.get("", response_model=DocumentListOut)
def list_documents(
    source_tag: Optional[str] = Query(None),
    status: Optional[str] = Query(None, pattern="^(pending|processing|done|error)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    q = db.query(Document)
    if source_tag:
        q = q.filter(Document.source_tag == source_tag)
    if status:
        q = q.filter(Document.status == status)
    total = q.count()
    items = (
        q.order_by(Document.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    return DocumentListOut(total=total, items=items)


@router.get("/{document_id}", response_model=DocumentOut)
def get_document(document_id: str, db: Session = Depends(get_db)):
    return _get_or_404(db, document_id)


@router.get("/{document_id}/status")
def get_document_status(document_id: str, db: Session = Depends(get_db)):
    doc = _get_or_404(db, document_id)
    return {
        "document_id": doc.id,
        "status": doc.status,
        "page_count": doc.page_count,
        "has_redactions": doc.has_redactions,
        "error_message": doc.error_message,
        "updated_at": doc.updated_at,
    }


@router.get("/{document_id}/pages", response_model=list[PageOut])
def list_pages(document_id: str, db: Session = Depends(get_db)):
    _get_or_404(db, document_id)
    pages = (
        db.query(DocumentPage)
        .filter_by(document_id=document_id)
        .order_by(DocumentPage.page_number)
        .all()
    )
    return [_enrich_page(db, p) for p in pages]


@router.get("/{document_id}/pages/{page_number}", response_model=PageOut)
def get_page(document_id: str, page_number: int, db: Session = Depends(get_db)):
    _get_or_404(db, document_id)
    page = db.query(DocumentPage).filter_by(document_id=document_id, page_number=page_number).first()
    if not page:
        raise HTTPException(404, f"Page {page_number} not found")
    return _enrich_page(db, page)


def _enrich_page(db: Session, page: DocumentPage) -> PageOut:
    mentions = (
        db.query(Mention, Entity)
        .join(Entity, Entity.id == Mention.entity_id)
        .filter(Mention.document_id == page.document_id, Mention.page_number == page.page_number)
        .all()
    )
    spans = [
        PageEntitySpan(
            id=entity.id,
            entity_type=entity.entity_type,
            canonical_name=entity.canonical_name,
            char_start=mention.char_start,
            char_end=mention.char_end,
            confidence=mention.confidence,
            extraction_method=mention.extraction_method,
        )
        for mention, entity in mentions
    ]
    return PageOut(
        id=page.id,
        document_id=page.document_id,
        page_number=page.page_number,
        final_text=page.final_text,
        word_count=page.word_count,
        ocr_used=page.ocr_used,
        has_redactions=page.has_redactions,
        entities=spans,
    )


@router.get("/{document_id}/redactions", response_model=list[RedactionFlagOut])
def list_redaction_flags(document_id: str, db: Session = Depends(get_db)):
    _get_or_404(db, document_id)
    return (
        db.query(RedactionFlag)
        .filter_by(document_id=document_id)
        .order_by(RedactionFlag.page_number, RedactionFlag.id)
        .all()
    )


def _wipe_document_derivatives(db: Session, document_id: str) -> None:
    mention_entity_ids = [
        row[0]
        for row in db.query(Mention.entity_id).filter(Mention.document_id == document_id).distinct().all()
        if row[0]
    ]

    db.execute(text("DELETE FROM mentions WHERE document_id = :id"), {"id": document_id})
    db.execute(text("DELETE FROM document_pages WHERE document_id = :id"), {"id": document_id})
    db.execute(text("DELETE FROM redaction_flags WHERE document_id = :id"), {"id": document_id})

    try:
        db.execute(text("DELETE FROM claims WHERE document_id = :id"), {"id": document_id})
    except Exception:
        pass

    try:
        db.execute(text("DELETE FROM semantic_chunks WHERE document_id = :id"), {"id": document_id})
    except Exception:
        pass

    if settings.database_url.startswith("sqlite"):
        try:
            db.execute(text("DELETE FROM fts_pages WHERE document_id = :id"), {"id": document_id})
        except Exception:
            pass

    if mention_entity_ids:
        for entity_id in mention_entity_ids:
            count = db.query(func.count(Mention.id)).filter(Mention.entity_id == entity_id).scalar() or 0
            db.query(Entity).filter(Entity.id == entity_id).update({"mention_count": count})

        rels = db.query(EntityRelationship).filter(
            EntityRelationship.entity_a_id.in_(mention_entity_ids) |
            EntityRelationship.entity_b_id.in_(mention_entity_ids)
        ).all()
        for rel in rels:
            if (rel.weight or 0) <= 0 or (rel.doc_count or 0) <= 0:
                db.delete(rel)

        db.query(Entity).filter(
            Entity.id.in_(mention_entity_ids),
            Entity.mention_count == 0,
        ).delete(synchronize_session=False)

    db.commit()


@router.delete("/{document_id}")
def delete_document(document_id: str, db: Session = Depends(get_db)):
    doc = _get_or_404(db, document_id)

    _wipe_document_derivatives(db, document_id)

    try:
        storage.delete_file(doc.filename)
    except Exception:
        pass

    db.delete(doc)
    db.commit()
    return {"deleted": document_id, "filename": doc.filename}


@router.post("/{document_id}/reingest")
def reingest_document(document_id: str, db: Session = Depends(get_db)):
    from apps.api.services.ingestion import run_ingestion
    import threading

    doc = _get_or_404(db, document_id)

    if doc.status == "processing":
        raise HTTPException(409, "Document is currently processing")

    _wipe_document_derivatives(db, document_id)

    doc.status = "pending"
    doc.page_count = 0
    doc.has_redactions = False
    doc.error_message = None
    db.commit()

    t = threading.Thread(
        target=run_ingestion,
        args=(document_id, settings.database_url),
        daemon=True,
    )
    t.start()

    return {"reingesting": document_id, "filename": doc.filename}
