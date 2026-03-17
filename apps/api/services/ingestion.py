"""
apps/api/services/ingestion.py

Document ingestion pipeline. Runs in a FastAPI BackgroundTask thread.

Pipeline stages (each is isolated — a stage failure updates status and stops):

  Stage 1  LOAD      — fetch Document record, mark status=processing
  Stage 2  EXTRACT   — pull text from each page (pdfplumber → pypdf fallback)
  Stage 3  OCR       — augment sparse PDF pages with OCR when USE_OCR=true
  Stage 4  PERSIST   — write DocumentPage rows, run NER, write Entity+Mention rows
  Stage 5  INDEX     — insert page text into FTS5 virtual table
  Stage 6  REDACT    — detect redaction regions (requires PyMuPDF; skips if absent)
  Stage 7  DONE      — set status=done, record completion

Each stage is a named function with a clear input/output contract.
Error isolation: an exception in stage N sets status=error and halts;
earlier committed stages are preserved.
"""

import logging
from sqlalchemy import create_engine
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import text, update
from sqlalchemy.orm import Session

from apps.api.config import settings
from apps.api.services.canonicalize import run_canonicalization, is_noise_entity
from apps.api.services.relationship_extraction import extract_relationships
from apps.api.models import Document, DocumentPage, Entity, EntityRelationship, Mention, RedactionFlag
from apps.api.services.entity_extraction import extract_entities
from apps.api.services.claim_extraction import extract_claims, ClaimCandidate
from apps.api.services.semantic_bridge import index_page as sem_index_page, index_entity as sem_index_entity, is_available as sem_available
from apps.api.services.storage import storage

log = logging.getLogger(__name__)


def _wipe_document_outputs(db: Session, document_id: str, source_tag: str | None = None) -> None:
    db.query(Mention).filter_by(document_id=document_id).delete()
    db.query(DocumentPage).filter_by(document_id=document_id).delete()
    db.query(RedactionFlag).filter_by(document_id=document_id).delete()
    try:
        from apps.api.models import Claim
        db.query(Claim).filter_by(document_id=document_id).delete()
    except Exception:
        pass
    try:
        db.execute(text("DELETE FROM semantic_chunks WHERE document_id = :id"), {"id": document_id})
    except Exception:
        pass
    try:
        db.execute(text("DELETE FROM fts_pages WHERE document_id = :id"), {"id": document_id})
    except Exception:
        pass
    if source_tag is not None:
        try:
            db.execute(
                text("DELETE FROM fts_pages WHERE document_id = :id OR source_tag = :source_tag"),
                {"id": document_id, "source_tag": source_tag},
            )
        except Exception:
            pass
    db.commit()



# ═══════════════════════════════════════════════════════════════════════════════
# Stage 2 — Text extraction
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_pdf_pages(file_path: Path) -> dict[int, str]:
    """
    Extract native text from a PDF, one entry per page (1-indexed).
    Tries pdfplumber first (better layout reconstruction), then pypdf.
    Returns an empty string for pages with no extractable text.
    """
    try:
        import pdfplumber  # type: ignore
        pages: dict[int, str] = {}
        with pdfplumber.open(str(file_path)) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                pages[i] = (page.extract_text() or "").strip()
        return pages
    except Exception as exc:
        log.warning("[extract] pdfplumber failed (%s), falling back to pypdf", exc)

    try:
        from pypdf import PdfReader  # type: ignore
        reader = PdfReader(str(file_path))
        return {i + 1: (p.extract_text() or "").strip() for i, p in enumerate(reader.pages)}
    except Exception as exc:
        log.error("[extract] pypdf also failed: %s", exc)
        return {1: ""}


def _extract_text(file_path: Path) -> dict[int, str]:
    """
    Route to the correct extractor based on file extension.
    Returns {page_number: text}. Raises ValueError for unsupported types.
    """
    suffix = file_path.suffix.lower()

    if suffix == ".pdf":
        return _extract_pdf_pages(file_path)

    if suffix in (".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp"):
        # Single-page image — text comes from OCR in stage 3.
        return {1: ""}

    if suffix in (".txt", ".md"):
        return {1: file_path.read_text(errors="replace")}

    raise ValueError(f"Unsupported file type: {suffix}")


# ═══════════════════════════════════════════════════════════════════════════════
# Stage 3 — OCR augmentation
# ═══════════════════════════════════════════════════════════════════════════════

_OCR_SPARSE_THRESHOLD = 40  # characters; fewer than this triggers OCR for a PDF page


def _ocr_single_image(file_path: Path) -> str:
    """Run pytesseract on a standalone image file."""
    try:
        import pytesseract  # type: ignore
        from PIL import Image  # type: ignore
        img = Image.open(str(file_path)).convert("L")
        return pytesseract.image_to_string(img, lang=settings.ocr_language).strip()
    except ImportError:
        log.info("[ocr] pytesseract/Pillow not installed — OCR skipped for %s", file_path.name)
        return ""
    except Exception as exc:
        log.error("[ocr] failed for %s: %s", file_path.name, exc)
        return ""


def _run_ocr_stage(
    file_path: Path,
    page_texts: dict[int, str],
) -> dict[int, tuple[str, bool]]:
    """
    Stage 3: Augment sparse PDF pages (or image pages) with OCR.

    Returns {page_number: (final_text, ocr_was_used)}.

    For PDFs: uses PyMuPDF to render pages as images, then pytesseract to OCR.
    For image files: runs pytesseract directly on the file.
    OCR is skipped gracefully if neither PyMuPDF nor pytesseract is installed.
    """
    suffix = file_path.suffix.lower()
    result: dict[int, tuple[str, bool]] = {}

    if suffix in (".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp"):
        ocr_text = _ocr_single_image(file_path)
        result[1] = (ocr_text, bool(ocr_text))
        return result

    # PDF: augment pages that have too little native text
    if suffix != ".pdf":
        return {pn: (text, False) for pn, text in page_texts.items()}

    try:
        import fitz         # type: ignore (PyMuPDF)
        import pytesseract  # type: ignore
        from PIL import Image  # type: ignore
    except ImportError:
        log.info("[ocr] PyMuPDF or pytesseract not installed — OCR skipped")
        return {pn: (text, False) for pn, text in page_texts.items()}

    try:
        pdf_doc = fitz.open(str(file_path))
    except Exception as exc:
        log.warning("[ocr] fitz.open failed: %s", exc)
        return {pn: (text, False) for pn, text in page_texts.items()}

    for page_num, native_text in page_texts.items():
        if len(native_text) >= _OCR_SPARSE_THRESHOLD:
            result[page_num] = (native_text, False)
            continue
        try:
            page = pdf_doc[page_num - 1]
            pix  = page.get_pixmap(matrix=fitz.Matrix(300 / 72, 300 / 72))
            img  = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            ocr  = pytesseract.image_to_string(img, lang=settings.ocr_language).strip()
            result[page_num] = (ocr if ocr else native_text, bool(ocr))
            if ocr:
                log.debug("[ocr] page %d: OCR produced %d chars", page_num, len(ocr))
        except Exception as exc:
            log.warning("[ocr] page %d failed: %s", page_num, exc)
            result[page_num] = (native_text, False)

    pdf_doc.close()
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Stage 5 — FTS indexing
# ═══════════════════════════════════════════════════════════════════════════════

def _index_page_fts(
    db: Session,
    document_id: str,
    page_number: int,
    source_tag: str,
    filename: str,
    body: str,
) -> None:
    """Insert one page into the FTS5 virtual table. No-op for non-SQLite or missing FTS table."""
    if not settings.database_url.startswith("sqlite"):
        return
    try:
        exists = db.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='fts_pages'" )).fetchone()
        if not exists:
            log.warning("[fts] fts_pages missing; skipping FTS insert for %s page %s", document_id, page_number)
            return
        db.execute(
            text("""
                INSERT INTO fts_pages (document_id, page_number, source_tag, filename, body)
                VALUES (:did, :pn, :st, :fn, :body)
            """),
            {"did": document_id, "pn": page_number, "st": source_tag, "fn": filename, "body": body},
        )
    except Exception as exc:
        log.warning("[fts] insert skipped for %s page %s: %s", document_id, page_number, exc)
        db.rollback()


# ═══════════════════════════════════════════════════════════════════════════════
# Stage 5.5 — Claim extraction
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_and_persist_claims(
    document_id: str,
    pages: dict,
    entity_map: dict,  # canonical_name -> entity_id
    db: Session,
) -> int:
    """
    Extract attributed claims from each page and persist to claims table.
    entity_map is used to resolve speaker/subject names to entity IDs.
    Returns count of claims created.
    """
    from apps.api.models import Claim
    from apps.api.config import settings

    known_entities = list(entity_map.keys())
    total = 0

    for page_num, text in pages.items():
        if not text or len(text) < 50:
            continue
        try:
            candidates = extract_claims(
                text=text,
                known_entities=known_entities,
                use_ollama=getattr(settings, "use_ollama", False),
                ollama_url=getattr(settings, "ollama_url", "http://localhost:11434"),
                ollama_model=getattr(settings, "ollama_model", "phi3"),
            )
            for cand in candidates:
                # Resolve names to entity IDs (case-insensitive)
                speaker_id = None
                subject_id = None
                if cand.speaker_name:
                    key = cand.speaker_name.lower()
                    speaker_id = next((v for k, v in entity_map.items() if k.lower() == key or key in k.lower()), None)
                if cand.subject_name:
                    key = cand.subject_name.lower()
                    subject_id = next((v for k, v in entity_map.items() if k.lower() == key or key in k.lower()), None)

                claim = Claim(
                    document_id=document_id,
                    page_number=page_num,
                    speaker_entity_id=speaker_id,
                    subject_entity_id=subject_id,
                    claim_text=cand.claim_text,
                    claim_type=cand.claim_type,
                    sentiment=cand.sentiment,
                    confidence=cand.confidence,
                    extraction_method=cand.method,
                )
                db.add(claim)
                total += 1
            db.commit()
        except Exception as exc:
            log.warning("[claims] Page %d extraction failed: %s", page_num, exc)
            db.rollback()

    log.info("[claims] Created %d claims for document %s", total, document_id)
    return total

# ═══════════════════════════════════════════════════════════════════════════════
# Stage 6 — Redaction detection
# ═══════════════════════════════════════════════════════════════════════════════

def _detect_redactions(
    file_path: Path,
    document_id: str,
    page_texts: dict[int, str],
) -> list[RedactionFlag]:
    """
    Detect likely redactions in a PDF. Requires PyMuPDF (pip install pymupdf).
    Returns [] silently if PyMuPDF is absent or detection is disabled.

    Strategies:
      black_box   — filled rectangles with dark fill (≥30×8 PDF points)
      hidden_text — words present in the PDF stream absent from extracted text
    """
    if not settings.use_redaction_detection:
        return []

    try:
        import fitz  # type: ignore
    except ImportError:
        log.info("[redact] PyMuPDF not installed — skipping (pip install pymupdf)")
        return []

    try:
        doc = fitz.open(str(file_path))
    except Exception as exc:
        log.warning("[redact] fitz.open failed for %s: %s", file_path.name, exc)
        return []

    flags: list[RedactionFlag] = []

    for page_idx in range(len(doc)):
        page    = doc[page_idx]
        page_num = page_idx + 1

        for path in page.get_drawings():
            if path.get("type") != "f":
                continue
            fill = path.get("fill")
            if fill and all(c <= 0.15 for c in fill[:3]):
                rect = path["rect"]
                if rect.width >= 30 and rect.height >= 8:
                    flags.append(RedactionFlag(
                        document_id=document_id,
                        page_number=page_num,
                        flag_type="black_box",
                        bounding_box={
                            "x": round(rect.x0, 2), "y": round(rect.y0, 2),
                            "w": round(rect.width, 2), "h": round(rect.height, 2),
                        },
                        confidence=0.80,
                    ))

        native_text = page_texts.get(page_num, "")
        for word_data in page.get_text("words"):
            word = word_data[4].strip()
            if len(word) > 3 and word not in native_text:
                flags.append(RedactionFlag(
                    document_id=document_id,
                    page_number=page_num,
                    flag_type="hidden_text",
                    bounding_box={
                        "x": round(word_data[0], 2), "y": round(word_data[1], 2),
                        "w": round(word_data[2] - word_data[0], 2),
                        "h": round(word_data[3] - word_data[1], 2),
                    },
                    hidden_text=word[:200],
                    confidence=0.75,
                ))
                break  # one hidden-text flag per page is sufficient

    doc.close()
    return flags


# ═══════════════════════════════════════════════════════════════════════════════
# Stage 4 helpers — entity persistence
# ═══════════════════════════════════════════════════════════════════════════════

def _upsert_entity(
    db: Session,
    entity_type: str,
    canonical_name: str,
    raw_name: str,
    confidence: float,
) -> Entity:
    """Get or create an Entity row; merge aliases. Does not commit."""
    existing = (
        db.query(Entity)
        .filter_by(entity_type=entity_type, canonical_name=canonical_name)
        .first()
    )
    if existing:
        aliases = list(existing.aliases or [])
        if raw_name not in aliases and raw_name != canonical_name:
            aliases.append(raw_name)
            existing.aliases = aliases
        return existing

    entity = Entity(
        entity_type=entity_type,
        canonical_name=canonical_name,
        aliases=[raw_name] if raw_name != canonical_name else [],
        confidence=confidence,
        review_status="auto",
    )
    db.add(entity)
    db.flush()
    return entity



def _upsert_typed_relationships(
    db: Session,
    document_id: str,
    full_text: str,
    spacy_model: str = "en_core_web_sm",
) -> int:
    """
    Extract typed relationships for a document and upsert into entity_relationships.
    For existing edges: keep higher-confidence type. Increment weight on re-ingest.
    """
    from apps.api.services.relationship_extraction import extract_relationships

    # Build entity name → id map from mentions in this document
    from apps.api.models import Mention
    mention_rows = (
        db.query(Mention, Entity)
        .join(Entity, Entity.id == Mention.entity_id)
        .filter(Mention.document_id == document_id)
        .all()
    )
    entity_name_to_id = {e.canonical_name: e.id for _, e in mention_rows}

    if not entity_name_to_id:
        log.debug("[%s] No entities for relationship extraction", document_id)
        return 0

    candidates = extract_relationships(
        text=full_text,
        entity_name_to_id=entity_name_to_id,
        document_id=document_id,
        spacy_model=spacy_model,
    )

    upserted = 0
    for c in candidates:
        # Normalize pair order
        id_a = min(c.source_id, c.target_id)
        id_b = max(c.source_id, c.target_id)

        existing = (
            db.query(EntityRelationship)
            .filter_by(entity_a_id=id_a, entity_b_id=id_b)
            .first()
        )

        if existing:
            existing.weight    += 1
            existing.doc_count  = max(existing.doc_count, 1)
            # Upgrade relationship_type if new extraction has higher confidence
            existing_conf = existing.confidence or 0.0
            if c.confidence > existing_conf:
                existing.relationship_type = c.relationship_type
                existing.confidence        = c.confidence
                existing.sentence_span     = c.sentence_span
        else:
            db.add(EntityRelationship(
                entity_a_id       = id_a,
                entity_b_id       = id_b,
                weight            = 1,
                doc_count         = 1,
                relationship_type = c.relationship_type,
                confidence        = c.confidence,
                sentence_span     = c.sentence_span,
            ))
        upserted += 1

    log.info("[%s] Stage 6.5/7 — %d typed relationships upserted", document_id, upserted)
    return upserted

def _upsert_relationships(db: Session, document_id: str) -> int:
    """
    Build or update entity co-occurrence relationships for a document.
    For every pair of distinct entities that appear in the same document,
    increment their shared weight by 1. If the relationship doesn't exist yet,
    create it. Always stores (min_id, max_id) to avoid duplicates.
    Returns the number of pairs processed.
    """
    from itertools import combinations

    # Collect distinct entity IDs that appear in this document
    entity_ids = [
        row[0] for row in
        db.execute(
            text("SELECT DISTINCT entity_id FROM mentions WHERE document_id = :doc_id"),
            {"doc_id": document_id},
        ).fetchall()
    ]

    pairs_processed = 0
    for id_a, id_b in combinations(sorted(entity_ids), 2):
        existing = (
            db.query(EntityRelationship)
            .filter_by(entity_a_id=id_a, entity_b_id=id_b)
            .first()
        )
        if existing:
            existing.weight   += 1
            existing.doc_count = (
                db.execute(
                    text("""
                        SELECT COUNT(DISTINCT m1.document_id)
                        FROM mentions m1
                        JOIN mentions m2 ON m1.document_id = m2.document_id
                        WHERE m1.entity_id = :a AND m2.entity_id = :b
                    """),
                    {"a": id_a, "b": id_b},
                ).scalar() or 1
            )
        else:
            db.add(EntityRelationship(
                entity_a_id=id_a,
                entity_b_id=id_b,
                weight=1,
                doc_count=1,
            ))
        pairs_processed += 1

    return pairs_processed


# ═══════════════════════════════════════════════════════════════════════════════
# Main pipeline entry points
# ═══════════════════════════════════════════════════════════════════════════════

def run_ingestion(document_id: str, db_url: str) -> None:
    """
    Full ingestion pipeline for one document. Opens its own DB session.
    Called by FastAPI BackgroundTasks — runs in a separate thread.
    """
    from apps.api.database import SessionLocal

    db = SessionLocal()

    try:
        doc = db.query(Document).filter_by(id=document_id).first()
        if not doc:
            return

        _wipe_document_outputs(db, document_id, getattr(doc, "source_tag", None))

        doc = db.query(Document).filter_by(id=document_id).first()
        if not doc:
            return

        doc.status = "processing"
        doc.page_count = 0
        doc.has_redactions = False
        doc.error_message = None
        db.commit()

        _run_pipeline(document_id, db)

        doc = db.query(Document).filter_by(id=document_id).first()
        if doc:
            doc.status = "done"
            doc.error_message = None
            db.commit()

    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        log.exception("[pipeline] fatal error for document %s", document_id)
        doc = db.query(Document).filter_by(id=document_id).first()
        if doc:
            doc.status = "error"
            doc.error_message = str(exc)[:500]
            db.commit()
    finally:
        db.close()


def _run_pipeline(document_id: str, db: Session) -> None:
    t0 = time.monotonic()

    # ── Stage 1: Load ────────────────────────────────────────────────────────
    doc = db.query(Document).filter_by(id=document_id).first()
    if not doc:
        raise ValueError(f"Document {document_id} not found in database")

    log.info("[%s] Starting ingestion: %s", document_id, doc.filename)
    doc.status = "processing"
    db.commit()

    file_path = storage.resolve(doc.storage_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Stored file missing: {file_path}")

    # ── Stage 2: Text extraction ─────────────────────────────────────────────
    log.info("[%s] Stage 2/7 — text extraction", document_id)
    page_texts = _extract_text(file_path)
    doc.page_count = len(page_texts)
    db.commit()

    # ── Stage 3: OCR augmentation ────────────────────────────────────────────
    if settings.use_ocr:
        log.info("[%s] Stage 3/7 — OCR augmentation (%d pages)", document_id, len(page_texts))
        page_data = _run_ocr_stage(file_path, page_texts)
    else:
        page_data = {pn: (text, False) for pn, text in page_texts.items()}

    # ── Stage 4+5: Persist pages, NER, FTS index ─────────────────────────────
    log.info("[%s] Stage 4/7 — persisting pages and extracting entities", document_id)
    total_entities = 0

    for page_num in sorted(page_data.keys()):
        final_text, ocr_used = page_data[page_num]
        final_text = final_text.strip()

        page_row = DocumentPage(
            document_id=document_id,
            page_number=page_num,
            raw_text=page_texts.get(page_num, ""),
            ocr_text=final_text if ocr_used else "",
            final_text=final_text,
            word_count=len(final_text.split()) if final_text else 0,
            ocr_used=ocr_used,
        )
        db.add(page_row)
        db.flush()

        # Stage 5: FTS
        _index_page_fts(db, document_id, page_num, doc.source_tag, doc.filename, final_text)

        # Stage 4 continued: NER → Entity + Mention rows
        if final_text:
            extracted = extract_entities(
                text=final_text,
                document_id=document_id,
                page_number=page_num,
                spacy_model=settings.spacy_model,
            )
            for ex in extracted:
                try:
                    entity = _upsert_entity(
                        db,
                        entity_type=ex.entity_type,
                        canonical_name=ex.canonical_name,
                        raw_name=ex.raw_name,
                        confidence=ex.confidence,
                    )
                    if entity is None:
                        continue
                    db.add(Mention(
                        entity_id=entity.id,
                        document_id=document_id,
                        page_number=page_num,
                        char_start=ex.char_start,
                        char_end=ex.char_end,
                        context_text=ex.context[:400],
                        extraction_method=ex.extraction_method,
                        confidence=ex.confidence,
                    ))
                    total_entities += 1
                except Exception as exc:
                    log.warning(
                        "[%s] entity persist failed (%s @ %d:%d): %s",
                        document_id, ex.canonical_name, ex.char_start, ex.char_end, exc,
                    )
                    db.rollback()

        db.commit()    # Assemble full document text for relationship extraction
    full_text = " ".join(page_texts.get(pn, "") for pn in sorted(page_texts.keys()))
    # ── Stage 4.5: Update mention counts ────────────────────────────────────
    try:
        db.execute(text("""
            UPDATE entities SET mention_count = (
                SELECT COUNT(*) FROM mentions WHERE mentions.entity_id = entities.id
            )
            WHERE id IN (
                SELECT DISTINCT entity_id FROM mentions WHERE document_id = :doc_id
            )
        """), {"doc_id": document_id})
        db.commit()
        log.debug("[%s] Stage 4.5/7 — mention counts updated", document_id)
    except Exception as exc:
        log.warning("[run_ingestion] mention count update failed: %s", exc)

    # ── Stage 5.2: Semantic indexing ─────────────────────────────────────────
    try:
        if sem_available():
            sem_indexed = 0
            for pg in db.query(DocumentPage).filter_by(document_id=document_id).all():
                if pg.final_text:
                    ok = sem_index_page(
                        document_id=document_id,
                        page_number=pg.page_number,
                        text=pg.final_text,
                        doc_type=doc.doc_type,
                        source_tag=doc.source_tag,
                        filename=doc.filename,
                    )
                    if ok:
                        sem_indexed += 1
            # Index entity contexts
            entity_ids_in_doc = [
                m.entity_id for m in
                db.query(Mention.entity_id).filter_by(document_id=document_id).distinct()
            ]
            for eid in entity_ids_in_doc:
                entity = db.query(Entity).filter_by(id=eid).first()
                if not entity:
                    continue
                mentions = db.query(Mention).filter_by(
                    entity_id=eid, document_id=document_id
                ).limit(5).all()
                contexts = [m.context_text for m in mentions if m.context_text]
                if contexts:
                    sem_index_entity(
                        entity_id=eid,
                        entity_name=entity.canonical_name,
                        entity_type=entity.entity_type,
                        context_texts=contexts,
                        document_ids=[document_id],
                    )
            log.info("[%s] Stage 5.2/7 — semantic indexed %d pages", document_id, sem_indexed)
        else:
            log.debug("[%s] Stage 5.2/7 — semantic service offline, skipping", document_id)
    except Exception as exc:
        log.warning("[run_ingestion] Semantic index stage skipped: %s", exc)

    # ── Stage 5.5: Claim extraction ──────────────────────────────────────────
    try:
        entity_map = {
            e.canonical_name: e.id
            for e in db.query(Entity).join(
                Mention, Mention.entity_id == Entity.id
            ).filter(Mention.document_id == document_id).all()
        }
        final_pages = {
            pg.page_number: pg.final_text
            for pg in db.query(DocumentPage).filter_by(document_id=document_id).all()
        }
        _extract_and_persist_claims(document_id, final_pages, entity_map, db)
    except Exception as exc:
        log.warning("[run_ingestion] Claims stage skipped: %s", exc)

    # ── Stage 6: Redaction detection ─────────────────────────────────────────
    redaction_count = 0
    if file_path.suffix.lower() == ".pdf":
        log.info("[%s] Stage 6/7 — redaction detection", document_id)
        flags = _detect_redactions(file_path, document_id, page_texts)
        redaction_count = len(flags)

        if flags:
            for flag in flags:
                db.add(flag)

            flagged_pages = {f.page_number for f in flags}
            db.execute(
                update(DocumentPage)
                .where(
                    DocumentPage.document_id == document_id,
                    DocumentPage.page_number.in_(flagged_pages),
                )
                .values(has_redactions=True)
            )
            doc.has_redactions = True

        db.commit()

    # ── Stage 6.5: Typed relationship extraction ──────────────────────────────
    log.info("[%s] Stage 6.5/7 — typed relationship extraction", document_id)
    _upsert_typed_relationships(db, document_id, full_text, settings.spacy_model)
    db.commit()

    # ── Stage 6.6: Canonicalization ───────────────────────────────────────────
    log.info("[%s] Stage 6.6/7 — running canonicalization pass", document_id)
    canon_summary = run_canonicalization(db, commit=True)
    log.info("[%s] Stage 6.6/7 — canon: %s", document_id, canon_summary)

    # ── Stage 7: Done ─────────────────────────────────────────────────────────
    doc.status     = "done"
    doc.updated_at = datetime.utcnow()
    db.commit()

    elapsed = time.monotonic() - t0
    log.info(
        "[%s] Stage 7/7 — done in %.1fs | pages=%d entities=%d redactions=%d",
        document_id, elapsed, doc.page_count, total_entities, redaction_count,
    )
