"""
apps/api/routes/annotations.py
Create, list, and soft-delete annotations on document pages.
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from apps.api.database import get_db
from apps.api.models import Annotation, Document
from apps.api.schemas import AnnotationCreate, AnnotationListOut, AnnotationOut

router = APIRouter()


@router.post("", response_model=AnnotationOut, status_code=201)
def create_annotation(body: AnnotationCreate, db: Session = Depends(get_db)):
    # Verify document exists
    doc = db.query(Document).filter_by(id=body.document_id).first()
    if not doc:
        raise HTTPException(404, f"Document {body.document_id} not found")

    annotation = Annotation(
        document_id=body.document_id,
        page_number=body.page_number,
        annotation_type=body.annotation_type,
        char_start=body.char_start,
        char_end=body.char_end,
        highlighted_text=body.highlighted_text,
        note_text=body.note_text,
        color=body.color,
        author=body.author,
    )
    db.add(annotation)
    db.commit()
    db.refresh(annotation)
    return annotation


@router.get("", response_model=AnnotationListOut)
def list_annotations(
    document_id: Optional[str] = Query(None),
    page_number: Optional[int] = Query(None),
    author: Optional[str] = Query(None),
    annotation_type: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    q = db.query(Annotation).filter(Annotation.is_deleted == False)

    if document_id:
        q = q.filter(Annotation.document_id == document_id)
    if page_number is not None:
        q = q.filter(Annotation.page_number == page_number)
    if author:
        q = q.filter(Annotation.author == author)
    if annotation_type:
        q = q.filter(Annotation.annotation_type == annotation_type)

    total = q.count()
    items = (
        q.order_by(Annotation.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    return AnnotationListOut(total=total, items=items)


@router.get("/{annotation_id}", response_model=AnnotationOut)
def get_annotation(annotation_id: str, db: Session = Depends(get_db)):
    ann = db.query(Annotation).filter_by(id=annotation_id, is_deleted=False).first()
    if not ann:
        raise HTTPException(404, "Annotation not found")
    return ann


@router.delete("/{annotation_id}", status_code=204)
def delete_annotation(annotation_id: str, db: Session = Depends(get_db)):
    ann = db.query(Annotation).filter_by(id=annotation_id).first()
    if not ann:
        raise HTTPException(404, "Annotation not found")
    ann.is_deleted = True
    db.commit()
