"""
OpenSight — Bundle API Routes
apps/api/routes/bundles.py

Mount in apps/api/main.py:
    from apps.api.routes.bundles import router as bundle_router
    app.include_router(bundle_router, prefix="/api/v1")
"""

from __future__ import annotations

import os
from typing import Optional

from fastapi import APIRouter, Form, HTTPException, UploadFile, File
from fastapi.responses import FileResponse

from apps.narrative.bundle_engine import BundleExporter, BundleImporter

router   = APIRouter(tags=["bundles"])
exporter = BundleExporter()
importer = BundleImporter()


@router.post("/bundles/export")
def export_bundle(
    source_tag:          str  = Form(...),
    title:               str  = Form(""),
    author:              str  = Form(""),
    notes:               str  = Form(""),
    include_documents:   bool = Form(True),
):
    """
    Export an investigation as a .osight bundle file.
    Returns the file as a download.
    """
    try:
        path = exporter.export(
            source_tag=source_tag,
            title=title,
            author=author,
            notes=notes,
            include_documents=include_documents,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not os.path.exists(path):
        raise HTTPException(status_code=500, detail="Bundle file not created")

    filename = os.path.basename(path)
    return FileResponse(
        path=path,
        media_type="application/zip",
        filename=filename,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/bundles/import")
async def import_bundle(file: UploadFile = File(...)):
    """
    Import a .osight bundle into this OpenSight instance.
    Remaps all IDs to avoid collisions with existing data.
    """
    if not file.filename or not file.filename.endswith(".osight"):
        raise HTTPException(
            status_code=415,
            detail="File must be a .osight bundle"
        )

    # Write upload to temp file
    import tempfile
    suffix = ".osight"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        result = importer.import_bundle(tmp_path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        os.unlink(tmp_path)

    return result


@router.get("/bundles/list")
def list_exportable():
    """
    List all source_tags available for export.
    """
    import sqlite3
    with sqlite3.connect("opensight.db") as con:
        rows = con.execute(
            "SELECT source_tag, COUNT(*) as docs "
            "FROM documents WHERE source_tag IS NOT NULL AND source_tag != '' "
            "GROUP BY source_tag"
        ).fetchall()
    return [{"source_tag": r[0], "document_count": r[1]} for r in rows]
