"""
apps/api/main.py
"""
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from apps.api.config import settings
from apps.api.database import engine
try:
    from apps.api.database import Base
except ImportError:
    from apps.api.models import Base
from apps.api.routes import documents, search, entities, annotations, graph
from apps.api.routes.analysis import router as analysis_router
from apps.api.routes.redaction_audit import router as redaction_router
from apps.api.routes.leads import router as leads_router
from apps.api.routes.claims import router as claims_router
from apps.api.routes.intelligence import router as intelligence_router
from apps.api.routes.narrative import router as narrative_router
from apps.api.routes.narrative_intel import router as narrative_intel_router
from apps.api.routes.provenance import router as provenance_router
from apps.api.routes.bundles import router as bundle_router
from apps.api.routes.dedup import router as dedup_router

log = logging.getLogger("opensight")

Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="OpenSight",
    description="Investigative document intelligence platform",
    version=settings.app_version,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── API routers (all before StaticFiles) ───────────────────────────────────────
app.include_router(documents.router,   prefix="/api/v1/documents",   tags=["documents"])
app.include_router(search.router,      prefix="/api/v1/search",      tags=["search"])
app.include_router(dedup_router,       prefix="/api/v1/entities",    tags=["dedup"])
app.include_router(entities.router,    prefix="/api/v1/entities",    tags=["entities"])
app.include_router(annotations.router, prefix="/api/v1/annotations", tags=["annotations"])
app.include_router(graph.router,       prefix="/api/v1/graph",       tags=["graph"])
app.include_router(analysis_router,    prefix="/api/v1/analysis",    tags=["analysis"])
app.include_router(redaction_router,   prefix="/api/v1/redaction",   tags=["redaction"])
app.include_router(leads_router,        prefix="/api/v1/analysis",    tags=["leads"])
app.include_router(claims_router,       prefix="/api/v1",             tags=["claims"])
app.include_router(intelligence_router, prefix="/api/v1",             tags=["intelligence"])
app.include_router(narrative_router,    prefix="/api/v1",             tags=["narrative_nie"])


@app.get("/health", tags=["health"])
def health():
    return {
        "status":                  "ok",
        "version":                 settings.app_version,
        "database":                settings.database_url.split("://")[0],
        "use_ocr":                 settings.use_ocr,
        "use_redaction_detection": settings.use_redaction_detection,
    }


@app.get("/", include_in_schema=False)
def root():
    return JSONResponse({
        "platform": "OpenSight",
        "version":  settings.app_version,
        "docs":     "/docs",
    })


# ── Static UI — MUST BE LAST ───────────────────────────────────────────────────
_ui_dir = Path("/home/bad_banana/Downloads/opensight")
app.mount("/ui", StaticFiles(directory=str(_ui_dir), html=True), name="ui")

app.include_router(narrative_intel_router)
app.include_router(provenance_router, prefix="/api/v1")
app.include_router(bundle_router, prefix="/api/v1")
from apps.api.routes import graph_metrics
from apps.api.routes.graph_insights import router as graph_insights_router
from apps.api.routes.graph_timeline import router as graph_timeline_router
from apps.api.routes.narrative_summary import router as narrative_summary_router
app.include_router(graph_metrics.router, prefix="/api/v1")
app.include_router(graph_insights_router, prefix="/api/v1")
app.include_router(graph_timeline_router, prefix="/api/v1")
app.include_router(narrative_summary_router, prefix="/api/v1")
