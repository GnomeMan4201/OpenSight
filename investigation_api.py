from __future__ import annotations
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from investigations.case_manager import create_case, load_case, save_case, list_cases
from modules.data_ingestion.document_ingest import ingest_text_file
from modules.analysis.narrative_analysis import summarize_case
from modules.graph_engine.intel_graph import build_case_graph
from modules.timeline_engine.timeline_builder import build_timeline
from modules.visualization.graph_visualizer import write_graph_html

app = FastAPI(title="OpenSight Investigation Engine")

class CaseCreate(BaseModel):
    title: str
    description: str = ""

class IngestRequest(BaseModel):
    path: str

@app.get("/health")
def health():
    return {"ok": True, "service": "investigation_api"}

@app.get("/cases")
def api_list_cases():
    return list_cases()

@app.post("/cases")
def api_create_case(payload: CaseCreate):
    return create_case(payload.title, payload.description)

@app.get("/cases/{case_id}")
def api_get_case(case_id: str):
    try:
        return load_case(case_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="case not found")

@app.post("/cases/{case_id}/ingest")
def api_ingest(case_id: str, payload: IngestRequest):
    try:
        case = load_case(case_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="case not found")

    doc = ingest_text_file(payload.path)
    case["documents"].append(doc)

    seen = {(e["type"], e["value"]) for e in case.get("entities", [])}
    for entity in doc["entities"]:
        key = (entity["type"], entity["value"])
        if key not in seen:
            case["entities"].append(entity)
            seen.add(key)

    case["events"] = build_timeline(case)
    save_case(case)
    return {"ok": True, "document": doc, "case": case}

@app.get("/cases/{case_id}/summary")
def api_case_summary(case_id: str):
    try:
        case = load_case(case_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="case not found")
    return summarize_case(case)

@app.get("/cases/{case_id}/graph")
def api_case_graph(case_id: str):
    try:
        case = load_case(case_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="case not found")
    return build_case_graph(case)

@app.post("/cases/{case_id}/graph/render")
def api_render_graph(case_id: str):
    try:
        case = load_case(case_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="case not found")
    graph = build_case_graph(case)
    out_path = write_graph_html(graph)
    return {"ok": True, "html": out_path}

@app.get("/cases/{case_id}/timeline")
def api_case_timeline(case_id: str):
    try:
        case = load_case(case_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="case not found")
    return build_timeline(case)
