from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from apps.api.database import get_db
from apps.api.models import EntityRelationship, Entity, Mention, Document
from apps.api.services.graph_insights import build_nx_graph, top_broker_paths, graph_communities

router = APIRouter(tags=["graph_insights"])


def _source_entity_ids(db: Session, source_tag: str | None):
    if not source_tag:
        return None
    rows = (
        db.query(Mention.entity_id)
        .join(Document, Document.id == Mention.document_id)
        .filter(Document.source_tag == source_tag)
        .distinct()
        .all()
    )
    return {r[0] for r in rows}


@router.get("/graph/broker-paths/{entity_id}")
def graph_broker_paths(
    entity_id: str,
    limit: int = Query(8, ge=1, le=25),
    source_tag: str | None = Query(None),
    db: Session = Depends(get_db),
):
    rels = db.query(EntityRelationship).filter(EntityRelationship.weight >= 1).all()

    entity_ids = _source_entity_ids(db, source_tag)
    if entity_ids is not None:
        rels = [r for r in rels if r.entity_a_id in entity_ids and r.entity_b_id in entity_ids]

    G = build_nx_graph(rels)
    raw = top_broker_paths(G, entity_id, limit=limit)

    entity_rows = db.query(Entity).all()
    emap = {str(e.id): e for e in entity_rows}

    out = []
    for item in raw:
        out.append({
            "length": item["length"],
            "bridge_position": item["bridge_position"],
            "bridge_score": item["bridge_score"],
            "nodes": [
                {
                    "id": nid,
                    "canonical_name": emap[nid].canonical_name if nid in emap else nid,
                    "entity_type": emap[nid].entity_type if nid in emap else "unknown",
                }
                for nid in item["path"]
            ]
        })

    return {
        "entity_id": entity_id,
        "count": len(out),
        "paths": out,
    }


@router.get("/graph/communities")
def get_graph_communities(
    source_tag: str | None = Query(None),
    db: Session = Depends(get_db),
):
    rels = db.query(EntityRelationship).filter(EntityRelationship.weight >= 1).all()

    entity_ids = _source_entity_ids(db, source_tag)
    if entity_ids is not None:
        rels = [r for r in rels if r.entity_a_id in entity_ids and r.entity_b_id in entity_ids]

    G = build_nx_graph(rels)
    return graph_communities(G)
