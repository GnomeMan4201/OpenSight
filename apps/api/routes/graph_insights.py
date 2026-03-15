from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from apps.api.database import get_db
from apps.api.models import EntityRelationship, Entity
from apps.api.services.graph_insights import build_nx_graph, top_broker_paths, graph_communities

router = APIRouter(tags=["graph_insights"])


@router.get("/graph/broker-paths/{entity_id}")
def graph_broker_paths(
    entity_id: str,
    limit: int = Query(8, ge=1, le=25),
    db: Session = Depends(get_db),
):
    rels = db.query(EntityRelationship).filter(EntityRelationship.weight >= 1).all()
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
def get_graph_communities(db: Session = Depends(get_db)):
    rels = db.query(EntityRelationship).filter(EntityRelationship.weight >= 1).all()
    G = build_nx_graph(rels)
    return graph_communities(G)
