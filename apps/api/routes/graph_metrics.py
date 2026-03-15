from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from apps.api.database import get_db
from apps.api.models import EntityRelationship, Entity, Mention
from apps.api.services.graph_metrics import compute_graph_metrics

router = APIRouter(tags=["graph_metrics"])


def _build_metrics(db: Session):
    rows = (
        db.query(EntityRelationship)
        .filter(EntityRelationship.weight >= 1)
        .all()
    )

    edge_list = []
    node_ids = set()

    for r in rows:
        src = r.entity_a_id
        tgt = r.entity_b_id
        if not src or not tgt:
            continue
        edge_list.append({"source": src, "target": tgt})
        node_ids.add(src)
        node_ids.add(tgt)

    nodes = [{"id": n} for n in node_ids]
    metrics = compute_graph_metrics(nodes, edge_list)
    return metrics, node_ids


@router.get("/graph/metrics")
def graph_metrics(db: Session = Depends(get_db)):
    metrics, _ = _build_metrics(db)
    return metrics


@router.get("/graph/metrics/map")
def graph_metrics_map(db: Session = Depends(get_db)):
    metrics, node_ids = _build_metrics(db)

    mention_rows = (
        db.query(Mention.entity_id, func.count(Mention.id))
        .group_by(Mention.entity_id)
        .all()
    )
    mention_map = {entity_id: int(count) for entity_id, count in mention_rows}

    entity_rows = db.query(Entity).filter(Entity.id.in_(list(node_ids))).all()
    entity_map = {e.id: e for e in entity_rows}

    out = {}
    for entity_id in node_ids:
        e = entity_map.get(entity_id)
        out[str(entity_id)] = {
            "id": entity_id,
            "canonical_name": e.canonical_name if e else entity_id,
            "entity_type": e.entity_type if e else "unknown",
            "mention_count": mention_map.get(entity_id, 0),
            "degree": float(metrics["degree"].get(str(entity_id), 0.0)),
            "betweenness": float(metrics["betweenness"].get(str(entity_id), 0.0)),
            "eigenvector": float(metrics["eigenvector"].get(str(entity_id), 0.0)),
        }
    return {
        "node_count": metrics["node_count"],
        "edge_count": metrics["edge_count"],
        "metrics": out,
    }


@router.get("/graph/metrics/top")
def graph_metrics_top(
    limit: int = Query(25, ge=1, le=200),
    db: Session = Depends(get_db),
):
    payload = graph_metrics_map(db)
    results = list(payload["metrics"].values())
    results.sort(
        key=lambda x: (
            x["betweenness"],
            x["degree"],
            x["eigenvector"],
            x["mention_count"],
        ),
        reverse=True,
    )
    return {
        "node_count": payload["node_count"],
        "edge_count": payload["edge_count"],
        "results": results[:limit],
    }
