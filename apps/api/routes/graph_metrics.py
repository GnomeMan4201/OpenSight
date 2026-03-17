from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from apps.api.database import get_db
from apps.api.models import EntityRelationship, Entity, Mention, Document
from apps.api.services.graph_metrics import compute_graph_metrics

router = APIRouter(tags=["graph_metrics"])


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


def _build_metrics(db: Session, source_tag: str | None = None):
    entity_ids = _source_entity_ids(db, source_tag)

    q = db.query(EntityRelationship).filter(EntityRelationship.weight >= 1)
    rows = q.all()

    if entity_ids is not None:
        rows = [
            r for r in rows
            if r.entity_a_id in entity_ids and r.entity_b_id in entity_ids
        ]

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
def graph_metrics(
    source_tag: str | None = Query(None),
    db: Session = Depends(get_db),
):
    metrics, _ = _build_metrics(db, source_tag=source_tag)
    return metrics


@router.get("/graph/metrics/map")
def graph_metrics_map(
    source_tag: str | None = Query(None),
    db: Session = Depends(get_db),
):
    metrics, node_ids = _build_metrics(db, source_tag=source_tag)

    mention_q = (
        db.query(Mention.entity_id, func.count(Mention.id))
        .join(Document, Document.id == Mention.document_id)
    )
    if source_tag:
        mention_q = mention_q.filter(Document.source_tag == source_tag)

    mention_rows = mention_q.group_by(Mention.entity_id).all()
    mention_map = {entity_id: int(count) for entity_id, count in mention_rows}

    entity_rows = db.query(Entity).filter(Entity.id.in_(list(node_ids))).all() if node_ids else []
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
    source_tag: str | None = Query(None),
    db: Session = Depends(get_db),
):
    payload = graph_metrics_map(source_tag=source_tag, db=db)
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
