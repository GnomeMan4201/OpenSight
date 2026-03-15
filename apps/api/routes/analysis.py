"""
apps/api/routes/analysis.py

Analytical endpoints — cluster discovery, timeline reconstruction.
"""
from collections import defaultdict
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from apps.api.database import get_db
from apps.api.models import EntityRelationship, Entity, Mention, Document

router = APIRouter(tags=["analysis"])


@router.get("/clusters")
def discover_clusters(
    min_weight: int = Query(1, ge=1, description="Minimum relationship weight to include"),
    db: Session = Depends(get_db),
):
    """
    Connected-component clustering on the entity relationship graph.
    Returns groups of entities that are directly or indirectly connected,
    sorted by cluster size descending.
    """
    rels = db.query(EntityRelationship).filter(EntityRelationship.weight >= min_weight).all()

    if not rels:
        return {"clusters": [], "total_entities": 0, "total_clusters": 0}

    graph = defaultdict(set)
    all_nodes = set()
    for r in rels:
        graph[r.entity_a_id].add(r.entity_b_id)
        graph[r.entity_b_id].add(r.entity_a_id)
        all_nodes.add(r.entity_a_id)
        all_nodes.add(r.entity_b_id)

    visited = set()
    clusters = []

    for node in all_nodes:
        if node in visited:
            continue
        stack = [node]
        group = []
        while stack:
            n = stack.pop()
            if n in visited:
                continue
            visited.add(n)
            group.append(n)
            stack.extend(graph[n] - visited)
        if len(group) > 1:
            clusters.append(group)

    # Fetch entity details and mention counts
    mention_counts = dict(
        db.query(Mention.entity_id, func.count(Mention.id))
        .group_by(Mention.entity_id)
        .all()
    )

    result = []
    for cluster in clusters:
        entities = db.query(Entity).filter(Entity.id.in_(cluster)).all()
        # Sort cluster members by mention count desc
        entities.sort(key=lambda e: mention_counts.get(e.id, 0), reverse=True)
        result.append({
            "size": len(entities),
            "total_mentions": sum(mention_counts.get(e.id, 0) for e in entities),
            "entity_types": list({e.entity_type for e in entities}),
            "entities": [
                {
                    "id": e.id,
                    "name": e.canonical_name,
                    "type": e.entity_type,
                    "mentions": mention_counts.get(e.id, 0),
                }
                for e in entities
            ],
        })

    result.sort(key=lambda x: x["size"], reverse=True)

    return {
        "clusters": result,
        "total_entities": len(all_nodes),
        "total_clusters": len(result),
    }


@router.get("/timeline")
def build_timeline(
    entity_id: str = Query(None, description="Filter timeline to one entity"),
    db: Session = Depends(get_db),
):
    """
    Extract date entities and correlate them with co-occurring entities
    and source documents to produce a rough chronological timeline.
    """
    # Get all Date-type entities, optionally filtered by co-occurrence with a target
    date_entities = (
        db.query(Entity)
        .filter(Entity.entity_type == "Date")
        .order_by(Entity.canonical_name)
        .limit(200)
        .all()
    )

    mention_map = {}  # entity_id -> [{doc, page, context}]
    for de in date_entities:
        rows = (
            db.query(Mention, Document)
            .join(Document, Document.id == Mention.document_id)
            .filter(Mention.entity_id == de.id)
            .all()
        )
        mention_map[de.id] = [
            {
                "document_id": doc.id,
                "filename": doc.filename,
                "page_number": m.page_number,
                "context": m.context_text[:200],
            }
            for m, doc in rows
        ]

    # For each date, find co-occurring entities in the same documents
    timeline = []
    for de in date_entities:
        doc_ids = {m["document_id"] for m in mention_map.get(de.id, [])}
        if not doc_ids:
            continue

        # Entities that appear in the same documents as this date
        coentities = (
            db.query(Entity)
            .join(Mention, Mention.entity_id == Entity.id)
            .filter(
                Mention.document_id.in_(doc_ids),
                Entity.id != de.id,
                Entity.entity_type != "Date",
            )
            .distinct()
            .limit(10)
            .all()
        )

        if entity_id and not any(e.id == entity_id for e in coentities):
            continue

        timeline.append({
            "date": de.canonical_name,
            "sources": mention_map[de.id],
            "entities": [
                {"id": e.id, "name": e.canonical_name, "type": e.entity_type}
                for e in coentities
            ],
        })

    return {"timeline": timeline, "total_events": len(timeline)}
