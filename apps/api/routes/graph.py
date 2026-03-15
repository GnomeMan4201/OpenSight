"""
apps/api/routes/graph.py

Entity relationship graph endpoints.

GET /api/v1/graph/entities/{entity_id}/network
    — direct connections for one entity (depth=1)

GET /api/v1/graph/entities/{entity_id}/graph?depth=2&min_weight=1
    — expanded graph up to N hops

GET /api/v1/graph/relationships?entity_type=Aircraft&min_weight=2
    — filterable list of all relationships
"""

from __future__ import annotations

import logging
from collections import deque
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from apps.api.database import get_db
from apps.api.models import Entity, EntityRelationship

log = logging.getLogger(__name__)
router = APIRouter(tags=["graph"])


# ── Schemas ───────────────────────────────────────────────────────────────────

class EntityNode(BaseModel):
    id:             str
    entity_type:    str
    canonical_name: str
    mention_count:  int = 0

class RelationshipEdge(BaseModel):
    source:    str   # entity id
    target:    str   # entity id
    weight:    int
    doc_count: int

class NetworkResponse(BaseModel):
    entity:      EntityNode
    connections: list[dict]   # [{entity, weight, doc_count}]
    total:       int

class GraphResponse(BaseModel):
    nodes: list[EntityNode]
    edges: list[RelationshipEdge]
    depth: int

class RelationshipOut(BaseModel):
    id:          str
    entity_a:    EntityNode
    entity_b:    EntityNode
    weight:      int
    doc_count:   int


# ── Helpers ───────────────────────────────────────────────────────────────────

def _entity_node(e: Entity, db: Session) -> EntityNode:
    from sqlalchemy import func
    from apps.api.models import Mention
    count = db.query(func.count(Mention.id)).filter_by(entity_id=e.id).scalar() or 0
    return EntityNode(
        id=e.id,
        entity_type=e.entity_type,
        canonical_name=e.canonical_name,
        mention_count=count,
    )

def _get_entity_or_404(entity_id: str, db: Session) -> Entity:
    e = db.query(Entity).filter_by(id=entity_id).first()
    if not e:
        raise HTTPException(status_code=404, detail={"error": "entity_not_found", "id": entity_id})
    return e

def _neighbors(entity_id: str, db: Session, min_weight: int = 1) -> list[EntityRelationship]:
    """All relationships touching this entity."""
    return (
        db.query(EntityRelationship)
        .filter(
            (EntityRelationship.entity_a_id == entity_id) |
            (EntityRelationship.entity_b_id == entity_id),
            EntityRelationship.weight >= min_weight,
        )
        .order_by(EntityRelationship.weight.desc())
        .all()
    )


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/entities/{entity_id}/network", response_model=NetworkResponse)
def entity_network(
    entity_id:  str,
    min_weight: int = Query(1, ge=1, description="Minimum co-occurrence weight to include"),
    db:         Session = Depends(get_db),
):
    """
    Direct connections for one entity (depth=1 neighbourhood).
    Returns the entity itself plus all entities it co-occurs with,
    sorted by weight descending.
    """
    entity = _get_entity_or_404(entity_id, db)
    rels   = _neighbors(entity_id, db, min_weight)

    connections = []
    for rel in rels:
        other_id = rel.entity_b_id if rel.entity_a_id == entity_id else rel.entity_a_id
        other    = db.query(Entity).filter_by(id=other_id).first()
        if other:
            connections.append({
                "entity":    _entity_node(other, db),
                "weight":    rel.weight,
                "doc_count": rel.doc_count,
            })

    return NetworkResponse(
        entity=_entity_node(entity, db),
        connections=connections,
        total=len(connections),
    )


@router.get("/entities/{entity_id}/graph", response_model=GraphResponse)
def entity_graph(
    entity_id:  str,
    depth:      int = Query(2, ge=1, le=4, description="Number of hops from the seed entity"),
    min_weight: int = Query(1, ge=1),
    db:         Session = Depends(get_db),
):
    """
    BFS-expanded graph up to `depth` hops from a seed entity.
    Returns nodes (entities) and edges (relationships) suitable for
    graph visualization libraries (react-force-graph, vis.js, etc.).
    """
    _get_entity_or_404(entity_id, db)

    visited_nodes: set[str] = set()
    visited_edges: set[tuple] = set()
    nodes: list[EntityNode] = []
    edges: list[RelationshipEdge] = []

    queue: deque[tuple[str, int]] = deque([(entity_id, 0)])

    while queue:
        current_id, current_depth = queue.popleft()
        if current_id in visited_nodes:
            continue
        visited_nodes.add(current_id)

        entity = db.query(Entity).filter_by(id=current_id).first()
        if entity:
            nodes.append(_entity_node(entity, db))

        if current_depth >= depth:
            continue

        for rel in _neighbors(current_id, db, min_weight):
            edge_key = (min(rel.entity_a_id, rel.entity_b_id),
                        max(rel.entity_a_id, rel.entity_b_id))
            if edge_key not in visited_edges:
                visited_edges.add(edge_key)
                edges.append(RelationshipEdge(
                    source=rel.entity_a_id,
                    target=rel.entity_b_id,
                    weight=rel.weight,
                    doc_count=rel.doc_count,
                ))

            neighbor_id = rel.entity_b_id if rel.entity_a_id == current_id else rel.entity_a_id
            if neighbor_id not in visited_nodes:
                queue.append((neighbor_id, current_depth + 1))

    return GraphResponse(nodes=nodes, edges=edges, depth=depth)


@router.get("/relationships", response_model=list[RelationshipOut])
def list_relationships(
    entity_type: Optional[str] = Query(None, description="Filter by entity type (both sides)"),
    min_weight:  int           = Query(1,    ge=1),
    limit:       int           = Query(50,   ge=1, le=500),
    offset:      int           = Query(0,    ge=0),
    db:          Session       = Depends(get_db),
):
    """
    All entity relationships, filterable and paginated.
    Useful for finding the strongest connections in the corpus.
    """
    q = (
        db.query(EntityRelationship)
        .filter(EntityRelationship.weight >= min_weight)
        .order_by(EntityRelationship.weight.desc())
    )

    if entity_type:
        q = (
            q.join(Entity, Entity.id == EntityRelationship.entity_a_id)
             .filter(Entity.entity_type == entity_type)
        )

    rels = q.offset(offset).limit(limit).all()

    results = []
    for rel in rels:
        ea = db.query(Entity).filter_by(id=rel.entity_a_id).first()
        eb = db.query(Entity).filter_by(id=rel.entity_b_id).first()
        if ea and eb:
            results.append(RelationshipOut(
                id=rel.id,
                entity_a=_entity_node(ea, db),
                entity_b=_entity_node(eb, db),
                weight=rel.weight,
                doc_count=rel.doc_count,
            ))

    return results
