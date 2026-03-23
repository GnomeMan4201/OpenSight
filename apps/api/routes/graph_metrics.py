"""
apps/api/routes/graph_metrics.py

Graph analytics endpoints.

GET /api/v1/graph/metrics
    Full centrality report with entity names (not UUIDs).
    Returns: node_count, edge_count, top_N lists for degree/betweenness/eigenvector.

GET /api/v1/graph/metrics/map
    Raw {entity_name: score} maps for all centrality types.
    Used by the graph visualization layer.

GET /api/v1/graph/metrics/top
    Top N entities by combined influence score.
    Used by the broker/influence panels.
"""

from __future__ import annotations

import logging
import math
from typing import Optional

import networkx as nx
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from apps.api.database import get_db
from apps.api.models import Entity, EntityRelationship

log = logging.getLogger(__name__)
router = APIRouter(tags=["graph_metrics"])


def _build_graph(db: Session, min_weight: int = 1) -> tuple[nx.Graph, dict[str, str]]:
    """
    Build a NetworkX graph from entity_relationships.
    Returns (G, id_to_name mapping).
    """
    entities = db.query(Entity).all()
    id_to_name = {e.id: e.canonical_name for e in entities}
    id_to_type = {e.id: e.entity_type for e in entities}

    G = nx.Graph()
    for e in entities:
        G.add_node(e.id, name=e.canonical_name, entity_type=e.entity_type,
                   mention_count=e.mention_count)

    rels = db.query(EntityRelationship).filter(
        EntityRelationship.weight >= min_weight
    ).all()
    for r in rels:
        if r.entity_a_id in id_to_name and r.entity_b_id in id_to_name:
            G.add_edge(r.entity_a_id, r.entity_b_id,
                       weight=r.weight, doc_count=r.doc_count)

    return G, id_to_name


def _safe_eigenvector(G: nx.Graph) -> dict[str, float]:
    try:
        return nx.eigenvector_centrality_numpy(G, weight="weight")
    except Exception:
        try:
            return nx.eigenvector_centrality(G, weight="weight", max_iter=500)
        except Exception:
            return nx.degree_centrality(G)


def _combined_score(
    node_id: str,
    pagerank: dict, betweenness: dict, eigenvector: dict
) -> float:
    return (
        0.40 * pagerank.get(node_id, 0) +
        0.35 * betweenness.get(node_id, 0) +
        0.25 * eigenvector.get(node_id, 0)
    )


def _normalize(d: dict) -> dict:
    max_v = max(d.values(), default=1) or 1
    return {k: v / max_v for k, v in d.items()}


@router.get("/graph/metrics")
def get_graph_metrics(
    top_n: int = Query(15, ge=1, le=100),
    min_weight: int = Query(1, ge=1),
    db: Session = Depends(get_db),
):
    """
    Full graph analytics report.
    All maps use entity canonical_name as key (not UUID).
    """
    G, id_to_name = _build_graph(db, min_weight)

    if G.number_of_nodes() == 0:
        return {"node_count": 0, "edge_count": 0, "top_entities": []}

    # Centrality
    pagerank     = nx.pagerank(G, weight="weight")
    betweenness  = nx.betweenness_centrality(G, weight="weight", normalized=True)
    eigenvector  = _safe_eigenvector(G)
    degree       = nx.degree_centrality(G)

    # Normalize all to [0,1]
    pr_n  = _normalize(pagerank)
    bc_n  = _normalize(betweenness)
    ec_n  = _normalize(eigenvector)
    deg_n = _normalize(degree)

    # Combined influence score
    combined = {
        nid: 0.40 * pr_n.get(nid, 0) + 0.35 * bc_n.get(nid, 0) + 0.25 * ec_n.get(nid, 0)
        for nid in G.nodes()
    }

    # Convert to name-keyed maps (top N only for readability)
    top_ids = sorted(combined, key=combined.get, reverse=True)[:top_n]

    top_entities = []
    for nid in top_ids:
        name = id_to_name.get(nid, nid)
        node_data = G.nodes[nid]
        top_entities.append({
            "id":               nid,
            "name":             name,
            "entity_type":      node_data.get("entity_type", "Unknown"),
            "mention_count":    node_data.get("mention_count", 0),
            "degree":           round(deg_n.get(nid, 0), 4),
            "betweenness":      round(bc_n.get(nid, 0), 4),
            "pagerank":         round(pr_n.get(nid, 0), 4),
            "eigenvector":      round(ec_n.get(nid, 0), 4),
            "influence_score":  round(combined.get(nid, 0), 4),
            "connections":      G.degree(nid),
        })

    return {
        "node_count":   G.number_of_nodes(),
        "edge_count":   G.number_of_edges(),
        "top_entities": top_entities,
        # Legacy UUID maps (kept for backward compat with frontend)
        "degree":       {id_to_name.get(k, k): round(v, 4) for k, v in deg_n.items()},
        "betweenness":  {id_to_name.get(k, k): round(v, 4) for k, v in bc_n.items()},
        "eigenvector":  {id_to_name.get(k, k): round(v, 4) for k, v in ec_n.items()},
        "pagerank":     {id_to_name.get(k, k): round(v, 4) for k, v in pr_n.items()},
    }


@router.get("/graph/metrics/map")
def get_metrics_map(
    metric: str = Query("betweenness", enum=["betweenness", "pagerank", "eigenvector", "degree", "influence"]),
    min_weight: int = Query(1, ge=1),
    db: Session = Depends(get_db),
):
    """
    Returns {entity_name: score} for the requested metric.
    Used by visualization heatmap overlays.
    """
    G, id_to_name = _build_graph(db, min_weight)

    if G.number_of_nodes() == 0:
        return {"metric": metric, "scores": {}}

    if metric == "betweenness":
        raw = nx.betweenness_centrality(G, weight="weight", normalized=True)
    elif metric == "pagerank":
        raw = nx.pagerank(G, weight="weight")
    elif metric == "eigenvector":
        raw = _safe_eigenvector(G)
    elif metric == "degree":
        raw = nx.degree_centrality(G)
    elif metric == "influence":
        pr  = _normalize(nx.pagerank(G, weight="weight"))
        bc  = _normalize(nx.betweenness_centrality(G, weight="weight", normalized=True))
        ec  = _normalize(_safe_eigenvector(G))
        raw = {n: 0.4*pr.get(n,0) + 0.35*bc.get(n,0) + 0.25*ec.get(n,0) for n in G.nodes()}
    else:
        raw = {}

    scores = {id_to_name.get(k, k): round(v, 4) for k, v in raw.items()}
    return {
        "metric": metric,
        "scores": dict(sorted(scores.items(), key=lambda x: x[1], reverse=True))
    }


@router.get("/graph/metrics/top")
def get_top_entities(
    metric: str = Query("influence", enum=["influence", "betweenness", "pagerank", "degree"]),
    top_n: int = Query(10, ge=1, le=50),
    entity_type: Optional[str] = Query(None, description="Filter: Person, Organization, Location"),
    min_weight: int = Query(1, ge=1),
    db: Session = Depends(get_db),
):
    """
    Top N entities by chosen metric.
    Optionally filtered by entity_type.
    Used by the broker detection and influence panels.
    """
    G, id_to_name = _build_graph(db, min_weight)

    if G.number_of_nodes() == 0:
        return {"metric": metric, "top_n": top_n, "entities": []}

    pr  = nx.pagerank(G, weight="weight")
    bc  = nx.betweenness_centrality(G, weight="weight", normalized=True)
    ec  = _safe_eigenvector(G)
    deg = nx.degree_centrality(G)

    pr_n  = _normalize(pr)
    bc_n  = _normalize(bc)
    ec_n  = _normalize(ec)
    deg_n = _normalize(deg)

    influence = {
        n: 0.4*pr_n.get(n,0) + 0.35*bc_n.get(n,0) + 0.25*ec_n.get(n,0)
        for n in G.nodes()
    }

    score_map = {
        "influence":   influence,
        "betweenness": bc_n,
        "pagerank":    pr_n,
        "degree":      deg_n,
    }[metric]

    # Filter by entity_type if requested
    candidates = list(G.nodes())
    if entity_type:
        candidates = [
            n for n in candidates
            if G.nodes[n].get("entity_type", "").lower() == entity_type.lower()
        ]

    top_ids = sorted(candidates, key=lambda n: score_map.get(n, 0), reverse=True)[:top_n]

    entities = []
    for nid in top_ids:
        node = G.nodes[nid]
        entities.append({
            "id":            nid,
            "name":          id_to_name.get(nid, nid),
            "entity_type":   node.get("entity_type", "Unknown"),
            "mention_count": node.get("mention_count", 0),
            "score":         round(score_map.get(nid, 0), 4),
            "connections":   G.degree(nid),
            "betweenness":   round(bc_n.get(nid, 0), 4),
            "pagerank":      round(pr_n.get(nid, 0), 4),
        })

    return {"metric": metric, "top_n": top_n, "entities": entities}
