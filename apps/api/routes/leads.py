"""
apps/api/routes/leads.py

Investigative Lead Discovery Engine — OpenSight v0.6

Automatically surfaces anomalous/high-value entities using four detection patterns:

  1. GATEKEEPER  — low mention count, high graph degree (structural importance hidden)
  2. BRIDGE      — entity connects otherwise separate clusters (cross-network intermediary)
  3. SPIKE       — entity appears concentrated in one time window vs. corpus-wide spread
  4. OUTLIER_DOC — document introducing many entities not seen elsewhere

Each lead is scored 0.0–1.0 and ranked for the investigator.
"""

from __future__ import annotations
from collections import defaultdict
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from apps.api.database import get_db
from apps.api.models import Entity, EntityRelationship, Mention, Document

router = APIRouter(tags=["leads"])


# ── helpers ────────────────────────────────────────────────────────────────────

def _build_adjacency(rels: list) -> dict[str, set[str]]:
    g: dict[str, set[str]] = defaultdict(set)
    for r in rels:
        g[r.entity_a_id].add(r.entity_b_id)
        g[r.entity_b_id].add(r.entity_a_id)
    return g


def _connected_components(graph: dict[str, set[str]]) -> list[set[str]]:
    visited: set[str] = set()
    components: list[set[str]] = []
    for node in graph:
        if node in visited:
            continue
        stack = [node]
        group: set[str] = set()
        while stack:
            n = stack.pop()
            if n in visited:
                continue
            visited.add(n)
            group.add(n)
            stack.extend(graph[n] - visited)
        if group:
            components.append(group)
    return components


# ── main endpoint ─────────────────────────────────────────────────────────────

@router.get("/leads")
def discover_leads(
    min_weight:      int   = Query(1,    ge=1),
    top_n:           int   = Query(20,   ge=1, le=100),
    gatekeeper_min_degree: int = Query(5, ge=1),
    db: Session = Depends(get_db),
):
    """
    Returns ranked investigative leads across four detection categories.
    """
    rels     = db.query(EntityRelationship).filter(EntityRelationship.weight >= min_weight).all()
    entities = db.query(Entity).all()
    mentions = db.query(Mention).all()

    entity_map = {e.id: e for e in entities}
    graph      = _build_adjacency(rels)
    degree     = {eid: len(nbrs) for eid, nbrs in graph.items()}

    # mention_count per entity
    mention_counts: dict[str, int] = defaultdict(int)
    for m in mentions:
        mention_counts[m.entity_id] += 1

    # doc_ids per entity
    entity_docs: dict[str, set[str]] = defaultdict(set)
    for m in mentions:
        entity_docs[m.entity_id].add(m.document_id)

    # entities per doc
    doc_entities: dict[str, set[str]] = defaultdict(set)
    for m in mentions:
        doc_entities[m.document_id].add(m.entity_id)

    leads: list[dict] = []

    # ── 1. GATEKEEPER — high degree, low mentions ──────────────────────────────
    all_degrees = [d for d in degree.values() if d > 0]
    max_degree  = max(all_degrees) if all_degrees else 1

    for eid, deg in degree.items():
        if deg < gatekeeper_min_degree:
            continue
        mc = mention_counts.get(eid, 0)
        if mc == 0:
            continue
        # Score: high degree relative to corpus max, penalised by mention count
        raw_score  = (deg / max_degree) * (1 / (1 + mc * 0.15))
        confidence = min(round(raw_score, 3), 0.99)
        if confidence < 0.3:
            continue
        e = entity_map.get(eid)
        if not e:
            continue
        leads.append({
            "type":       "gatekeeper",
            "label":      "Hidden Gatekeeper",
            "entity_id":  eid,
            "entity_name": e.canonical_name,
            "entity_type": e.entity_type,
            "confidence": confidence,
            "reason":     f"High centrality ({deg} connections) but only {mc} mention{'s' if mc!=1 else ''} — structurally important but rarely cited",
            "stats": {"connections": deg, "mentions": mc},
        })

    # ── 2. BRIDGE — connects otherwise separate components ────────────────────
    components = _connected_components(graph)
    comp_index: dict[str, int] = {}
    for i, comp in enumerate(components):
        for nid in comp:
            comp_index[nid] = i

    for eid, nbrs in graph.items():
        if len(nbrs) < 2:
            continue
        neighbour_comps = set()
        # temporarily remove this node and check if its neighbours span multiple components
        g2 = {k: v - {eid} for k, v in graph.items()}
        g2.pop(eid, None)
        sub_components = _connected_components(g2)
        sub_index: dict[str, int] = {}
        for i, comp in enumerate(sub_components):
            for nid in comp:
                sub_index[nid] = i
        for nbr in nbrs:
            neighbour_comps.add(sub_index.get(nbr, -1))
        n_clusters_bridged = len(neighbour_comps - {-1})
        if n_clusters_bridged < 2:
            continue
        mc = mention_counts.get(eid, 0)
        deg = len(nbrs)
        confidence = min(round(0.55 + (n_clusters_bridged - 2) * 0.1 + (deg / max_degree) * 0.25, 3), 0.99)
        e = entity_map.get(eid)
        if not e:
            continue
        leads.append({
            "type":        "bridge",
            "label":       "Cross-Cluster Bridge",
            "entity_id":   eid,
            "entity_name": e.canonical_name,
            "entity_type": e.entity_type,
            "confidence":  confidence,
            "reason":      f"Bridges {n_clusters_bridged} otherwise separate network clusters — likely key intermediary",
            "stats": {"clusters_bridged": n_clusters_bridged, "connections": deg, "mentions": mc},
        })

    # ── 3. SPIKE — entity concentrated in few docs relative to corpus size ─────
    total_docs = db.query(func.count(Document.id)).scalar() or 1
    for eid, docs in entity_docs.items():
        if len(docs) < 2:
            continue
        concentration = len(docs) / total_docs  # 0..1
        # High concentration in few docs is a spike when mention_count is also high
        mc  = mention_counts.get(eid, 0)
        if mc < 3:
            continue
        mentions_per_doc = mc / len(docs)
        # Spike score: many mentions concentrated in few docs
        spike_score = min(round((mentions_per_doc / (mc + 1)) * (1 - concentration) + concentration * 0.3, 3), 0.99)
        if spike_score < 0.35:
            continue
        e = entity_map.get(eid)
        if not e:
            continue
        leads.append({
            "type":        "spike",
            "label":       "Activity Spike",
            "entity_id":   eid,
            "entity_name": e.canonical_name,
            "entity_type": e.entity_type,
            "confidence":  round(spike_score, 3),
            "reason":      f"{mc} mentions concentrated in {len(docs)} of {total_docs} document{'s' if total_docs!=1 else ''} — event-driven activity pattern",
            "stats": {"mentions": mc, "docs": len(docs), "total_docs": total_docs},
        })

    # ── 4. OUTLIER DOCUMENT — introduces many unique entities ─────────────────
    all_entity_ids = set(e.id for e in entities)
    for doc_id, doc_ents in doc_entities.items():
        other_ents: set[str] = set()
        for other_doc, other_ents_set in doc_entities.items():
            if other_doc != doc_id:
                other_ents |= other_ents_set
        unique = doc_ents - other_ents
        if len(unique) < 3:
            continue
        uniqueness_ratio = len(unique) / max(len(doc_ents), 1)
        confidence = min(round(0.4 + uniqueness_ratio * 0.5 + len(unique) * 0.02, 3), 0.99)
        if confidence < 0.4:
            continue
        doc = db.query(Document).filter_by(id=doc_id).first()
        if not doc:
            continue
        leads.append({
            "type":        "outlier_doc",
            "label":       "Document Outlier",
            "entity_id":   None,
            "entity_name": doc.filename,
            "entity_type": "Document",
            "confidence":  confidence,
            "reason":      f"Introduces {len(unique)} unique entities not seen in other documents — deserves priority review",
            "stats": {"unique_entities": len(unique), "total_entities_in_doc": len(doc_ents)},
            "document_id": doc_id,
        })

    # ── rank and return ────────────────────────────────────────────────────────
    leads.sort(key=lambda x: x["confidence"], reverse=True)
    # Deduplicate by entity_id (keep highest score per entity)
    seen_entities: set[str] = set()
    deduped = []
    for lead in leads:
        key = lead.get("entity_id") or lead.get("document_id", "")
        if key in seen_entities:
            continue
        seen_entities.add(key)
        deduped.append(lead)

    top = deduped[:top_n]

    return {
        "total": len(deduped),
        "shown": len(top),
        "leads": top,
        "summary": {
            "gatekeeper":  sum(1 for l in deduped if l["type"] == "gatekeeper"),
            "bridge":      sum(1 for l in deduped if l["type"] == "bridge"),
            "spike":       sum(1 for l in deduped if l["type"] == "spike"),
            "outlier_doc": sum(1 for l in deduped if l["type"] == "outlier_doc"),
        }
    }
