"""
apps/api/services/dossier.py — entity dossier assembler
"""
from __future__ import annotations
import json, logging
from typing import Any
import networkx as nx
from sqlalchemy.orm import Session
from apps.api.models import Entity, EntityRelationship, Mention, Document
from apps.api.services.graph_insights import build_nx_graph, graph_communities, top_broker_paths

log = logging.getLogger(__name__)

def _safe_aliases(raw: Any) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(a) for a in raw if a]
    try:
        parsed = json.loads(raw)
        return [str(a) for a in parsed if a] if isinstance(parsed, list) else []
    except Exception:
        return [str(raw)] if raw else []

def build_dossier(entity_id: str, db: Session) -> dict | None:
    entity = db.query(Entity).filter_by(id=entity_id).first()
    if not entity:
        return None

    # ── mentions + documents ──────────────────────────────────────────────────
    mention_rows = (
        db.query(Mention, Document)
        .join(Document, Document.id == Mention.document_id)
        .filter(Mention.entity_id == entity_id)
        .all()
    )
    source_tags = sorted({doc.source_tag for _, doc in mention_rows if doc.source_tag})

    entity_block = {
        "id":            entity.id,
        "name":          entity.canonical_name,
        "type":          entity.entity_type,
        "aliases":       _safe_aliases(entity.aliases),
        "mention_count": entity.mention_count,
        "confidence":    entity.confidence,
        "source_tags":   source_tags,
    }

    # ── graph + centrality ────────────────────────────────────────────────────
    rels = db.query(EntityRelationship).filter(EntityRelationship.weight >= 1).all()
    G = build_nx_graph(rels)

    if entity_id in G:
        pr  = nx.pagerank(G, weight="weight")
        bc  = nx.betweenness_centrality(G, weight="weight", normalized=True)
        deg = nx.degree_centrality(G)
        try:
            ec = nx.eigenvector_centrality_numpy(G, weight="weight")
        except Exception:
            try:
                ec = nx.eigenvector_centrality(G, weight="weight", max_iter=500)
            except Exception:
                ec = deg

        def _norm(d):
            mx = max(d.values(), default=1) or 1
            return {k: v / mx for k, v in d.items()}

        pr_n  = _norm(pr)
        bc_n  = _norm(bc)
        ec_n  = _norm(ec)

        influence = (0.40 * pr_n.get(entity_id, 0) +
                     0.35 * bc_n.get(entity_id, 0) +
                     0.25 * ec_n.get(entity_id, 0))

        metrics_block = {
            "influence":   round(influence, 4),
            "betweenness": round(bc_n.get(entity_id, 0), 4),
            "pagerank":    round(pr_n.get(entity_id, 0), 4),
            "degree":      round(deg.get(entity_id, 0), 4),
            "eigenvector": round(ec_n.get(entity_id, 0), 4),
            "connections": G.degree(entity_id),
        }
    else:
        bc_n = {}
        metrics_block = {
            "influence": 0.0, "betweenness": 0.0, "pagerank": 0.0,
            "degree": 0.0, "eigenvector": 0.0, "connections": 0,
        }

    # ── communities ───────────────────────────────────────────────────────────
    emap = {str(e.id): e for e in db.query(Entity).all()}
    comm_data = graph_communities(G, entity_map=emap)
    membership = comm_data.get("membership", {})
    own_comm_id = membership.get(entity_id)
    own_comm = next(
        (c for c in comm_data.get("communities", []) if c["community_id"] == own_comm_id),
        None
    )

    neighbor_comms = {
        membership.get(str(nbr))
        for nbr in (G.neighbors(entity_id) if entity_id in G else [])
        if membership.get(str(nbr)) is not None
    }
    metrics_block["communities_bridged"] = len(neighbor_comms - {own_comm_id})

    community_block = {
        "community_id":        own_comm_id,
        "community_key_actor": own_comm["key_actor"] if own_comm else None,
        "community_size":      own_comm["size"] if own_comm else 0,
        "community_density":   own_comm["density"] if own_comm else 0.0,
        "community_members": [
            {"name": m["name"], "type": m["entity_type"], "mentions": m["mention_count"]}
            for m in (own_comm["members"] if own_comm else [])
            if m["name"] != entity.canonical_name
        ],
    }

    # ── documents + snippets ──────────────────────────────────────────────────
    doc_map: dict[str, dict] = {}
    for mention, doc in mention_rows:
        did = doc.id
        if did not in doc_map:
            doc_map[did] = {
                "id": doc.id,
                "filename": doc.filename,
                "source_tag": doc.source_tag,
                "snippets": [],
            }
        # column is context_text
        ctx = getattr(mention, "context_text", None)
        if ctx and len(doc_map[did]["snippets"]) < 3:
            doc_map[did]["snippets"].append({
                "page":    getattr(mention, "page_number", None),
                "context": ctx[:300].strip(),
            })

    documents_block = sorted(doc_map.values(), key=lambda d: d["source_tag"] or "")

    # ── relationships ─────────────────────────────────────────────────────────
    rels_a = db.query(EntityRelationship).filter_by(entity_a_id=entity_id).all()
    rels_b = db.query(EntityRelationship).filter_by(entity_b_id=entity_id).all()

    raw_rels = []
    for r in rels_a:
        other = emap.get(r.entity_b_id)
        if other:
            raw_rels.append({"entity_id": r.entity_b_id, "name": other.canonical_name,
                             "type": other.entity_type, "weight": r.weight,
                             "doc_count": r.doc_count, "direction": "outbound"})
    for r in rels_b:
        other = emap.get(r.entity_a_id)
        if other:
            raw_rels.append({"entity_id": r.entity_a_id, "name": other.canonical_name,
                             "type": other.entity_type, "weight": r.weight,
                             "doc_count": r.doc_count, "direction": "inbound"})

    seen: set[str] = set()
    relationships_block = []
    for rel in sorted(raw_rels, key=lambda r: r["weight"], reverse=True):
        if rel["entity_id"] not in seen:
            seen.add(rel["entity_id"])
            relationships_block.append(rel)

    # ── provenance ────────────────────────────────────────────────────────────
    extraction_methods = list({
        getattr(m, "extraction_method", "unknown") or "unknown"
        for m, _ in mention_rows
    })
    dates = [str(getattr(m, "created_at", "")) for m, _ in mention_rows if getattr(m, "created_at", None)]
    first_seen = min(dates) if dates else None

    provenance_block = {
        "extraction_methods": extraction_methods or ["unknown"],
        "first_seen":         first_seen,
        "aliases_merged":     _safe_aliases(entity.aliases),
        "raw_mention_count":  len(mention_rows),
        "review_status":      getattr(entity, "review_status", "auto"),
        "review_notes":       getattr(entity, "review_notes", None),
        "confidence":         entity.confidence,
    }

    # ── broker paths (only if meaningful broker) ──────────────────────────────
    broker_paths_block: list[dict] = []
    if metrics_block.get("betweenness", 0) > 0.01 and entity_id in G:
        for p in top_broker_paths(G, entity_id, limit=5):
            broker_paths_block.append({
                "length":       p["length"],
                "bridge_score": p["bridge_score"],
                "path": [
                    {"id": nid,
                     "name": emap[nid].canonical_name if nid in emap else nid,
                     "type": emap[nid].entity_type if nid in emap else "unknown"}
                    for nid in p["path"]
                ],
            })

    return {
        "entity":        entity_block,
        "metrics":       metrics_block,
        "community":     community_block,
        "documents":     documents_block,
        "relationships": relationships_block,
        "provenance":    provenance_block,
        "broker_paths":  broker_paths_block,
    }
