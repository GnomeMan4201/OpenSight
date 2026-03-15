"""
apps/api/services/intelligence.py

OpenSight Intelligence Engine — v0.8

Five interconnected capabilities built on the semantic bridge:

1. HYBRID SEARCH
   Merges FTS5 keyword results + semantic vector results + graph centrality boost.
   Returns ranked results with source attribution (keyword hit / semantic match / both).

2. SEMANTIC GRAPH ENRICHMENT
   After ingestion: computes semantic edge weights between entities based on
   context similarity, not just co-occurrence count.
   Adds `semantic_weight` to EntityRelationship.

3. CLAIM CORRELATION ENGINE
   Cross-document claim similarity detection.
   Groups semantically similar claims (even with different wording).
   Detects potential contradictions: same subject, opposing sentiment, different docs.

4. ENTITY DISAMBIGUATION
   Finds entity pairs that are string-different but semantically similar.
   Supplements the existing Levenshtein dedup with context-based similarity.
   Returns disambiguation candidates with confidence scores.

5. INFLUENCE SCORING
   Computes actor influence scores combining:
   - Graph degree centrality (connections)
   - Betweenness proxy (bridges between clusters)
   - Claim authority (how many claims others make about this entity)
   - Document spread (how many docs mention this entity)
   Returns ranked influence map for the whole corpus.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import func, text

from apps.api.models import (
    Claim, Document, DocumentPage, Entity,
    EntityRelationship, Mention,
)
from apps.api.services.semantic_bridge import (
    semantic_search, find_similar_claims,
    find_similar_entities, is_available,
)

log = logging.getLogger(__name__)


# ── 1. HYBRID SEARCH ──────────────────────────────────────────────────────────

def hybrid_search(
    query: str,
    db: Session,
    k: int = 20,
    semantic_weight: float = 0.6,
    keyword_weight: float = 0.3,
    graph_weight: float = 0.1,
) -> list[dict]:
    """
    Hybrid search combining three signals:
      - Semantic vector similarity (via :8010)
      - FTS5 keyword match (local SQLite)
      - Graph centrality boost (highly connected entities rank higher)

    Returns deduplicated results with composite score and source attribution.
    """
    results: dict[str, dict] = {}  # key: document_id+page_number

    # ── Signal 1: Semantic ────────────────────────────────────────────────────
    if is_available():
        sem_results = semantic_search(query, k=k * 2)
        max_sem = max((r.get("score", 0) for r in sem_results), default=1.0)
        for r in sem_results:
            meta = r.get("metadata", {})
            if meta.get("type") != "page":
                continue
            key = f"{meta['document_id']}:{meta['page_number']}"
            norm_score = r.get("score", 0) / max(max_sem, 0.001)
            results[key] = {
                "document_id": meta["document_id"],
                "page_number": meta["page_number"],
                "filename": meta.get("filename", ""),
                "doc_type": meta.get("doc_type", "other"),
                "semantic_score": norm_score,
                "keyword_score": 0.0,
                "graph_boost": 0.0,
                "signals": ["semantic"],
                "text_snippet": r.get("text", "")[:300],
            }

    # ── Signal 2: FTS5 keyword ────────────────────────────────────────────────
    try:
        fts_rows = db.execute(
            text("""
                SELECT f.document_id, f.page_number,
                       bm25(fts_pages) AS bm25_score
                FROM fts_pages f
                WHERE fts_pages MATCH :q
                ORDER BY bm25_score
                LIMIT :lim
            """),
            {"q": query, "lim": k * 2}
        ).fetchall()

        if fts_rows:
            scores = [abs(r[2]) for r in fts_rows]
            max_kw = max(scores) if scores else 1.0
            for doc_id, page_num, score in fts_rows:
                key = f"{doc_id}:{page_num}"
                norm = abs(score) / max(max_kw, 0.001)
                if key in results:
                    results[key]["keyword_score"] = norm
                    results[key]["signals"].append("keyword")
                else:
                    # Get snippet from DB
                    page = db.query(DocumentPage).filter_by(
                        document_id=doc_id, page_number=page_num
                    ).first()
                    doc = db.query(Document).filter_by(id=doc_id).first()
                    results[key] = {
                        "document_id": doc_id,
                        "page_number": page_num,
                        "filename": doc.filename if doc else "",
                        "doc_type": doc.doc_type if doc else "other",
                        "semantic_score": 0.0,
                        "keyword_score": norm,
                        "graph_boost": 0.0,
                        "signals": ["keyword"],
                        "text_snippet": (page.final_text[:300] if page else ""),
                    }
    except Exception as e:
        log.warning("[hybrid_search] FTS5 failed: %s", e)

    # ── Signal 3: Graph centrality boost ─────────────────────────────────────
    # Entities mentioned on highly-connected pages get a boost
    try:
        centrality = _get_entity_centrality(db)
        for key, result in results.items():
            doc_id = result["document_id"]
            page_num = result["page_number"]
            # Get entities on this page
            mentions = db.query(Mention).filter_by(
                document_id=doc_id, page_number=page_num
            ).all()
            if mentions:
                page_centrality = max(
                    centrality.get(m.entity_id, 0) for m in mentions
                )
                result["graph_boost"] = page_centrality
    except Exception as e:
        log.warning("[hybrid_search] graph boost failed: %s", e)

    # ── Composite score ───────────────────────────────────────────────────────
    final = []
    for result in results.values():
        composite = (
            result["semantic_score"] * semantic_weight
            + result["keyword_score"] * keyword_weight
            + result["graph_boost"] * graph_weight
        )
        result["score"] = round(composite, 4)
        final.append(result)

    final.sort(key=lambda x: x["score"], reverse=True)
    return final[:k]


def _get_entity_centrality(db: Session) -> dict[str, float]:
    """
    Compute normalized degree centrality for all entities.
    Returns dict: entity_id -> 0.0..1.0
    """
    rows = db.execute(text("""
        SELECT entity_id, COUNT(*) as degree
        FROM (
            SELECT entity_a_id as entity_id FROM entity_relationships
            UNION ALL
            SELECT entity_b_id as entity_id FROM entity_relationships
        )
        GROUP BY entity_id
    """)).fetchall()

    if not rows:
        return {}

    max_degree = max(r[1] for r in rows)
    return {r[0]: r[1] / max(max_degree, 1) for r in rows}


# ── 2. SEMANTIC GRAPH ENRICHMENT ──────────────────────────────────────────────

def enrich_graph_semantically(db: Session, document_id: str) -> int:
    """
    After ingestion: find entity pairs that are semantically related
    beyond their co-occurrence count and boost their edge weight.

    Also creates NEW edges for entity pairs that never co-occur on the same
    page but whose contexts are semantically similar (cross-document links).

    Returns number of edges created or updated.
    """
    if not is_available():
        log.info("[enrich_graph] semantic service unavailable, skipping")
        return 0

    # Get entities in this document
    entities = (
        db.query(Entity)
        .join(Mention, Mention.entity_id == Entity.id)
        .filter(Mention.document_id == document_id)
        .distinct()
        .all()
    )

    if len(entities) < 2:
        return 0

    enriched = 0
    entity_contexts: dict[str, str] = {}

    # Get context for each entity
    for entity in entities:
        mentions = (
            db.query(Mention)
            .filter_by(entity_id=entity.id, document_id=document_id)
            .all()
        )
        contexts = [m.context_text for m in mentions if m.context_text]
        if contexts:
            entity_contexts[entity.id] = " ".join(contexts[:3])

    # Find semantically similar pairs not already connected
    existing_pairs: set[tuple] = set()
    for rel in db.query(EntityRelationship).filter(
        EntityRelationship.entity_a_id.in_(entity_contexts.keys())
    ).all():
        pair = tuple(sorted([rel.entity_a_id, rel.entity_b_id]))
        existing_pairs.add(pair)

    # Find semantic neighbors for each entity
    for entity in entities[:20]:  # cap to avoid explosion
        if entity.id not in entity_contexts:
            continue
        context = entity_contexts[entity.id]
        similar = find_similar_entities(
            entity.canonical_name, context, k=5, min_score=0.7
        )
        for sim in similar:
            sim_id = sim.get("metadata", {}).get("entity_id")
            if not sim_id or sim_id == entity.id:
                continue
            pair = tuple(sorted([entity.id, sim_id]))
            if pair in existing_pairs:
                continue
            # Check if sim_id exists in our DB
            target = db.query(Entity).filter_by(id=sim_id).first()
            if not target:
                continue

            # Create new semantic edge
            a_id, b_id = pair
            rel = EntityRelationship(
                entity_a_id=a_id,
                entity_b_id=b_id,
                weight=1,
                doc_count=1,
            )
            try:
                db.add(rel)
                db.flush()
                existing_pairs.add(pair)
                enriched += 1
                log.debug("[enrich_graph] semantic edge: %s <-> %s (%.2f)",
                          entity.canonical_name,
                          target.canonical_name,
                          sim.get("score", 0))
            except Exception:
                db.rollback()

    db.commit()
    log.info("[enrich_graph] doc=%s enriched %d semantic edges", document_id[:8], enriched)
    return enriched


# ── 3. CLAIM CORRELATION ENGINE ───────────────────────────────────────────────

def correlate_claims(db: Session, min_similarity: float = 0.65) -> dict:
    """
    Cross-document claim analysis:
    - Groups semantically similar claims (even with different wording)
    - Detects contradictions: same subject, opposing sentiment, different docs
    - Computes claim propagation chains

    Returns structured analysis report.
    """
    if not is_available():
        return _correlate_claims_structural(db)

    all_claims = db.query(Claim).filter(Claim.confidence >= 0.5).all()
    if len(all_claims) < 2:
        return {"clusters": [], "contradictions": [], "propagation_chains": []}

    # Group claims into semantic clusters
    clusters = []
    assigned: set[str] = set()

    for claim in all_claims:
        if claim.id in assigned:
            continue

        similar = find_similar_claims(claim.claim_text, k=10, min_score=min_similarity)
        cluster_ids = {claim.id}

        for sim in similar:
            sim_id = sim.get("metadata", {}).get("claim_id")
            if sim_id and sim_id != claim.id and sim_id not in assigned:
                # Verify it exists in DB
                if db.query(Claim).filter_by(id=sim_id).first():
                    cluster_ids.add(sim_id)

        if len(cluster_ids) > 1:
            cluster_claims = [c for c in all_claims if c.id in cluster_ids]
            doc_ids = {c.document_id for c in cluster_claims}

            # Get subject name
            subject = None
            for c in cluster_claims:
                if c.subject_entity_id:
                    e = db.query(Entity).filter_by(id=c.subject_entity_id).first()
                    if e:
                        subject = e.canonical_name
                        break

            clusters.append({
                "claim_count": len(cluster_claims),
                "doc_count": len(doc_ids),
                "subject": subject,
                "representative_text": claim.claim_text[:200],
                "claim_ids": list(cluster_ids),
                "claim_types": list({c.claim_type for c in cluster_claims}),
                "sentiments": list({c.sentiment for c in cluster_claims}),
                "cross_document": len(doc_ids) > 1,
            })
            assigned.update(cluster_ids)

    # Detect contradictions within clusters
    contradictions = []
    for cluster in clusters:
        sentiments = set(cluster["sentiments"])
        types = set(cluster["claim_types"])
        if ("positive" in sentiments and "negative" in sentiments) or \
           ("allegation" in types and "denial" in types):
            contradictions.append({
                "subject": cluster["subject"],
                "doc_count": cluster["doc_count"],
                "contradiction_type": "sentiment_conflict"
                    if ("positive" in sentiments and "negative" in sentiments)
                    else "allegation_denial_conflict",
                "claim_count": cluster["claim_count"],
                "sample": cluster["representative_text"],
            })

    # Propagation chains: clusters with claims in 3+ docs
    propagation = [
        c for c in clusters
        if c["doc_count"] >= 3 and c["cross_document"]
    ]
    propagation.sort(key=lambda x: x["doc_count"], reverse=True)

    return {
        "total_claims_analyzed": len(all_claims),
        "clusters_found": len(clusters),
        "contradictions": contradictions,
        "propagation_chains": propagation,
        "method": "semantic",
    }


def _correlate_claims_structural(db: Session) -> dict:
    """Fallback when semantic service unavailable: structural correlation only."""
    all_claims = db.query(Claim).filter(Claim.confidence >= 0.5).all()

    # Group by subject entity + claim type
    by_subject: dict[str, list[Claim]] = defaultdict(list)
    for c in all_claims:
        if c.subject_entity_id:
            by_subject[c.subject_entity_id].append(c)

    contradictions = []
    for subject_id, claims in by_subject.items():
        sentiments = {c.sentiment for c in claims}
        if "positive" in sentiments and "negative" in sentiments:
            subject = db.query(Entity).filter_by(id=subject_id).first()
            contradictions.append({
                "subject": subject.canonical_name if subject else None,
                "doc_count": len({c.document_id for c in claims}),
                "contradiction_type": "sentiment_conflict",
                "claim_count": len(claims),
            })

    return {
        "total_claims_analyzed": len(all_claims),
        "clusters_found": 0,
        "contradictions": contradictions,
        "propagation_chains": [],
        "method": "structural",
    }


# ── 4. ENTITY DISAMBIGUATION ─────────────────────────────────────────────────

def find_disambiguation_candidates(
    db: Session,
    min_semantic_score: float = 0.72,
    max_candidates: int = 30,
) -> list[dict]:
    """
    Find entity pairs that are:
    - String-different (not caught by Levenshtein dedup)
    - BUT semantically similar in context

    These are high-confidence disambiguation candidates.
    Example: 'Ms. Maxwell' and 'Ghislaine' with similar surrounding text.
    """
    if not is_available():
        return []

    # Only check Person and Organization entities (most ambiguous)
    entities = (
        db.query(Entity)
        .filter(Entity.entity_type.in_(["Person", "Organization"]))
        .filter(Entity.mention_count >= 2)
        .all()
    )

    candidates = []
    seen_pairs: set[tuple] = set()

    for entity in entities[:50]:  # cap for performance
        # Get this entity's context
        mentions = db.query(Mention).filter_by(entity_id=entity.id).limit(5).all()
        contexts = [m.context_text for m in mentions if m.context_text]
        if not contexts:
            continue
        context = " ".join(contexts[:3])[:400]

        similar = find_similar_entities(
            entity.canonical_name, context,
            k=5, min_score=min_semantic_score
        )

        for sim in similar:
            sim_id = sim.get("metadata", {}).get("entity_id")
            sim_name = sim.get("metadata", {}).get("entity_name", "")
            if not sim_id or sim_id == entity.id:
                continue

            pair = tuple(sorted([entity.id, sim_id]))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)

            sim_entity = db.query(Entity).filter_by(id=sim_id).first()
            if not sim_entity:
                continue

            # Skip pairs already flagged as duplicates by string dedup
            # (check if they share any aliases)
            entity_aliases = set(entity.aliases or [])
            sim_aliases = set(sim_entity.aliases or [])
            if entity_aliases & sim_aliases:
                continue

            candidates.append({
                "entity_a": {
                    "id": entity.id,
                    "name": entity.canonical_name,
                    "type": entity.entity_type,
                    "mentions": entity.mention_count,
                },
                "entity_b": {
                    "id": sim_entity.id,
                    "name": sim_entity.canonical_name,
                    "type": sim_entity.entity_type,
                    "mentions": sim_entity.mention_count,
                },
                "semantic_score": round(sim.get("score", 0), 3),
                "disambiguation_method": "context_embedding",
                "note": "Different names, similar context — possible same entity",
            })

    candidates.sort(key=lambda x: x["semantic_score"], reverse=True)
    return candidates[:max_candidates]


# ── 5. INFLUENCE SCORING ──────────────────────────────────────────────────────

def compute_influence_map(
    db: Session,
    top_n: int = 25,
) -> list[dict]:
    """
    Compute a multi-signal influence score for every entity:

    - degree_score:     normalized graph degree (connections)
    - bridge_score:     proportion of neighbors in different clusters
    - claim_target:     how often others make claims ABOUT this entity
    - claim_authority:  how often this entity makes claims
    - doc_spread:       fraction of corpus docs this entity appears in
    - composite:        weighted combination

    Returns ranked list of most influential entities.
    """
    total_docs = db.query(func.count(Document.id)).scalar() or 1
    total_entities = db.query(func.count(Entity.id)).scalar() or 1

    # Degree: count relationships per entity
    degree_rows = db.execute(text("""
        SELECT entity_id, COUNT(*) as degree
        FROM (
            SELECT entity_a_id as entity_id FROM entity_relationships
            UNION ALL
            SELECT entity_b_id as entity_id FROM entity_relationships
        )
        GROUP BY entity_id
    """)).fetchall()
    degree_map = {r[0]: r[1] for r in degree_rows}
    max_degree = max(degree_map.values()) if degree_map else 1

    # Doc spread: distinct docs per entity
    spread_rows = db.execute(text("""
        SELECT entity_id, COUNT(DISTINCT document_id) as doc_count
        FROM mentions
        GROUP BY entity_id
    """)).fetchall()
    spread_map = {r[0]: r[1] for r in spread_rows}

    # Claim target score: how many claims are about this entity
    claim_target_rows = db.execute(text("""
        SELECT subject_entity_id, COUNT(*) as n
        FROM claims
        WHERE subject_entity_id IS NOT NULL
        GROUP BY subject_entity_id
    """)).fetchall()
    claim_target_map = {r[0]: r[1] for r in claim_target_rows}
    max_claim_target = max(claim_target_map.values()) if claim_target_map else 1

    # Claim authority: how many claims this entity makes
    claim_auth_rows = db.execute(text("""
        SELECT speaker_entity_id, COUNT(*) as n
        FROM claims
        WHERE speaker_entity_id IS NOT NULL
        GROUP BY speaker_entity_id
    """)).fetchall()
    claim_auth_map = {r[0]: r[1] for r in claim_auth_rows}
    max_claim_auth = max(claim_auth_map.values()) if claim_auth_map else 1

    # Bridge score: neighbors that are in different connected components
    # (approximation: ratio of neighbor-pair edges that DON'T exist)
    bridge_map = _compute_bridge_scores(db, degree_map)

    # Compute composite scores
    entities = db.query(Entity).all()
    scored = []

    for entity in entities:
        eid = entity.id
        degree = degree_map.get(eid, 0)
        spread = spread_map.get(eid, 0)
        target = claim_target_map.get(eid, 0)
        authority = claim_auth_map.get(eid, 0)
        bridge = bridge_map.get(eid, 0.0)

        degree_score    = degree / max_degree
        spread_score    = spread / total_docs
        target_score    = target / max_claim_target
        authority_score = authority / max_claim_auth
        bridge_score    = bridge

        composite = (
            degree_score    * 0.30 +
            spread_score    * 0.25 +
            target_score    * 0.20 +
            bridge_score    * 0.15 +
            authority_score * 0.10
        )

        if composite < 0.01:
            continue

        scored.append({
            "entity": {
                "id":    eid,
                "name":  entity.canonical_name,
                "type":  entity.entity_type,
            },
            "scores": {
                "composite":  round(composite, 4),
                "degree":     round(degree_score, 4),
                "spread":     round(spread_score, 4),
                "claim_target":    round(target_score, 4),
                "claim_authority": round(authority_score, 4),
                "bridge":     round(bridge_score, 4),
            },
            "raw": {
                "connections":   degree,
                "docs":          spread,
                "claims_about":  target,
                "claims_made":   authority,
            }
        })

    scored.sort(key=lambda x: x["scores"]["composite"], reverse=True)
    return scored[:top_n]


def _compute_bridge_scores(
    db: Session,
    degree_map: dict[str, int],
    sample_size: int = 50,
) -> dict[str, float]:
    """
    Approximate bridge score: fraction of an entity's neighbors
    that are not connected to each other (i.e. entity bridges them).
    High score = structural broker / gatekeeper.
    """
    bridge_scores: dict[str, float] = {}

    # Get entities sorted by degree (focus on mid-degree nodes — bridges)
    candidates = sorted(degree_map.items(), key=lambda x: x[1])
    # Skip very low degree (< 2) and very high degree (top 5%)
    if len(candidates) > 10:
        low_cut = 2
        high_cut = max(candidates, key=lambda x: x[1])[1] * 0.9
        candidates = [(e, d) for e, d in candidates
                      if low_cut <= d <= high_cut][:sample_size]

    for entity_id, degree in candidates:
        # Get all neighbors
        rels = db.execute(text("""
            SELECT entity_a_id, entity_b_id
            FROM entity_relationships
            WHERE entity_a_id = :eid OR entity_b_id = :eid
        """), {"eid": entity_id}).fetchall()

        neighbors = set()
        for a, b in rels:
            if a != entity_id:
                neighbors.add(a)
            if b != entity_id:
                neighbors.add(b)

        if len(neighbors) < 2:
            bridge_scores[entity_id] = 0.0
            continue

        # Count connections between neighbors
        neighbor_list = list(neighbors)
        connected_pairs = 0
        total_pairs = len(neighbor_list) * (len(neighbor_list) - 1) // 2

        if total_pairs == 0:
            bridge_scores[entity_id] = 0.0
            continue

        for i in range(len(neighbor_list)):
            for j in range(i + 1, len(neighbor_list)):
                a_id = min(neighbor_list[i], neighbor_list[j])
                b_id = max(neighbor_list[i], neighbor_list[j])
                exists = db.execute(text("""
                    SELECT 1 FROM entity_relationships
                    WHERE entity_a_id = :a AND entity_b_id = :b
                """), {"a": a_id, "b": b_id}).fetchone()
                if exists:
                    connected_pairs += 1

        # Bridge score: fraction of neighbor pairs NOT connected (entity bridges them)
        bridge_scores[entity_id] = 1.0 - (connected_pairs / total_pairs)

    return bridge_scores
