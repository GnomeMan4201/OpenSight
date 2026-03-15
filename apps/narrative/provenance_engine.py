"""
OpenSight — Narrative Provenance Engine
apps/narrative/provenance_engine.py

Builds directed provenance graphs over claim corpora.
Models narrative information flow: seed detection, propagation,
mutation scoring, broker identification, and source reliability.

Requires: semantic microservice at http://127.0.0.1:8010
Database:  opensight.db (SQLite)
"""

from __future__ import annotations

import json
import math
import sqlite3
import time
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional

import httpx

log = logging.getLogger("opensight.provenance")

SEMANTIC_URL = "http://127.0.0.1:8010"
DB_PATH = "opensight.db"

# ─────────────────────────────────────────────────────────────
# Tuning weights
# ─────────────────────────────────────────────────────────────
SEED_TEMPORAL_WEIGHT   = 0.35
SEED_SEMANTIC_WEIGHT   = 0.40
SEED_REACH_WEIGHT      = 0.25

PROPAGATION_SIM_FLOOR  = 0.65   # minimum cosine to link claims
MUTATION_FLAG_THRESH   = 0.28   # mutation score above this → flagged
COHERENCE_TIGHT_THRESH = 0.75   # thread coherence: tight vs. loose


# ─────────────────────────────────────────────────────────────
# Data containers
# ─────────────────────────────────────────────────────────────

@dataclass
class ClaimRecord:
    id: int
    document_id: int
    text: str
    actor: Optional[str]
    source_tag: Optional[str]
    doc_date: Optional[str]          # ISO string or None
    embedding: Optional[list[float]] = field(default=None, repr=False)


@dataclass
class ProvenanceEdge:
    parent_claim_id: int
    child_claim_id: int
    similarity: float
    generation_delta: int = 1


@dataclass
class ThreadResult:
    thread_id: int
    seed_claim_id: int
    seed_document_id: int
    node_count: int
    document_span: int
    actor_count: int
    coherence_score: float
    manipulation_flag: bool
    summary: str


# ─────────────────────────────────────────────────────────────
# DB schema migration
# ─────────────────────────────────────────────────────────────

MIGRATION_SQL = """
-- Source reliability layer
CREATE TABLE IF NOT EXISTS narrative_sources (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_id            INTEGER NOT NULL REFERENCES claims(id),
    source_type         TEXT NOT NULL,          -- see SOURCE_TYPES
    reliability_score   REAL DEFAULT 0.5,
    citation_text       TEXT,
    cited_claim_id      INTEGER REFERENCES claims(id),
    created_at          TEXT DEFAULT (datetime('now'))
);

-- Thread: one coherent propagation chain
CREATE TABLE IF NOT EXISTS narrative_threads (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    seed_claim_id       INTEGER NOT NULL REFERENCES claims(id),
    seed_document_id    INTEGER NOT NULL REFERENCES documents(id),
    source_tag          TEXT,
    thread_summary      TEXT,
    actor_count         INTEGER DEFAULT 0,
    document_span       INTEGER DEFAULT 0,
    temporal_span_days  INTEGER DEFAULT 0,
    node_count          INTEGER DEFAULT 0,
    coherence_score     REAL DEFAULT 0.0,
    manipulation_flag   INTEGER DEFAULT 0,
    created_at          TEXT DEFAULT (datetime('now'))
);

-- Node: one claim's position in a thread
CREATE TABLE IF NOT EXISTS narrative_provenance_nodes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id           INTEGER NOT NULL REFERENCES narrative_threads(id),
    claim_id            INTEGER NOT NULL REFERENCES claims(id),
    document_id         INTEGER NOT NULL,
    generation          INTEGER DEFAULT 0,      -- 0=seed, 1=first echo…
    mutation_score      REAL DEFAULT 0.0,       -- drift from seed
    downstream_reach    INTEGER DEFAULT 0,      -- descendant count
    is_seed             INTEGER DEFAULT 0,
    mutation_flagged    INTEGER DEFAULT 0,
    created_at          TEXT DEFAULT (datetime('now'))
);

-- Edge: directed link between nodes in propagation DAG
CREATE TABLE IF NOT EXISTS narrative_provenance_edges (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id           INTEGER NOT NULL REFERENCES narrative_threads(id),
    parent_claim_id     INTEGER NOT NULL REFERENCES claims(id),
    child_claim_id      INTEGER NOT NULL REFERENCES claims(id),
    similarity_score    REAL NOT NULL,
    generation_delta    INTEGER DEFAULT 1,
    created_at          TEXT DEFAULT (datetime('now'))
);

-- Broker: actor sitting at narrative junction points
CREATE TABLE IF NOT EXISTS narrative_brokers (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    source_tag          TEXT,
    entity_id           INTEGER REFERENCES entities(id),
    actor_name          TEXT NOT NULL,
    threads_connected   INTEGER DEFAULT 0,
    betweenness         REAL DEFAULT 0.0,
    in_degree           INTEGER DEFAULT 0,
    out_degree          INTEGER DEFAULT 0,
    is_amplifier        INTEGER DEFAULT 0,
    is_originator       INTEGER DEFAULT 0,
    is_suppressor       INTEGER DEFAULT 0,
    broker_type         TEXT,                   -- amplifier|originator|bridge|suppressor
    created_at          TEXT DEFAULT (datetime('now'))
);

-- Typed contradictions (richer than temporal_anomalies)
CREATE TABLE IF NOT EXISTS narrative_contradictions_typed (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    source_tag          TEXT,
    claim_a_id          INTEGER NOT NULL REFERENCES claims(id),
    claim_b_id          INTEGER NOT NULL REFERENCES claims(id),
    contradiction_type  TEXT NOT NULL,          -- factual|temporal|identity|causal|presence
    actor_overlap       TEXT,                   -- JSON list of shared actors
    severity            REAL DEFAULT 0.5,
    document_a_id       INTEGER,
    document_b_id       INTEGER,
    investigator_note   TEXT,
    created_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_prov_nodes_thread  ON narrative_provenance_nodes(thread_id);
CREATE INDEX IF NOT EXISTS idx_prov_nodes_claim   ON narrative_provenance_nodes(claim_id);
CREATE INDEX IF NOT EXISTS idx_prov_edges_thread  ON narrative_provenance_edges(thread_id);
CREATE INDEX IF NOT EXISTS idx_prov_edges_parent  ON narrative_provenance_edges(parent_claim_id);
CREATE INDEX IF NOT EXISTS idx_brokers_actor      ON narrative_brokers(actor_name);
CREATE INDEX IF NOT EXISTS idx_sources_claim      ON narrative_sources(claim_id);
"""

SOURCE_TYPES = {
    "witness_statement":  0.80,
    "court_record":       0.90,
    "law_enforcement":    0.85,
    "sworn_affidavit":    0.88,
    "journalism":         0.60,
    "expert_report":      0.75,
    "secondary_reporting":0.40,
    "hearsay":            0.20,
    "unknown":            0.30,
}


def run_migration(db_path: str = DB_PATH) -> None:
    """Apply schema migration. Safe to re-run (IF NOT EXISTS)."""
    with sqlite3.connect(db_path, timeout=30) as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.executescript(MIGRATION_SQL)
    log.info("Provenance schema migration complete.")


# ─────────────────────────────────────────────────────────────
# Semantic helpers
# ─────────────────────────────────────────────────────────────

def _embed_batch(texts: list[str]) -> list[list[float]]:
    """Call semantic microservice. Returns embeddings in same order."""
    if not texts:
        return []
    try:
        resp = httpx.post(
            f"{SEMANTIC_URL}/embed",
            json={"texts": texts},
            timeout=60.0,
        )
        resp.raise_for_status()
        return resp.json()["vectors"]
    except Exception as exc:
        log.error("Embedding call failed: %s", exc)
        return [[0.0] * 384 for _ in texts]


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na  = math.sqrt(sum(x * x for x in a))
    nb  = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _centroid(embeddings: list[list[float]]) -> list[float]:
    if not embeddings:
        return []
    dim = len(embeddings[0])
    c = [0.0] * dim
    for emb in embeddings:
        for i, v in enumerate(emb):
            c[i] += v
    n = len(embeddings)
    return [v / n for v in c]


# ─────────────────────────────────────────────────────────────
# Date helpers
# ─────────────────────────────────────────────────────────────

def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    return None


def _date_rank(claims: list[ClaimRecord]) -> dict[int, float]:
    """Return normalised earliest-date rank per claim (1.0 = earliest)."""
    dated = [(c.id, _parse_date(c.doc_date)) for c in claims if _parse_date(c.doc_date)]
    if not dated:
        return {c.id: 0.5 for c in claims}
    min_d = min(d for _, d in dated)
    max_d = max(d for _, d in dated)
    span  = max(1, (max_d - min_d).days)
    ranks = {}
    for cid, d in dated:
        ranks[cid] = 1.0 - (d - min_d).days / span   # earlier → closer to 1
    for c in claims:
        if c.id not in ranks:
            ranks[c.id] = 0.0
    return ranks


# ─────────────────────────────────────────────────────────────
# Core engine
# ─────────────────────────────────────────────────────────────

class NarrativeProvenanceEngine:
    def _connect(self):
        con = sqlite3.connect(self.db_path, timeout=30)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass
        try:
            con.execute("PRAGMA busy_timeout=30000;")
        except Exception:
            pass
        return con

    """
    Builds narrative provenance graphs from claim corpora.

    Usage:
        engine = NarrativeProvenanceEngine()
        result = engine.build(source_tag="epstein-fbi")
    """

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        run_migration(db_path)

    # ── public API ───────────────────────────────────────────

    def build(self, source_tag: Optional[str] = None) -> dict:
        """Full provenance build for a source_tag (or all claims)."""
        log.info("Provenance build started. source_tag=%s", source_tag)

        claims = self._load_claims(source_tag)
        if len(claims) < 2:
            return {"status": "insufficient_claims", "count": len(claims)}

        # Embed all claims
        log.info("Embedding %d claims…", len(claims))
        embeddings = _embed_batch([c.text for c in claims])
        for c, emb in zip(claims, embeddings):
            c.embedding = emb

        # Cluster into thread groups by semantic similarity
        clusters = self._cluster_claims(claims)
        log.info("Found %d clusters.", len(clusters))

        self._clear_provenance(source_tag)

        thread_ids = []
        for cluster in clusters:
            if len(cluster) < 2:
                continue
            tid = self._build_thread(cluster, source_tag)
            if tid:
                thread_ids.append(tid)

        # Cross-thread analysis
        self._detect_contradictions(claims, source_tag)
        self._compute_brokers(source_tag)

        summary = self._summary(source_tag)
        log.info("Provenance build complete: %s", summary)
        return summary

    def get_threads(self, source_tag: Optional[str] = None) -> list[dict]:
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            q = "SELECT * FROM narrative_threads"
            params: list = []
            if source_tag:
                q += " WHERE source_tag = ?"
                params.append(source_tag)
            q += " ORDER BY coherence_score DESC"
            return [dict(r) for r in con.execute(q, params).fetchall()]

    def get_thread_provenance(self, thread_id: int) -> dict:
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            thread = dict(con.execute(
                "SELECT * FROM narrative_threads WHERE id=?", (thread_id,)
            ).fetchone() or {})
            nodes = [dict(r) for r in con.execute(
                "SELECT n.*, c.claim_text AS text, e.canonical_name AS actor FROM narrative_provenance_nodes n "
                "JOIN claims c ON c.id = n.claim_id "
                "LEFT JOIN entities e ON e.id = c.speaker_entity_id "
                "WHERE n.thread_id=? ORDER BY n.generation, n.mutation_score",
                (thread_id,)
            ).fetchall()]
            edges = [dict(r) for r in con.execute(
                "SELECT * FROM narrative_provenance_edges WHERE thread_id=?",
                (thread_id,)
            ).fetchall()]
        return {"thread": thread, "nodes": nodes, "edges": edges}

    def get_brokers(self, source_tag: Optional[str] = None) -> list[dict]:
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            q = "SELECT * FROM narrative_brokers"
            params: list = []
            if source_tag:
                q += " WHERE source_tag = ?"
                params.append(source_tag)
            q += " ORDER BY betweenness DESC"
            return [dict(r) for r in con.execute(q, params).fetchall()]

    def get_contradictions(self, source_tag: Optional[str] = None) -> list[dict]:
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            q = ("SELECT ct.*, "
                 "ca.claim_text AS text_a, cb.claim_text AS text_b, "
                 "ea.canonical_name AS actor_a, eb.canonical_name AS actor_b "
                 "FROM narrative_contradictions_typed ct "
                 "JOIN claims ca ON ca.id = ct.claim_a_id "
                 "JOIN claims cb ON cb.id = ct.claim_b_id "
                 "LEFT JOIN entities ea ON ea.id = ca.speaker_entity_id "
                 "LEFT JOIN entities eb ON eb.id = cb.speaker_entity_id")
            params: list = []
            if source_tag:
                q += " WHERE ct.source_tag = ?"
                params.append(source_tag)
            q += " ORDER BY ct.severity DESC"
            return [dict(r) for r in con.execute(q, params).fetchall()]

    def get_manipulation_flags(self, source_tag: Optional[str] = None) -> list[dict]:
        """Threads with manipulation_flag=1 and supporting evidence."""
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            q = ("SELECT t.*, "
                 "COUNT(DISTINCT n.id) AS mutated_nodes "
                 "FROM narrative_threads t "
                 "LEFT JOIN narrative_provenance_nodes n "
                 "  ON n.thread_id=t.id AND n.mutation_flagged=1 "
                 "WHERE t.manipulation_flag=1")
            params: list = []
            if source_tag:
                q += " AND t.source_tag=?"
                params.append(source_tag)
            q += " GROUP BY t.id ORDER BY mutated_nodes DESC"
            return [dict(r) for r in con.execute(q, params).fetchall()]

    # ── internal: load ───────────────────────────────────────

    def _load_claims(self, source_tag: Optional[str]) -> list[ClaimRecord]:
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            if source_tag:
                rows = con.execute(
                    "SELECT c.id, c.document_id, c.claim_text AS text, e.canonical_name AS actor, "
                    "d.source_tag, d.created_at AS doc_date "
                    "FROM claims c "
                    "JOIN documents d ON d.id = c.document_id "
                    "LEFT JOIN entities e ON e.id = c.speaker_entity_id "
                    "WHERE d.source_tag = ? AND c.claim_text IS NOT NULL "
                    "ORDER BY d.created_at",
                    (source_tag,)
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT c.id, c.document_id, c.claim_text AS text, e.canonical_name AS actor, "
                    "d.source_tag, d.created_at AS doc_date "
                    "FROM claims c "
                    "JOIN documents d ON d.id = c.document_id "
                    "LEFT JOIN entities e ON e.id = c.speaker_entity_id "
                    "WHERE c.claim_text IS NOT NULL "
                    "ORDER BY d.created_at"
                ).fetchall()
        return [
            ClaimRecord(
                id=r["id"],
                document_id=r["document_id"],
                text=r["text"],
                actor=r["actor"],
                source_tag=r["source_tag"],
                doc_date=r["doc_date"],
            )
            for r in rows
        ]

    # ── internal: clustering ─────────────────────────────────

    def _cluster_claims(
        self,
        claims: list[ClaimRecord],
        sim_threshold: float = 0.62,
    ) -> list[list[ClaimRecord]]:
        """
        Greedy single-pass clustering by cosine similarity.
        O(n²) — fine for corpora up to ~5 000 claims.
        For larger corpora, replace with FAISS or HNSW.
        """
        used   = set()
        groups = []

        for i, ci in enumerate(claims):
            if i in used:
                continue
            group = [ci]
            used.add(i)
            for j, cj in enumerate(claims):
                if j in used:
                    continue
                sim = _cosine(ci.embedding, cj.embedding)
                if sim >= sim_threshold:
                    group.append(cj)
                    used.add(j)
            groups.append(group)

        return groups

    # ── internal: thread build ───────────────────────────────

    def _build_thread(
        self,
        cluster: list[ClaimRecord],
        source_tag: Optional[str],
    ) -> Optional[int]:
        centroid  = _centroid([c.embedding for c in cluster])
        date_rank = _date_rank(cluster)

        # Pre-compute downstream reach via pairwise similarity DAG
        reach: dict[int, int] = self._estimate_reach(cluster)

        max_reach  = max(reach.values()) if reach else 1
        all_sims   = [_cosine(c.embedding, centroid) for c in cluster]
        max_sim    = max(all_sims) or 1

        # Score each claim as potential seed
        scores = {}
        for c, sim in zip(cluster, all_sims):
            scores[c.id] = (
                SEED_TEMPORAL_WEIGHT * date_rank.get(c.id, 0.0)
                + SEED_SEMANTIC_WEIGHT * (sim / max_sim)
                + SEED_REACH_WEIGHT   * (reach.get(c.id, 0) / max_reach)
            )

        seed = max(cluster, key=lambda c: scores[c.id])

        # Build propagation DAG from seed outward
        dag   = self._build_dag(seed, cluster)
        nodes = dag["nodes"]          # {claim_id: {generation, mutation, reach}}
        edges = dag["edges"]          # list[ProvenanceEdge]

        if not nodes:
            return None

        # Thread-level metrics
        doc_ids   = {c.document_id for c in cluster}
        actors    = {c.actor for c in cluster if c.actor}
        all_dates = [_parse_date(c.doc_date) for c in cluster if _parse_date(c.doc_date)]
        span_days = 0
        if len(all_dates) >= 2:
            span_days = (max(all_dates) - min(all_dates)).days

        coherence = self._thread_coherence(cluster, centroid)
        mutation_scores = [nodes[cid]["mutation"] for cid in nodes]
        high_mut = sum(1 for m in mutation_scores if m > MUTATION_FLAG_THRESH)
        manip_flag = (
            high_mut >= 2
            and coherence < COHERENCE_TIGHT_THRESH
            and len(actors) >= 2
        )

        summary_text = (
            f"Thread seeded by claim {seed.id} "
            f"({seed.actor or 'unknown actor'}), "
            f"{len(nodes)} nodes across {len(doc_ids)} documents."
        )

        with self._connect() as con:
            cur = con.execute(
                "INSERT INTO narrative_threads "
                "(seed_claim_id, seed_document_id, source_tag, thread_summary, "
                " actor_count, document_span, temporal_span_days, node_count, "
                " coherence_score, manipulation_flag) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    seed.id, seed.document_id, source_tag, summary_text,
                    len(actors), len(doc_ids), span_days, len(nodes),
                    round(coherence, 4), int(manip_flag),
                ),
            )
            thread_id = cur.lastrowid

            # Insert nodes
            con.executemany(
                "INSERT INTO narrative_provenance_nodes "
                "(thread_id, claim_id, document_id, generation, mutation_score, "
                " downstream_reach, is_seed, mutation_flagged) "
                "VALUES (?,?,?,?,?,?,?,?)",
                [
                    (
                        thread_id,
                        cid,
                        nodes[cid]["document_id"],
                        nodes[cid]["generation"],
                        round(nodes[cid]["mutation"], 4),
                        nodes[cid]["reach"],
                        int(cid == seed.id),
                        int(nodes[cid]["mutation"] > MUTATION_FLAG_THRESH),
                    )
                    for cid in nodes
                ],
            )

            # Insert edges
            con.executemany(
                "INSERT INTO narrative_provenance_edges "
                "(thread_id, parent_claim_id, child_claim_id, "
                " similarity_score, generation_delta) "
                "VALUES (?,?,?,?,?)",
                [
                    (thread_id, e.parent_claim_id, e.child_claim_id,
                     round(e.similarity, 4), e.generation_delta)
                    for e in edges
                ],
            )

        return thread_id

    def _estimate_reach(self, cluster: list[ClaimRecord]) -> dict[int, int]:
        """Count how many other claims each claim is more-similar-than-floor to."""
        reach = defaultdict(int)
        for i, ci in enumerate(cluster):
            for j, cj in enumerate(cluster):
                if i == j:
                    continue
                if _cosine(ci.embedding, cj.embedding) >= PROPAGATION_SIM_FLOOR:
                    reach[ci.id] += 1
        return dict(reach)

    def _build_dag(
        self,
        seed: ClaimRecord,
        cluster: list[ClaimRecord],
    ) -> dict:
        """
        BFS from seed, linking claims by similarity and temporal ordering.
        Returns nodes dict and edges list.
        """
        claim_map   = {c.id: c for c in cluster}
        nodes: dict[int, dict] = {}
        edges: list[ProvenanceEdge] = []
        queue       = [seed.id]
        gen_map     = {seed.id: 0}

        nodes[seed.id] = {
            "generation":   0,
            "mutation":     0.0,
            "reach":        0,
            "document_id":  seed.document_id,
        }

        # Sort cluster by date for temporal ordering
        dated_cluster = sorted(
            cluster,
            key=lambda c: (_parse_date(c.doc_date) or date.min),
        )

        seed_emb = seed.embedding

        while queue:
            parent_id = queue.pop(0)
            parent    = claim_map[parent_id]
            parent_dt = _parse_date(parent.doc_date)

            for child in dated_cluster:
                if child.id == parent_id:
                    continue
                if child.id in nodes:
                    continue

                # Temporal gate: child must come after parent (or be undated)
                child_dt = _parse_date(child.doc_date)
                if parent_dt and child_dt and child_dt < parent_dt:
                    continue

                sim = _cosine(parent.embedding, child.embedding)
                if sim < PROPAGATION_SIM_FLOOR:
                    continue

                gen = gen_map[parent_id] + 1
                mut = 1.0 - _cosine(seed_emb, child.embedding)

                gen_map[child.id] = gen
                nodes[child.id] = {
                    "generation":   gen,
                    "mutation":     mut,
                    "reach":        0,
                    "document_id":  child.document_id,
                }
                edges.append(ProvenanceEdge(
                    parent_claim_id=parent_id,
                    child_claim_id=child.id,
                    similarity=sim,
                ))
                queue.append(child.id)

        # Back-fill downstream reach
        for e in edges:
            if e.parent_claim_id in nodes:
                nodes[e.parent_claim_id]["reach"] += 1

        return {"nodes": nodes, "edges": edges}

    def _thread_coherence(
        self,
        cluster: list[ClaimRecord],
        centroid: list[float],
    ) -> float:
        """Mean cosine similarity to centroid = thread coherence."""
        sims = [_cosine(c.embedding, centroid) for c in cluster]
        return sum(sims) / len(sims) if sims else 0.0

    # ── internal: contradictions ─────────────────────────────

    def _detect_contradictions(
        self,
        claims: list[ClaimRecord],
        source_tag: Optional[str],
    ) -> None:
        """
        Pair-wise contradiction detection.
        Heuristics:
          - High semantic similarity but mismatched actors → identity contradiction
          - Same actor, high similarity, opposite sentiment  → factual contradiction
          - Same actor, date conflict                        → temporal contradiction
          - Low similarity, same entity cluster              → causal contradiction
        """
        rows = []
        claim_map = {c.id: c for c in claims}

        for i, ca in enumerate(claims):
            for j, cb in enumerate(claims):
                if j <= i:
                    continue

                sim = _cosine(ca.embedding, cb.embedding)

                # Not similar enough to be related
                if sim < 0.50:
                    continue

                c_type, severity = self._classify_contradiction(ca, cb, sim)
                if not c_type:
                    continue

                actor_overlap = []
                if ca.actor and cb.actor:
                    a_set = set(ca.actor.lower().split())
                    b_set = set(cb.actor.lower().split())
                    overlap = a_set & b_set
                    if overlap:
                        actor_overlap = list(overlap)

                rows.append((
                    source_tag,
                    ca.id, cb.id,
                    c_type,
                    json.dumps(actor_overlap),
                    round(severity, 4),
                    ca.document_id,
                    cb.document_id,
                ))

        if rows:
            with self._connect() as con:
                con.executemany(
                    "INSERT INTO narrative_contradictions_typed "
                    "(source_tag, claim_a_id, claim_b_id, contradiction_type, "
                    " actor_overlap, severity, document_a_id, document_b_id) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    rows,
                )
            log.info("Inserted %d typed contradictions.", len(rows))

    def _classify_contradiction(
        self,
        ca: ClaimRecord,
        cb: ClaimRecord,
        sim: float,
    ) -> tuple[Optional[str], float]:
        """
        Returns (contradiction_type, severity) or (None, 0).
        Severity = how investigatively significant this is.
        """
        same_actor = (
            ca.actor and cb.actor
            and ca.actor.lower().strip() == cb.actor.lower().strip()
        )
        diff_actor = (
            ca.actor and cb.actor
            and ca.actor.lower().strip() != cb.actor.lower().strip()
        )

        da = _parse_date(ca.doc_date)
        db = _parse_date(cb.doc_date)

        # Temporal: same actor, same topic, very different dates
        if same_actor and da and db:
            gap = abs((da - db).days)
            if gap > 365 and sim > 0.70:
                return "temporal", min(0.95, 0.5 + sim * 0.5)

        # Identity: high similarity but claims attributed to different actors
        if diff_actor and sim > 0.78:
            return "identity", sim

        # Factual: same actor, high topic similarity
        if same_actor and sim > 0.72:
            # Simple negation heuristic
            neg_words = {"not", "never", "denied", "false", "incorrect",
                         "no", "refuted", "disputed"}
            a_words = set((ca.text or '').lower().split()) if ca.text else set()
            b_words = set((cb.text or '').lower().split()) if cb.text else set()
            if (neg_words & a_words) != (neg_words & b_words):
                return "factual", sim

        # Presence: very similar claim, radically different document sources
        if (sim > 0.68
                and ca.document_id != cb.document_id
                and diff_actor):
            return "presence", sim * 0.8

        return None, 0.0

    # ── internal: broker detection ───────────────────────────

    def _compute_brokers(self, source_tag: Optional[str]) -> None:
        """
        Graph-theoretic broker detection over the provenance edge table.
        Approximates betweenness by counting path intersections through actors.
        """
        with self._connect() as con:
            con.row_factory = sqlite3.Row

            q = (
                "SELECT e.parent_claim_id, e.child_claim_id, "
                "ep.canonical_name AS parent_actor, ec.canonical_name AS child_actor, "
                "e.thread_id "
                "FROM narrative_provenance_edges e "
                "JOIN claims cp ON cp.id = e.parent_claim_id "
                "JOIN claims cc ON cc.id = e.child_claim_id "
                "LEFT JOIN entities ep ON ep.id = cp.speaker_entity_id "
                "LEFT JOIN entities ec ON ec.id = cc.speaker_entity_id"
            )
            params: list = []
            if source_tag:
                q += (
                    " JOIN narrative_threads t ON t.id = e.thread_id "
                    "WHERE t.source_tag = ?"
                )
                params.append(source_tag)

            edges = con.execute(q, params).fetchall()

        if not edges:
            return

        # Build actor-level graph
        actor_in:     defaultdict[str, int]       = defaultdict(int)
        actor_out:    defaultdict[str, int]        = defaultdict(int)
        actor_threads: defaultdict[str, set]       = defaultdict(set)
        # Betweenness approximation: count edges passing through each actor
        actor_between: defaultdict[str, float]     = defaultdict(float)

        for e in edges:
            pa = e["parent_actor"] or ""
            ca = e["child_actor"]  or ""
            tid = e["thread_id"]

            if pa:
                actor_out[pa]     += 1
                actor_threads[pa].add(tid)
            if ca:
                actor_in[ca]      += 1
                actor_threads[ca].add(tid)

            # Both actors present on same edge = ca acts as relay
            if pa and ca and pa != ca:
                actor_between[ca] += 1.0

        all_actors = set(actor_in) | set(actor_out) | set(actor_between)
        max_between = max(actor_between.values(), default=1) or 1

        rows = []
        for actor in all_actors:
            if not actor.strip():
                continue
            out = actor_out[actor]
            inp = actor_in[actor]
            bet = actor_between[actor] / max_between
            threads = len(actor_threads[actor])

            is_amplifier  = out > inp * 2 and out >= 3
            is_originator = inp == 0 and out >= 2
            is_suppressor = inp >= 3 and out == 0

            if bet > 0.5:
                b_type = "bridge"
            elif is_amplifier:
                b_type = "amplifier"
            elif is_originator:
                b_type = "originator"
            elif is_suppressor:
                b_type = "suppressor"
            else:
                b_type = "relay"

            rows.append((
                source_tag, actor, threads,
                round(bet, 4), inp, out,
                int(is_amplifier), int(is_originator), int(is_suppressor),
                b_type,
            ))

        if rows:
            with self._connect() as con:
                con.executemany(
                    "INSERT INTO narrative_brokers "
                    "(source_tag, actor_name, threads_connected, betweenness, "
                    " in_degree, out_degree, is_amplifier, is_originator, "
                    " is_suppressor, broker_type) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    rows,
                )
            log.info("Inserted %d broker records.", len(rows))

    # ── internal: cleanup + summary ──────────────────────────

    def _clear_provenance(self, source_tag: Optional[str]) -> None:
        """Remove previous run data for this source_tag."""
        with self._connect() as con:
            if source_tag:
                # Get thread ids for this tag
                tids = [r[0] for r in con.execute(
                    "SELECT id FROM narrative_threads WHERE source_tag=?",
                    (source_tag,)
                ).fetchall()]
                for tid in tids:
                    con.execute(
                        "DELETE FROM narrative_provenance_nodes WHERE thread_id=?",
                        (tid,)
                    )
                    con.execute(
                        "DELETE FROM narrative_provenance_edges WHERE thread_id=?",
                        (tid,)
                    )
                con.execute(
                    "DELETE FROM narrative_threads WHERE source_tag=?",
                    (source_tag,)
                )
                con.execute(
                    "DELETE FROM narrative_brokers WHERE source_tag=?",
                    (source_tag,)
                )
                con.execute(
                    "DELETE FROM narrative_contradictions_typed WHERE source_tag=?",
                    (source_tag,)
                )
            else:
                for tbl in [
                    "narrative_provenance_nodes",
                    "narrative_provenance_edges",
                    "narrative_threads",
                    "narrative_brokers",
                    "narrative_contradictions_typed",
                ]:
                    con.execute(f"DELETE FROM {tbl}")

    def _summary(self, source_tag: Optional[str]) -> dict:
        with self._connect() as con:
            def count(tbl, col="source_tag"):
                if source_tag:
                    return con.execute(
                        f"SELECT COUNT(*) FROM {tbl} WHERE {col}=?",
                        (source_tag,)
                    ).fetchone()[0]
                return con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]

            return {
                "status":            "ok",
                "source_tag":        source_tag,
                "threads":           count("narrative_threads"),
                "provenance_nodes":  con.execute(
                    "SELECT COUNT(*) FROM narrative_provenance_nodes"
                ).fetchone()[0],
                "provenance_edges":  con.execute(
                    "SELECT COUNT(*) FROM narrative_provenance_edges"
                ).fetchone()[0],
                "brokers":           count("narrative_brokers"),
                "contradictions":    count("narrative_contradictions_typed"),
                "manipulation_flags": con.execute(
                    "SELECT COUNT(*) FROM narrative_threads WHERE manipulation_flag=1"
                    + (" AND source_tag=?" if source_tag else ""),
                    (source_tag,) if source_tag else ()
                ).fetchone()[0],
            }


# ─────────────────────────────────────────────────────────────
# Source classification helper (call from ingest pipeline)
# ─────────────────────────────────────────────────────────────

def classify_source(
    claim_id: int,
    source_type: str,
    citation_text: Optional[str] = None,
    cited_claim_id: Optional[int] = None,
    db_path: str = DB_PATH,
) -> None:
    """
    Attach a source reliability record to a claim.
    Call this during document ingestion when source type is known.
    """
    reliability = SOURCE_TYPES.get(source_type, 0.30)
    with sqlite3.connect(db_path, timeout=30) as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute(
            "INSERT INTO narrative_sources "
            "(claim_id, source_type, reliability_score, citation_text, cited_claim_id) "
            "VALUES (?,?,?,?,?)",
            (claim_id, source_type, reliability, citation_text, cited_claim_id),
        )
