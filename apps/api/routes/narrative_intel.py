from __future__ import annotations

import os
import sqlite3
from fastapi import APIRouter

from apps.narrative.intel_engine import NarrativeIntelEngine

router = APIRouter(prefix="/api/v1/narrative-intel", tags=["narrative-intel"])

DB_PATH = os.getenv("OPENSIGHT_DB_PATH", "./opensight.db")
engine = NarrativeIntelEngine(DB_PATH)


@router.post("/build")
def build_narrative_intel(source_tag: str | None = None):
    return engine.build(source_tag=source_tag)


@router.get("/summary")
def narrative_intel_summary():
    return engine.summary()


@router.get("/actor/{actor_name}")
def narrative_intel_actor(actor_name: str):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        events = conn.execute(
            """
            SELECT id, event_type, normalized_date, location_text, canonical_summary, support_count, contradiction_count, confidence
            FROM narrative_canonical_events
            WHERE lower(primary_actor) = lower(?)
            ORDER BY confidence DESC, id DESC
            LIMIT 50
            """,
            (actor_name,),
        ).fetchall()

        anomalies = conn.execute(
            """
            SELECT anomaly_type, score, rationale
            FROM narrative_temporal_anomalies
            WHERE lower(actor_name) = lower(?)
            ORDER BY score DESC, id DESC
            LIMIT 20
            """,
            (actor_name,),
        ).fetchall()

        linked_claims = []
        table_row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='narrative_claim_entity_links'"
        ).fetchone()

        if table_row:
            linked_claims = conn.execute(
                """
                SELECT l.claim_id, l.entity_type, l.score, substr(c.claim_text,1,220) AS claim_text
                FROM narrative_claim_entity_links l
                JOIN claims c ON c.id = l.claim_id
                WHERE lower(l.canonical_name) = lower(?)
                ORDER BY l.score DESC, l.claim_id DESC
                LIMIT 25
                """,
                (actor_name,),
            ).fetchall()

        return {
            "actor": actor_name,
            "events": [dict(r) for r in events],
            "anomalies": [dict(r) for r in anomalies],
            "linked_claims": [dict(r) for r in linked_claims],
        }
    finally:
        conn.close()


@router.get("/overview")
def narrative_intel_overview():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        docs = conn.execute(
            """
            SELECT source_tag, COUNT(*) AS docs
            FROM documents
            GROUP BY source_tag
            ORDER BY docs DESC, source_tag
            """
        ).fetchall()

        claims = conn.execute(
            """
            SELECT d.source_tag, COUNT(*) AS claims
            FROM claims c
            JOIN documents d ON d.id = c.document_id
            GROUP BY d.source_tag
            ORDER BY claims DESC, d.source_tag
            """
        ).fetchall()

        entities = conn.execute(
            """
            SELECT entity_type, COUNT(*) AS n
            FROM entities
            GROUP BY entity_type
            ORDER BY n DESC, entity_type
            """
        ).fetchall()

        current_events = conn.execute(
            """
            SELECT primary_actor, event_type, normalized_date, location_text, substr(canonical_summary,1,180) AS summary
            FROM narrative_canonical_events
            ORDER BY id DESC
            LIMIT 25
            """
        ).fetchall()

        return {
            "documents_by_source_tag": [dict(r) for r in docs],
            "claims_by_source_tag": [dict(r) for r in claims],
            "entity_types": [dict(r) for r in entities],
            "current_events": [dict(r) for r in current_events],
        }
    finally:
        conn.close()


@router.get("/clusters")
def narrative_intel_clusters():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT id, cluster_key, label, claim_count, top_actor, exemplar_text
            FROM narrative_propagation_clusters
            ORDER BY claim_count DESC, id DESC
            LIMIT 100
            """
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.get("/clusters/{cluster_id}")
def narrative_intel_cluster(cluster_id: int):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cluster = conn.execute(
            """
            SELECT id, cluster_key, label, claim_count, top_actor, exemplar_text
            FROM narrative_propagation_clusters
            WHERE id = ?
            """,
            (cluster_id,),
        ).fetchone()

        claims = conn.execute(
            """
            SELECT c.id, c.document_id, c.page_number, c.claim_type, substr(c.claim_text,1,260) AS claim_text
            FROM narrative_cluster_claim_map m
            JOIN claims c ON c.id = m.claim_id
            WHERE m.cluster_id = ?
            ORDER BY c.document_id, c.page_number, c.id
            """,
            (cluster_id,),
        ).fetchall()

        return {
            "cluster": dict(cluster) if cluster else None,
            "claims": [dict(r) for r in claims],
        }
    finally:
        conn.close()
