"""
OpenSight — Investigation Bundle Engine
apps/narrative/bundle_engine.py

Export an entire investigation as a portable .osight bundle (zip).
Import a bundle into a local OpenSight instance.

Bundle structure:
    bundle/
     ├── case.json           case metadata
     ├── documents/          original files (optional, can be stub-only)
     ├── claims.json
     ├── entities.json
     ├── threads.json
     ├── provenance_nodes.json
     ├── provenance_edges.json
     ├── contradictions.json
     ├── brokers.json
     └── notes.json
"""

from __future__ import annotations

import io
import json
import os
import shutil
import sqlite3
import tempfile
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

DB_PATH      = "opensight.db"
UPLOAD_DIR   = "uploads"
BUNDLE_EXT   = ".osight"
SCHEMA_VER   = "1.0"


# ─────────────────────────────────────────────────────────────
# Export
# ─────────────────────────────────────────────────────────────

class BundleExporter:
    """
    Exports a source_tag investigation into a .osight bundle file.

    Usage:
        exporter = BundleExporter()
        path = exporter.export(
            source_tag="epstein-fbi",
            title="Epstein FBI FOIA Analysis",
            author="bad_banana",
            include_documents=True,
        )
        # path → /tmp/epstein-fbi-20260312.osight
    """

    def __init__(self, db_path: str = DB_PATH, upload_dir: str = UPLOAD_DIR):
        self.db_path    = db_path
        self.upload_dir = upload_dir

    def export(
        self,
        source_tag: str,
        title: str = "",
        author: str = "",
        notes: str = "",
        include_documents: bool = True,
        output_path: Optional[str] = None,
    ) -> str:
        """
        Build bundle and return path to the .osight file.
        """
        with sqlite3.connect(self.db_path) as con:
            con.row_factory = sqlite3.Row
            data = self._collect(con, source_tag)

        case_meta = {
            "schema_version":   SCHEMA_VER,
            "bundle_id":        str(uuid.uuid4()),
            "title":            title or f"Investigation: {source_tag}",
            "author":           author,
            "source_tag":       source_tag,
            "created":          datetime.now(timezone.utc).isoformat(),
            "document_count":   len(data["documents"]),
            "claim_count":      len(data["claims"]),
            "entity_count":     len(data["entities"]),
            "thread_count":     len(data["threads"]),
            "contradiction_count": len(data["contradictions"]),
            "broker_count":     len(data["brokers"]),
            "notes":            notes,
        }

        if not output_path:
            safe_tag   = source_tag.replace("/", "-").replace(" ", "_")
            date_str   = datetime.now().strftime("%Y%m%d")
            output_path = f"/tmp/{safe_tag}-{date_str}{BUNDLE_EXT}"

        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
            # Metadata
            zf.writestr("bundle/case.json",          json.dumps(case_meta,             indent=2))
            zf.writestr("bundle/claims.json",         json.dumps(data["claims"],        indent=2))
            zf.writestr("bundle/entities.json",       json.dumps(data["entities"],      indent=2))
            zf.writestr("bundle/threads.json",        json.dumps(data["threads"],       indent=2))
            zf.writestr("bundle/provenance_nodes.json", json.dumps(data["prov_nodes"], indent=2))
            zf.writestr("bundle/provenance_edges.json", json.dumps(data["prov_edges"], indent=2))
            zf.writestr("bundle/contradictions.json", json.dumps(data["contradictions"], indent=2))
            zf.writestr("bundle/brokers.json",        json.dumps(data["brokers"],       indent=2))
            zf.writestr("bundle/notes.json",          json.dumps({"notes": notes},      indent=2))

            # Document manifest (always included)
            doc_manifest = [
                {
                    "id":            d["id"],
                    "original_name": d["original_name"],
                    "source_tag":    d["source_tag"],
                    "storage_path":  d["storage_path"],
                    "page_count":    d["page_count"],
                    "doc_type":      d["doc_type"],
                    "created_at":    d["created_at"],
                }
                for d in data["documents"]
            ]
            zf.writestr("bundle/documents/manifest.json", json.dumps(doc_manifest, indent=2))

            # Actual document files (optional)
            if include_documents:
                for doc in data["documents"]:
                    storage_path = doc["storage_path"]
                    if not storage_path:
                        continue
                    full_path = Path(self.upload_dir) / storage_path
                    if full_path.exists():
                        zf.write(str(full_path), f"bundle/documents/files/{Path(storage_path).name}")

        return output_path

    def _collect(self, con: sqlite3.Connection, source_tag: str) -> dict:
        def rows(q, *args):
            return [dict(r) for r in con.execute(q, args).fetchall()]

        documents = rows(
            "SELECT * FROM documents WHERE source_tag = ?", source_tag
        )
        doc_ids = tuple(d["id"] for d in documents)

        if not doc_ids:
            return {
                "documents": [], "claims": [], "entities": [],
                "threads": [], "prov_nodes": [], "prov_edges": [],
                "contradictions": [], "brokers": [],
            }

        placeholders = ",".join("?" * len(doc_ids))

        claims = rows(
            f"SELECT * FROM claims WHERE document_id IN ({placeholders})",
            *doc_ids
        )
        claim_ids = tuple(c["id"] for c in claims)

        # Entities referenced by these claims
        entity_ids = set()
        for c in claims:
            if c.get("speaker_entity_id"):
                entity_ids.add(c["speaker_entity_id"])
            if c.get("subject_entity_id"):
                entity_ids.add(c["subject_entity_id"])

        entities = []
        if entity_ids:
            ep = ",".join("?" * len(entity_ids))
            entities = rows(f"SELECT * FROM entities WHERE id IN ({ep})", *entity_ids)

        # Narrative threads
        threads = rows(
            "SELECT * FROM narrative_threads WHERE source_tag = ?", source_tag
        )
        thread_ids = tuple(t["id"] for t in threads)

        prov_nodes, prov_edges = [], []
        if thread_ids:
            tp = ",".join("?" * len(thread_ids))
            prov_nodes = rows(
                f"SELECT * FROM narrative_provenance_nodes WHERE thread_id IN ({tp})",
                *thread_ids
            )
            prov_edges = rows(
                f"SELECT * FROM narrative_provenance_edges WHERE thread_id IN ({tp})",
                *thread_ids
            )

        contradictions = rows(
            "SELECT * FROM narrative_contradictions_typed WHERE source_tag = ?",
            source_tag
        )
        brokers = rows(
            "SELECT * FROM narrative_brokers WHERE source_tag = ?", source_tag
        )

        return {
            "documents":      documents,
            "claims":         claims,
            "entities":       entities,
            "threads":        threads,
            "prov_nodes":     prov_nodes,
            "prov_edges":     prov_edges,
            "contradictions": contradictions,
            "brokers":        brokers,
        }


# ─────────────────────────────────────────────────────────────
# Import
# ─────────────────────────────────────────────────────────────

class BundleImporter:
    """
    Imports a .osight bundle into the local OpenSight instance.

    Handles ID remapping so imported data doesn't collide with
    existing records. Documents are copied into the upload store.

    Usage:
        importer = BundleImporter()
        result   = importer.import_bundle("/path/to/case.osight")
    """

    def __init__(self, db_path: str = DB_PATH, upload_dir: str = UPLOAD_DIR):
        self.db_path    = db_path
        self.upload_dir = upload_dir

    def import_bundle(self, bundle_path: str) -> dict:
        if not zipfile.is_zipfile(bundle_path):
            raise ValueError("Not a valid .osight bundle")

        with zipfile.ZipFile(bundle_path, "r") as zf:
            names = zf.namelist()
            if "bundle/case.json" not in names:
                raise ValueError("Missing bundle/case.json — not a valid bundle")

            case_meta     = json.loads(zf.read("bundle/case.json"))
            claims        = json.loads(zf.read("bundle/claims.json"))
            entities      = json.loads(zf.read("bundle/entities.json"))
            threads       = json.loads(zf.read("bundle/threads.json"))
            prov_nodes    = json.loads(zf.read("bundle/provenance_nodes.json"))
            prov_edges    = json.loads(zf.read("bundle/provenance_edges.json"))
            contradictions= json.loads(zf.read("bundle/contradictions.json"))
            brokers       = json.loads(zf.read("bundle/brokers.json"))
            manifest      = json.loads(zf.read("bundle/documents/manifest.json"))

            # Copy document files
            source_tag = case_meta["source_tag"]
            dest_dir   = Path(self.upload_dir) / source_tag
            dest_dir.mkdir(parents=True, exist_ok=True)

            file_map: dict[str, str] = {}  # original filename → new storage_path
            for name in names:
                if name.startswith("bundle/documents/files/"):
                    fname = Path(name).name
                    dest  = dest_dir / fname
                    with zf.open(name) as src, open(dest, "wb") as dst:
                        dst.write(src.read())
                    file_map[fname] = f"{source_tag}/{fname}"

        with sqlite3.connect(self.db_path) as con:
            stats = self._write(
                con, case_meta, manifest, claims, entities,
                threads, prov_nodes, prov_edges, contradictions,
                brokers, file_map
            )

        return {
            "status":       "ok",
            "bundle_id":    case_meta.get("bundle_id"),
            "title":        case_meta.get("title"),
            "source_tag":   source_tag,
            **stats,
        }

    def _write(
        self,
        con: sqlite3.Connection,
        case_meta: dict,
        manifest: list,
        claims: list,
        entities: list,
        threads: list,
        prov_nodes: list,
        prov_edges: list,
        contradictions: list,
        brokers: list,
        file_map: dict,
    ) -> dict:
        source_tag = case_meta["source_tag"]

        # Check for existing source_tag — append suffix if collision
        existing = con.execute(
            "SELECT COUNT(*) FROM documents WHERE source_tag = ?", (source_tag,)
        ).fetchone()[0]
        if existing:
            source_tag = f"{source_tag}-imported-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # ID remapping tables (old_id → new_id)
        doc_id_map:    dict[str, str] = {}
        entity_id_map: dict[str, str] = {}
        claim_id_map:  dict[str, str] = {}

        # Insert documents
        for doc in manifest:
            new_id = str(uuid.uuid4())
            doc_id_map[doc["id"]] = new_id
            fname = Path(doc.get("storage_path", "")).name
            new_storage = file_map.get(fname, doc.get("storage_path", ""))
            try:
                con.execute(
                    "INSERT OR IGNORE INTO documents "
                    "(id, filename, original_name, sha256_hash, source_tag, "
                    " storage_path, page_count, doc_type, status, created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        new_id,
                        fname or doc["original_name"],
                        doc["original_name"],
                        f"imported-{new_id}",   # placeholder hash
                        source_tag,
                        new_storage,
                        doc.get("page_count", 0),
                        doc.get("doc_type", "imported"),
                        "imported",
                        doc.get("created_at", datetime.now().isoformat()),
                    )
                )
            except Exception:
                doc_id_map[doc["id"]] = doc["id"]   # fall back to original

        # Insert entities
        for ent in entities:
            new_id = str(uuid.uuid4())
            entity_id_map[ent["id"]] = new_id
            try:
                con.execute(
                    "INSERT OR IGNORE INTO entities "
                    "(id, entity_type, canonical_name, aliases, confidence, "
                    " review_status, created_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (
                        new_id,
                        ent.get("entity_type", "unknown"),
                        ent.get("canonical_name", ""),
                        ent.get("aliases", "[]"),
                        ent.get("confidence", 0.5),
                        "imported",
                        ent.get("created_at", datetime.now().isoformat()),
                    )
                )
            except Exception:
                entity_id_map[ent["id"]] = ent["id"]

        # Insert claims
        for claim in claims:
            new_id    = str(uuid.uuid4())
            claim_id_map[claim["id"]] = new_id
            new_doc   = doc_id_map.get(claim.get("document_id", ""), claim.get("document_id", ""))
            new_spk   = entity_id_map.get(claim.get("speaker_entity_id") or "", None)
            new_subj  = entity_id_map.get(claim.get("subject_entity_id") or "", None)
            try:
                con.execute(
                    "INSERT OR IGNORE INTO claims "
                    "(id, document_id, page_number, speaker_entity_id, "
                    " subject_entity_id, claim_text, claim_type, sentiment, "
                    " confidence, extraction_method, created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        new_id,
                        new_doc,
                        claim.get("page_number", 0),
                        new_spk,
                        new_subj,
                        claim.get("claim_text", ""),
                        claim.get("claim_type", "imported"),
                        claim.get("sentiment", "neutral"),
                        claim.get("confidence", 0.5),
                        "imported",
                        claim.get("created_at", datetime.now().isoformat()),
                    )
                )
            except Exception:
                claim_id_map[claim["id"]] = claim["id"]

        # Insert narrative threads + provenance
        thread_id_map: dict[int, int] = {}
        for thread in threads:
            old_tid      = thread["id"]
            new_seed_doc = doc_id_map.get(thread.get("seed_document_id", ""), thread.get("seed_document_id", ""))
            new_seed_cl  = claim_id_map.get(thread.get("seed_claim_id", ""), thread.get("seed_claim_id", ""))
            cur = con.execute(
                "INSERT INTO narrative_threads "
                "(seed_claim_id, seed_document_id, source_tag, thread_summary, "
                " actor_count, document_span, temporal_span_days, node_count, "
                " coherence_score, manipulation_flag, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    new_seed_cl,
                    new_seed_doc,
                    source_tag,
                    thread.get("thread_summary", ""),
                    thread.get("actor_count", 0),
                    thread.get("document_span", 0),
                    thread.get("temporal_span_days", 0),
                    thread.get("node_count", 0),
                    thread.get("coherence_score", 0.0),
                    thread.get("manipulation_flag", 0),
                    thread.get("created_at", datetime.now().isoformat()),
                )
            )
            thread_id_map[old_tid] = cur.lastrowid

        for node in prov_nodes:
            new_tid   = thread_id_map.get(node.get("thread_id", 0), node.get("thread_id", 0))
            new_cl    = claim_id_map.get(node.get("claim_id", ""), node.get("claim_id", ""))
            new_doc   = doc_id_map.get(str(node.get("document_id", "")), str(node.get("document_id", "")))
            try:
                con.execute(
                    "INSERT INTO narrative_provenance_nodes "
                    "(thread_id, claim_id, document_id, generation, mutation_score, "
                    " downstream_reach, is_seed, mutation_flagged, created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        new_tid, new_cl, new_doc,
                        node.get("generation", 0),
                        node.get("mutation_score", 0.0),
                        node.get("downstream_reach", 0),
                        node.get("is_seed", 0),
                        node.get("mutation_flagged", 0),
                        node.get("created_at", datetime.now().isoformat()),
                    )
                )
            except Exception:
                pass

        for edge in prov_edges:
            new_tid    = thread_id_map.get(edge.get("thread_id", 0), edge.get("thread_id", 0))
            new_parent = claim_id_map.get(edge.get("parent_claim_id", ""), edge.get("parent_claim_id", ""))
            new_child  = claim_id_map.get(edge.get("child_claim_id", ""), edge.get("child_claim_id", ""))
            try:
                con.execute(
                    "INSERT INTO narrative_provenance_edges "
                    "(thread_id, parent_claim_id, child_claim_id, "
                    " similarity_score, generation_delta, created_at) "
                    "VALUES (?,?,?,?,?,?)",
                    (
                        new_tid, new_parent, new_child,
                        edge.get("similarity_score", 0.0),
                        edge.get("generation_delta", 1),
                        edge.get("created_at", datetime.now().isoformat()),
                    )
                )
            except Exception:
                pass

        for c in contradictions:
            try:
                con.execute(
                    "INSERT INTO narrative_contradictions_typed "
                    "(source_tag, claim_a_id, claim_b_id, contradiction_type, "
                    " actor_overlap, severity, document_a_id, document_b_id) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (
                        source_tag,
                        claim_id_map.get(c.get("claim_a_id", ""), c.get("claim_a_id", "")),
                        claim_id_map.get(c.get("claim_b_id", ""), c.get("claim_b_id", "")),
                        c.get("contradiction_type", "unknown"),
                        c.get("actor_overlap", "[]"),
                        c.get("severity", 0.5),
                        doc_id_map.get(str(c.get("document_a_id", "")), str(c.get("document_a_id", ""))),
                        doc_id_map.get(str(c.get("document_b_id", "")), str(c.get("document_b_id", ""))),
                    )
                )
            except Exception:
                pass

        for b in brokers:
            try:
                con.execute(
                    "INSERT INTO narrative_brokers "
                    "(source_tag, actor_name, threads_connected, betweenness, "
                    " in_degree, out_degree, is_amplifier, is_originator, "
                    " is_suppressor, broker_type) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        source_tag,
                        b.get("actor_name", ""),
                        b.get("threads_connected", 0),
                        b.get("betweenness", 0.0),
                        b.get("in_degree", 0),
                        b.get("out_degree", 0),
                        b.get("is_amplifier", 0),
                        b.get("is_originator", 0),
                        b.get("is_suppressor", 0),
                        b.get("broker_type", "relay"),
                    )
                )
            except Exception:
                pass

        return {
            "documents_imported":   len(doc_id_map),
            "claims_imported":      len(claim_id_map),
            "entities_imported":    len(entity_id_map),
            "threads_imported":     len(thread_id_map),
            "imported_source_tag":  source_tag,
        }
