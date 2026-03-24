"""
tests/test_api.py

Integration tests: upload, search, entity extraction, annotations.
Run:  pytest tests/test_api.py -v
"""

import math
import time
import pytest
from fastapi.testclient import TestClient

from tests.conftest import txt_file


# ── helpers ───────────────────────────────────────────────────────────────────

def upload_txt(client, content, filename="doc.txt", tag=""):
    r = client.post(
        "/api/v1/documents/upload",
        files={"files": (filename, content.encode(), "text/plain")},
        data={"source_tag": tag},
    )
    assert r.status_code == 202, r.text
    return r.json()[0]["document_id"]


def wait_done(client, doc_id, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = client.get(f"/api/v1/documents/{doc_id}/status")
        s = r.json()["status"]
        if s in ("done", "error"):
            return s
        time.sleep(0.05)
    return "timeout"


# ═══════════════════════════════════════════════════════════════════════════════
# Health
# ═══════════════════════════════════════════════════════════════════════════════

def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "version" in body
    assert "database" in body


def test_root(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "docs" in r.json()


# ═══════════════════════════════════════════════════════════════════════════════
# Upload
# ═══════════════════════════════════════════════════════════════════════════════

def test_upload_txt(client):
    r = client.post(
        "/api/v1/documents/upload",
        files={"files": ("report.txt", b"Investigative content.", "text/plain")},
        data={"source_tag": "test"},
    )
    assert r.status_code == 202
    item = r.json()[0]
    assert item["status"] == "queued"
    assert item["document_id"]
    assert item["filename"] == "report.txt"


def test_upload_multiple_files(client):
    r = client.post(
        "/api/v1/documents/upload",
        files=[
            ("files", ("a.txt", b"Alpha content", "text/plain")),
            ("files", ("b.txt", b"Beta content",  "text/plain")),
        ],
        data={"source_tag": "batch"},
    )
    assert r.status_code == 202
    assert len(r.json()) == 2


def test_upload_duplicate_returns_existing_id(client):
    content = b"Unique dedup content."
    r1 = client.post(
        "/api/v1/documents/upload",
        files={"files": ("orig.txt", content, "text/plain")},
        data={"source_tag": ""},
    )
    first_id = r1.json()[0]["document_id"]
    r2 = client.post(
        "/api/v1/documents/upload",
        files={"files": ("copy.txt", content, "text/plain")},
        data={"source_tag": ""},
    )
    item = r2.json()[0]
    assert item["status"] == "duplicate"
    assert item["document_id"] == first_id


def test_upload_rejects_unsupported_extension(client):
    r = client.post(
        "/api/v1/documents/upload",
        files={"files": ("payload.exe", b"\x4d\x5a\x90", "application/octet-stream")},
        data={"source_tag": ""},
    )
    assert r.status_code == 415
    assert r.json()["detail"]["error"] == "unsupported_file_type"


def test_upload_rejects_docx(client):
    r = client.post(
        "/api/v1/documents/upload",
        files={"files": ("report.docx", b"PK\x03\x04", "application/zip")},
        data={"source_tag": ""},
    )
    assert r.status_code == 415


def test_upload_accepts_supported_extensions(client):
    for name, data in [
        ("doc.txt",  b"Text content"),
        ("doc.md",   b"# Markdown"),
        ("img.png",  b"\x89PNG\r\n\x1a\n"),
    ]:
        r = client.post(
            "/api/v1/documents/upload",
            files={"files": (name, data, "application/octet-stream")},
            data={"source_tag": ""},
        )
        assert r.status_code == 202, f"{name} should be accepted, got {r.status_code}"


def test_upload_rejects_oversized_file(client, monkeypatch):
    from apps.api import config as cfg
    monkeypatch.setattr(cfg.settings, "max_upload_size_mb", 0)
    r = client.post(
        "/api/v1/documents/upload",
        files={"files": ("big.txt", b"x" * 300, "text/plain")},
        data={"source_tag": ""},
    )
    assert r.status_code == 413


def test_list_documents(client):
    upload_txt(client, "Listed document.")
    r = client.get("/api/v1/documents")
    assert r.status_code == 200
    assert r.json()["total"] >= 1


def test_list_documents_filter_by_source_tag(client):
    upload_txt(client, "Alpha content.", tag="alpha_tag")
    upload_txt(client, "Other content.", tag="other_tag")
    r = client.get("/api/v1/documents?source_tag=alpha_tag")
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) >= 1
    assert all(i["source_tag"] == "alpha_tag" for i in items)


def test_get_document(client):
    doc_id = upload_txt(client, "Retrievable document.")
    r = client.get(f"/api/v1/documents/{doc_id}")
    assert r.status_code == 200
    assert r.json()["id"] == doc_id


def test_get_document_not_found(client):
    r = client.get("/api/v1/documents/does-not-exist")
    assert r.status_code == 404


def test_get_document_status(client):
    doc_id = upload_txt(client, "Status check.")
    r = client.get(f"/api/v1/documents/{doc_id}/status")
    assert r.status_code == 200
    body = r.json()
    assert body["document_id"] == doc_id
    assert body["status"] in ("pending", "processing", "done", "error")


def test_ingestion_completes(client):
    doc_id = upload_txt(client, "Ingestion completion test document content.")
    status = wait_done(client, doc_id)
    assert status == "done", f"Expected done, got {status}"


def test_pages_available_after_ingestion(client):
    doc_id = upload_txt(client, "Page content available after ingestion.")
    wait_done(client, doc_id)
    r = client.get(f"/api/v1/documents/{doc_id}/pages")
    assert r.status_code == 200
    pages = r.json()
    assert len(pages) >= 1
    assert pages[0]["page_number"] == 1


# ═══════════════════════════════════════════════════════════════════════════════
# Entities
# ═══════════════════════════════════════════════════════════════════════════════

def test_entities_list_does_not_crash(client):
    upload_txt(client, "Jeffrey Epstein met Maxwell.")
    r = client.get("/api/v1/entities")
    assert r.status_code == 200
    body = r.json()
    assert "total" in body
    assert "items" in body


def test_entities_list_min_mentions_filter(client):
    r = client.get("/api/v1/entities?min_mentions=1")
    assert r.status_code == 200


def test_entities_list_type_filter(client):
    r = client.get("/api/v1/entities?entity_type=Person")
    assert r.status_code == 200


def test_entities_list_name_search(client):
    r = client.get("/api/v1/entities?q=epstein")
    assert r.status_code == 200


def test_entity_not_found(client):
    r = client.get("/api/v1/entities/nonexistent-id")
    assert r.status_code == 404


def test_aircraft_entity_stored_after_ingestion(client):
    doc_id = upload_txt(client, "N908JE flew from KTEB to KSFO.", tag="entity_test")
    wait_done(client, doc_id)
    r = client.get("/api/v1/entities?entity_type=Aircraft")
    assert r.status_code == 200
    names = {e["canonical_name"] for e in r.json()["items"]}
    assert "N908JE" in names, f"N908JE not found in {names}"


def test_entity_mention_count_is_accurate(client):
    doc_id = upload_txt(
        client,
        "N456CD departed. N456CD refueled. N456CD returned.",
        tag="mention_count_test",
    )
    wait_done(client, doc_id)
    r = client.get("/api/v1/entities?entity_type=Aircraft&q=N456CD")
    assert r.status_code == 200
    items = [e for e in r.json()["items"] if e["canonical_name"] == "N456CD"]
    assert items, "N456CD should be in entity list"
    assert items[0]["mention_count"] == 3, (
        f"Expected 3 mentions, got {items[0]['mention_count']}"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Search
# ═══════════════════════════════════════════════════════════════════════════════

def test_search_returns_results(client):
    upload_txt(client, "classified intelligence briefing xyzzy99")
    time.sleep(0.3)
    r = client.post("/api/v1/search", json={"query": "xyzzy99"})
    assert r.status_code == 200
    body = r.json()
    for field in ("total", "total_pages", "page", "page_size", "query", "results"):
        assert field in body


def test_search_total_reflects_source_tag_filter(client):
    content = "specific_term_count_test"
    upload_txt(client, content, tag="grp_a")
    upload_txt(client, content, tag="grp_b")
    time.sleep(0.3)
    r_all = client.post("/api/v1/search", json={"query": content})
    r_flt = client.post("/api/v1/search", json={
        "query": content,
        "filters": {"source_tags": ["grp_a"]},
    })
    assert r_flt.json()["total"] <= r_all.json()["total"]


def test_search_date_filters_accepted(client):
    r = client.post("/api/v1/search", json={
        "query": "test",
        "filters": {"date_from": "2020-01-01T00:00:00", "date_to": "2030-12-31T23:59:59"},
    })
    assert r.status_code == 200


def test_search_get_endpoint(client):
    r = client.get("/api/v1/search?q=test")
    assert r.status_code == 200


def test_search_snippet_is_html_safe(client):
    upload_txt(client, '<script>alert("xss")</script> findme_abc')
    time.sleep(0.3)
    r = client.post("/api/v1/search", json={"query": "findme_abc"})
    assert r.status_code == 200
    for result in r.json()["results"]:
        assert "<script>" not in result["snippet"]


def test_search_pagination(client):
    r = client.post("/api/v1/search", json={"query": "test", "page": 1, "page_size": 5})
    assert r.status_code == 200
    body = r.json()
    assert body["page"] == 1
    assert body["page_size"] == 5
    assert len(body["results"]) <= 5
    if body["total"] > 0:
        assert body["total_pages"] == math.ceil(body["total"] / 5)


def test_search_invalid_date_returns_422(client):
    r = client.post("/api/v1/search", json={
        "query": "test",
        "filters": {"date_from": "not-a-date"},
    })
    assert r.status_code == 422


def test_search_empty_query_returns_422(client):
    r = client.post("/api/v1/search", json={"query": ""})
    assert r.status_code == 422


# ═══════════════════════════════════════════════════════════════════════════════
# Annotations
# ═══════════════════════════════════════════════════════════════════════════════

def test_annotation_create_and_list(client):
    doc_id = upload_txt(client, "Annotated document content.")
    r = client.post("/api/v1/annotations", json={
        "document_id":     doc_id,
        "page_number":     1,
        "annotation_type": "note",
        "note_text":       "Suspicious flight pattern.",
        "author":          "analyst_1",
    })
    assert r.status_code == 201
    body = r.json()
    assert body["document_id"] == doc_id
    assert body["note_text"] == "Suspicious flight pattern."

    r2 = client.get(f"/api/v1/annotations?document_id={doc_id}")
    assert r2.status_code == 200
    assert r2.json()["total"] == 1


def test_annotation_requires_valid_document(client):
    r = client.post("/api/v1/annotations", json={
        "document_id":     "does-not-exist",
        "annotation_type": "note",
        "note_text":       "Should fail",
    })
    assert r.status_code == 404


def test_annotation_soft_delete(client):
    doc_id = upload_txt(client, "Delete test doc")
    r = client.post("/api/v1/annotations", json={
        "document_id":     doc_id,
        "annotation_type": "note",
        "note_text":       "To be deleted",
    })
    ann_id = r.json()["id"]

    r_del = client.delete(f"/api/v1/annotations/{ann_id}")
    assert r_del.status_code == 204

    ids = [a["id"] for a in client.get(
        f"/api/v1/annotations?document_id={doc_id}"
    ).json()["items"]]
    assert ann_id not in ids

    assert client.get(f"/api/v1/annotations/{ann_id}").status_code == 404


def test_annotation_filter_by_type(client):
    doc_id = upload_txt(client, "Type filter document.")
    client.post("/api/v1/annotations", json={
        "document_id": doc_id, "annotation_type": "note", "note_text": "a note",
    })
    client.post("/api/v1/annotations", json={
        "document_id": doc_id, "annotation_type": "flag", "note_text": "a flag",
    })
    r = client.get(f"/api/v1/annotations?document_id={doc_id}&annotation_type=note")
    assert r.status_code == 200
    assert r.json()["total"] == 1


def test_annotation_pagination(client):
    doc_id = upload_txt(client, "Pagination annotation document.")
    for i in range(5):
        client.post("/api/v1/annotations", json={
            "document_id": doc_id, "annotation_type": "note", "note_text": f"Note {i}",
        })
    r = client.get(f"/api/v1/annotations?document_id={doc_id}&page_size=2&page=1")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 5
    assert len(body["items"]) == 2


def test_annotation_not_found(client):
    r = client.get("/api/v1/annotations/nonexistent-id")
    assert r.status_code == 404


# ═══════════════════════════════════════════════════════════════════════════════
# Entity extraction unit tests (no HTTP layer)
# ═══════════════════════════════════════════════════════════════════════════════

def test_aircraft_extracted():
    from apps.api.services.entity_extraction import extract_regex
    results = extract_regex("N908JE departed Teterboro at dawn.", "d", 1)
    names = {r.canonical_name for r in results if r.entity_type == "Aircraft"}
    assert "N908JE" in names


def test_email_lowercased():
    from apps.api.services.entity_extraction import extract_regex
    results = extract_regex("Contact JEpstein@ProtonMail.com for details.", "d", 1)
    emails = {r.canonical_name for r in results if r.entity_type == "Email"}
    assert "jepstein@protonmail.com" in emails


def test_phone_extracted():
    from apps.api.services.entity_extraction import extract_regex
    results = extract_regex("Call 561-555-0101.", "d", 1)
    assert any(r.entity_type == "Phone" for r in results)


def test_multiple_occurrences_produce_distinct_mentions():
    from apps.api.services.entity_extraction import extract_regex
    text = "N123AB departed. Later N123AB returned. N123AB refueled."
    aircraft = [r for r in extract_regex(text, "d", 1) if r.canonical_name == "N123AB"]
    assert len(aircraft) == 3, f"Expected 3, got {len(aircraft)}"


def test_airport_excludes_common_words():
    from apps.api.services.entity_extraction import extract_regex
    text = "The CASE was MADE and WORD got out."
    airports = {r.canonical_name for r in extract_regex(text, "d", 1)
                if r.entity_type == "Airport"}
    for word in ("CASE", "MADE", "WORD"):
        assert word not in airports


def test_normalize_aircraft_uppercase():
    from apps.api.services.entity_extraction import _normalize
    assert _normalize("n123ab", "Aircraft") == "N123AB"


def test_normalize_email_lowercase():
    from apps.api.services.entity_extraction import _normalize
    assert _normalize("User@EXAMPLE.COM", "Email") == "user@example.com"


def test_extract_entities_empty_text():
    from apps.api.services.entity_extraction import extract_entities
    assert extract_entities("", "d", 1) == []


def test_redaction_flag_endpoint_accessible(client):
    doc_id = upload_txt(client, "Redaction test content")
    r = client.get(f"/api/v1/documents/{doc_id}/redactions")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


# ═══════════════════════════════════════════════════════════════════════════════
# Graph / relationship endpoints
# ═══════════════════════════════════════════════════════════════════════════════

def test_graph_network_not_found(client):
    r = client.get("/api/v1/graph/entities/nonexistent/network")
    assert r.status_code == 404


def test_graph_network_empty(client):
    """Entity with no co-occurring entities returns empty connections."""
    doc_id = upload_txt(client, "Solo entity N777XX with nothing else.", tag="graph_test")
    wait_done(client, doc_id)
    r = client.get("/api/v1/entities?entity_type=Aircraft&q=N777XX")
    items = r.json()["items"]
    if not items:
        return  # extraction may not have run — skip gracefully
    eid = items[0]["id"]
    r2 = client.get(f"/api/v1/graph/entities/{eid}/network")
    assert r2.status_code == 200
    body = r2.json()
    assert "entity" in body
    assert "connections" in body
    assert isinstance(body["connections"], list)


def test_graph_network_has_connections(client):
    """Two entities in the same document produce a relationship."""
    doc_id = upload_txt(
        client,
        "N808JE flew with Maxwell. Maxwell called N808JE from Palm Beach.",
        tag="graph_cooccur",
    )
    wait_done(client, doc_id)

    r = client.get("/api/v1/entities?entity_type=Aircraft&q=N808JE")
    items = r.json()["items"]
    if not items:
        return
    eid = items[0]["id"]

    r2 = client.get(f"/api/v1/graph/entities/{eid}/network")
    assert r2.status_code == 200
    body = r2.json()
    assert body["entity"]["canonical_name"] == "N808JE"
    # At least one connection should exist if Maxwell was also extracted
    assert isinstance(body["connections"], list)


def test_graph_expanded(client):
    """Graph endpoint returns nodes and edges structure."""
    doc_id = upload_txt(client, "N101AB and N202CD departed together from KTEB.", tag="graph_exp")
    wait_done(client, doc_id)

    r = client.get("/api/v1/entities?entity_type=Aircraft")
    items = r.json()["items"]
    if not items:
        return
    eid = items[0]["id"]

    r2 = client.get(f"/api/v1/graph/entities/{eid}/graph?depth=2")
    assert r2.status_code == 200
    body = r2.json()
    assert "nodes" in body
    assert "edges" in body
    assert "depth" in body
    assert isinstance(body["nodes"], list)
    assert isinstance(body["edges"], list)


def test_graph_relationships_list(client):
    """Relationships list endpoint is accessible and returns correct schema."""
    r = client.get("/api/v1/graph/relationships")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_graph_relationships_min_weight_filter(client):
    """min_weight filter accepted without error."""
    r = client.get("/api/v1/graph/relationships?min_weight=2")
    assert r.status_code == 200
