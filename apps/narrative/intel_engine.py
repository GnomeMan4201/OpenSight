from __future__ import annotations

import re
import sqlite3
from difflib import SequenceMatcher


NAME_RE = re.compile(r"\b([A-Z][a-z]{1,20}\s+[A-Z][a-z]{1,20})\b")
TITLE_NAME_RE = re.compile(r"\b(?:AUSA|Dr\.|Doctor|Mr\.|Mrs\.|Ms\.|Judge|Agent|SA|SAL)\s+([A-Z][a-z]{1,20}\s+[A-Z][a-z]{1,20})\b")
DATE_RE_1 = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b")
DATE_RE_2 = re.compile(r"\b(19\d{2}|20\d{2})\b")
LOC_RE_1 = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2})\b")
LOC_RE_2 = re.compile(r"\b(Palm Beach|Miami|Florida|New York|Manhattan)\b", re.I)


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    value = value.lower().strip()
    value = re.sub(r"[\r\n\t]+", " ", value)
    value = re.sub(r"[^a-z0-9\s:/._,\-]", "", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, normalize_text(a), normalize_text(b)).ratio()


def extract_sentences(text: str) -> list[str]:
    if not text:
        return []
    parts = re.split(r"(?<=[\.\!\?])\s+|\n+", text)
    return [p.strip() for p in parts if p.strip()]


def is_ocr_junk(text: str, case_mode: str) -> bool:
    if not text:
        return True

    t = text.strip()
    if len(t) < 20:
        return True

    nt = normalize_text(t)

    if case_mode == "foia":
        redaction_hits = len(re.findall(r"\bb[367][a-zA-Z0-9]?\b", t, flags=re.I))
        if redaction_hits >= 12:
            return True

        numeric_lines = 0
        lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
        if lines:
            for ln in lines[:40]:
                if re.fullmatch(r"[\d\s\-~.,]+", ln):
                    numeric_lines += 1
            if numeric_lines >= 8:
                return True

        boilerplate_markers = [
            "deleted page information sheet",
            "payment request",
            "foi/pa",
            "civil action#",
            "no duplication fee",
            "for this page",
            "federal bureau of investigation",
            "draft type",
            "request type",
            "payment type",
            "social security number",
            "bureau name",
            "vendor number",
            "group number",
            "obligation processing",
        ]
        hits = sum(1 for m in boilerplate_markers if m in nt)
        if hits >= 2:
            return True

    else:
        # narrative/legal-doc mode: keep much more text, only reject obvious garbage
        if len(nt.split()) < 6:
            return True
        if re.fullmatch(r"[\d\s\-~.,]+", t):
            return True
        if nt.count("b3") + nt.count("b6") + nt.count("b7") >= 8:
            return True

    alpha = sum(ch.isalpha() for ch in t)
    digits = sum(ch.isdigit() for ch in t)
    if alpha > 0 and digits > alpha * 1.5:
        return True

    return False


def clean_claim_text(text: str, case_mode: str) -> str:
    if not text:
        return ""

    lines = [ln.strip() for ln in text.splitlines()]
    kept = []

    skip_patterns = [
        r"^page\s+\d+",
        r"^b[367][a-z0-9]?\b",
        r"^federal bureau of investigation$",
        r"^foi/pa$",
        r"^deleted page information sheet$",
        r"^payment request$",
        r"^civil action#",
        r"^total deleted page",
        r"^x no duplication fee x$",
        r"^xx deleted page",
        r"^bureau name:",
        r"^social security number:",
        r"^request type:",
        r"^payment type:",
        r"^vendor number:",
        r"^group number:",
    ]

    for ln in lines:
        if not ln:
            continue
        n = normalize_text(ln)
        if case_mode == "foia" and any(re.match(p, n) for p in skip_patterns):
            continue
        if re.fullmatch(r"[\d\s\-~.,]+", ln):
            continue
        if len(n) <= 2:
            continue
        kept.append(ln)

    text = " ".join(kept)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:700 if case_mode == "case" else 500]


def extract_date(text: str) -> str | None:
    if not text:
        return None

    m = DATE_RE_1.search(text)
    if m:
        raw = m.group(1)
        parts = raw.split("/")
        if len(parts) == 3:
            mm, dd, yy = parts
            if len(yy) == 2:
                yy = "20" + yy if int(yy) < 40 else "19" + yy
            try:
                return f"{int(yy):04d}-{int(mm):02d}-{int(dd):02d}"
            except Exception:
                pass

    m = DATE_RE_2.search(text)
    if m:
        return f"{m.group(1)}-01-01"

    return None


def extract_location(text: str) -> str:
    if not text:
        return ""

    m = LOC_RE_1.search(text)
    if m:
        return m.group(1)

    m = LOC_RE_2.search(text)
    if m:
        return m.group(1)

    return ""


def infer_actor_regex(text: str, case_mode: str) -> str:
    if not text:
        return ""

    preferred = [
        "Jeffrey Epstein",
        "Ghislaine Maxwell",
        "Antonia Barnes",
        "Richard Gardner",
    ]
    for name in preferred:
        if name in text:
            return name

    m = TITLE_NAME_RE.search(text)
    if m:
        return m.group(1)

    matches = NAME_RE.findall(text)
    if not matches:
        return ""

    bad_phrases = {
        "Federal Bureau",
        "Deleted Page",
        "Page Information",
        "Civil Action",
        "Future Perspectives",
        "Introduction Context",
        "Historical And",
        "Comprehensive Summary",
        "Parental Alienation",
        "Computational Analysis",
        "Request Type",
        "Payment Type",
        "Total Deleted",
        "Child Prostitution",
        "No Duplication",
        "Social Security",
        "Drug Related",
        "Approved By",
        "Bureau Name",
        "Page Xx",
        "Ne Ne",
        "Ww Ww",
        "Page Page",
        "Drafted By",
        "Advance Balance",
        "Certified Copies",
        "Confidential Date",
        "Miami From",
        "Bio Pe",
        "Palm Beach",
        "State Florida",
        "Tae Tay",
        "The Potential",
        "Dynamics Introduction",
        "Issues This",
        "Healthy Co",
        "Computational Analysis",
        "Future Perspectives",
        "Scientific Critique",
        "Historical And",
        "Parental Alienation",
    }

    cleaned = []
    for m in matches:
        if m in bad_phrases:
            continue
        if len(m.split()) != 2:
            continue
        if m.split()[0] in {"The", "This", "That", "These", "Those"}:
            continue
        cleaned.append(m)

    if not cleaned:
        return ""

    if case_mode == "case":
        sents = extract_sentences(text)
        contextual = []
        for s in sents[:4]:
            ns = normalize_text(s)
            for m in cleaned:
                nm = normalize_text(m)
                if nm in ns and any(k in ns for k in [
                    "psychiatrist",
                    "judge",
                    "doctor",
                    "dr.",
                    "witness",
                    "parent",
                    "child",
                    "court",
                    "reported",
                    "stated",
                    "described",
                    "introduced by",
                    "according to",
                ]):
                    contextual.append(m)
        if contextual:
            cleaned = contextual

    counts = {}
    for m in cleaned:
        counts[m] = counts.get(m, 0) + 1
    return sorted(counts.items(), key=lambda x: (-x[1], x[0]))[0][0]


class NarrativeIntelEngine:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _table_exists(self, conn, name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=?",
            (name,),
        ).fetchone()
        return row is not None

    def _case_mode(self, source_tag: str | None) -> str:
        if source_tag == "pascal":
            return "case"
        return "foia"

    def _ensure_schema(self, conn):
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS narrative_canonical_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT NOT NULL UNIQUE,
                event_type TEXT,
                normalized_date TEXT,
                location_text TEXT,
                primary_actor TEXT,
                secondary_actor TEXT,
                object_text TEXT,
                canonical_summary TEXT,
                support_count INTEGER DEFAULT 0,
                contradiction_count INTEGER DEFAULT 0,
                confidence REAL DEFAULT 0.0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS narrative_event_claim_map (
                event_id INTEGER NOT NULL,
                claim_id TEXT NOT NULL,
                role TEXT DEFAULT 'support',
                PRIMARY KEY (event_id, claim_id)
            );

            CREATE TABLE IF NOT EXISTS narrative_claim_lineage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                src_claim_id TEXT NOT NULL,
                dst_claim_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                score REAL DEFAULT 0.0,
                rationale TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS narrative_temporal_anomalies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor_name TEXT NOT NULL,
                event_a_id INTEGER NOT NULL,
                event_b_id INTEGER NOT NULL,
                anomaly_type TEXT NOT NULL,
                score REAL DEFAULT 0.0,
                rationale TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS narrative_claim_entity_links (
                claim_id TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                canonical_name TEXT NOT NULL,
                entity_type TEXT,
                link_method TEXT DEFAULT 'name_match',
                score REAL DEFAULT 0.0,
                PRIMARY KEY (claim_id, entity_id)
            );
            """
        )

    def _clear_outputs(self, conn):
        conn.execute("DELETE FROM narrative_claim_lineage")
        conn.execute("DELETE FROM narrative_event_claim_map")
        conn.execute("DELETE FROM narrative_temporal_anomalies")
        conn.execute("DELETE FROM narrative_claim_entity_links")
        conn.execute("DELETE FROM narrative_canonical_events")

    def _fetch_claim_rows(self, conn, source_tag: str | None):
        if not self._table_exists(conn, "claims"):
            return []

        cols = [r["name"] for r in conn.execute("PRAGMA table_info(claims)").fetchall()]

        def has(name: str) -> bool:
            return name in cols

        id_col = "id" if has("id") else cols[0]

        if has("document_id") and self._table_exists(conn, "documents"):
            sql = f"""
                SELECT c.*, d.source_tag
                FROM claims c
                LEFT JOIN documents d ON d.id = c.document_id
                WHERE (? IS NULL OR d.source_tag = ?)
                ORDER BY c.{id_col}
            """
            return conn.execute(sql, (source_tag, source_tag)).fetchall()

        return conn.execute(f"SELECT * FROM claims ORDER BY {id_col}").fetchall()

    def _infer_actor(self, cleaned_text: str, case_mode: str) -> str:
        return infer_actor_regex(cleaned_text, case_mode)

    def _event_key(self, subject, predicate, obj, normalized_date, location_text):
        return "|".join([
            normalize_text(subject),
            normalize_text(predicate),
            normalize_text(obj)[:140],
            str(normalized_date or ""),
            normalize_text(location_text),
        ])

    def _link_claim_entities(self, conn, kept_rows, case_mode: str):
        if not self._table_exists(conn, "entities"):
            return

        cols = [r["name"] for r in conn.execute("PRAGMA table_info(entities)").fetchall()]
        if not {"id", "canonical_name", "entity_type"}.issubset(set(cols)):
            return

        entities = conn.execute("""
            SELECT id, canonical_name, entity_type, confidence, mention_count
            FROM entities
            WHERE length(trim(canonical_name)) >= 4
            ORDER BY mention_count DESC, confidence DESC, canonical_name
        """).fetchall()

        blocked = {
            "Comprehensive Summary",
            "Parental Alienation Dynamics",
            "Issues This",
            "Parental Alienation Syndrome",
            "Scientific Critique",
            "Harmful Parental Dynamics",
            "Statistical Gaps Quantifying",
            "Multifaceted Impact Children",
            "Dynamics Identifying",
            "The Potential",
        }

        usable = []
        for e in entities:
            cname = e["canonical_name"] or ""
            etype = (e["entity_type"] or "").lower()
            if cname in blocked:
                continue
            if case_mode == "case":
                if etype != "person":
                    continue
            else:
                if etype not in ("person", "org", "organization", "gpe", "location", "loc"):
                    continue
            usable.append((e["id"], cname, e["entity_type"]))

        for row in kept_rows:
            claim_id = row["id"]
            text = clean_claim_text(str(row["claim_text"] or ""), case_mode)
            nt = normalize_text(text)

            for entity_id, cname, etype in usable:
                nc = normalize_text(cname)
                if not nc or len(nc) < 4:
                    continue
                if nc in nt:
                    score = 1.0 if str(etype).lower() == "person" else 0.8
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO narrative_claim_entity_links
                        (claim_id, entity_id, canonical_name, entity_type, link_method, score)
                        VALUES (?, ?, ?, ?, 'name_match', ?)
                        """,
                        (claim_id, entity_id, cname, etype, score),
                    )

    def _build_lineage(self, conn, claims, case_mode: str):
        filtered = []
        for row in claims:
            txt = clean_claim_text(str(row["claim_text"] or ""), case_mode)
            if is_ocr_junk(txt, case_mode):
                continue
            actor = self._infer_actor(txt, case_mode)
            filtered.append((row, txt, actor))

        for i in range(len(filtered)):
            a, a_text, a_actor = filtered[i]
            a_id = a["id"]
            a_doc = a["document_id"]
            a_page = a["page_number"]
            a_type = str(a["claim_type"] or "")
            a_norm = normalize_text(a_text)

            for j in range(i + 1, len(filtered)):
                b, b_text, b_actor = filtered[j]
                b_id = b["id"]
                b_doc = b["document_id"]
                b_page = b["page_number"]
                b_type = str(b["claim_type"] or "")
                b_norm = normalize_text(b_text)

                sim = similarity(a_norm, b_norm)
                same_actor = bool(a_actor and b_actor and normalize_text(a_actor) == normalize_text(b_actor))
                same_type = a_type == b_type

                if sim >= 0.93 and a_doc != b_doc:
                    rel = "repeats"
                    score = sim
                    why = "near-identical claim text across documents"
                elif sim >= 0.82 and (a_doc != b_doc or a_page != b_page) and same_type:
                    rel = "supports"
                    score = sim
                    why = "high textual similarity and same claim_type"
                elif sim >= 0.72 and same_actor:
                    rel = "derives_from"
                    score = sim
                    why = "same resolved actor and similar text"
                else:
                    continue

                conn.execute(
                    """
                    INSERT INTO narrative_claim_lineage
                    (src_claim_id, dst_claim_id, relation_type, score, rationale)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (a_id, b_id, rel, score, why),
                )

    def _build_temporal_anomalies(self, conn):
        rows = conn.execute(
            """
            SELECT id, primary_actor, normalized_date, location_text
            FROM narrative_canonical_events
            WHERE coalesce(primary_actor,'') <> ''
              AND normalized_date IS NOT NULL
            ORDER BY primary_actor, normalized_date, id
            """
        ).fetchall()

        by_actor = {}
        for r in rows:
            by_actor.setdefault(r["primary_actor"], []).append(r)

        for actor, events in by_actor.items():
            for i in range(len(events)):
                a = events[i]
                for j in range(i + 1, len(events)):
                    b = events[j]
                    if a["normalized_date"] != b["normalized_date"]:
                        continue
                    la = normalize_text(a["location_text"] or "")
                    lb = normalize_text(b["location_text"] or "")
                    if la and lb and la != lb:
                        conn.execute(
                            """
                            INSERT INTO narrative_temporal_anomalies
                            (actor_name, event_a_id, event_b_id, anomaly_type, score, rationale)
                            VALUES (?, ?, ?, 'same_date_different_locations', ?, ?)
                            """,
                            (
                                actor,
                                a["id"],
                                b["id"],
                                0.8,
                                f"same actor has differing locations on same date: {a['location_text']} vs {b['location_text']}",
                            ),
                        )

    def build(self, source_tag: str | None = None):
        conn = self.connect()
        try:
            case_mode = self._case_mode(source_tag)
            self._ensure_schema(conn)
            self._clear_outputs(conn)

            rows = self._fetch_claim_rows(conn, source_tag)

            kept_rows = []
            for row in rows:
                raw_text = str(row["claim_text"] or "")
                if is_ocr_junk(raw_text, case_mode):
                    continue

                claim_id = row["id"]
                claim_type = str(row["claim_type"] or "statement")
                cleaned_text = clean_claim_text(raw_text, case_mode)
                normalized_date = extract_date(cleaned_text)
                location_text = extract_location(cleaned_text)
                target = ""

                subject = self._infer_actor(cleaned_text, case_mode)
                obj = cleaned_text[:280]
                summary = cleaned_text[:280]

                if not summary:
                    continue

                event_key = self._event_key(subject, claim_type, obj, normalized_date, location_text)

                conn.execute(
                    """
                    INSERT INTO narrative_canonical_events (
                        event_key, event_type, normalized_date, location_text,
                        primary_actor, secondary_actor, object_text,
                        canonical_summary, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(event_key) DO UPDATE SET
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        event_key,
                        claim_type,
                        normalized_date if normalized_date else None,
                        location_text,
                        subject,
                        target,
                        obj,
                        summary,
                    ),
                )

                event_id = conn.execute(
                    "SELECT id FROM narrative_canonical_events WHERE event_key=?",
                    (event_key,),
                ).fetchone()["id"]

                conn.execute(
                    """
                    INSERT OR IGNORE INTO narrative_event_claim_map (event_id, claim_id, role)
                    VALUES (?, ?, 'support')
                    """,
                    (event_id, claim_id),
                )

                kept_rows.append(row)

            metrics = conn.execute(
                """
                SELECT
                    event_id,
                    SUM(CASE WHEN role='support' THEN 1 ELSE 0 END) AS supports,
                    SUM(CASE WHEN role='contradiction' THEN 1 ELSE 0 END) AS contradictions
                FROM narrative_event_claim_map
                GROUP BY event_id
                """
            ).fetchall()

            for r in metrics:
                supports = r["supports"] or 0
                contradictions = r["contradictions"] or 0
                confidence = supports / (supports + contradictions + 1)
                conn.execute(
                    """
                    UPDATE narrative_canonical_events
                    SET support_count=?,
                        contradiction_count=?,
                        confidence=?,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (supports, contradictions, confidence, r["event_id"]),
                )

            self._link_claim_entities(conn, kept_rows, case_mode)
            self._build_lineage(conn, kept_rows, case_mode)
            self._build_temporal_anomalies(conn)

            conn.commit()

            return {
                "status": "ok",
                "source_tag": source_tag,
                "case_mode": case_mode,
                "claims_seen": len(rows),
                "claims_kept": len(kept_rows),
                "events": conn.execute("SELECT COUNT(*) FROM narrative_canonical_events").fetchone()[0],
                "claim_event_links": conn.execute("SELECT COUNT(*) FROM narrative_event_claim_map").fetchone()[0],
                "lineage_edges": conn.execute("SELECT COUNT(*) FROM narrative_claim_lineage").fetchone()[0],
                "temporal_anomalies": conn.execute("SELECT COUNT(*) FROM narrative_temporal_anomalies").fetchone()[0],
                "claim_entity_links": conn.execute("SELECT COUNT(*) FROM narrative_claim_entity_links").fetchone()[0],
            }
        finally:
            conn.close()

    def summary(self):
        conn = self.connect()
        try:
            self._ensure_schema(conn)
            return {
                "events": conn.execute("SELECT COUNT(*) FROM narrative_canonical_events").fetchone()[0],
                "claim_event_links": conn.execute("SELECT COUNT(*) FROM narrative_event_claim_map").fetchone()[0],
                "lineage_edges": conn.execute("SELECT COUNT(*) FROM narrative_claim_lineage").fetchone()[0],
                "temporal_anomalies": conn.execute("SELECT COUNT(*) FROM narrative_temporal_anomalies").fetchone()[0],
                "claim_entity_links": conn.execute("SELECT COUNT(*) FROM narrative_claim_entity_links").fetchone()[0],
            }
        finally:
            conn.close()
