"""
apps/narrative/engine.py
Narrative Intelligence Engine — full implementation.
"""
from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

from .models import ClaimExtraction, ClaimExtractionBatch
from .schema import SCHEMA_SQL

STOPWORDS = {
    "the","a","an","and","or","to","of","in","on","at","for","from",
    "with","by","that","this","is","was","were","be","been","as","it",
    "he","she","they","them","his","her","their"
}


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    value = value.lower().strip()
    value = re.sub(r"[\r\n\t]+", " ", value)
    value = re.sub(r"[^a-z0-9\s:/.-]", "", value)
    return re.sub(r"\s+", " ", value).strip()


def token_set(value: str) -> set:
    return {t for t in normalize_text(value).split() if t and t not in STOPWORDS}


def similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, normalize_text(a), normalize_text(b)).ratio()


def jaccard(a: str, b: str) -> float:
    sa, sb = token_set(a), token_set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / max(1, len(sa | sb))


def try_parse_date(text: str) -> Optional[str]:
    text = (text or "").strip()
    if not text:
        return None
    patterns = [
        "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y",
        "%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%Y",
    ]
    for fmt in patterns:
        try:
            dt = datetime.strptime(text, fmt)
            return f"{dt.year:04d}-01-01" if fmt == "%Y" else dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    return f"{m.group(1)}-01-01" if m else None


def infer_event_type(predicate: str, claim_type: str) -> str:
    p = normalize_text(predicate)
    if any(k in p for k in ("met", "meeting", "visited")): return "meeting"
    if any(k in p for k in ("flew", "traveled", "flight")): return "travel"
    if any(k in p for k in ("paid", "transferred", "wired")): return "financial"
    if any(k in p for k in ("called", "emailed", "wrote")): return "communication"
    return claim_type if claim_type in ("allegation","denial","observation") else "statement"


@dataclass
class TextUnit:
    unit_id: str
    document_id: str
    page_number: int
    source_tag: str
    text: str


class NarrativeEngine:
    def __init__(
        self,
        db_path: str,
        ollama_url: str = "http://127.0.0.1:11434/api/generate",
        ollama_model: str = "phi3",
    ) -> None:
        self.db_path = db_path
        self.ollama_url = ollama_url
        self.ollama_model = ollama_model

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        # Apply schema idempotently
        for stmt in SCHEMA_SQL.split(";"):
            stmt = stmt.strip()
            if stmt:
                try:
                    conn.execute(stmt)
                except Exception:
                    pass
        conn.commit()
        return conn

    def fetch_text_units(
        self,
        conn: sqlite3.Connection,
        source_tag: Optional[str] = None,
        max_units: Optional[int] = None,
    ) -> List[TextUnit]:
        sql = """
        SELECT unit_id, document_id, page_number, source_tag, text
        FROM opensight_text_units
        WHERE (? IS NULL OR source_tag = ?)
          AND text IS NOT NULL AND length(trim(text)) > 50
        ORDER BY document_id, page_number
        """
        params: list = [source_tag, source_tag]
        if max_units:
            sql += " LIMIT ?"
            params.append(max_units)
        try:
            rows = conn.execute(sql, params).fetchall()
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("[NIE] fetch_text_units failed: %s", e)
            return []
        return [TextUnit(
            unit_id=str(r["unit_id"]),
            document_id=str(r["document_id"]),
            page_number=int(r["page_number"] or 0),
            source_tag=str(r["source_tag"] or ""),
            text=str(r["text"] or ""),
        ) for r in rows]

    def split_into_passages(self, text: str, max_chars: int = 1400) -> List[Tuple[int,int,str]]:
        text = re.sub(r"\n{2,}", "\n\n", text.strip())
        paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
        passages: List[Tuple[int,int,str]] = []
        cursor = 0
        for p in paragraphs:
            start = text.find(p, cursor)
            if start < 0: start = cursor
            end = start + len(p)
            cursor = end
            if len(p) <= max_chars:
                passages.append((start, end, p))
                continue
            words = p.split()
            current: list = []
            cur_start = start
            running = 0
            for w in words:
                projected = running + len(w) + 1
                if projected > max_chars and current:
                    chunk = " ".join(current)
                    chunk_end = cur_start + len(chunk)
                    passages.append((cur_start, chunk_end, chunk))
                    cur_start = chunk_end + 1
                    current = [w]
                    running = len(w)
                else:
                    current.append(w)
                    running = projected
            if current:
                chunk = " ".join(current)
                passages.append((cur_start, cur_start + len(chunk), chunk))
        return passages

    def build_extraction_prompt(self, text: str) -> str:
        return f"""You are an investigative extraction engine.
Extract structured claims from the passage below.
Return ONLY valid JSON with this exact schema:
{{"claims":[{{"subject":"","predicate":"","object":"","speaker":"","target":"","claim_type":"statement","polarity":"affirmed","certainty":"medium","event_time_text":"","location_text":"","summary":"","quote":"","confidence":0.5}}]}}
claim_type: statement|denial|allegation|observation|travel|meeting|financial|communication
polarity: affirmed|denied|uncertain
certainty: low|medium|high
Rules: Extract only concrete investigatively relevant claims. Prefer attributed claims. Up to 8 claims. If none, return {{"claims":[]}}.
PASSAGE:
{text}"""

    def extract_claims_ollama(self, text: str) -> List[ClaimExtraction]:
        prompt = self.build_extraction_prompt(text[:1400])
        payload = {
            "model": self.ollama_model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0.0, "num_predict": 800},
        }
        try:
            import urllib.request
            data = json.dumps(payload).encode()
            req = urllib.request.Request(
                self.ollama_url, data=data,
                headers={"Content-Type": "application/json"}, method="POST"
            )
            with urllib.request.urlopen(req, timeout=120) as resp:
                result = json.loads(resp.read())
            raw = result.get("response", "").strip()
            # Repair common issues
            raw = re.sub(r"```[a-z]*", "", raw).strip().rstrip("`").strip()
            if not raw.startswith("{"): 
                m = re.search(r"\{.*\}", raw, re.DOTALL)
                raw = m.group() if m else "{}"
            parsed = json.loads(raw)
            batch = ClaimExtractionBatch.model_validate(parsed)
            return batch.claims
        except Exception:
            return self.extract_claims_fallback(text)

    def extract_claims_fallback(self, text: str) -> List[ClaimExtraction]:
        claims: List[ClaimExtraction] = []
        patterns = [
            (r"according to ([A-Z][A-Za-z .'-]+),\s*(.+?)[\.;]", "statement"),
            (r"([A-Z][A-Za-z .'-]+) stated that (.+?)[\.;]", "statement"),
            (r"([A-Z][A-Za-z .'-]+) denied (.+?)[\.;]", "denial"),
            (r"([A-Z][A-Za-z .'-]+) met with ([A-Z][A-Za-z .'-]+)", "meeting"),
            (r"([A-Z][A-Za-z .'-]+) traveled to ([A-Z][A-Za-z ,'-]+)", "travel"),
        ]
        for pattern, ctype in patterns:
            for m in re.finditer(pattern, text):
                if ctype in ("statement",):
                    s = m.group(2).strip()
                    claims.append(ClaimExtraction(
                        subject=m.group(1), predicate="stated", object=s[:200],
                        speaker=m.group(1), claim_type="statement", polarity="affirmed",
                        certainty="medium", summary=s[:300], quote=m.group(0)[:240], confidence=0.35,
                    ))
                elif ctype == "denial":
                    claims.append(ClaimExtraction(
                        subject=m.group(1), predicate="denied", object=m.group(2).strip()[:200],
                        speaker=m.group(1), claim_type="denial", polarity="denied",
                        certainty="medium", summary=f"{m.group(1)} denied {m.group(2)[:160]}",
                        quote=m.group(0)[:240], confidence=0.35,
                    ))
                elif ctype == "meeting":
                    claims.append(ClaimExtraction(
                        subject=m.group(1), predicate="met_with", object=m.group(2),
                        target=m.group(2), claim_type="meeting", polarity="affirmed",
                        certainty="medium", summary=f"{m.group(1)} met with {m.group(2)}",
                        quote=m.group(0), confidence=0.35,
                    ))
                elif ctype == "travel":
                    claims.append(ClaimExtraction(
                        subject=m.group(1), predicate="traveled_to", object=m.group(2),
                        claim_type="travel", polarity="affirmed", certainty="medium",
                        location_text=m.group(2),
                        summary=f"{m.group(1)} traveled to {m.group(2)}",
                        quote=m.group(0), confidence=0.35,
                    ))
        return claims[:8]

    def insert_evidence_span(self, conn, unit, char_start, char_end, text) -> int:
        cur = conn.execute(
            "INSERT INTO narrative_evidence_spans (unit_id,document_id,page_number,source_tag,char_start,char_end,text) VALUES (?,?,?,?,?,?,?)",
            (unit.unit_id, unit.document_id, unit.page_number, unit.source_tag, char_start, char_end, text)
        )
        return int(cur.lastrowid)

    def insert_claim(self, conn, evidence_span_id: int, claim: ClaimExtraction) -> int:
        cur = conn.execute(
            """INSERT INTO narrative_claims
            (evidence_span_id,subject,predicate,object,speaker,target,claim_type,polarity,certainty,
             event_time_text,normalized_date,location_text,summary,quote,extractor_confidence,
             normalized_subject,normalized_predicate,normalized_object,raw_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (evidence_span_id, claim.subject, claim.predicate, claim.object, claim.speaker,
             claim.target, claim.claim_type, claim.polarity, claim.certainty,
             claim.event_time_text, try_parse_date(claim.event_time_text),
             claim.location_text, claim.summary, claim.quote, claim.confidence,
             normalize_text(claim.subject), normalize_text(claim.predicate),
             normalize_text(claim.object), json.dumps(claim.model_dump()))
        )
        return int(cur.lastrowid)

    def upsert_event_for_claim(self, conn, claim_id: int) -> None:
        claim = conn.execute("SELECT * FROM narrative_claims WHERE id=?", (claim_id,)).fetchone()
        if not claim: return
        parts = [
            claim["normalized_subject"] or "",
            claim["normalized_predicate"] or "",
            claim["normalized_object"] or "",
            claim["normalized_date"] or "",
            normalize_text(claim["location_text"] or ""),
        ]
        event_key = "|".join(parts)
        event_type = infer_event_type(claim["predicate"] or "", claim["claim_type"] or "")
        conn.execute(
            """INSERT INTO narrative_events
            (event_key,event_type,primary_actor,secondary_actor,object,location_text,normalized_date,summary)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(event_key) DO UPDATE SET
              summary=CASE WHEN length(excluded.summary)>length(narrative_events.summary) THEN excluded.summary ELSE narrative_events.summary END""",
            (event_key, event_type, claim["subject"] or "", claim["target"] or "",
             claim["object"] or "", claim["location_text"] or "",
             claim["normalized_date"], claim["summary"] or "")
        )
        event_id = conn.execute("SELECT id FROM narrative_events WHERE event_key=?", (event_key,)).fetchone()["id"]
        conn.execute("INSERT OR IGNORE INTO narrative_claim_event_map (claim_id,event_id) VALUES (?,?)", (claim_id, event_id))

    def clear_source_tag(self, conn, source_tag: Optional[str]) -> None:
        if source_tag is None:
            for t in ["narrative_edges","narrative_claim_event_map","narrative_events",
                      "narrative_claims","narrative_evidence_spans","narrative_entity_scores"]:
                conn.execute(f"DELETE FROM {t}")
            return
        conn.execute("""DELETE FROM narrative_edges WHERE src_claim_id IN (
            SELECT c.id FROM narrative_claims c
            JOIN narrative_evidence_spans e ON e.id=c.evidence_span_id WHERE e.source_tag=?)
            OR dst_claim_id IN (
            SELECT c.id FROM narrative_claims c
            JOIN narrative_evidence_spans e ON e.id=c.evidence_span_id WHERE e.source_tag=?)""",
            (source_tag, source_tag))
        conn.execute("""DELETE FROM narrative_claim_event_map WHERE claim_id IN (
            SELECT c.id FROM narrative_claims c
            JOIN narrative_evidence_spans e ON e.id=c.evidence_span_id WHERE e.source_tag=?)""",
            (source_tag,))
        conn.execute("""DELETE FROM narrative_claims WHERE evidence_span_id IN (
            SELECT id FROM narrative_evidence_spans WHERE source_tag=?)""", (source_tag,))
        conn.execute("DELETE FROM narrative_evidence_spans WHERE source_tag=?", (source_tag,))
        conn.execute("DELETE FROM narrative_events WHERE id NOT IN (SELECT DISTINCT event_id FROM narrative_claim_event_map)")
        conn.execute("DELETE FROM narrative_entity_scores")

    def rebuild(self, source_tag=None, max_units=None, rebuild_edges=True) -> Dict[str, Any]:
        import logging
        log = logging.getLogger(__name__)
        conn = self.connect()
        try:
            self.clear_source_tag(conn, source_tag)
            units = self.fetch_text_units(conn, source_tag=source_tag, max_units=max_units)
            log.info("[NIE] rebuild: %d text units, source_tag=%s", len(units), source_tag)
            total_spans = 0
            total_claims = 0
            for unit in units:
                passages = self.split_into_passages(unit.text)
                for char_start, char_end, passage in passages:
                    span_id = self.insert_evidence_span(conn, unit, char_start, char_end, passage)
                    total_spans += 1
                    claims = self.extract_claims_ollama(passage)
                    for claim in claims:
                        claim_id = self.insert_claim(conn, span_id, claim)
                        self.upsert_event_for_claim(conn, claim_id)
                        total_claims += 1
                conn.commit()
            if rebuild_edges:
                self.build_contradictions(conn)
                self.build_propagation(conn)
                self.compute_entity_scores(conn)
            conn.commit()
            return {"status":"ok","units":len(units),"evidence_spans":total_spans,"claims":total_claims}
        except Exception as e:
            conn.rollback()
            return {"status":"error","error":str(e)}
        finally:
            conn.close()

    def build_contradictions(self, conn) -> None:
        claims = conn.execute("""
            SELECT c.*, e.document_id, e.page_number FROM narrative_claims c
            JOIN narrative_evidence_spans e ON e.id=c.evidence_span_id ORDER BY c.id
        """).fetchall()
        by_subj_pred = defaultdict(list)
        for c in claims:
            key = (c["normalized_subject"], c["normalized_predicate"])
            by_subj_pred[key].append(c)
        for group in by_subj_pred.values():
            if len(group) < 2: continue
            for i in range(len(group)):
                for j in range(i+1, len(group)):
                    a, b = group[i], group[j]
                    score = 0.0
                    rationale = []
                    if a["polarity"] != b["polarity"]:
                        score += 0.6; rationale.append("opposite polarity")
                    obj_sim = similarity(a["normalized_object"] or "", b["normalized_object"] or "")
                    if obj_sim < 0.35 and (a["normalized_object"] and b["normalized_object"]):
                        score += 0.35; rationale.append("divergent object")
                    if a["normalized_date"] and b["normalized_date"] and a["normalized_date"] != b["normalized_date"]:
                        score += 0.25; rationale.append("date mismatch")
                    if score >= 0.6:
                        try:
                            conn.execute(
                                "INSERT INTO narrative_edges (src_claim_id,dst_claim_id,edge_type,score,rationale) VALUES (?,?,'CONTRADICTS',?,?)",
                                (a["id"], b["id"], min(score, 1.0), "; ".join(rationale))
                            )
                        except Exception: pass

    def build_propagation(self, conn) -> None:
        claims = conn.execute("""
            SELECT c.*, e.document_id, e.page_number, e.text FROM narrative_claims c
            JOIN narrative_evidence_spans e ON e.id=c.evidence_span_id
            ORDER BY e.document_id, e.page_number, c.id
        """).fetchall()
        attrib_cues = ["according to","reported by","stated by","said","told investigators","advised","claimed"]
        for i in range(len(claims)):
            a = claims[i]
            for j in range(i+1, min(i+50, len(claims))):  # cap to avoid O(n²) explosion
                b = claims[j]
                if a["normalized_subject"] != b["normalized_subject"]: continue
                if a["normalized_predicate"] != b["normalized_predicate"]: continue
                sim = max(
                    similarity(a["summary"] or "", b["summary"] or ""),
                    similarity(a["object"] or "", b["object"] or "")
                )
                if sim >= 0.9 and a["document_id"] != b["document_id"]:
                    try:
                        conn.execute(
                            "INSERT INTO narrative_edges (src_claim_id,dst_claim_id,edge_type,score,rationale) VALUES (?,?,'REPEATS',?,'near-identical claim across documents')",
                            (a["id"], b["id"], sim)
                        )
                    except Exception: pass
                    continue
                t = normalize_text(b["text"] or "")
                cue = any(c in t for c in attrib_cues)
                spk_match = normalize_text(a["speaker"] or a["subject"]) == normalize_text(b["speaker"] or "")
                if cue and sim >= 0.55 and spk_match:
                    try:
                        conn.execute(
                            "INSERT INTO narrative_edges (src_claim_id,dst_claim_id,edge_type,score,rationale) VALUES (?,?,'DERIVES_FROM',?,'attributed claim derived from earlier source')",
                            (a["id"], b["id"], sim)
                        )
                    except Exception: pass
                elif sim >= 0.72 and a["document_id"] != b["document_id"]:
                    try:
                        conn.execute(
                            "INSERT INTO narrative_edges (src_claim_id,dst_claim_id,edge_type,score,rationale) VALUES (?,?,'SUPPORTS',?,'cross-document narrative reinforcement')",
                            (a["id"], b["id"], sim)
                        )
                    except Exception: pass

    def compute_entity_scores(self, conn) -> None:
        conn.execute("DELETE FROM narrative_entity_scores")
        claims = conn.execute("SELECT id,subject,speaker FROM narrative_claims WHERE coalesce(subject,'') <> ''").fetchall()
        edges = conn.execute("SELECT src_claim_id,dst_claim_id,edge_type,score FROM narrative_edges").fetchall()
        claim_subject = {c["id"]: (c["subject"] or "").strip() for c in claims}
        outgoing = defaultdict(list)
        incoming = Counter()
        witness = Counter()
        contradiction = Counter()
        for c in claims:
            subj = (c["subject"] or "").strip()
            if c["speaker"]: witness[subj] += 0.2
        for e in edges:
            src = claim_subject.get(e["src_claim_id"], "")
            dst = claim_subject.get(e["dst_claim_id"], "")
            if not src or not dst: continue
            outgoing[src].append((dst, float(e["score"] or 0), e["edge_type"]))
            incoming[dst] += 1
            if e["edge_type"] == "DERIVES_FROM": witness[src] += float(e["score"] or 0)
            elif e["edge_type"] == "REPEATS": witness[src] += 0.6 * float(e["score"] or 0)
            elif e["edge_type"] == "CONTRADICTS":
                contradiction[src] += 1; contradiction[dst] += 1
        entities = {c["subject"] for c in claims if c["subject"]}
        if not entities: return
        scores = {e: 1.0/len(entities) for e in entities}
        damping = 0.85
        for _ in range(20):
            nxt = {e: (1-damping)/len(entities) for e in entities}
            for src, links in outgoing.items():
                if not links: continue
                total_w = sum(max(0.001, w) for _, w, _ in links)
                for dst, w, _ in links:
                    nxt[dst] += damping * scores.get(src, 0) * (max(0.001,w)/total_w)
            scores = nxt
        for entity in entities:
            conn.execute(
                "INSERT INTO narrative_entity_scores (entity_name,influence_score,witness_score,contradiction_score) VALUES (?,?,?,?)",
                (entity, float(scores.get(entity,0)), float(witness.get(entity,0)), float(contradiction.get(entity,0)))
            )

    # ── Query APIs ──────────────────────────────────────────────────────────────

    def dossier(self, entity_name: str) -> Dict[str, Any]:
        conn = self.connect()
        try:
            score = conn.execute("SELECT * FROM narrative_entity_scores WHERE lower(entity_name)=lower(?)", (entity_name,)).fetchone()
            top_claims = conn.execute("""
                SELECT c.id,c.subject,c.predicate,c.object,c.summary,c.polarity,c.certainty,
                       c.normalized_date,c.location_text,c.extractor_confidence,e.document_id,e.page_number
                FROM narrative_claims c JOIN narrative_evidence_spans e ON e.id=c.evidence_span_id
                WHERE lower(c.subject)=lower(?) OR lower(c.object)=lower(?) OR lower(c.speaker)=lower(?)
                ORDER BY c.extractor_confidence DESC LIMIT 25
            """, (entity_name,entity_name,entity_name)).fetchall()
            contradictions = conn.execute("""
                SELECT ne.edge_type,ne.score,ne.rationale,
                       a.subject src_subject,a.summary src_summary,
                       b.subject dst_subject,b.summary dst_summary
                FROM narrative_edges ne
                JOIN narrative_claims a ON a.id=ne.src_claim_id
                JOIN narrative_claims b ON b.id=ne.dst_claim_id
                WHERE ne.edge_type='CONTRADICTS' AND (
                    lower(a.subject)=lower(?) OR lower(b.subject)=lower(?) OR
                    lower(a.object)=lower(?) OR lower(b.object)=lower(?)
                ) ORDER BY ne.score DESC LIMIT 20
            """, (entity_name,entity_name,entity_name,entity_name)).fetchall()
            timeline = conn.execute("""
                SELECT c.normalized_date,c.summary,c.subject,c.predicate,c.object,e.document_id,e.page_number
                FROM narrative_claims c JOIN narrative_evidence_spans e ON e.id=c.evidence_span_id
                WHERE (lower(c.subject)=lower(?) OR lower(c.object)=lower(?) OR lower(c.target)=lower(?))
                  AND c.normalized_date IS NOT NULL
                ORDER BY c.normalized_date ASC LIMIT 100
            """, (entity_name,entity_name,entity_name)).fetchall()
            inbound = conn.execute("""
                SELECT ne.edge_type,ne.score,a.subject src_subject,a.summary src_summary,b.summary dst_summary
                FROM narrative_edges ne
                JOIN narrative_claims a ON a.id=ne.src_claim_id
                JOIN narrative_claims b ON b.id=ne.dst_claim_id
                WHERE lower(b.subject)=lower(?) AND ne.edge_type IN ('DERIVES_FROM','REPEATS','SUPPORTS')
                ORDER BY ne.score DESC LIMIT 20
            """, (entity_name,)).fetchall()
            return {
                "entity": entity_name,
                "influence_score": float(score["influence_score"]) if score else 0.0,
                "witness_score": float(score["witness_score"]) if score else 0.0,
                "contradiction_score": float(score["contradiction_score"]) if score else 0.0,
                "top_claims": [dict(r) for r in top_claims],
                "contradictions": [dict(r) for r in contradictions],
                "timeline": [dict(r) for r in timeline],
                "inbound_propagation": [dict(r) for r in inbound],
            }
        finally:
            conn.close()

    def contradictions(self, min_score: float = 0.6, limit: int = 100) -> List[Dict]:
        conn = self.connect()
        try:
            rows = conn.execute("""
                SELECT ne.id,ne.score,ne.rationale,
                       a.subject src_subject,a.summary src_summary,ea.document_id src_doc,ea.page_number src_page,
                       b.subject dst_subject,b.summary dst_summary,eb.document_id dst_doc,eb.page_number dst_page
                FROM narrative_edges ne
                JOIN narrative_claims a ON a.id=ne.src_claim_id
                JOIN narrative_claims b ON b.id=ne.dst_claim_id
                JOIN narrative_evidence_spans ea ON ea.id=a.evidence_span_id
                JOIN narrative_evidence_spans eb ON eb.id=b.evidence_span_id
                WHERE ne.edge_type='CONTRADICTS' AND ne.score>=?
                ORDER BY ne.score DESC LIMIT ?
            """, (min_score, limit)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def merged_timeline(self, entity: Optional[str] = None, limit: int = 200) -> List[Dict]:
        conn = self.connect()
        try:
            if entity:
                rows = conn.execute("""
                    SELECT c.normalized_date,c.summary,c.subject,c.object,c.location_text,c.claim_type,
                           e.document_id,e.page_number,e.source_tag
                    FROM narrative_claims c JOIN narrative_evidence_spans e ON e.id=c.evidence_span_id
                    WHERE c.normalized_date IS NOT NULL
                      AND (lower(c.subject)=lower(?) OR lower(c.object)=lower(?) OR lower(c.target)=lower(?))
                    ORDER BY c.normalized_date ASC LIMIT ?
                """, (entity,entity,entity,limit)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT c.normalized_date,c.summary,c.subject,c.object,c.location_text,c.claim_type,
                           e.document_id,e.page_number,e.source_tag
                    FROM narrative_claims c JOIN narrative_evidence_spans e ON e.id=c.evidence_span_id
                    WHERE c.normalized_date IS NOT NULL
                    ORDER BY c.normalized_date ASC LIMIT ?
                """, (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def top_entities(self, limit: int = 25) -> List[Dict]:
        conn = self.connect()
        try:
            rows = conn.execute("""
                SELECT entity_name,influence_score,witness_score,contradiction_score,
                       (SELECT COUNT(*) FROM narrative_claims WHERE lower(subject)=lower(entity_name)) claim_count
                FROM narrative_entity_scores
                ORDER BY influence_score DESC LIMIT ?
            """, (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def status(self) -> Dict[str, Any]:
        conn = self.connect()
        try:
            spans = conn.execute("SELECT COUNT(*) FROM narrative_evidence_spans").fetchone()[0]
            claims = conn.execute("SELECT COUNT(*) FROM narrative_claims").fetchone()[0]
            events = conn.execute("SELECT COUNT(*) FROM narrative_events").fetchone()[0]
            edges = conn.execute("SELECT COUNT(*) FROM narrative_edges").fetchone()[0]
            edge_types = dict(conn.execute("SELECT edge_type, COUNT(*) FROM narrative_edges GROUP BY edge_type").fetchall())
            entities = conn.execute("SELECT COUNT(*) FROM narrative_entity_scores").fetchone()[0]
            units = conn.execute("SELECT COUNT(*) FROM opensight_text_units").fetchone()[0]
            return {"text_units":units,"evidence_spans":spans,"claims":claims,
                    "events":events,"edges":edges,"edge_types":edge_types,"scored_entities":entities}
        except Exception as e:
            return {"error": str(e)}
        finally:
            conn.close()
