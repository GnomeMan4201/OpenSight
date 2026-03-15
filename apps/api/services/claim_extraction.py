"""
apps/api/services/claim_extraction.py

Claim extraction pipeline — extracts attributed statements from document pages.

Strategy:
  1. Ollama (primary) — local LLM structured extraction via JSON prompt
  2. spaCy dependency parse (fallback) — subject-verb-object heuristic
  3. Sentence-window heuristic (final fallback) — any sentence containing
     two known entities is a candidate claim

Output: list of ClaimCandidate dataclasses, each with:
  speaker_name, subject_name, claim_text, claim_type, sentiment, confidence
"""

from __future__ import annotations
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

CLAIM_TYPES = frozenset({
    "allegation", "denial", "observation", "testimony", "ruling", "other"
})

SENTIMENTS = frozenset({"positive", "negative", "neutral"})


@dataclass
class ClaimCandidate:
    speaker_name:  Optional[str]   # None if no speaker identified
    subject_name:  Optional[str]   # None if no subject identified
    claim_text:    str
    claim_type:    str = "observation"
    sentiment:     str = "neutral"
    confidence:    float = 0.7
    method:        str = "heuristic"


# ── Ollama extraction ──────────────────────────────────────────────────────────

_OLLAMA_SYSTEM = """Return ONLY a JSON array of claims. No explanation. No markdown.
Format: [{"speaker":"name or null","subject":"name or null","claim":"text","type":"observation","sentiment":"neutral","confidence":0.7}]
Types: allegation, denial, observation, testimony, ruling, other
Sentiments: positive, negative, neutral
If none found: []"""


def extract_claims_ollama(
    text: str,
    known_entities: list[str],
    ollama_url: str = "http://localhost:11434",
    model: str = "phi3",
) -> list[ClaimCandidate]:
    """Call Ollama to extract claims. Returns [] on any failure."""
    try:
        import urllib.request
        entity_hint = ""
        if known_entities:
            entity_hint = f"\nKnown entities in this document: {', '.join(known_entities[:20])}"

        payload = json.dumps({
            "model": model,
            "system": _OLLAMA_SYSTEM,
            "prompt": f"Extract attributed statements from this text:{entity_hint}\n\n{text[:1200]}",
            "stream": False,
            "options": {"temperature": 0.1, "num_predict": 600},
        }).encode()

        req = urllib.request.Request(
            f"{ollama_url}/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())

        raw = result.get("response", "").strip()
        raw = re.sub(r"```[a-z]*", "", raw).strip().rstrip("`").strip()
        # Extract array
        m = re.search(r"\[.*?\]", raw, re.DOTALL)
        raw = m.group() if m else raw
        # Repair truncation
        if raw.count("[") > raw.count("]"):
            raw = raw.rstrip(",").rstrip() + "]"
        raw = re.sub(r",\s*]", "]", raw)
        raw = re.sub(r",\s*}", "}", raw)
        try:
            items = json.loads(raw)
        except json.JSONDecodeError:
            items = []
            for obj in re.finditer(r"\{[^{}]+\}", raw):
                try: items.append(json.loads(obj.group()))
                except: pass
            if not items: raise
        if not isinstance(items, list):
            return []

        claims = []
        for item in items:
            if not isinstance(item, dict):
                continue
            claim_text = str(item.get("claim", "")).strip()
            if not claim_text:
                continue
            claim_type = item.get("type", "other")
            if claim_type not in CLAIM_TYPES:
                claim_type = "other"
            sentiment = item.get("sentiment", "neutral")
            if sentiment not in SENTIMENTS:
                sentiment = "neutral"
            claims.append(ClaimCandidate(
                speaker_name=item.get("speaker") or None,
                subject_name=item.get("subject") or None,
                claim_text=claim_text,
                claim_type=claim_type,
                sentiment=sentiment,
                confidence=float(item.get("confidence", 0.7)),
                method="ollama",
            ))
        log.info("[claims] Ollama extracted %d claims", len(claims))
        return claims

    except Exception as exc:
        log.warning("[claims] Ollama extraction failed: %s", exc)
        return []


# ── spaCy dependency parse fallback ───────────────────────────────────────────

def extract_claims_spacy(
    text: str,
    known_entities: list[str],
) -> list[ClaimCandidate]:
    """
    Heuristic claim extraction using spaCy dependency parse.
    Looks for sentences where known entities appear as subject or object
    of a reporting verb (said, claimed, stated, testified, alleged, etc.)
    """
    try:
        import spacy
        nlp = spacy.load("en_core_web_sm")
    except Exception:
        return []

    REPORTING_VERBS = {
        "say", "claim", "state", "testify", "allege", "report", "argue",
        "assert", "contend", "deny", "admit", "confirm", "describe",
        "accuse", "suggest", "indicate", "note", "observe", "find",
    }
    NEGATIVE_VERBS = {"deny", "refute", "dispute", "reject", "contradict"}

    entity_set = {e.lower() for e in known_entities}
    claims = []

    doc = nlp(text[:50_000])

    for sent in doc.sents:
        sent_text = sent.text.strip()
        if len(sent_text) < 20:
            continue

        # Find reporting verbs in this sentence
        for token in sent:
            if token.lemma_.lower() not in REPORTING_VERBS:
                continue

            speaker = None
            subject = None

            # Find subject of the reporting verb
            for child in token.children:
                if child.dep_ in ("nsubj", "nsubjpass"):
                    chunk = child.text.strip()
                    if any(e in chunk.lower() for e in entity_set):
                        speaker = chunk

                # Find object (what is being claimed about)
                if child.dep_ in ("dobj", "attr", "prep"):
                    for grandchild in child.subtree:
                        chunk = grandchild.text.strip()
                        if any(e in chunk.lower() for e in entity_set) and chunk != speaker:
                            subject = chunk
                            break

            if speaker or subject:
                claim_type = "denial" if token.lemma_.lower() in NEGATIVE_VERBS else "testimony"
                claims.append(ClaimCandidate(
                    speaker_name=speaker,
                    subject_name=subject,
                    claim_text=sent_text[:1200],
                    claim_type=claim_type,
                    sentiment="negative" if claim_type == "denial" else "neutral",
                    confidence=0.6,
                    method="spacy",
                ))
            break  # one claim per sentence

    log.info("[claims] spaCy extracted %d claims", len(claims))
    return claims


# ── Sentence-window heuristic (final fallback) ────────────────────────────────

def extract_claims_heuristic(
    text: str,
    known_entities: list[str],
) -> list[ClaimCandidate]:
    """
    Any sentence containing 2+ known entities becomes a candidate claim.
    No speaker/subject assignment — just raw text with entity co-occurrence.
    """
    if not known_entities:
        return []

    sentences = re.split(r'(?<=[.!?])\s+', text)
    entity_set = {e.lower() for e in known_entities}
    claims = []

    for sent in sentences:
        sent = sent.strip()
        if len(sent) < 30:
            continue
        hits = [e for e in entity_set if e in sent.lower()]
        if len(hits) >= 2:
            claims.append(ClaimCandidate(
                speaker_name=None,
                subject_name=None,
                claim_text=sent[:500],
                claim_type="observation",
                sentiment="neutral",
                confidence=0.4,
                method="heuristic",
            ))

    return claims[:50]  # cap at 50 per page


# ── Main entry point ──────────────────────────────────────────────────────────


def _is_ocr_garbage(text: str) -> bool:
    """
    Returns True if the text is predominantly redaction codes,
    page numbers, or OCR noise rather than prose.
    """
    tokens = text.split()
    if not tokens:
        return True
    redaction_codes = {"b3", "b6", "b7c", "b7d", "b7e", "b7f", "b5", "b1", "b2",
                       "bo", "b6e", "b6é", "b7c-", "b6-"}
    code_hits = sum(1 for t in tokens if t.lower().strip(",-;") in redaction_codes)
    digit_hits = sum(1 for t in tokens if t.strip().isdigit())
    noise_ratio = (code_hits + digit_hits) / len(tokens)
    if noise_ratio > 0.35:
        return True
    # Must have at least a few real words (alpha, length > 3)
    real_words = sum(1 for t in tokens if t.isalpha() and len(t) > 3)
    if real_words < 5:
        return True
    return False

def extract_claims(
    text: str,
    known_entities: list[str],
    use_ollama: bool = True,
    ollama_url: str = "http://localhost:11434",
    ollama_model: str = "phi3",
) -> list[ClaimCandidate]:
    """
    Main entry point. Tries Ollama first, falls back to spaCy, then heuristic.
    """
    if _is_ocr_garbage(text):
        log.info("[claims] skipping OCR garbage block (%d chars)", len(text))
        return []
    if use_ollama:
        results = extract_claims_ollama(text, known_entities, ollama_url, ollama_model)
        if results:
            return results

    results = extract_claims_spacy(text, known_entities)
    if results:
        return results

    return extract_claims_heuristic(text, known_entities)
