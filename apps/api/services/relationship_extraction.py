"""
apps/api/services/relationship_extraction.py

Typed relationship extraction pipeline.
Three extraction passes in priority order:

  1. Dependency parse  — spaCy subject-verb-object triples
  2. Regex rules       — pattern-matched typed edges
  3. Co-occurrence     — fallback for entities in same sentence

Each pass returns a list of RelationshipCandidate.
The ingestion pipeline calls extract_relationships(text, entities, document_id).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)


# ── Relationship types ────────────────────────────────────────────────────────

class RT:
    """Relationship type constants."""
    # Legal
    FILED_AGAINST     = "filed_against"
    SUED_BY           = "sued_by"
    REPRESENTED_BY    = "represented_by"
    TESTIFIED_IN      = "testified_in"
    RULED_ON          = "ruled_on"
    APPEALED_TO       = "appealed_to"
    CHARGED_WITH      = "charged_with"
    CONVICTED_OF      = "convicted_of"
    SETTLED_WITH      = "settled_with"
    PRESIDED_OVER     = "presided_over"
    # Organizational
    WORKS_FOR         = "works_for"
    FOUNDED           = "founded"
    OWNS              = "owns"
    ACQUIRED          = "acquired"
    PARTNERED_WITH    = "partnered_with"
    SUBSIDIARY_OF     = "subsidiary_of"
    REGULATES         = "regulates"
    INVESTIGATES      = "investigates"
    EMPLOYED_BY       = "employed_by"
    APPOINTED_BY      = "appointed_by"
    # Financial
    INVESTED_IN       = "invested_in"
    FUNDED_BY         = "funded_by"
    PAID              = "paid"
    # Geographic
    LOCATED_IN        = "located_in"
    FILED_IN          = "filed_in"
    HEADQUARTERED_IN  = "headquartered_in"
    # Generic
    ASSOCIATED_WITH   = "associated_with"
    CO_OCCURRENCE     = "co_occurrence"
    MENTIONED_WITH    = "mentioned_with"


# ── Output type ───────────────────────────────────────────────────────────────

@dataclass
class RelationshipCandidate:
    source_name:       str
    target_name:       str
    relationship_type: str
    confidence:        float
    sentence_span:     str
    extraction_method: str          # "dep_parse" | "regex" | "co_occurrence"
    source_id:         Optional[str] = None   # filled by caller
    target_id:         Optional[str] = None   # filled by caller
    document_id:       Optional[str] = None


# ── Regex rule patterns ───────────────────────────────────────────────────────
# Each rule: (compiled_pattern, relationship_type, confidence, src_group, tgt_group)

_RULES: list[tuple] = []

def _r(pattern: str, rel_type: str, conf: float, src: int = 1, tgt: int = 2):
    _RULES.append((re.compile(pattern, re.I), rel_type, conf, src, tgt))

# Legal
_r(r'([\w\s\.]+)\s+(?:filed suit|filed a lawsuit|sued)\s+(?:against\s+)?([\w\s\.]+)',
   RT.FILED_AGAINST, 0.85)
_r(r'([\w\s\.]+)\s+v\.?\s+([\w\s\.]+)',
   RT.FILED_AGAINST, 0.75)
_r(r'(?:Judge|Justice|Hon\.?)\s+([\w\s]+)\s+(?:presided|ruled|heard)\s+(?:over\s+)?(?:the\s+)?([\w\s]+(?:case|matter|trial|hearing))',
   RT.PRESIDED_OVER, 0.82)
_r(r'([\w\s\.]+)\s+(?:testified|gave testimony)\s+(?:in|before)\s+([\w\s\.]+)',
   RT.TESTIFIED_IN, 0.80)
_r(r'([\w\s\.]+)\s+(?:appealed|filed an appeal)\s+(?:to\s+)?([\w\s\.]+)',
   RT.APPEALED_TO, 0.80)
_r(r'([\w\s\.]+)\s+(?:represented|counsel for|attorney for)\s+([\w\s\.]+)',
   RT.REPRESENTED_BY, 0.78, src=2, tgt=1)
_r(r'([\w\s\.]+)\s+(?:was charged|charged)\s+(?:with\s+)?([\w\s\.]+)',
   RT.CHARGED_WITH, 0.80)
_r(r'([\w\s\.]+)\s+(?:settled|reached a settlement)\s+(?:with\s+)?([\w\s\.]+)',
   RT.SETTLED_WITH, 0.78)

# Organizational
_r(r'([\w\s\.]+)\s+(?:CEO|CFO|CTO|president|chairman|director|officer)\s+(?:of\s+)?([\w\s\.]+)',
   RT.WORKS_FOR, 0.80)
_r(r'([\w\s\.]+)\s+(?:founded|co-founded|established)\s+([\w\s\.]+)',
   RT.FOUNDED, 0.82)
_r(r'([\w\s\.]+)\s+(?:acquired|purchased|bought)\s+([\w\s\.]+)',
   RT.ACQUIRED, 0.83)
_r(r'([\w\s\.]+)\s+(?:owns|owned by|a subsidiary of)\s+([\w\s\.]+)',
   RT.OWNS, 0.80)
_r(r'([\w\s\.]+)\s+(?:regulates|has jurisdiction over|oversees)\s+([\w\s\.]+)',
   RT.REGULATES, 0.80)
_r(r'([\w\s\.]+)\s+(?:investigated|is investigating|launched an investigation into)\s+([\w\s\.]+)',
   RT.INVESTIGATES, 0.80)
_r(r'([\w\s\.]+)\s+(?:appointed|nominated)\s+([\w\s\.]+)',
   RT.APPOINTED_BY, 0.78, src=2, tgt=1)

# Financial
_r(r'([\w\s\.]+)\s+(?:invested in|made an investment in)\s+([\w\s\.]+)',
   RT.INVESTED_IN, 0.80)
_r(r'([\w\s\.]+)\s+(?:funded by|received funding from)\s+([\w\s\.]+)',
   RT.FUNDED_BY, 0.80, src=1, tgt=2)
_r(r'([\w\s\.]+)\s+(?:paid|awarded)\s+\$[\d,]+\s+(?:to\s+)?([\w\s\.]+)',
   RT.PAID, 0.75)

# Geographic
_r(r'([\w\s\.]+)\s+(?:is based in|headquartered in|located in|office in)\s+([\w\s\.]+)',
   RT.HEADQUARTERED_IN, 0.78)
_r(r'([\w\s\.]+)\s+(?:filed in|filed with)\s+(?:the\s+)?([\w\s\.]+(?:Court|District|Circuit))',
   RT.FILED_IN, 0.82)


# ── spaCy dependency parse ────────────────────────────────────────────────────

# Verb → relationship_type mappings
_VERB_MAP: dict[str, str] = {
    "sue":        RT.FILED_AGAINST,
    "file":       RT.FILED_AGAINST,
    "charge":     RT.CHARGED_WITH,
    "accuse":     RT.CHARGED_WITH,
    "employ":     RT.EMPLOYED_BY,
    "hire":       RT.EMPLOYED_BY,
    "found":      RT.FOUNDED,
    "establish":  RT.FOUNDED,
    "acquire":    RT.ACQUIRED,
    "buy":        RT.ACQUIRED,
    "own":        RT.OWNS,
    "regulate":   RT.REGULATES,
    "oversee":    RT.REGULATES,
    "investigate":RT.INVESTIGATES,
    "fund":       RT.FUNDED_BY,
    "invest":     RT.INVESTED_IN,
    "pay":        RT.PAID,
    "appoint":    RT.APPOINTED_BY,
    "represent":  RT.REPRESENTED_BY,
    "settle":     RT.SETTLED_WITH,
    "appeal":     RT.APPEALED_TO,
    "testify":    RT.TESTIFIED_IN,
    "preside":    RT.PRESIDED_OVER,
    "rule":       RT.RULED_ON,
    "partner":    RT.PARTNERED_WITH,
    "merge":      RT.ACQUIRED,
    "convict":    RT.CONVICTED_OF,
}


def _extract_dep_parse(
    text: str,
    entity_names: set[str],
    document_id: str,
    spacy_model: str = "en_core_web_sm",
) -> list[RelationshipCandidate]:
    """Extract SVO triples using spaCy dependency parsing."""
    try:
        import spacy
        nlp = spacy.load(spacy_model)
    except Exception:
        return []

    results = []
    try:
        doc = nlp(text[:50000])  # cap for performance

        for sent in doc.sents:
            sent_text = sent.text.strip()
            if len(sent_text) < 10:
                continue

            for token in sent:
                # Find verb tokens
                if token.pos_ not in ("VERB", "AUX"):
                    continue

                lemma = token.lemma_.lower()
                rel_type = _VERB_MAP.get(lemma)
                if not rel_type:
                    continue

                # Find subject and object
                subj = None
                obj  = None
                for child in token.children:
                    if child.dep_ in ("nsubj", "nsubjpass") and subj is None:
                        subj = _get_span_text(child)
                    if child.dep_ in ("dobj", "pobj", "attr", "oprd") and obj is None:
                        obj = _get_span_text(child)

                if not subj or not obj:
                    continue

                # Match against known entity names (fuzzy)
                src_match = _match_entity(subj, entity_names)
                tgt_match = _match_entity(obj, entity_names)

                if src_match and tgt_match and src_match != tgt_match:
                    results.append(RelationshipCandidate(
                        source_name=src_match,
                        target_name=tgt_match,
                        relationship_type=rel_type,
                        confidence=0.72,
                        sentence_span=sent_text[:200],
                        extraction_method="dep_parse",
                        document_id=document_id,
                    ))

    except Exception as e:
        log.warning("[rel_extract] dep_parse error: %s", e)

    return results


def _get_span_text(token) -> str:
    """Get the full noun phrase for a token."""
    try:
        return token.subtree and " ".join(t.text for t in token.subtree).strip()
    except Exception:
        return token.text


def _match_entity(text: str, entity_names: set[str]) -> Optional[str]:
    """Return the entity name if text contains or matches it."""
    text_lower = text.lower().strip()
    for name in entity_names:
        name_lower = name.lower()
        if name_lower in text_lower or text_lower in name_lower:
            if len(name) > 2:
                return name
    return None


# ── Regex extraction ──────────────────────────────────────────────────────────

def _extract_regex(
    text: str,
    entity_names: set[str],
    document_id: str,
) -> list[RelationshipCandidate]:
    """Apply regex rules to extract typed relationships."""
    results = []

    # Split into sentences (simple)
    sentences = re.split(r'(?<=[.!?])\s+', text)

    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) < 15:
            continue

        for pattern, rel_type, conf, src_grp, tgt_grp in _RULES:
            for match in pattern.finditer(sentence):
                try:
                    src_text = match.group(src_grp).strip()
                    tgt_text = match.group(tgt_grp).strip()
                except (IndexError, AttributeError):
                    continue

                src_match = _match_entity(src_text, entity_names)
                tgt_match = _match_entity(tgt_text, entity_names)

                if src_match and tgt_match and src_match != tgt_match:
                    results.append(RelationshipCandidate(
                        source_name=src_match,
                        target_name=tgt_match,
                        relationship_type=rel_type,
                        confidence=conf,
                        sentence_span=sentence[:200],
                        extraction_method="regex",
                        document_id=document_id,
                    ))

    return results


# ── Co-occurrence fallback ────────────────────────────────────────────────────

def _extract_cooccurrence(
    text: str,
    entity_names: set[str],
    document_id: str,
) -> list[RelationshipCandidate]:
    """
    Sentence-level co-occurrence — only fires if no typed relationship
    was found between a pair. Lower confidence than typed extractions.
    """
    results = []
    sentences = re.split(r'(?<=[.!?])\s+', text)

    for sentence in sentences:
        found = [name for name in entity_names if name.lower() in sentence.lower()]
        if len(found) < 2:
            continue
        for i in range(len(found)):
            for j in range(i + 1, len(found)):
                results.append(RelationshipCandidate(
                    source_name=found[i],
                    target_name=found[j],
                    relationship_type=RT.MENTIONED_WITH,
                    confidence=0.40,
                    sentence_span=sentence[:200],
                    extraction_method="co_occurrence",
                    document_id=document_id,
                ))

    return results


# ── Deduplication ─────────────────────────────────────────────────────────────

def _deduplicate(
    candidates: list[RelationshipCandidate],
) -> list[RelationshipCandidate]:
    """
    For each (source, target) pair, keep the highest-confidence typed
    relationship. Discard co_occurrence if a typed edge exists for the same pair.
    """
    # Group by normalized pair
    best: dict[tuple[str, str], RelationshipCandidate] = {}

    for c in candidates:
        # Normalize pair (alphabetical) for undirected dedup
        key = (min(c.source_name, c.target_name), max(c.source_name, c.target_name))

        existing = best.get(key)
        if existing is None:
            best[key] = c
            continue

        # Prefer typed over co_occurrence
        existing_typed = existing.relationship_type not in (RT.CO_OCCURRENCE, RT.MENTIONED_WITH)
        new_typed      = c.relationship_type not in (RT.CO_OCCURRENCE, RT.MENTIONED_WITH)

        if new_typed and not existing_typed:
            best[key] = c
        elif new_typed and existing_typed and c.confidence > existing.confidence:
            best[key] = c
        elif not new_typed and not existing_typed and c.confidence > existing.confidence:
            best[key] = c

    return list(best.values())


# ── Public API ────────────────────────────────────────────────────────────────

def extract_relationships(
    text: str,
    entity_name_to_id: dict[str, str],   # {canonical_name: entity_id}
    document_id: str,
    spacy_model: str = "en_core_web_sm",
    run_dep_parse: bool = True,
) -> list[RelationshipCandidate]:
    """
    Full relationship extraction pipeline for a document.

    Args:
        text:               Document text
        entity_name_to_id:  Mapping of canonical entity names to their DB IDs
        document_id:        Document ID for provenance
        spacy_model:        spaCy model name for dependency parsing
        run_dep_parse:      Whether to run dep parse (skip if spaCy unavailable)

    Returns:
        Deduplicated list of RelationshipCandidate with source_id/target_id filled.
    """
    if not text or not entity_name_to_id:
        return []

    entity_names = set(entity_name_to_id.keys())

    candidates: list[RelationshipCandidate] = []

    # Pass 1: dependency parse
    if run_dep_parse:
        dep_results = _extract_dep_parse(text, entity_names, document_id, spacy_model)
        candidates.extend(dep_results)
        log.debug("[rel_extract] dep_parse: %d candidates", len(dep_results))

    # Pass 2: regex rules
    regex_results = _extract_regex(text, entity_names, document_id)
    candidates.extend(regex_results)
    log.debug("[rel_extract] regex: %d candidates", len(regex_results))

    # Pass 3: co-occurrence fallback for any pair not already covered
    covered_pairs = {
        (min(c.source_name, c.target_name), max(c.source_name, c.target_name))
        for c in candidates
        if c.relationship_type not in (RT.CO_OCCURRENCE, RT.MENTIONED_WITH)
    }

    cooc_results = _extract_cooccurrence(text, entity_names, document_id)
    for r in cooc_results:
        pair = (min(r.source_name, r.target_name), max(r.source_name, r.target_name))
        if pair not in covered_pairs:
            candidates.append(r)

    # Deduplicate
    deduped = _deduplicate(candidates)

    # Fill in entity IDs
    for c in deduped:
        c.source_id   = entity_name_to_id.get(c.source_name)
        c.target_id   = entity_name_to_id.get(c.target_name)
        c.document_id = document_id

    # Only return candidates where both IDs resolved
    resolved = [c for c in deduped if c.source_id and c.target_id]
    log.debug("[rel_extract] total after dedup+resolve: %d", len(resolved))
    return resolved
