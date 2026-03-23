from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Dict, List


@dataclass
class RelationshipCandidate:
    source_id: str
    target_id: str
    relationship_type: str
    confidence: float
    sentence_span: str | None = None


_FILED_AGAINST_RE = re.compile(
    r"(?P<a>[A-Z][A-Za-z0-9&.,'\- ]+?)\s+v\.\s+(?P<b>[A-Z][A-Za-z0-9&.,'\- ]+)",
    re.IGNORECASE,
)


def extract_relationships(
    text: str,
    entity_name_to_id: Dict[str, str],
    document_id: str,
    spacy_model: str = "en_core_web_sm",
) -> List[RelationshipCandidate]:
    candidates: List[RelationshipCandidate] = []

    names = sorted(entity_name_to_id.keys(), key=len, reverse=True)
    lower_map = {name.lower(): (name, entity_name_to_id[name]) for name in names}
    text_lower = text.lower()

    found = []
    for name in names:
        idx = text_lower.find(name.lower())
        if idx >= 0:
            found.append((idx, name, entity_name_to_id[name]))

    for i in range(len(found)):
        for j in range(i + 1, len(found)):
            _, name_a, id_a = found[i]
            _, name_b, id_b = found[j]
            if id_a == id_b:
                continue
            candidates.append(RelationshipCandidate(
                source_id=id_a,
                target_id=id_b,
                relationship_type="mentioned_with",
                confidence=0.65,
                sentence_span=f"{name_a} <> {name_b}",
            ))

    for m in _FILED_AGAINST_RE.finditer(text):
        a_raw = m.group("a").strip().lower()
        b_raw = m.group("b").strip().lower()
        if a_raw in lower_map and b_raw in lower_map:
            _, a_id = lower_map[a_raw]
            _, b_id = lower_map[b_raw]
            if a_id != b_id:
                candidates.append(RelationshipCandidate(
                    source_id=a_id,
                    target_id=b_id,
                    relationship_type="filed_against",
                    confidence=0.72,
                    sentence_span=m.group(0)[:300],
                ))

    dedup = {}
    for c in candidates:
        pair = tuple(sorted((c.source_id, c.target_id)))
        key = (pair[0], pair[1], c.relationship_type)
        prev = dedup.get(key)
        if prev is None or c.confidence > prev.confidence:
            dedup[key] = c

    return list(dedup.values())
