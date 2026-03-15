"""
apps/api/services/dedup.py

Fuzzy entity deduplication engine.

Strategies (in order of confidence):
  1. Exact canonical_name match (already handled by upsert — these never duplicate)
  2. Substring containment — "Epstein" in "Jeffrey Epstein"
  3. Token overlap — shared significant tokens above threshold
  4. Levenshtein ratio — edit distance for short names / typos
  5. Known alias patterns — "JE" → "Jeffrey Epstein" initials match

Returns scored candidate pairs for human review.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import re


@dataclass
class DuplicateCandidate:
    entity_a_id: str
    entity_a_name: str
    entity_b_id: str
    entity_b_name: str
    confidence: float          # 0.0 – 1.0
    reason: str                # human-readable explanation
    entity_type: str


def _tokens(name: str) -> set[str]:
    """Significant tokens — strips stop words and short tokens."""
    STOP = {"the", "of", "and", "a", "an", "in", "for", "llc", "inc",
            "corp", "co", "ltd", "mr", "mrs", "ms", "dr"}
    return {t for t in re.findall(r"[a-z]+", name.lower()) if len(t) > 2 and t not in STOP}


def _levenshtein(a: str, b: str) -> int:
    if len(a) < len(b):
        return _levenshtein(b, a)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            curr.append(min(prev[j] + (ca != cb), curr[j] + 1, prev[j + 1] + 1))
        prev = curr
    return prev[-1]


def _initials_match(short: str, full: str) -> bool:
    """Check if short looks like initials of full name. 'JE' → 'Jeffrey Epstein'"""
    if len(short) > 4 or not short.isupper():
        return False
    words = [w for w in full.split() if w and w[0].isupper()]
    if not words:
        return False
    initials = "".join(w[0] for w in words)
    return short == initials


def find_duplicate_candidates(
    entities: list[dict],
    min_confidence: float = 0.55,
) -> list[DuplicateCandidate]:
    """
    entities: list of dicts with keys: id, canonical_name, entity_type, mention_count
    Returns deduplicated list of candidate pairs sorted by confidence desc.
    """
    candidates: list[DuplicateCandidate] = []
    seen_pairs: set[tuple[str, str]] = set()

    # Group by type — only compare same-type entities
    by_type: dict[str, list[dict]] = {}
    for e in entities:
        t = e["entity_type"]
        by_type.setdefault(t, []).append(e)

    for etype, group in by_type.items():
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a, b = group[i], group[j]
                pair_key = tuple(sorted([a["id"], b["id"]]))
                if pair_key in seen_pairs:
                    continue

                na = a["canonical_name"].strip()
                nb = b["canonical_name"].strip()
                na_lower = na.lower()
                nb_lower = nb.lower()

                confidence = 0.0
                reason = ""

                # Strategy 1: one is substring of the other
                if na_lower in nb_lower or nb_lower in na_lower:
                    shorter, longer = (na, nb) if len(na) <= len(nb) else (nb, na)
                    # Avoid flagging very short substrings like "Al" in "Alabama"
                    if len(shorter) >= 4:
                        confidence = 0.85
                        reason = f"'{shorter}' is contained in '{longer}'"

                # Strategy 2: high token overlap
                if confidence < 0.7:
                    ta, tb = _tokens(na), _tokens(nb)
                    if ta and tb:
                        overlap = len(ta & tb) / max(len(ta | tb), 1)
                        if overlap >= 0.6:
                            confidence = max(confidence, 0.55 + overlap * 0.35)
                            reason = f"Token overlap {overlap:.0%}: shared {ta & tb}"

                # Strategy 3: Levenshtein for short-ish names
                if confidence < 0.7 and len(na) <= 30 and len(nb) <= 30:
                    dist = _levenshtein(na_lower, nb_lower)
                    max_len = max(len(na), len(nb))
                    ratio = 1 - dist / max_len
                    if ratio >= 0.75:
                        confidence = max(confidence, ratio * 0.85)
                        reason = f"Edit distance {dist} ({ratio:.0%} similar)"

                # Strategy 4: initials match (Person only)
                if confidence < 0.7 and etype == "Person":
                    if _initials_match(na, nb) or _initials_match(nb, na):
                        short = na if len(na) < len(nb) else nb
                        full  = nb if len(na) < len(nb) else na
                        confidence = max(confidence, 0.75)
                        reason = f"'{short}' matches initials of '{full}'"

                if confidence >= min_confidence:
                    seen_pairs.add(pair_key)
                    candidates.append(DuplicateCandidate(
                        entity_a_id=a["id"],
                        entity_a_name=na,
                        entity_b_id=b["id"],
                        entity_b_name=nb,
                        confidence=round(confidence, 3),
                        reason=reason,
                        entity_type=etype,
                    ))

    candidates.sort(key=lambda c: c.confidence, reverse=True)
    return candidates
