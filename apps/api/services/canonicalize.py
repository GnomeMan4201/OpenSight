"""
apps/api/services/canonicalize.py

Post-ingest canonicalization pipeline.
Called automatically after every document ingest (Stage 6.6).
Also callable manually via scripts/canonicalize.sh.

Passes (in order):
  0. Noise deletion     — remove entities matching noise rules
  1. Cross-type merge   — "Federal Trade Commission" (Person) → (Organization)
  2. Judge-prefix merge — "Judge William Avery" → "William Avery"
  3. Explicit merges    — "District Court" → "U.S. District Court" etc.
  4. Type fixes         — force correct entity_type for known entities

All passes are idempotent.
"""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict

from sqlalchemy.orm import Session

from apps.api.models import Entity, EntityRelationship, Mention

log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

# entity_type preference order for cross-type merge winner selection
_TYPE_RANK = {"Organization": 3, "Location": 2, "Person": 1, "Event": 0}

# Names that should always be deleted regardless of type
_JUNK_NAMES: frozenset[str] = frozenset({
    # Case citation artifacts
    "Apex Mobile v. Orion Devices", "States v. Rivergate Holdings",
    "States v. Sterling Diagnostics - Witness", "Horizon Systems - Supreme Court",
    "Trademark Office - Vertex Labs",
    # Trailing-dash / arrow artifacts
    "Martin Cole -", "Helix Circuits -", "Helix Circuits - Court",
    "Vertex Labs -", "Nova Cloud - Codeforge", "Vertex Labs - Precedent",
    # Role/generic labels
    "Chief Financial Officer", "Exchange Commission",
    "U.S.", "API", "Tech", "Brightcart Platform", "BrightCart Platform",
    "Northstar Logic", "High Court Review", "Expert Report Submitted",
    "Damages Model", "Document Production", "Financial Records",
    "Internal Emails", "Oral Argument", "Verdict Form",
    "Economic Expert", "Market Analysis", "Regulatory Notice",
    "State Coalition", "Justice Antitrust Division", "Ad Exchange",
    "Vector Stack", "Signal Grid", "Harbor Capital",
    "Summary Judgment", "Summary Judgment Motion",
    "Corporate Fraud Trial", "Patent Litigation",
})

# Noise suffixes — any entity ending with these is junk
_NOISE_SUFFIXES: tuple[str, ...] = (
    " date", " event", " cluster", " case", " relationships",
    " relationship", " ecosystem", " dispute", " proceeding", " network",
    " platform",
)

# Type-only noise: Date entities are always removed
_NOISE_TYPES: frozenset[str] = frozenset({"Date"})

# Person token blocklist — if any token matches, it's not a human name
_NON_NAME_TOKENS: frozenset[str] = frozenset({
    "district", "court", "circuit", "division", "department",
    "bureau", "office", "commission", "coalition", "council",
    "committee", "tribunal", "authority", "agency", "board",
    "expert", "witness", "analysis", "notice", "advisory",
    "regulatory", "review", "market", "exchange", "capital",
    "group", "panel", "motion", "argument", "production",
    "records", "emails", "model", "report", "testimony",
    "form", "order", "verdict", "grid", "stack", "signal",
    "platform", "logic", "tech", "systems", "software",
    "labs", "diagnostics",
})

# Explicit (keep_name, kill_name) merges
_EXPLICIT_MERGES: list[tuple[str, str]] = [
    ("U.S. District Court",  "District Court"),
    ("Pine Harbor Capital",  "Harbor Capital"),
    ("Federal Trade Commission", "FTC"),
]

# Forced type corrections: {canonical_name: correct_type}
_TYPE_FIXES: dict[str, str] = {
    "Nova Cloud":               "Organization",
    "Federal Trade Commission": "Organization",
    "United States":            "Location",
    "Northern District":        "Location",
    "Southern District":        "Location",
    "Western District":         "Location",
    "New York":                 "Location",
    "California":               "Location",
    "Washington":               "Location",
    "Supreme Court":            "Organization",
    "U.S. District Court":      "Organization",
    "Court of Appeals":         "Organization",
    "Apex Mobile":              "Organization",
    "Atlas Software":           "Organization",
    "Nimbus Systems":           "Organization",
    "SyntaxWorks":              "Organization",
    "Horizon Systems":          "Organization",
    "Signal":                   "Organization",
    "Pine Harbor Capital":      "Organization",
    "Meridian Biolabs":         "Organization",
    "CodeForge":                "Organization",
    "Vertex Labs":              "Organization",
    "Helix Circuits":           "Organization",
    "Metro Ad Exchange":        "Organization",
    "Titan Search":             "Organization",
    "Trademark Office":         "Organization",
    "Omnidata Retail":          "Organization",
    "Rivergate Holdings":       "Organization",
    "Sterling Diagnostics":     "Organization",
    "Department of Justice Antitrust Division": "Organization",
}


# ── Noise classification ───────────────────────────────────────────────────────

def is_noise_entity(name: str, entity_type: str) -> bool:
    """Return True if this entity should never exist in the DB."""
    if not name or not name.strip():
        return True
    name = name.strip()

    # Markup artifacts
    if "--" in name or "→" in name:
        return True

    # Noise types
    if entity_type in _NOISE_TYPES:
        return True

    # Hard junk list
    if name in _JUNK_NAMES or name.lower() in {j.lower() for j in _JUNK_NAMES}:
        return True

    # Noise suffixes
    nl = name.lower()
    if any(nl.endswith(s) for s in _NOISE_SUFFIXES):
        return True

    tokens = name.split()
    n = len(tokens)

    # Too long, starts with article, starts with digit
    if n > 6:
        return True
    if not name[0].isalpha():
        return True
    if tokens[0].lower() in ("the", "a", "an", "this", "that"):
        return True

    # Person: token blocklist + max 3 tokens
    if entity_type == "Person":
        if n > 3:
            return True
        if {t.lower() for t in tokens} & _NON_NAME_TOKENS:
            return True

    return False


# ── Merge helper ───────────────────────────────────────────────────────────────

def _merge_into(keep_id: str, kill_id: str, db: Session) -> None:
    """
    Merge kill_id entity into keep_id.
    Relationships are NOT rewritten — deleted instead.
    Stage 6.5 rebuilds from mentions on next ingest.
    Avoids StaleDataError from in-place ORM mutation with typed rows.
    """
    if keep_id == kill_id:
        return

    db.expire_all()

    keep = db.query(Entity).filter_by(id=keep_id).first()
    kill = db.query(Entity).filter_by(id=kill_id).first()
    if not keep or not kill:
        log.debug("[canon] _merge_into: entity missing keep=%s kill=%s", keep_id[:8], kill_id[:8])
        return

    keep_aliases = list(keep.aliases or [])
    for name in [kill.canonical_name, *(kill.aliases or [])]:
        if name and name != keep.canonical_name and name not in keep_aliases:
            keep_aliases.append(name)
    keep.aliases = keep_aliases
    keep.confidence = max(keep.confidence or 0.0, kill.confidence or 0.0)
    db.flush()

    db.query(Mention).filter_by(entity_id=kill_id).update(
        {"entity_id": keep_id}, synchronize_session=False)

    db.query(EntityRelationship).filter(
        (EntityRelationship.entity_a_id == kill_id) |
        (EntityRelationship.entity_b_id == kill_id)
    ).delete(synchronize_session=False)

    db.query(Entity).filter_by(id=kill_id).delete(synchronize_session=False)

    new_count = db.query(Mention).filter_by(entity_id=keep_id).count()
    db.query(Entity).filter_by(id=keep_id).update(
        {"mention_count": new_count}, synchronize_session=False)

    db.flush()


def _delete_entity(entity_id: str, db: Session) -> None:
    db.query(EntityRelationship).filter(
        (EntityRelationship.entity_a_id == entity_id) |
        (EntityRelationship.entity_b_id == entity_id)
    ).delete(synchronize_session=False)
    db.query(Mention).filter_by(entity_id=entity_id).delete(synchronize_session=False)
    db.query(Entity).filter_by(id=entity_id).delete(synchronize_session=False)
    db.flush()


# ── Canonicalization passes ───────────────────────────────────────────────────

def _pass0_noise(db: Session) -> int:
    """Delete all entities matching noise rules."""
    rows = db.query(Entity).all()
    deleted = 0
    for e in rows:
        if is_noise_entity(e.canonical_name, e.entity_type):
            _delete_entity(e.id, db)
            deleted += 1
            log.debug("[canon] noise deleted: [%s] %s", e.entity_type, e.canonical_name)
    return deleted


def _pass1_cross_type(db: Session) -> int:
    """Merge entities with the same canonical_name but different types."""
    rows = db.query(Entity).all()
    by_name: dict[str, list[tuple]] = defaultdict(list)
    for e in rows:
        by_name[e.canonical_name].append((e.id, e.entity_type, e.confidence or 0, e.mention_count or 0))

    merge_pairs: list[tuple[str, str]] = []
    for name, entries in by_name.items():
        if len(entries) <= 1:
            continue
        winner = max(entries, key=lambda t: (t[2], _TYPE_RANK.get(t[1], 0), t[3]))
        for entry in entries:
            if entry[0] != winner[0]:
                merge_pairs.append((winner[0], entry[0]))

    merged = 0
    for keep_id, kill_id in merge_pairs:
        if not db.query(Entity.id).filter_by(id=keep_id).first():
            continue
        if not db.query(Entity.id).filter_by(id=kill_id).first():
            continue
        _merge_into(keep_id, kill_id, db)
        merged += 1
        log.debug("[canon] cross-type merge: %s → %s", kill_id[:8], keep_id[:8])
    return merged


def _pass2_judge_prefix(db: Session) -> int:
    """Merge Judge X into X (or rename if no bare form exists)."""
    name_to_id: dict[str, str] = {
        e.canonical_name: e.id
        for e in db.query(Entity).all()
    }

    resolved = 0
    for name, eid in list(name_to_id.items()):
        if not name.startswith("Judge "):
            continue
        bare = name[len("Judge "):]
        if not db.query(Entity.id).filter_by(id=eid).first():
            continue
        if bare in name_to_id:
            bare_id = name_to_id[bare]
            if not db.query(Entity.id).filter_by(id=bare_id).first():
                continue
            _merge_into(bare_id, eid, db)
            log.debug("[canon] judge-prefix merge: '%s' → '%s'", name, bare)
        else:
            db.query(Entity).filter_by(id=eid).update(
                {"canonical_name": bare}, synchronize_session=False)
            db.flush()
            name_to_id[bare] = eid
            log.debug("[canon] judge-prefix rename: '%s' → '%s'", name, bare)
        resolved += 1
    return resolved


def _pass3_explicit(db: Session) -> int:
    """Apply explicit (keep, kill) merge pairs."""
    rows = db.query(Entity).all()
    all_names = {e.canonical_name: e for e in rows}
    merged = 0

    for keep_name, kill_name in _EXPLICIT_MERGES:
        if keep_name in all_names and kill_name in all_names:
            _merge_into(all_names[keep_name].id, all_names[kill_name].id, db)
            merged += 1
            log.debug("[canon] explicit merge: '%s' → '%s'", kill_name, keep_name)
    return merged


def _pass4_type_fixes(db: Session) -> int:
    """Force correct entity_type for known entity names."""
    fixed = 0
    for name, correct_type in _TYPE_FIXES.items():
        result = db.query(Entity).filter(
            Entity.canonical_name == name,
            Entity.entity_type != correct_type,
        ).all()
        for e in result:
            e.entity_type = correct_type
            fixed += 1
            log.debug("[canon] type fix: '%s' → %s", name, correct_type)
    db.flush()
    return fixed


# ── Public API ────────────────────────────────────────────────────────────────

def run_canonicalization(db: Session, commit: bool = True) -> dict:
    """
    Run all canonicalization passes against the full entity table.
    Called after every document ingest (Stage 6.6) and by the manual script.

    Returns a summary dict with counts per pass.
    """
    log.info("[canon] Starting canonicalization pass")

    noise   = _pass0_noise(db)
    cross   = _pass1_cross_type(db)
    judge   = _pass2_judge_prefix(db)
    explicit = _pass3_explicit(db)
    types   = _pass4_type_fixes(db)

    if commit:
        db.commit()

    total_entities = db.query(Entity).count()
    summary = {
        "noise_deleted":   noise,
        "cross_type_merged": cross,
        "judge_prefix_resolved": judge,
        "explicit_merged": explicit,
        "type_fixes":      types,
        "entities_remaining": total_entities,
    }
    log.info("[canon] Done: %s", summary)
    return summary
