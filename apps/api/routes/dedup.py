"""
apps/api/routes/dedup.py

Entity deduplication, merge, and alias management.

GET  /api/v1/entities/duplicates          — fuzzy candidate pairs
POST /api/v1/entities/{id}/merge/{tid}    — merge target into source
POST /api/v1/entities/{id}/alias          — add alias without merging
DELETE /api/v1/entities/{id}/alias/{alias} — remove alias
POST /api/v1/entities/{id}/dismiss-duplicate/{tid} — mark pair as not duplicate
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text

from apps.api.database import get_db
from apps.api.models import Entity, Mention, EntityRelationship, RedactionFlag
from apps.api.services.dedup import find_duplicate_candidates

router = APIRouter(tags=["dedup"])


# ── Duplicate candidates ──────────────────────────────────────────────────────

@router.get("/duplicates")
def get_duplicate_candidates(
    min_confidence: float = Query(0.55, ge=0.1, le=1.0),
    entity_type:    str   = Query(None),
    limit:          int   = Query(100, ge=1, le=500),
    db:             Session = Depends(get_db),
):
    q = db.query(Entity)
    if entity_type:
        q = q.filter(Entity.entity_type == entity_type)
    entities = q.all()

    raw = [
        {
            "id": e.id,
            "canonical_name": e.canonical_name,
            "entity_type": e.entity_type,
            "mention_count": e.mention_count,
            "aliases": e.aliases or [],
        }
        for e in entities
    ]

    candidates = find_duplicate_candidates(raw, min_confidence=min_confidence)

    # Filter out dismissed pairs
    dismissed = _get_dismissed_pairs(db)
    candidates = [
        c for c in candidates
        if tuple(sorted([c.entity_a_id, c.entity_b_id])) not in dismissed
    ]

    return {
        "total": len(candidates[:limit]),
        "candidates": [
            {
                "entity_a": {
                    "id": c.entity_a_id,
                    "name": c.entity_a_name,
                    "type": c.entity_type,
                    "mentions": next((e["mention_count"] for e in raw if e["id"] == c.entity_a_id), 0),
                    "aliases": next((e["aliases"] for e in raw if e["id"] == c.entity_a_id), []),
                },
                "entity_b": {
                    "id": c.entity_b_id,
                    "name": c.entity_b_name,
                    "type": c.entity_type,
                    "mentions": next((e["mention_count"] for e in raw if e["id"] == c.entity_b_id), 0),
                    "aliases": next((e["aliases"] for e in raw if e["id"] == c.entity_b_id), []),
                },
                "confidence": c.confidence,
                "reason": c.reason,
                "entity_type": c.entity_type,
            }
            for c in candidates[:limit]
        ],
    }


# ── Merge ─────────────────────────────────────────────────────────────────────

@router.post("/merge/{source_id}/into/{target_id}")
def merge_entities(
    source_id: str,
    target_id: str,
    db: Session = Depends(get_db),
):
    """
    Merge source into target:
    - All mentions of source → repointed to target
    - All relationships involving source → repointed to target (deduplicate weights)
    - Source's canonical_name added to target's aliases
    - Source entity deleted
    """
    source = db.query(Entity).filter_by(id=source_id).first()
    target = db.query(Entity).filter_by(id=target_id).first()
    if not source:
        raise HTTPException(404, f"Source entity {source_id} not found")
    if not target:
        raise HTTPException(404, f"Target entity {target_id} not found")
    if source_id == target_id:
        raise HTTPException(400, "Cannot merge entity into itself")

    # 1. Add source name + its aliases to target's alias list
    aliases = list(target.aliases or [])
    if source.canonical_name not in aliases and source.canonical_name != target.canonical_name:
        aliases.append(source.canonical_name)
    for a in (source.aliases or []):
        if a not in aliases:
            aliases.append(a)
    target.aliases = aliases

    # 2. Repoint mentions
    db.query(Mention).filter_by(entity_id=source_id).update({"entity_id": target_id})

    # 3. Repoint relationships — careful about uniqueness constraint
    #    entity_a_id is always lexicographically smaller
    source_rels = db.query(EntityRelationship).filter(
        (EntityRelationship.entity_a_id == source_id) |
        (EntityRelationship.entity_b_id == source_id)
    ).all()

    for rel in source_rels:
        # Determine the "other" entity
        other_id = rel.entity_b_id if rel.entity_a_id == source_id else rel.entity_a_id

        if other_id == target_id:
            # Self-loop after merge — just delete
            db.delete(rel)
            continue

        # Build new canonical pair (a < b lexicographically)
        new_a, new_b = tuple(sorted([target_id, other_id]))

        # Check if this relationship already exists
        existing = db.query(EntityRelationship).filter_by(
            entity_a_id=new_a, entity_b_id=new_b
        ).first()

        if existing:
            # Merge weights
            existing.weight    += rel.weight
            existing.doc_count = max(existing.doc_count, rel.doc_count)
            db.delete(rel)
        else:
            rel.entity_a_id = new_a
            rel.entity_b_id = new_b

    # 4. Update target mention count
    target.mention_count = (target.mention_count or 0) + (source.mention_count or 0)

    # 5. Delete source
    db.delete(source)
    db.commit()

    return {
        "merged": source.canonical_name,
        "into": target.canonical_name,
        "target_id": target_id,
        "aliases": target.aliases,
        "new_mention_count": target.mention_count,
    }


# ── Aliases ───────────────────────────────────────────────────────────────────

@router.post("/{entity_id}/alias")
def add_alias(entity_id: str, body: dict, db: Session = Depends(get_db)):
    """Add an alias to an entity without merging. body: {"alias": "JE"}"""
    entity = db.query(Entity).filter_by(id=entity_id).first()
    if not entity:
        raise HTTPException(404, "Entity not found")
    alias = (body.get("alias") or "").strip()
    if not alias:
        raise HTTPException(400, "alias required")
    aliases = list(entity.aliases or [])
    if alias not in aliases:
        aliases.append(alias)
        entity.aliases = aliases
        db.commit()
    return {"id": entity_id, "aliases": entity.aliases}


@router.delete("/{entity_id}/alias/{alias}")
def remove_alias(entity_id: str, alias: str, db: Session = Depends(get_db)):
    entity = db.query(Entity).filter_by(id=entity_id).first()
    if not entity:
        raise HTTPException(404, "Entity not found")
    aliases = [a for a in (entity.aliases or []) if a != alias]
    entity.aliases = aliases
    db.commit()
    return {"id": entity_id, "aliases": entity.aliases}


# ── Dismiss duplicate pair ────────────────────────────────────────────────────

@router.post("/dismiss-duplicate")
def dismiss_duplicate(body: dict, db: Session = Depends(get_db)):
    """Mark a pair as 'not duplicates' so they don't keep appearing."""
    a_id = body.get("entity_a_id")
    b_id = body.get("entity_b_id")
    if not a_id or not b_id:
        raise HTTPException(400, "entity_a_id and entity_b_id required")
    key = "|".join(sorted([a_id, b_id]))
    # Store in a simple KV on entity A's review_notes as JSON marker
    # In production this would be a proper DismissedPair table
    entity = db.query(Entity).filter_by(id=a_id).first()
    if entity:
        notes = entity.review_notes or ""
        marker = f"[DISMISSED_PAIR:{b_id}]"
        if marker not in notes:
            entity.review_notes = notes + marker
            db.commit()
    return {"dismissed": key}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_dismissed_pairs(db: Session) -> set[tuple[str, str]]:
    """Read dismissed pairs from entity review_notes markers."""
    import re
    dismissed = set()
    entities = db.query(Entity).filter(
        Entity.review_notes.isnot(None),
        Entity.review_notes.like("%DISMISSED_PAIR%"),
    ).all()
    for e in entities:
        for match in re.finditer(r"\[DISMISSED_PAIR:([^\]]+)\]", e.review_notes or ""):
            other_id = match.group(1)
            dismissed.add(tuple(sorted([e.id, other_id])))
    return dismissed
