from __future__ import annotations

from collections import defaultdict

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from apps.api.database import get_db
from apps.api.models import Mention, Entity

router = APIRouter(tags=["graph_timeline"])


@router.get("/graph/timeline/snapshots")
def graph_timeline_snapshots(
    bucket: str = Query("month"),
    db: Session = Depends(get_db),
):
    mentions = db.query(Mention).all()

    grouped = defaultdict(list)
    for m in mentions:
        dt = getattr(m, "mention_date", None)
        if not dt:
            continue
        if bucket == "day":
            key = dt.strftime("%Y-%m-%d")
        elif bucket == "year":
            key = dt.strftime("%Y")
        else:
            key = dt.strftime("%Y-%m")
        grouped[key].append(m)

    entity_rows = db.query(Entity).all()
    emap = {e.id: e for e in entity_rows}

    out = []
    for key in sorted(grouped.keys()):
        counts = defaultdict(int)
        for m in grouped[key]:
            if getattr(m, "entity_id", None):
                counts[m.entity_id] += 1

        top_nodes = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:40]
        out.append({
            "bucket": key,
            "node_count": len(counts),
            "top_nodes": [
                {
                    "id": eid,
                    "canonical_name": emap[eid].canonical_name if eid in emap else str(eid),
                    "entity_type": emap[eid].entity_type if eid in emap else "unknown",
                    "mention_count": cnt,
                }
                for eid, cnt in top_nodes
            ]
        })

    return {
        "bucket_mode": bucket,
        "frames": out,
    }
