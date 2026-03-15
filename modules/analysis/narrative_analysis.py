from __future__ import annotations
from collections import Counter

def summarize_case(case: dict) -> dict:
    entity_counter = Counter()
    type_counter = Counter()

    for e in case.get("entities", []):
        entity_counter[e["value"]] += e.get("frequency", 1)
        type_counter[e["type"]] += 1

    return {
        "top_entities": [{"value": k, "score": v} for k, v in entity_counter.most_common(15)],
        "entity_type_counts": dict(type_counter),
        "document_count": len(case.get("documents", [])),
    }
