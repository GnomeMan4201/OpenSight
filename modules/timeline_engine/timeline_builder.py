from __future__ import annotations

def build_timeline(case: dict) -> list[dict]:
    events = []
    for doc in case.get("documents", []):
        for entity in doc.get("entities", []):
            if entity.get("type") == "date":
                events.append({
                    "date": entity["value"],
                    "label": f'Date referenced in "{doc["title"]}"',
                    "document_id": doc["id"],
                })
    events.sort(key=lambda x: x["date"])
    return events
