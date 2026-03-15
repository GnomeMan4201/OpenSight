from __future__ import annotations

import re
from datetime import datetime
from typing import List, Dict, Any

DATE_PATTERNS = [
    r"\b\d{4}-\d{2}-\d{2}\b",
    r"\b\d{4}/\d{2}/\d{2}\b",
    r"\b\d{1,2}/\d{1,2}/\d{4}\b",
]

MONTHS = (
    "January|February|March|April|May|June|July|August|September|October|November|December|"
    "Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
)
LONG_DATE_RE = re.compile(rf"\b(?:{MONTHS})\s+\d{{1,2}},\s+\d{{4}}\b", re.IGNORECASE)


def _parse_date(raw: str) -> str | None:
    raw = raw.strip()

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            pass

    try:
        return datetime.strptime(raw, "%B %d, %Y").date().isoformat()
    except ValueError:
        pass

    try:
        return datetime.strptime(raw, "%b %d, %Y").date().isoformat()
    except ValueError:
        pass

    return None


def extract_timeline(text: str, context_window: int = 180) -> List[Dict[str, Any]]:
    text = text or ""
    events: List[Dict[str, Any]] = []

    spans = []
    for pattern in DATE_PATTERNS:
        for m in re.finditer(pattern, text):
            spans.append((m.start(), m.end(), m.group(0)))

    for m in LONG_DATE_RE.finditer(text):
        spans.append((m.start(), m.end(), m.group(0)))

    seen = set()
    for start, end, raw in sorted(spans, key=lambda x: x[0]):
        iso = _parse_date(raw)
        if not iso:
            continue

        key = (iso, start)
        if key in seen:
            continue
        seen.add(key)

        left = max(0, start - context_window)
        right = min(len(text), end + context_window)
        context = " ".join(text[left:right].split())

        events.append(
            {
                "date": iso,
                "raw_date": raw,
                "context": context,
                "position": start,
            }
        )

    events.sort(key=lambda x: x["date"])
    return events
