from __future__ import annotations
import re
from collections import Counter

WORD_RE = re.compile(r"\b[A-Z][a-zA-Z0-9_-]{2,}\b")
EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b")
DOMAIN_RE = re.compile(r"\b(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}\b")
IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
DATE_RE = re.compile(r"\b(?:19|20)\d{2}-\d{2}-\d{2}\b")

def extract_entities(text: str) -> list[dict]:
    counts = Counter()

    for m in EMAIL_RE.findall(text):
        counts[("email", m)] += 1
    for m in DOMAIN_RE.findall(text):
        counts[("domain", m)] += 1
    for m in IP_RE.findall(text):
        counts[("ip", m)] += 1
    for m in DATE_RE.findall(text):
        counts[("date", m)] += 1
    for m in WORD_RE.findall(text):
        counts[("name", m)] += 1

    entities = []
    for (etype, value), freq in counts.most_common():
        entities.append({
            "type": etype,
            "value": value,
            "frequency": freq,
        })
    return entities
