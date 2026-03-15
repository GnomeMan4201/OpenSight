from __future__ import annotations

from threading import Lock
from typing import List, Dict, Any

import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity


_MODEL = None
_MODEL_LOCK = Lock()


def get_model() -> SentenceTransformer:
    global _MODEL
    with _MODEL_LOCK:
        if _MODEL is None:
            _MODEL = SentenceTransformer("all-MiniLM-L6-v2")
        return _MODEL


class SemanticSearchEngine:
    def __init__(self) -> None:
        self.texts: List[str] = []
        self.meta: List[Dict[str, Any]] = []
        self.embeddings: List[np.ndarray] = []
        self.lock = Lock()

    def index(self, text: str, metadata: Dict[str, Any] | None = None) -> Dict[str, Any]:
        text = (text or "").strip()
        if not text:
            return {"status": "skipped", "reason": "empty_text"}

        model = get_model()
        emb = model.encode([text], normalize_embeddings=True)[0]

        with self.lock:
            self.texts.append(text)
            self.meta.append(metadata or {})
            self.embeddings.append(np.array(emb, dtype=np.float32))

        return {"status": "indexed", "items": 1}

    def bulk_index(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        indexed = 0
        skipped = 0
        for item in items:
            result = self.index(item.get("text", ""), item.get("metadata"))
            if result["status"] == "indexed":
                indexed += 1
            else:
                skipped += 1
        return {"status": "ok", "indexed": indexed, "skipped": skipped, "total": len(items)}

    def search(self, query: str, k: int = 5) -> List[Dict[str, Any]]:
        query = (query or "").strip()
        if not query:
            return []

        with self.lock:
            if not self.embeddings:
                return []
            matrix = np.vstack(self.embeddings)
            texts = list(self.texts)
            meta = list(self.meta)

        model = get_model()
        q = model.encode([query], normalize_embeddings=True)[0]
        sims = cosine_similarity([q], matrix)[0]
        idx = np.argsort(sims)[::-1][: max(1, k)]

        results = []
        for i in idx:
            results.append(
                {
                    "text": texts[i],
                    "score": float(sims[i]),
                    "metadata": meta[i],
                }
            )
        return results


semantic_engine = SemanticSearchEngine()
