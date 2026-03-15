from __future__ import annotations
from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
import numpy as np, uvicorn

app = FastAPI(title="OpenSight Semantic Service")
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
model = SentenceTransformer(MODEL_NAME)
_store: list[dict] = []

class EmbedRequest(BaseModel):
    texts: list[str]
class IndexRequest(BaseModel):
    text: str
    metadata: dict = {}
class SearchRequest(BaseModel):
    text: str
    k: int = 5

@app.get("/health")
def health():
    return {"status":"ok","service":"semantic","model":MODEL_NAME,"port":8010}

@app.post("/embed")
def embed(req: EmbedRequest):
    return {"count":len(req.texts),"vectors":model.encode(req.texts,normalize_embeddings=True).tolist()}

@app.post("/index")
def index(req: IndexRequest):
    v = model.encode([req.text],normalize_embeddings=True)[0].tolist()
    _store.append({"vector":v,"text":req.text,"metadata":req.metadata})
    return {"indexed":True,"total":len(_store)}

@app.post("/search")
def search(req: SearchRequest):
    if not _store: return {"results":[]}
    import numpy as np
    q = np.array(model.encode([req.text],normalize_embeddings=True)[0])
    vecs = np.array([e["vector"] for e in _store])
    scores = vecs @ q
    top_k = min(req.k,len(_store))
    idx = np.argsort(scores)[::-1][:top_k]
    return {"results":[{"score":float(scores[i]),"text":_store[i]["text"],"metadata":_store[i]["metadata"]} for i in idx]}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8010)
