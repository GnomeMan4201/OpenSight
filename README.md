# OpenSight

**Investigative document analysis backend.**

OpenSight processes large document archives — FOIA releases, leak dumps, court filings — and makes them searchable by content, entity, and metadata. It runs as a local FastAPI service with SQLite persistence. No cloud dependencies, no message queues, no external services required.

---

## What it does

| Capability | Status |
|---|---|
| Document upload (PDF, TXT, MD, images) | ✓ |
| Text extraction (pdfplumber → pypdf fallback) | ✓ |
| OCR for scanned pages (pytesseract + PyMuPDF) | ✓ optional |
| Named entity recognition (spaCy + regex) | ✓ |
| Full-text search with FTS5 and relevance ranking | ✓ |
| Entity filtering, source tag filtering, date range filtering | ✓ |
| Redaction detection (black-box regions, hidden text) | ✓ optional |
| Annotations (notes, highlights, flags) | ✓ |
| Duplicate detection by SHA-256 hash | ✓ |
| Paginated API with correct counts | ✓ |

---

## Architecture

```
┌─────────────┐     HTTP      ┌──────────────────────────────────────┐
│   Client    │ ────────────▶ │           FastAPI (main.py)          │
└─────────────┘               └──────┬───────────┬───────────────────┘
                                     │           │
                        ┌────────────▼───┐   ┌───▼───────────────────┐
                        │  BackgroundTask │   │      Route handlers    │
                        │  (ingestion)   │   │  /documents /search    │
                        └────────┬───────┘   │  /entities /annotations│
                                 │           └───────────┬────────────┘
                    ┌────────────▼────────────────┐      │
                    │      Ingestion pipeline      │      │
                    │  1. Text extraction          │      │
                    │  2. OCR (optional)           │      │
                    │  3. Entity extraction        │      │
                    │  4. FTS5 indexing            │      │
                    │  5. Redaction detection      │      │
                    └────────────┬────────────────┘      │
                                 │                        │
                    ┌────────────▼────────────────────────▼────┐
                    │            SQLite (WAL mode)              │
                    │  documents  document_pages  entities      │
                    │  mentions   annotations     redaction_flags│
                    │  fts_pages (FTS5 virtual table)           │
                    └───────────────────────────────────────────┘
                                 │
                    ┌────────────▼────────────────┐
                    │       Local filesystem       │
                    │  uploads/{source_tag}/{uuid} │
                    └─────────────────────────────┘
```

### Service layer

| Module | Responsibility |
|---|---|
| `services/storage.py` | Streaming file write, dedup, path traversal guard |
| `services/ingestion.py` | Pipeline orchestration (7 explicit stages) |
| `services/entity_extraction.py` | spaCy NER + regex (auto-detected) |
| `services/search.py` | FTS5 CTE search, ILIKE fallback for PostgreSQL |

### Entity extraction strategy

spaCy is the primary NER engine when `en_core_web_sm` (or another model) is loadable. Regex handles domain-specific types that spaCy does not cover reliably: aircraft tail numbers, phone numbers, email addresses, ICAO airport codes, physical addresses.

If spaCy is not installed, regex runs for all types at lower confidence. The system degrades gracefully — no configuration change required.

---

## Quick start

```bash
git clone https://github.com/your-org/opensight
cd opensight

python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Optional: spaCy NER (recommended)
pip install spacy
python -m spacy download en_core_web_sm

cp .env.example .env            # edit if needed

uvicorn apps.api.main:app --reload
```

The API is now running at `http://localhost:8000`.
Interactive docs: `http://localhost:8000/docs`

---

## Optional dependencies

### spaCy (recommended)

Enables high-accuracy NER for PERSON, ORG, GPE, DATE entities.

```bash
pip install spacy
python -m spacy download en_core_web_sm
```

The system detects spaCy automatically on startup. No config change needed.

### OCR for scanned documents

Required for scanned PDFs and image uploads (PNG, JPG, TIFF).

```bash
# macOS
brew install tesseract
pip install pymupdf pytesseract Pillow

# Ubuntu / Debian
apt install tesseract-ocr
pip install pymupdf pytesseract Pillow
```

Enable in `.env`:
```
USE_OCR=true
```

### Redaction detection

Detects filled black-box regions and hidden text layers in PDFs.

```bash
pip install pymupdf
```

Enabled by default (`USE_REDACTION_DETECTION=true`). Disabled gracefully if PyMuPDF is absent.

---

## Configuration

Copy `.env.example` to `.env`. All settings have sensible defaults for local development.

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite:///./opensight.db` | SQLite path or PostgreSQL URL |
| `UPLOAD_DIR` | `./uploads` | Document storage root |
| `MAX_UPLOAD_SIZE_MB` | `500` | Per-file upload limit |
| `CORS_ORIGINS` | `http://localhost:3000,...` | Comma-separated allowed origins |
| `SPACY_MODEL` | `en_core_web_sm` | spaCy model name |
| `USE_OCR` | `false` | Enable OCR stage |
| `OCR_LANGUAGE` | `eng` | Tesseract language code |
| `USE_REDACTION_DETECTION` | `true` | Enable redaction detection |
| `DEBUG` | `false` | SQLAlchemy echo mode |

---

## API reference

### Health

```
GET /health
```

### Documents

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/documents/upload` | Upload one or more documents |
| `GET` | `/api/v1/documents` | List documents (`?source_tag=`, `?status=`, `?page=`) |
| `GET` | `/api/v1/documents/{id}` | Get document metadata |
| `GET` | `/api/v1/documents/{id}/status` | Poll ingestion status |
| `GET` | `/api/v1/documents/{id}/pages` | List all pages with entity spans |
| `GET` | `/api/v1/documents/{id}/pages/{n}` | Get a single page |
| `GET` | `/api/v1/documents/{id}/redaction-flags` | List detected redactions |
| `PATCH` | `/api/v1/documents/{id}/redaction-flags/{fid}/reviewed` | Mark flag reviewed |

**Upload example:**
```bash
curl -X POST http://localhost:8000/api/v1/documents/upload \
  -F "files=@report.pdf" \
  -F "source_tag=foia_2024"
```

Poll for completion:
```bash
curl http://localhost:8000/api/v1/documents/{id}/status
```

### Search

```
POST /api/v1/search
GET  /api/v1/search?q=...
```

**POST body:**
```json
{
  "query": "Palm Beach flight logs",
  "page": 1,
  "page_size": 20,
  "filters": {
    "source_tags": ["foia_2024"],
    "entity_names": ["Jeffrey Epstein"],
    "has_redactions": true,
    "date_from": "2020-01-01T00:00:00",
    "date_to":   "2024-12-31T23:59:59"
  }
}
```

**Response:**
```json
{
  "total": 42,
  "total_pages": 3,
  "page": 1,
  "page_size": 20,
  "query": "Palm Beach flight logs",
  "results": [
    {
      "document_id": "...",
      "filename": "report.pdf",
      "source_tag": "foia_2024",
      "page_number": 3,
      "score": 0.94,
      "snippet": "...flew to <mark>Palm Beach</mark> on...",
      "entity_names": ["Jeffrey Epstein", "Palm Beach"],
      "has_redactions": false
    }
  ]
}
```

Snippets are HTML-escaped with `<mark>` tags for term highlighting. Safe to render as `innerHTML`.

### Entities

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/entities` | List entities (`?entity_type=`, `?q=`, `?min_mentions=`) |
| `GET` | `/api/v1/entities/{id}` | Get entity with mention and document counts |
| `GET` | `/api/v1/entities/{id}/mentions` | All occurrences with document context |
| `PATCH` | `/api/v1/entities/{id}/review` | Set review status (`confirmed`/`disputed`/`auto`) |

**Entity types:** `Person`, `Organization`, `Location`, `Aircraft`, `Phone`, `Email`, `Date`, `Airport`, `Address`

### Annotations

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/annotations` | Create annotation |
| `GET` | `/api/v1/annotations` | List (`?document_id=`, `?author=`, `?annotation_type=`) |
| `GET` | `/api/v1/annotations/{id}` | Get annotation |
| `DELETE` | `/api/v1/annotations/{id}` | Soft-delete annotation |

**Annotation types:** `note`, `highlight`, `flag`, `redaction_note`

---

## Running tests

```bash
pip install pytest httpx
pytest tests/ -v
```

The test suite uses an in-memory SQLite database and does not touch the filesystem or the production database.

---

## Repository layout

```
opensight/
├── apps/api/
│   ├── config.py           Settings (pydantic-settings, .env)
│   ├── database.py         Engine, session, FTS5 init
│   ├── main.py             FastAPI app, CORS, router registration
│   ├── models.py           ORM models (Document, Entity, Mention, …)
│   ├── schemas.py          Pydantic request/response models
│   ├── routes/
│   │   ├── documents.py    Upload, list, pages, redaction flags
│   │   ├── search.py       POST and GET search endpoints
│   │   ├── entities.py     Entity listing, mentions, review
│   │   └── annotations.py  CRUD for annotations
│   └── services/
│       ├── storage.py      Streaming upload, path traversal guard
│       ├── ingestion.py    7-stage ingestion pipeline
│       ├── entity_extraction.py  spaCy + regex NER
│       └── search.py       FTS5 CTE search, ILIKE fallback
├── tests/
│   ├── conftest.py         Fixtures (in-memory DB, TestClient)
│   ├── test_api.py         Integration tests (pytest + httpx)
│   └── run_tests.py        Stdlib-only unit tests (no pytest required)
├── uploads/                Document storage (gitignored)
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## Docker

```bash
docker compose up --build
```

The API will be available at `http://localhost:8000`.

---

## Roadmap

These features are planned but not yet implemented:

- **Frontend** — React-based document viewer with annotation overlay
- **Async ingestion queue** — Celery + Redis for large batch uploads
- **PostgreSQL** — production-grade persistence (DATABASE_URL swap, no code changes)
- **Entity deduplication** — merge similar entity names (fuzzy match, manual review)
- **AI summarisation** — Ollama integration for per-document briefings
- **Export** — Markdown / JSON briefing export
- **Authentication** — JWT-based user model
- **Rate limiting** — upload and search endpoint throttling
- **Semantic search** — vector embeddings via pgvector or ChromaDB

---

## Supported file types

| Extension | Extraction method |
|---|---|
| `.pdf` | pdfplumber, pypdf, optional OCR |
| `.txt`, `.md` | Direct read |
| `.png`, `.jpg`, `.jpeg`, `.tiff`, `.tif`, `.bmp` | OCR (requires `USE_OCR=true`) |

Unsupported types (`.docx`, `.xlsx`, `.exe`, etc.) are rejected with HTTP 415.
