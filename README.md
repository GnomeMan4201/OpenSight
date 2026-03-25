# OpenSight

Document intelligence and OSINT platform for local corpus analysis.

Ingest documents, extract entities, build a knowledge graph, detect communities,
generate LLM summaries, and export investigation bundles — all running locally,
no cloud required.

## What it does

- **Document ingestion** — txt, json, markdown, structured or unstructured
- **Entity extraction** — people, orgs, locations, dates with relationship mapping
- **Knowledge graph** — force-directed visualization, typed edges, community detection
- **Timeline engine** — chronological event reconstruction across documents
- **Narrative analysis** — LLM-assisted summarization via local or API-connected models
- **Investigation bundles** — export/import `.osight` packages for sharing findings

## Stack

FastAPI · SQLAlchemy · SQLite (FTS5) · Python 3.12 · Docker

## Quick start
```bash
cp .env.example .env   # add your API key if using LLM features
bash start_services.sh
```

The API runs on `:8000`, semantic service on `:8010`.

## Examples

The `examples/` directory contains synthetic demo datasets:
- `opensight_synthetic_legal_dataset/` — 400 structured legal documents
- `opensight_demo_wow_json/` — 5,000 document JSON corpus
- `opensight_demo_wow_txt/` — 5,000 document plain text corpus

## Requirements

See `requirements.txt`. Use a virtualenv.

## Structure
```
apps/api/          FastAPI backend + routes
modules/           entity extraction, graph engine, timeline, analysis
semantic_service.py  semantic search microservice
ui/                investigation dashboard
examples/          demo datasets
tests/             test suite
```

---

*LANimals collective // badBANANA*
