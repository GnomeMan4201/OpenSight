"""
apps/api/database.py

SQLAlchemy engine, session factory, and database initialization.

Pool choice rationale:
  - StaticPool (previous) is designed for in-memory SQLite (:memory:). It reuses
    a single connection, which serializes all access and defeats WAL mode entirely.
  - NullPool gives each thread its own connection with no reuse. Combined with
    WAL mode this is safe for concurrent readers + one writer, and has negligible
    connection-open overhead on SQLite.
  - For PostgreSQL, swap DATABASE_URL and NullPool is still acceptable until you
    need connection pooling (add QueuePool with pool_size when you hit throughput
    limits).
"""

from sqlalchemy import create_engine, text, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool

from apps.api.config import settings


def _make_engine():
    url = settings.database_url
    if url.startswith("sqlite"):
        engine = create_engine(
            url,
            connect_args={"check_same_thread": False},
            # NullPool: no connection reuse; each session owns its connection.
            # Correct for file-backed SQLite under concurrent FastAPI + background-task load.
            poolclass=NullPool,
            echo=settings.debug,
        )

        @event.listens_for(engine, "connect")
        def _on_connect(conn, _record):
            # WAL mode: readers do not block writers; writers do not block readers.
            conn.execute("PRAGMA journal_mode=WAL")
            # NORMAL sync: safe with WAL, much faster than FULL.
            conn.execute("PRAGMA synchronous=NORMAL")
            # Enforce FK constraints (SQLite disables them by default).
            conn.execute("PRAGMA foreign_keys=ON")
            # 64 MB page cache per connection.
            conn.execute("PRAGMA cache_size=-65536")

        return engine
    else:
        # PostgreSQL / other: basic engine, no special pool config for MVP.
        # v0.2: add pool_size, max_overflow, pool_pre_ping here.
        return create_engine(url, echo=settings.debug)


engine = _make_engine()
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def get_db():
    """FastAPI dependency: yields a session, guarantees close."""
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """
    Create all ORM tables and the FTS5 search index.
    Idempotent — safe to call on every startup.
    """
    from apps.api import models  # noqa: F401 — registers metadata
    from apps.api.models import Base

    Base.metadata.create_all(bind=engine)
    _init_fts(engine)


def _init_fts(eng) -> None:
    """
    SQLite FTS5 virtual table for full-text search.
    source_tag and filename are UNINDEXED so they can be used in WHERE joins
    without contributing to the FTS index size.
    """
    if not settings.database_url.startswith("sqlite"):
        return

    with eng.connect() as conn:
        conn.execute(text("""
            CREATE VIRTUAL TABLE IF NOT EXISTS fts_pages
            USING fts5(
                document_id   UNINDEXED,
                page_number   UNINDEXED,
                source_tag    UNINDEXED,
                filename      UNINDEXED,
                body,
                tokenize      = 'porter ascii'
            )
        """))
        conn.commit()
