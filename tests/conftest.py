"""
tests/conftest.py

After the ingestion.py fix (using SessionLocal from database.py instead of
create_engine(db_url)), we only need to patch two things per test:

  1. apps.api.database.SessionLocal — used by get_db() AND by run_ingestion()
  2. apps.api.database.engine       — used by _init_fts and any direct refs

Both the route layer AND the background ingestion thread now use the same
patched SessionLocal, so they all hit the same database.

We still use a temp file (not :memory:) because WAL-mode SQLite works best
with a named file, and it avoids any residual StaticPool edge cases.
"""

import os
import tempfile
import pytest

os.environ.setdefault("UPLOAD_DIR", "/tmp/opensight_test_uploads")
os.environ.setdefault("DEBUG",      "false")
os.environ.setdefault("USE_OCR",    "false")

import apps.api.database as _db_module
from apps.api.main import app
from apps.api.database import get_db, _init_fts
from apps.api.models import Base

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope="function")
def db_engine():
    fd, tmp_path = tempfile.mkstemp(suffix=".db", prefix="opensight_test_")
    os.close(fd)
    url = f"sqlite:///{tmp_path}"

    engine = create_engine(url, connect_args={"check_same_thread": False})

    @event.listens_for(engine, "connect")
    def _pragmas(conn, _):
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(bind=engine)
    _init_fts(engine)

    TestSession = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    # Patch both — engine for any direct refs, SessionLocal for get_db + ingestion
    _orig_engine      = _db_module.engine
    _orig_session_cls = _db_module.SessionLocal
    _db_module.engine       = engine
    _db_module.SessionLocal = TestSession

    yield engine

    _db_module.engine       = _orig_engine
    _db_module.SessionLocal = _orig_session_cls
    engine.dispose()
    for suffix in ("", "-shm", "-wal"):
        try:
            os.unlink(tmp_path + suffix)
        except FileNotFoundError:
            pass


@pytest.fixture(scope="function")
def db_session(db_engine):
    Session = sessionmaker(bind=db_engine, autocommit=False, autoflush=False)
    s = Session()
    yield s
    s.rollback()
    s.close()


@pytest.fixture(scope="function")
def client(db_engine):
    Session = sessionmaker(bind=db_engine, autocommit=False, autoflush=False)

    def _override():
        s = Session()
        try:
            yield s
        finally:
            s.close()

    app.dependency_overrides[get_db] = _override
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    app.dependency_overrides.clear()


def txt_file(content: str = "Test document content.") -> bytes:
    return content.encode()
