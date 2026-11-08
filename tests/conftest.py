import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cert_orchestrator.models import Base


@pytest.fixture
def sqlite_engine(tmp_path):
    """File-backed SQLite schema, so separate sessions see only what has been committed."""
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'unit.db'}", future=True)
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def session_factory(sqlite_engine):
    """Return a callable that opens a new session on the test database, as the relay expects."""

    def _open() -> Session:
        return Session(sqlite_engine)

    return _open


@pytest.fixture
def db_session(sqlite_engine):
    """One open session on the test database, closed after the test."""
    with Session(sqlite_engine) as session:
        yield session
