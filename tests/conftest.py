import io
import json
import logging
from collections.abc import Callable, Iterator
from datetime import datetime, tzinfo
from types import ModuleType
from typing import Any

import pytest
from prometheus_client import REGISTRY
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cert_orchestrator.logging_config import JsonFormatter
from cert_orchestrator.logging_context import ContextFilter
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


@pytest.fixture
def json_log() -> Iterator[Callable[[], list[dict[str, Any]]]]:
    """Capture root-logger output as the service writes it: JsonFormatter and ContextFilter on the handler.

    The callable parses every captured line, so a line that is not one JSON object fails the test.
    """
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(ContextFilter())
    root = logging.getLogger()
    saved_level = root.level
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    try:
        yield lambda: [json.loads(line) for line in stream.getvalue().splitlines()]
    finally:
        root.removeHandler(handler)
        root.setLevel(saved_level)


@pytest.fixture
def metric_value() -> Callable[..., float]:
    """Return a reader for one exported sample of the default registry; the sample must exist."""

    def _value(name: str, **labels: str) -> float:
        value = REGISTRY.get_sample_value(name, labels)
        assert value is not None, f"{name} {labels} is not exported"
        return value

    return _value


class _FrozenClock:
    """Stands in for a module's datetime so that datetime.now(tz) returns one fixed instant."""

    def __init__(self, instant: datetime) -> None:
        self._instant = instant

    def now(self, tz: tzinfo | None = None) -> datetime:
        return self._instant


@pytest.fixture
def freeze_clock(monkeypatch) -> Callable[[ModuleType, datetime], None]:
    """Return a setter that makes one module's datetime.now() return a fixed instant for the rest of the test."""

    def _freeze(module: ModuleType, instant: datetime) -> None:
        monkeypatch.setattr(module, "datetime", _FrozenClock(instant))

    return _freeze
