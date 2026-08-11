from contextlib import contextmanager

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cert_orchestrator.models import Base
from cert_orchestrator.messaging.handlers import LifecycleMessageHandler


class StubPublisher:
    def __init__(self):
        self.completions = []
        self.retries = []

    async def publish_completion(self, event):
        self.completions.append(event)

    async def publish_retry(self, payload, delay_seconds: int):
        self.retries.append((payload, delay_seconds))


def build_session() -> Session:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return Session(engine)


@pytest.mark.asyncio
async def test_handler_processes_new_event_and_publishes_completion(monkeypatch):
    session = build_session()

    @contextmanager
    def _session_provider():
        yield session

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", _session_provider)

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)

    payload = {
        "event_id": "evt-1",
        "certificate_id": "cert-1",
        "payload": {},
    }

    await handler.handle(payload)

    assert len(publisher.completions) == 1
    assert publisher.completions[0].status == "COMPLETED"
    assert publisher.retries == []


@pytest.mark.asyncio
async def test_handler_skips_duplicate_event(monkeypatch):
    session = build_session()

    @contextmanager
    def _session_provider():
        yield session

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", _session_provider)

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)

    payload = {
        "event_id": "evt-dup",
        "certificate_id": "cert-dup",
        "payload": {},
    }

    await handler.handle(payload)
    await handler.handle(payload)

    assert len(publisher.completions) == 1
    assert publisher.retries == []


@pytest.mark.asyncio
async def test_handler_failure_schedules_retry_with_backoff(monkeypatch):
    session = build_session()

    @contextmanager
    def _session_provider():
        yield session

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", _session_provider)

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)

    payload = {
        "event_id": "evt-fail",
        "certificate_id": "cert-fail",
        "payload": {"should_fail": True},
    }

    await handler.handle(payload)

    assert publisher.completions == []
    assert len(publisher.retries) == 1
    retry_payload, delay = publisher.retries[0]
    assert retry_payload["event_id"] == "evt-fail:retry:1"
    assert delay == 5
