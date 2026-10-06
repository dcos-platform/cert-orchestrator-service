from contextlib import contextmanager

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cert_orchestrator.messaging.handlers import LifecycleMessageHandler
from cert_orchestrator.messaging.inbound import InboundMessage
from cert_orchestrator.models import Base, ProcessedEvent


class StubPublisher:
    def __init__(self):
        self.completions = []
        self.retries = []

    async def publish_completion(self, event):
        self.completions.append(event)

    async def publish_retry(self, payload, delay_seconds: int, routing_key: str, headers):
        self.retries.append((payload, delay_seconds, routing_key, headers))


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

    message = InboundMessage(
        body={"event_id": "evt-1", "certificate_id": "cert-1", "payload": {}},
        routing_key="test.routing",
        headers={},
    )

    await handler.handle(message)

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

    message = InboundMessage(
        body={"event_id": "evt-dup", "certificate_id": "cert-dup", "payload": {}},
        routing_key="test.routing",
        headers={},
    )

    await handler.handle(message)
    await handler.handle(message)

    assert len(publisher.completions) == 1
    assert publisher.retries == []


@pytest.mark.asyncio
async def test_handler_failure_schedules_retry_with_backoff(monkeypatch):
    from cert_orchestrator.config import settings

    session = build_session()

    @contextmanager
    def _session_provider():
        yield session

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", _session_provider)

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)

    message = InboundMessage(
        body={"event_id": "evt-fail", "certificate_id": "cert-fail", "payload": {"should_fail": True}},
        routing_key="test.routing",
        headers={"x-test": "value"},
    )

    await handler.handle(message)

    assert publisher.completions == []
    assert len(publisher.retries) == 1
    retry_payload, delay, routing_key, headers = publisher.retries[0]
    assert retry_payload["event_id"] == "evt-fail:retry:1"
    assert delay == settings.backoff_seconds
    assert routing_key == "test.routing"
    assert headers == {"x-test": "value"}


@pytest.mark.asyncio
async def test_handler_receive_routing_key_in_retry(monkeypatch):
    session = build_session()

    @contextmanager
    def _session_provider():
        yield session

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", _session_provider)

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)

    message = InboundMessage(
        body={"event_id": "evt-key", "certificate_id": "cert-key", "payload": {"should_fail": True}},
        routing_key="cert.lifecycle.events",
        headers={},
    )

    await handler.handle(message)

    retry_payload, delay, routing_key, headers = publisher.retries[0]
    assert routing_key == "cert.lifecycle.events"


@pytest.mark.asyncio
async def test_handler_publish_completion_before_commit(monkeypatch):
    session = build_session()
    commit_called = False

    original_commit = session.commit

    def commit_wrapper():
        nonlocal commit_called
        commit_called = True
        return original_commit()

    session.commit = commit_wrapper

    @contextmanager
    def _session_provider():
        yield session

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", _session_provider)

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)

    message = InboundMessage(
        body={"event_id": "evt-pub", "certificate_id": "cert-pub", "payload": {}},
        routing_key="test.routing",
        headers={},
    )

    await handler.handle(message)

    assert publisher.completions
    assert commit_called
    assert len(publisher.completions) == 1


@pytest.mark.asyncio
async def test_handler_completion_publish_failure_does_not_commit(monkeypatch):
    session = build_session()

    @contextmanager
    def _session_provider():
        try:
            yield session
        finally:
            session.rollback()

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", _session_provider)

    class FailingPublisher(StubPublisher):
        async def publish_completion(self, event):
            raise RuntimeError("publish failed")

    publisher = FailingPublisher()
    handler = LifecycleMessageHandler(publisher)

    message = InboundMessage(
        body={"event_id": "evt-fail-pub", "certificate_id": "cert-fail-pub", "payload": {}},
        routing_key="test.routing",
        headers={},
    )

    with pytest.raises(RuntimeError, match="publish failed"):
        await handler.handle(message)

    processed = session.query(ProcessedEvent).filter_by(event_id="evt-fail-pub").first()
    assert processed is None

    publisher2 = StubPublisher()
    handler2 = LifecycleMessageHandler(publisher2)
    await handler2.handle(message)
    assert len(publisher2.completions) == 1


@pytest.mark.asyncio
async def test_handler_retry_publish_failure_does_not_commit(monkeypatch):
    session = build_session()

    @contextmanager
    def _session_provider():
        try:
            yield session
        finally:
            session.rollback()

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", _session_provider)

    class FailingPublisher(StubPublisher):
        async def publish_retry(self, payload, delay_seconds: int, routing_key: str, headers):
            raise RuntimeError("retry publish failed")

    publisher = FailingPublisher()
    handler = LifecycleMessageHandler(publisher)

    message = InboundMessage(
        body={"event_id": "evt-retry-fail", "certificate_id": "cert-retry-fail", "payload": {"should_fail": True}},
        routing_key="test.routing",
        headers={},
    )

    with pytest.raises(RuntimeError, match="retry publish failed"):
        await handler.handle(message)

    processed = session.query(ProcessedEvent).filter_by(event_id="evt-retry-fail").first()
    assert processed is None

    publisher2 = StubPublisher()
    handler2 = LifecycleMessageHandler(publisher2)
    await handler2.handle(message)
    assert len(publisher2.retries) == 1


@pytest.mark.asyncio
async def test_handler_completion_publish_succeeds_but_commit_fails(monkeypatch):
    session = build_session()

    @contextmanager
    def _session_provider():
        try:
            yield session
        finally:
            session.rollback()

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", _session_provider)

    original_commit = session.commit
    commit_call_count = 0

    def commit_wrapper():
        nonlocal commit_call_count
        commit_call_count += 1
        if commit_call_count == 1:
            raise RuntimeError("commit failed")
        return original_commit()

    session.commit = commit_wrapper

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)

    message = InboundMessage(
        body={
            "event_id": "evt-commit-fail-comp",
            "certificate_id": "cert-commit-fail-comp",
            "payload": {},
        },
        routing_key="test.routing",
        headers={},
    )

    with pytest.raises(RuntimeError, match="commit failed"):
        await handler.handle(message)

    assert len(publisher.completions) == 1

    processed = session.query(ProcessedEvent).filter_by(event_id="evt-commit-fail-comp").first()
    assert processed is None

    publisher2 = StubPublisher()
    handler2 = LifecycleMessageHandler(publisher2)
    await handler2.handle(message)
    assert len(publisher2.completions) == 1
    assert publisher2.completions[0].event_id == "evt-commit-fail-comp"


@pytest.mark.asyncio
async def test_handler_retry_publish_succeeds_but_commit_fails(monkeypatch):
    session = build_session()

    @contextmanager
    def _session_provider():
        try:
            yield session
        finally:
            session.rollback()

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", _session_provider)

    original_commit = session.commit
    commit_call_count = 0

    def commit_wrapper():
        nonlocal commit_call_count
        commit_call_count += 1
        if commit_call_count == 1:
            raise RuntimeError("commit failed")
        return original_commit()

    session.commit = commit_wrapper

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)

    message = InboundMessage(
        body={
            "event_id": "evt-commit-fail-retry",
            "certificate_id": "cert-commit-fail-retry",
            "payload": {"should_fail": True},
        },
        routing_key="test.routing",
        headers={},
    )

    with pytest.raises(RuntimeError, match="commit failed"):
        await handler.handle(message)

    assert len(publisher.retries) == 1

    processed = session.query(ProcessedEvent).filter_by(event_id="evt-commit-fail-retry").first()
    assert processed is None

    publisher2 = StubPublisher()
    handler2 = LifecycleMessageHandler(publisher2)
    await handler2.handle(message)
    assert len(publisher2.retries) == 1
    assert publisher2.retries[0][0]["event_id"].startswith("evt-commit-fail-retry")
