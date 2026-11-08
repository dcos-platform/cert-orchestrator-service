from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any, NamedTuple

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cert_orchestrator.config import settings
from cert_orchestrator.messaging.handlers import LifecycleMessageHandler
from cert_orchestrator.messaging.inbound import HEADER_ROOT_EVENT_ID, InboundMessage, UnsupportedOperationError
from cert_orchestrator.models import Base, CertificateLifecycle, LifecycleOperation, LifecycleState, ProcessedEvent


class RetryCall(NamedTuple):
    payload: dict
    delay_seconds: int
    routing_key: str
    headers: Mapping[str, Any]
    exchange: str


class StubPublisher:
    def __init__(self):
        self.completions = []
        self.retries: list[RetryCall] = []

    async def publish_completion(self, event):
        self.completions.append(event)

    async def publish_retry(self, payload, delay_seconds: int, routing_key: str, headers, exchange: str):
        self.retries.append(RetryCall(payload, delay_seconds, routing_key, headers, exchange))


@pytest.fixture
def three_retries(monkeypatch):
    """Fix max_retries at 3 and backoff at 5 seconds so retry chains are fully predictable."""
    monkeypatch.setattr(settings, "max_retries", 3)
    monkeypatch.setattr(settings, "backoff_seconds", 5)


def build_session() -> Session:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return Session(engine)


def use_session(monkeypatch, session: Session) -> None:
    @contextmanager
    def _session_provider():
        yield session

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", _session_provider)


def make_message(
    event_id: str,
    certificate_id: str,
    routing_key: str = "cert.created",
    should_fail: bool = False,
    headers: Mapping[str, Any] | None = None,
) -> InboundMessage:
    payload = {"should_fail": True} if should_fail else {}
    return InboundMessage(
        body={"event_id": event_id, "certificate_id": certificate_id, "payload": payload},
        routing_key=routing_key,
        headers=headers or {},
        exchange="cert.events",
    )


def inbound_from_retry(retry: RetryCall) -> InboundMessage:
    return InboundMessage(
        body=retry.payload,
        routing_key=retry.routing_key,
        headers=retry.headers,
        exchange=retry.exchange,
    )


async def drive_retry_chain(
    handler: LifecycleMessageHandler,
    publisher: StubPublisher,
    message: InboundMessage,
) -> None:
    """Handle the message, then feed every published retry back into the handler until none is left."""
    await handler.handle(message)
    fed = 0
    while fed < len(publisher.retries):
        retry = publisher.retries[fed]
        fed += 1
        await handler.handle(inbound_from_retry(retry))


def record_state(session: Session) -> tuple[LifecycleState, int]:
    row = session.query(CertificateLifecycle).one()
    return row.state, row.retry_count


async def exhaust_create(certificate_id: str) -> None:
    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)
    message = make_message(f"evt-create-{certificate_id}", certificate_id, should_fail=True)
    await drive_retry_chain(handler, publisher, message)


@pytest.mark.asyncio
async def test_handler_processes_new_event_and_publishes_completion(monkeypatch):
    session = build_session()
    use_session(monkeypatch, session)

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)

    await handler.handle(make_message("evt-1", "cert-1"))

    assert len(publisher.completions) == 1
    assert publisher.completions[0].status == "COMPLETED"
    assert publisher.retries == []


@pytest.mark.asyncio
async def test_handler_skips_duplicate_event(monkeypatch):
    session = build_session()
    use_session(monkeypatch, session)

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)
    message = make_message("evt-dup", "cert-dup")

    await handler.handle(message)
    await handler.handle(message)

    assert len(publisher.completions) == 1
    assert publisher.retries == []


@pytest.mark.asyncio
async def test_handler_failure_schedules_retry_with_backoff(monkeypatch):
    session = build_session()
    use_session(monkeypatch, session)

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)

    await handler.handle(make_message("evt-fail", "cert-fail", should_fail=True, headers={"x-test": "value"}))

    assert publisher.completions == []
    assert len(publisher.retries) == 1
    retry = publisher.retries[0]
    assert retry.payload["event_id"] == "evt-fail:retry:1"
    assert retry.delay_seconds == settings.backoff_seconds
    assert retry.routing_key == "cert.created"
    assert retry.headers == {"x-test": "value", HEADER_ROOT_EVENT_ID: "evt-fail"}
    assert retry.exchange == "cert.events"


@pytest.mark.asyncio
async def test_handler_receive_routing_key_in_retry(monkeypatch):
    session = build_session()
    use_session(monkeypatch, session)

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)

    await handler.handle(make_message("evt-key", "cert-key", routing_key="cert.renewed", should_fail=True))

    assert publisher.retries[0].routing_key == "cert.renewed"


@pytest.mark.asyncio
async def test_handler_publish_completion_before_commit(monkeypatch):
    session = build_session()
    order: list[str] = []
    original_commit = session.commit

    def commit_wrapper():
        order.append("commit")
        return original_commit()

    session.commit = commit_wrapper
    use_session(monkeypatch, session)

    class RecordingPublisher(StubPublisher):
        async def publish_completion(self, event):
            order.append("publish")
            await super().publish_completion(event)

    publisher = RecordingPublisher()
    handler = LifecycleMessageHandler(publisher)

    await handler.handle(make_message("evt-pub", "cert-pub"))

    assert order == ["publish", "commit"]
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
    message = make_message("evt-fail-pub", "cert-fail-pub")

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
        async def publish_retry(self, payload, delay_seconds: int, routing_key: str, headers, exchange: str):
            raise RuntimeError("retry publish failed")

    publisher = FailingPublisher()
    handler = LifecycleMessageHandler(publisher)
    message = make_message("evt-retry-fail", "cert-retry-fail", should_fail=True)

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
    message = make_message("evt-commit-fail-comp", "cert-commit-fail-comp")

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
    message = make_message("evt-commit-fail-retry", "cert-commit-fail-retry", should_fail=True)

    with pytest.raises(RuntimeError, match="commit failed"):
        await handler.handle(message)

    assert len(publisher.retries) == 1

    processed = session.query(ProcessedEvent).filter_by(event_id="evt-commit-fail-retry").first()
    assert processed is None

    publisher2 = StubPublisher()
    handler2 = LifecycleMessageHandler(publisher2)
    await handler2.handle(message)
    assert len(publisher2.retries) == 1
    assert publisher2.retries[0].payload["event_id"].startswith("evt-commit-fail-retry")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("routing_key", "operation"),
    [("cert.created", LifecycleOperation.CREATE), ("cert.renewed", LifecycleOperation.RENEW)],
)
async def test_create_and_renew_record_operation_and_publish_completion(monkeypatch, routing_key, operation):
    session = build_session()
    use_session(monkeypatch, session)

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)

    await handler.handle(make_message("evt-ok", "cert-ok", routing_key=routing_key))

    row = session.query(CertificateLifecycle).one()
    assert row.operation == operation
    assert row.state == LifecycleState.COMPLETED
    assert [event.status for event in publisher.completions] == ["COMPLETED"]
    assert publisher.retries == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("routing_key", "operation"),
    [("cert.revoked", LifecycleOperation.REVOKE), ("cert.expired", LifecycleOperation.EXPIRE)],
)
async def test_revoke_and_expire_record_completed_without_completion_or_retry(monkeypatch, routing_key, operation):
    session = build_session()
    use_session(monkeypatch, session)

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)

    await handler.handle(make_message("evt-rec", "cert-rec", routing_key=routing_key, should_fail=True))

    row = session.query(CertificateLifecycle).one()
    assert row.operation == operation
    assert row.state == LifecycleState.COMPLETED
    assert row.retry_count == 0
    assert row.last_error is None
    assert publisher.completions == []
    assert publisher.retries == []


@pytest.mark.asyncio
async def test_unsupported_routing_key_raises_before_any_session_is_opened(monkeypatch):
    session = build_session()
    opened: list[bool] = []

    @contextmanager
    def _session_provider():
        opened.append(True)
        yield session

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", _session_provider)

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)

    with pytest.raises(UnsupportedOperationError):
        await handler.handle(make_message("evt-unknown", "cert-unknown", routing_key="cert.unknown"))

    assert opened == []
    assert session.query(ProcessedEvent).count() == 0
    assert session.query(CertificateLifecycle).count() == 0
    assert publisher.completions == []
    assert publisher.retries == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("headers", "expected_root"),
    [({}, "evt-root"), ({HEADER_ROOT_EVENT_ID: "root-x"}, "root-x")],
)
async def test_record_is_keyed_by_root_header_or_event_id(monkeypatch, headers, expected_root):
    session = build_session()
    use_session(monkeypatch, session)

    handler = LifecycleMessageHandler(StubPublisher())
    await handler.handle(make_message("evt-root", "cert-root", headers=headers))

    assert session.query(CertificateLifecycle).one().root_event_id == expected_root


@pytest.mark.asyncio
async def test_retry_chain_ends_failed_on_one_record_with_root_on_every_retry(monkeypatch, three_retries):
    session = build_session()
    use_session(monkeypatch, session)

    publisher = StubPublisher()
    handler = LifecycleMessageHandler(publisher)

    await handler.handle(make_message("evt-chain", "cert-chain", should_fail=True))
    assert record_state(session) == (LifecycleState.PROCESSING, 1)

    await handler.handle(inbound_from_retry(publisher.retries[0]))
    assert record_state(session) == (LifecycleState.PROCESSING, 2)

    await handler.handle(inbound_from_retry(publisher.retries[1]))
    assert record_state(session) == (LifecycleState.FAILED, 3)

    assert [retry.delay_seconds for retry in publisher.retries] == [5, 10]
    # Each retry id extends the id of the event that produced it: f"{event.event_id}:retry:{retry_count}".
    assert [retry.payload["event_id"] for retry in publisher.retries] == [
        "evt-chain:retry:1",
        "evt-chain:retry:1:retry:2",
    ]
    assert all(retry.headers[HEADER_ROOT_EVENT_ID] == "evt-chain" for retry in publisher.retries)
    assert session.query(CertificateLifecycle).count() == 1
    assert [(event.status, event.retry_count) for event in publisher.completions] == [("FAILED", 3)]


@pytest.mark.asyncio
async def test_regression_zero_retries_bug_renew_after_exhausted_create_retries(monkeypatch, three_retries):
    session = build_session()
    use_session(monkeypatch, session)
    await exhaust_create("cert-x")

    renew_publisher = StubPublisher()
    renew = make_message("evt-renew", "cert-x", routing_key="cert.renewed", should_fail=True)
    await LifecycleMessageHandler(renew_publisher).handle(renew)

    renew_row = session.query(CertificateLifecycle).filter_by(operation=LifecycleOperation.RENEW).one()
    assert renew_row.state == LifecycleState.PROCESSING
    assert renew_row.retry_count == 1
    assert len(renew_publisher.retries) == 1
    assert renew_publisher.retries[0].payload["event_id"] == "evt-renew:retry:1"


@pytest.mark.asyncio
async def test_regression_zero_retries_bug_successful_renew_completes_with_zero_retries(monkeypatch, three_retries):
    session = build_session()
    use_session(monkeypatch, session)
    await exhaust_create("cert-x")

    renew_publisher = StubPublisher()
    renew = make_message("evt-renew-ok", "cert-x", routing_key="cert.renewed")
    await LifecycleMessageHandler(renew_publisher).handle(renew)

    renew_row = session.query(CertificateLifecycle).filter_by(operation=LifecycleOperation.RENEW).one()
    assert renew_row.state == LifecycleState.COMPLETED
    assert renew_row.retry_count == 0
    assert renew_publisher.retries == []
    assert [(event.status, event.retry_count) for event in renew_publisher.completions] == [("COMPLETED", 0)]
