import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import Mock

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from cert_orchestrator.config import settings
from cert_orchestrator.logging_context import CONTEXT_FIELDS, current_context
from cert_orchestrator.messaging.handlers import LifecycleMessageHandler
from cert_orchestrator.messaging.inbound import (
    HEADER_ATTEMPT,
    HEADER_CORRELATION_ID,
    HEADER_ORIGINAL_EXCHANGE,
    HEADER_ORIGINAL_ROUTING_KEY,
    HEADER_ROOT_EVENT_ID,
    HEADER_X_DEATH,
    InboundMessage,
    UnsupportedOperationError,
)
from cert_orchestrator.models import (
    Base,
    CertificateLifecycle,
    LifecycleOperation,
    LifecycleState,
    OutboxKind,
    OutboxMessage,
    OutboxState,
    ProcessedEvent,
)


@pytest.fixture
def engine(tmp_path) -> Iterator[Engine]:
    """File-backed database, so a second session sees only what the handler has committed."""
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'handler.db'}", future=True)
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def session(engine, monkeypatch) -> Session:
    handler_session = Session(engine)

    @contextmanager
    def _session_provider():
        try:
            yield handler_session
        finally:
            handler_session.rollback()

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", _session_provider)
    return handler_session


@pytest.fixture
def three_retries(monkeypatch):
    """Fix max_retries at 3 and backoff at 5 seconds so retry chains are fully predictable."""
    monkeypatch.setattr(settings, "max_retries", 3)
    monkeypatch.setattr(settings, "backoff_seconds", 5)


def make_message(
    event_id: str,
    certificate_id: str,
    routing_key: str = "cert.created",
    should_fail: bool = False,
    headers: dict | None = None,
) -> InboundMessage:
    payload = {"should_fail": True} if should_fail else {}
    return InboundMessage(
        body={"event_id": event_id, "certificate_id": certificate_id, "payload": payload},
        routing_key=routing_key,
        headers=headers or {},
        exchange="cert.events",
    )


def inbound_from_row(row: OutboxMessage) -> InboundMessage:
    """Feed a stored RETRY row back in, as the broker would deliver it."""
    return InboundMessage(
        body=json.loads(row.body),
        routing_key=row.headers[HEADER_ORIGINAL_ROUTING_KEY],
        headers=row.headers,
        exchange=row.headers[HEADER_ORIGINAL_EXCHANGE],
    )


def outbox_rows(engine: Engine) -> list[OutboxMessage]:
    with Session(engine) as reader:
        return list(reader.query(OutboxMessage).order_by(OutboxMessage.id).all())


def processed_event_ids(engine: Engine) -> list[str]:
    with Session(engine) as reader:
        return sorted(row.event_id for row in reader.query(ProcessedEvent).all())


def lifecycles(engine: Engine) -> list[CertificateLifecycle]:
    with Session(engine) as reader:
        return list(reader.query(CertificateLifecycle).order_by(CertificateLifecycle.id).all())


def next_outbox_id(engine: Engine) -> int:
    return max((row.id for row in outbox_rows(engine)), default=0) + 1


async def drive_retry_chain(engine: Engine, handler: LifecycleMessageHandler, message: InboundMessage) -> None:
    """Handle the message, then feed back every RETRY row that this chain produced, until none is left."""
    first_id = next_outbox_id(engine)
    await handler.handle(message)
    fed = 0
    while True:
        retries = [row for row in outbox_rows(engine) if row.kind == OutboxKind.RETRY and row.id >= first_id]
        if fed == len(retries):
            return
        await handler.handle(inbound_from_row(retries[fed]))
        fed += 1


async def exhaust_create(engine: Engine, certificate_id: str) -> None:
    await drive_retry_chain(
        engine,
        LifecycleMessageHandler(),
        make_message(f"evt-create-{certificate_id}", certificate_id, should_fail=True),
    )


@pytest.mark.asyncio
async def test_create_success_writes_one_completion_row_committed_with_its_processed_event(engine, session):
    wake = Mock()

    await LifecycleMessageHandler(wake=wake).handle(make_message("evt-1", "cert-1"))

    (row,) = outbox_rows(engine)
    assert row.kind == OutboxKind.COMPLETION
    assert row.state == OutboxState.PENDING
    assert row.message_id == "evt-1"
    assert row.delay_seconds is None
    expected_body = '{"event_id":"evt-1","certificate_id":"cert-1","status":"COMPLETED","retry_count":0,"error":null}'
    assert row.body == expected_body
    assert row.headers == {HEADER_CORRELATION_ID: "evt-1"}
    assert processed_event_ids(engine) == ["evt-1"]
    assert [lifecycle.state for lifecycle in lifecycles(engine)] == [LifecycleState.COMPLETED]
    wake.assert_called_once_with()


@pytest.mark.asyncio
async def test_failure_writes_one_retry_row_with_payload_delay_and_headers(engine, session):
    wake = Mock()
    inbound = {
        "x-test": "value",
        HEADER_CORRELATION_ID: "corr-in",
        HEADER_ATTEMPT: 2,
        HEADER_X_DEATH: [{"reason": "expired"}],
    }

    await LifecycleMessageHandler(wake=wake).handle(
        make_message("evt-fail", "cert-fail", should_fail=True, headers=inbound)
    )

    (row,) = outbox_rows(engine)
    assert row.kind == OutboxKind.RETRY
    assert row.state == OutboxState.PENDING
    assert row.message_id == "evt-fail:retry:1"
    assert row.delay_seconds == settings.backoff_seconds
    payload = json.loads(row.body)
    assert payload["event_id"] == "evt-fail:retry:1"
    assert payload["payload"] == {"should_fail": True}
    assert row.headers == {
        "x-test": "value",
        HEADER_CORRELATION_ID: "corr-in",
        HEADER_ROOT_EVENT_ID: "evt-fail",
        HEADER_ORIGINAL_ROUTING_KEY: "cert.created",
        HEADER_ORIGINAL_EXCHANGE: "cert.events",
    }
    assert processed_event_ids(engine) == ["evt-fail"]
    (lifecycle,) = lifecycles(engine)
    assert (lifecycle.state, lifecycle.retry_count) == (LifecycleState.PROCESSING, 1)
    wake.assert_called_once_with()


@pytest.mark.asyncio
async def test_renew_retry_keeps_its_routing_key(engine, session):
    await LifecycleMessageHandler().handle(
        make_message("evt-key", "cert-key", routing_key="cert.renewed", should_fail=True)
    )

    (row,) = outbox_rows(engine)
    assert row.headers[HEADER_ORIGINAL_ROUTING_KEY] == "cert.renewed"


@pytest.mark.asyncio
@pytest.mark.parametrize("inbound", ["corr-in", " padded-id ", "c" * 36])
async def test_valid_inbound_correlation_header_is_carried_to_the_completion_unchanged(engine, session, inbound):
    await LifecycleMessageHandler().handle(make_message("evt-c", "cert-c", headers={HEADER_CORRELATION_ID: inbound}))

    (row,) = outbox_rows(engine)
    assert row.headers == {HEADER_CORRELATION_ID: inbound}


@pytest.mark.asyncio
@pytest.mark.parametrize("inbound", ["", "   ", "c" * 37, 12345, b"corr-in"])
async def test_invalid_inbound_correlation_header_falls_back_to_the_root_event_id(engine, session, inbound):
    await LifecycleMessageHandler().handle(make_message("evt-c", "cert-c", headers={HEADER_CORRELATION_ID: inbound}))

    (row,) = outbox_rows(engine)
    assert row.headers == {HEADER_CORRELATION_ID: "evt-c"}


@pytest.mark.asyncio
async def test_invalid_root_event_id_and_no_inbound_header_leaves_completion_without_correlation(engine, session):
    await LifecycleMessageHandler().handle(make_message("evt-c", "cert-c", headers={HEADER_ROOT_EVENT_ID: "r" * 37}))

    (row,) = outbox_rows(engine)
    assert row.headers == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("routing_key", ["cert.revoked", "cert.expired"])
async def test_revoke_and_expire_write_no_row_and_no_wake(engine, session, routing_key):
    wake = Mock()

    await LifecycleMessageHandler(wake=wake).handle(
        make_message("evt-rec", "cert-rec", routing_key=routing_key, should_fail=True)
    )

    assert outbox_rows(engine) == []
    (lifecycle,) = lifecycles(engine)
    assert lifecycle.state == LifecycleState.COMPLETED
    assert lifecycle.retry_count == 0
    assert lifecycle.last_error is None
    wake.assert_not_called()


@pytest.mark.asyncio
async def test_duplicate_event_writes_no_second_row_and_no_second_wake(engine, session):
    wake = Mock()
    handler = LifecycleMessageHandler(wake=wake)
    message = make_message("evt-dup", "cert-dup")

    await handler.handle(message)
    await handler.handle(message)

    assert len(outbox_rows(engine)) == 1
    wake.assert_called_once_with()


@pytest.mark.asyncio
async def test_unsupported_routing_key_raises_before_any_session_is_opened(engine, monkeypatch):
    opened: list[bool] = []

    @contextmanager
    def _session_provider():
        opened.append(True)
        yield Session(engine)

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", _session_provider)
    wake = Mock()

    with pytest.raises(UnsupportedOperationError):
        await LifecycleMessageHandler(wake=wake).handle(
            make_message("evt-unknown", "cert-unknown", routing_key="cert.unknown")
        )

    assert opened == []
    assert outbox_rows(engine) == []
    assert processed_event_ids(engine) == []
    wake.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("headers", "expected_root"),
    [({}, "evt-root"), ({HEADER_ROOT_EVENT_ID: "root-x"}, "root-x")],
)
async def test_record_is_keyed_by_root_header_or_event_id(engine, session, headers, expected_root):
    await LifecycleMessageHandler().handle(make_message("evt-root", "cert-root", headers=headers))

    (lifecycle,) = lifecycles(engine)
    assert lifecycle.root_event_id == expected_root
    assert lifecycle.operation == LifecycleOperation.CREATE


@pytest.mark.asyncio
async def test_wake_is_not_called_when_the_commit_fails(session, monkeypatch):
    def failing_commit() -> None:
        raise RuntimeError("commit failed")

    monkeypatch.setattr(session, "commit", failing_commit)
    wake = Mock()

    with pytest.raises(RuntimeError, match="commit failed"):
        await LifecycleMessageHandler(wake=wake).handle(make_message("evt-cf", "cert-cf"))

    wake.assert_not_called()


@pytest.mark.asyncio
async def test_handler_without_wake_still_writes_the_row(engine, session):
    await LifecycleMessageHandler().handle(make_message("evt-nw", "cert-nw", should_fail=True))

    assert len(outbox_rows(engine)) == 1


@pytest.mark.asyncio
async def test_handler_exception_leaves_no_outbox_row_and_no_processed_event(engine, session, monkeypatch):
    """A failure at commit, after the outbox row was flushed, leaves nothing behind; redelivery then writes it once."""
    original_commit = session.commit
    attempts: list[int] = []

    def commit_failing_first_time() -> None:
        attempts.append(1)
        if len(attempts) == 1:
            # The row must already be flushed into this transaction when the commit fails.
            assert session.query(OutboxMessage).count() == 1
            raise RuntimeError("commit failed")
        original_commit()

    monkeypatch.setattr(session, "commit", commit_failing_first_time)
    wake = Mock()
    handler = LifecycleMessageHandler(wake=wake)

    with pytest.raises(RuntimeError, match="commit failed"):
        await handler.handle(make_message("evt-l9", "cert-l9"))

    assert outbox_rows(engine) == []
    assert processed_event_ids(engine) == []
    wake.assert_not_called()

    await handler.handle(make_message("evt-l9", "cert-l9"))

    (row,) = outbox_rows(engine)
    assert row.message_id == "evt-l9"
    assert processed_event_ids(engine) == ["evt-l9"]
    wake.assert_called_once_with()


@pytest.mark.asyncio
async def test_retry_chain_ends_failed_on_one_lifecycle_with_root_on_every_retry(engine, session, three_retries):
    handler = LifecycleMessageHandler()
    await handler.handle(
        make_message("evt-chain", "cert-chain", should_fail=True, headers={HEADER_CORRELATION_ID: "corr-chain"})
    )
    assert [(lc.state, lc.retry_count) for lc in lifecycles(engine)] == [(LifecycleState.PROCESSING, 1)]

    (first_retry,) = outbox_rows(engine)
    await handler.handle(inbound_from_row(first_retry))
    assert [(lc.state, lc.retry_count) for lc in lifecycles(engine)] == [(LifecycleState.PROCESSING, 2)]

    retries = [row for row in outbox_rows(engine) if row.kind == OutboxKind.RETRY]
    await handler.handle(inbound_from_row(retries[1]))

    assert [(lc.state, lc.retry_count) for lc in lifecycles(engine)] == [(LifecycleState.FAILED, 3)]
    retries = [row for row in outbox_rows(engine) if row.kind == OutboxKind.RETRY]
    assert [row.delay_seconds for row in retries] == [5, 10]
    assert [json.loads(row.body)["event_id"] for row in retries] == [
        "evt-chain:retry:1",
        "evt-chain:retry:1:retry:2",
    ]
    assert all(row.headers[HEADER_ROOT_EVENT_ID] == "evt-chain" for row in retries)
    completions = [row for row in outbox_rows(engine) if row.kind == OutboxKind.COMPLETION]
    assert [json.loads(row.body)["status"] for row in completions] == ["FAILED"]
    assert [json.loads(row.body)["retry_count"] for row in completions] == [3]
    assert [row.headers for row in completions] == [{HEADER_CORRELATION_ID: "corr-chain"}]


@pytest.mark.asyncio
async def test_regression_zero_retries_bug_renew_after_exhausted_create_retries(engine, session, three_retries):
    await exhaust_create(engine, "cert-x")

    renew = make_message("evt-renew", "cert-x", routing_key="cert.renewed", should_fail=True)
    await LifecycleMessageHandler().handle(renew)

    renew_lifecycle = [lc for lc in lifecycles(engine) if lc.operation == LifecycleOperation.RENEW]
    assert [(lc.state, lc.retry_count) for lc in renew_lifecycle] == [(LifecycleState.PROCESSING, 1)]
    renew_retries = [row for row in outbox_rows(engine) if row.message_id.startswith("evt-renew:retry:")]
    assert [row.message_id for row in renew_retries] == ["evt-renew:retry:1"]


@pytest.mark.asyncio
async def test_regression_zero_retries_bug_successful_renew_completes_with_zero_retries(engine, session, three_retries):
    await exhaust_create(engine, "cert-x")

    await LifecycleMessageHandler().handle(make_message("evt-renew-ok", "cert-x", routing_key="cert.renewed"))

    renew_lifecycle = [lc for lc in lifecycles(engine) if lc.operation == LifecycleOperation.RENEW]
    assert [(lc.state, lc.retry_count) for lc in renew_lifecycle] == [(LifecycleState.COMPLETED, 0)]
    completion = [row for row in outbox_rows(engine) if row.message_id == "evt-renew-ok"]
    assert [json.loads(row.body)["status"] for row in completion] == ["COMPLETED"]
    assert [json.loads(row.body)["retry_count"] for row in completion] == [0]


CONSUMED_TOTAL = "cert_orchestrator_events_consumed_total"
TRANSITIONS_TOTAL = "cert_orchestrator_lifecycle_transitions_total"
PROCESSING_COUNT = "cert_orchestrator_event_processing_seconds_count"


def processing_lines(json_log: Callable[[], list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [line for line in json_log() if line["message"] == "Processed lifecycle event"]


@pytest.mark.asyncio
async def test_processed_line_carries_the_context_and_the_completion_correlation_id(engine, session, json_log):
    await LifecycleMessageHandler().handle(
        make_message("evt-ctx", "cert-ctx", headers={HEADER_CORRELATION_ID: "corr-ctx"})
    )

    (row,) = outbox_rows(engine)
    (line,) = processing_lines(json_log)
    assert line["correlationId"] == row.headers[HEADER_CORRELATION_ID] == "corr-ctx"
    assert (line["certificateId"], line["eventId"], line["operation"], line["state"]) == (
        "cert-ctx",
        "evt-ctx",
        "CREATE",
        "COMPLETED",
    )
    assert current_context() == dict.fromkeys(CONTEXT_FIELDS), "the context is restored once handling returns"


@pytest.mark.asyncio
async def test_a_retried_attempt_without_an_inbound_id_logs_the_root_event_id(engine, session, json_log, three_retries):
    handler = LifecycleMessageHandler()
    await handler.handle(make_message("evt-root-log", "cert-root-log", should_fail=True))
    (retry,) = outbox_rows(engine)

    await handler.handle(inbound_from_row(retry))

    lines = processing_lines(json_log)
    assert [line["correlationId"] for line in lines] == ["evt-root-log", "evt-root-log"]
    assert [line["state"] for line in lines] == ["PROCESSING", "PROCESSING"]


@pytest.mark.asyncio
async def test_duplicate_skip_line_carries_the_context_and_no_state(engine, session, json_log):
    handler = LifecycleMessageHandler()
    message = make_message("evt-dup-log", "cert-dup-log", headers={HEADER_CORRELATION_ID: "corr-dup"})
    await handler.handle(message)

    await handler.handle(message)

    (skip,) = [line for line in json_log() if line["message"] == "Skipping duplicate event"]
    assert (skip["correlationId"], skip["certificateId"], skip["eventId"], skip["operation"]) == (
        "corr-dup",
        "cert-dup-log",
        "evt-dup-log",
        "CREATE",
    )
    assert "state" not in skip


@pytest.mark.asyncio
async def test_a_chain_with_no_valid_correlation_id_logs_none(engine, session, json_log):
    message = make_message(
        "evt-none",
        "cert-none",
        headers={HEADER_CORRELATION_ID: "c" * 37, HEADER_ROOT_EVENT_ID: "r" * 37},
    )

    await LifecycleMessageHandler().handle(message)

    (line,) = processing_lines(json_log)
    assert "correlationId" not in line


@pytest.mark.asyncio
async def test_an_invalid_body_raises_before_any_context_is_set(engine, session, json_log):
    bad = InboundMessage(
        body={"certificate_id": "cert-bad"},
        routing_key="cert.created",
        headers={HEADER_CORRELATION_ID: "corr-bad"},
        exchange="cert.events",
    )

    with pytest.raises(ValidationError):
        await LifecycleMessageHandler().handle(bad)

    assert json_log() == []
    assert current_context() == dict.fromkeys(CONTEXT_FIELDS)


@pytest.mark.asyncio
async def test_a_processed_create_moves_the_processed_transition_and_timing_counters_by_one(
    engine, session, metric_value
):
    before = (
        metric_value(CONSUMED_TOTAL, operation="CREATE", outcome="processed"),
        metric_value(TRANSITIONS_TOTAL, operation="CREATE", state="COMPLETED"),
        metric_value(PROCESSING_COUNT, operation="CREATE"),
    )

    await LifecycleMessageHandler().handle(make_message("evt-m-ok", "cert-m-ok"))

    after = (
        metric_value(CONSUMED_TOTAL, operation="CREATE", outcome="processed"),
        metric_value(TRANSITIONS_TOTAL, operation="CREATE", state="COMPLETED"),
        metric_value(PROCESSING_COUNT, operation="CREATE"),
    )
    assert [a - b for a, b in zip(after, before, strict=True)] == [1, 1, 1]


@pytest.mark.asyncio
async def test_a_duplicate_moves_only_the_duplicate_and_timing_counters(engine, session, metric_value):
    handler = LifecycleMessageHandler()
    message = make_message("evt-m-dup", "cert-m-dup")
    await handler.handle(message)
    before = (
        metric_value(CONSUMED_TOTAL, operation="CREATE", outcome="duplicate"),
        metric_value(CONSUMED_TOTAL, operation="CREATE", outcome="processed"),
        metric_value(TRANSITIONS_TOTAL, operation="CREATE", state="COMPLETED"),
        metric_value(PROCESSING_COUNT, operation="CREATE"),
    )

    await handler.handle(message)

    after = (
        metric_value(CONSUMED_TOTAL, operation="CREATE", outcome="duplicate"),
        metric_value(CONSUMED_TOTAL, operation="CREATE", outcome="processed"),
        metric_value(TRANSITIONS_TOTAL, operation="CREATE", state="COMPLETED"),
        metric_value(PROCESSING_COUNT, operation="CREATE"),
    )
    assert [a - b for a, b in zip(after, before, strict=True)] == [1, 0, 0, 1]


@pytest.mark.asyncio
async def test_a_retry_chain_ending_failed_moves_processing_and_failed_by_the_expected_amounts(
    engine, session, metric_value, three_retries
):
    def snapshot() -> tuple[float, ...]:
        return (
            metric_value(TRANSITIONS_TOTAL, operation="CREATE", state="PROCESSING"),
            metric_value(TRANSITIONS_TOTAL, operation="CREATE", state="FAILED"),
            metric_value(CONSUMED_TOTAL, operation="CREATE", outcome="processed"),
            metric_value(PROCESSING_COUNT, operation="CREATE"),
        )

    before = snapshot()

    await drive_retry_chain(
        engine, LifecycleMessageHandler(), make_message("evt-m-chain", "cert-m-chain", should_fail=True)
    )

    assert [a - b for a, b in zip(snapshot(), before, strict=True)] == [2, 1, 3, 3]


@pytest.mark.asyncio
async def test_a_revoke_moves_its_transition_and_writes_no_outbox_row(engine, session, metric_value):
    before = metric_value(TRANSITIONS_TOTAL, operation="REVOKE", state="COMPLETED")

    await LifecycleMessageHandler().handle(make_message("evt-m-rev", "cert-m-rev", routing_key="cert.revoked"))

    assert metric_value(TRANSITIONS_TOTAL, operation="REVOKE", state="COMPLETED") - before == 1


@pytest.mark.asyncio
async def test_a_rolled_back_handling_moves_timing_but_no_processed_or_transition_counter(
    engine, session, monkeypatch, metric_value
):
    def failing_commit() -> None:
        raise RuntimeError("commit failed")

    monkeypatch.setattr(session, "commit", failing_commit)
    before = (
        metric_value(CONSUMED_TOTAL, operation="CREATE", outcome="processed"),
        metric_value(TRANSITIONS_TOTAL, operation="CREATE", state="COMPLETED"),
        metric_value(PROCESSING_COUNT, operation="CREATE"),
    )

    with pytest.raises(RuntimeError, match="commit failed"):
        await LifecycleMessageHandler().handle(make_message("evt-m-rb", "cert-m-rb"))

    after = (
        metric_value(CONSUMED_TOTAL, operation="CREATE", outcome="processed"),
        metric_value(TRANSITIONS_TOTAL, operation="CREATE", state="COMPLETED"),
        metric_value(PROCESSING_COUNT, operation="CREATE"),
    )
    assert [a - b for a, b in zip(after, before, strict=True)] == [0, 0, 1]


@pytest.mark.asyncio
async def test_an_unsupported_routing_key_moves_no_processing_timer(engine, session, metric_value):
    before = metric_value(PROCESSING_COUNT, operation="CREATE")

    with pytest.raises(UnsupportedOperationError):
        await LifecycleMessageHandler().handle(make_message("evt-m-unk", "cert-m-unk", routing_key="cert.unknown"))

    assert metric_value(PROCESSING_COUNT, operation="CREATE") == before
