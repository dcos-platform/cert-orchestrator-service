import asyncio
import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy.orm import Session

from cert_orchestrator import models
from cert_orchestrator.config import Settings
from cert_orchestrator.messaging import relay as relay_module
from cert_orchestrator.messaging.inbound import HEADER_ORIGINAL_ROUTING_KEY, HEADER_ROOT_EVENT_ID
from cert_orchestrator.messaging.relay import LAST_ERROR_MAX_LENGTH, OutboxRelay
from cert_orchestrator.models import OutboxMessage, OutboxState
from cert_orchestrator.persistence.outbox import OutboxRepository
from cert_orchestrator.schemas import CompletionEvent

RELAY_LOGGER = "cert_orchestrator.messaging.relay"
T0 = datetime(2026, 11, 8, 12, 0, tzinfo=UTC)
PENDING_GAUGE = "cert_orchestrator_outbox_pending"
OLDEST_GAUGE = "cert_orchestrator_outbox_oldest_pending_age_seconds"
PUBLISHED = "cert_orchestrator_outbox_published_total"
PUBLISH_FAILURES = "cert_orchestrator_outbox_publish_failures_total"
PASS_FAILURES = "cert_orchestrator_relay_pass_failures_total"


class FakePublisher:
    """Records each publish attempt and raises for the message ids it was told to refuse."""

    def __init__(self, fail_message_ids: frozenset[str] = frozenset(), error: Exception | None = None) -> None:
        self.fail_message_ids = fail_message_ids
        self.error = error or RuntimeError("broker unavailable")
        self.attempts: list[str] = []
        self.published: list[str] = []

    async def publish_outbox_message(self, row: OutboxMessage) -> None:
        self.attempts.append(row.message_id)
        if row.message_id in self.fail_message_ids:
            raise self.error
        self.published.append(row.message_id)


def make_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "outbox_batch_size": 50,
        "outbox_poll_interval_seconds": 0.01,
        "outbox_backoff_base_seconds": 60.0,
        "outbox_backoff_max_seconds": 60.0,
        "outbox_alert_after_attempts": 5,
    }
    values.update(overrides)
    return Settings(**values)


def seed(session_factory: Callable[[], Session], *message_ids: str) -> None:
    with session_factory() as session:
        repository = OutboxRepository(session)
        for message_id in message_ids:
            event = CompletionEvent(event_id=message_id, certificate_id="cert-1", status="COMPLETED", retry_count=0)
            repository.enqueue_completion(event, None)
        session.commit()


def read_rows(session_factory: Callable[[], Session]) -> list[OutboxMessage]:
    with session_factory() as session:
        return list(session.query(OutboxMessage).order_by(OutboxMessage.id).all())


async def wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition was not reached in time")
        await asyncio.sleep(0.01)


def relay_records(caplog: pytest.LogCaptureFixture, level: int) -> list[logging.LogRecord]:
    return [record for record in caplog.records if record.name == RELAY_LOGGER and record.levelno == level]


@pytest.mark.asyncio
async def test_run_once_publishes_due_rows_and_marks_them_sent(session_factory):
    seed(session_factory, "evt-1", "evt-2")
    publisher = FakePublisher()
    relay = OutboxRelay(session_factory, publisher, make_settings())

    sent = await relay.run_once()

    assert sent == 2
    assert publisher.published == ["evt-1", "evt-2"]
    rows = read_rows(session_factory)
    assert [row.state for row in rows] == [OutboxState.SENT, OutboxState.SENT]
    assert all(row.sent_at is not None for row in rows)


@pytest.mark.asyncio
async def test_failed_publish_keeps_the_row_pending_and_backs_it_off(session_factory):
    seed(session_factory, "evt-f")
    publisher = FakePublisher(fail_message_ids=frozenset({"evt-f"}))
    relay = OutboxRelay(session_factory, publisher, make_settings())
    before = datetime.now(UTC).replace(tzinfo=None)

    assert await relay.run_once() == 0

    (row,) = read_rows(session_factory)
    assert row.state == OutboxState.PENDING
    assert row.sent_at is None
    assert row.attempt_count == 1
    assert row.last_error == "RuntimeError: broker unavailable"
    assert row.available_at >= before + timedelta(seconds=59)

    assert await relay.run_once() == 0
    assert publisher.attempts == ["evt-f"]
    (row,) = read_rows(session_factory)
    assert row.attempt_count == 1


@pytest.mark.asyncio
async def test_one_failing_row_does_not_stop_the_rest_of_the_batch(session_factory):
    seed(session_factory, "evt-a", "evt-b", "evt-c")
    publisher = FakePublisher(fail_message_ids=frozenset({"evt-b"}))
    relay = OutboxRelay(session_factory, publisher, make_settings())

    assert await relay.run_once() == 2

    states = {row.message_id: row.state for row in read_rows(session_factory)}
    assert states == {"evt-a": OutboxState.SENT, "evt-b": OutboxState.PENDING, "evt-c": OutboxState.SENT}


@pytest.mark.asyncio
async def test_batch_size_limits_how_many_rows_one_pass_claims(session_factory):
    seed(session_factory, *[f"evt-{index}" for index in range(5)])
    relay = OutboxRelay(session_factory, FakePublisher(), make_settings(outbox_batch_size=2))

    assert await relay.run_once() == 2

    assert [row.state for row in read_rows(session_factory)].count(OutboxState.SENT) == 2


@pytest.mark.asyncio
async def test_empty_outbox_returns_zero(session_factory):
    publisher = FakePublisher()
    relay = OutboxRelay(session_factory, publisher, make_settings())

    assert await relay.run_once() == 0
    assert publisher.attempts == []


@pytest.mark.asyncio
async def test_last_error_is_truncated_to_the_maximum_length(session_factory):
    seed(session_factory, "evt-long")
    publisher = FakePublisher(
        fail_message_ids=frozenset({"evt-long"}),
        error=RuntimeError("x" * (LAST_ERROR_MAX_LENGTH * 2)),
    )
    relay = OutboxRelay(session_factory, publisher, make_settings())

    await relay.run_once()

    (row,) = read_rows(session_factory)
    assert len(row.last_error) == LAST_ERROR_MAX_LENGTH


@pytest.mark.asyncio
async def test_commit_failure_propagates_and_leaves_rows_pending(session_factory):
    seed(session_factory, "evt-c")
    publisher = FakePublisher()

    def failing_factory() -> Session:
        session = session_factory()
        session.commit = _raise_commit_error  # type: ignore[method-assign]
        return session

    relay = OutboxRelay(failing_factory, publisher, make_settings())

    with pytest.raises(OSError, match="commit refused"):
        await relay.run_once()

    assert publisher.published == ["evt-c"]
    (row,) = read_rows(session_factory)
    assert row.state == OutboxState.PENDING


def _raise_commit_error() -> None:
    raise OSError("commit refused")


@pytest.mark.asyncio
async def test_alert_is_logged_once_at_the_threshold_and_the_row_stays_pending(session_factory, caplog):
    seed(session_factory, "evt-alert")
    publisher = FakePublisher(fail_message_ids=frozenset({"evt-alert"}))
    no_backoff = make_settings(outbox_backoff_base_seconds=0.0, outbox_backoff_max_seconds=0.0)
    relay = OutboxRelay(session_factory, publisher, no_backoff)

    with caplog.at_level(logging.ERROR, logger=RELAY_LOGGER):
        for _ in range(6):
            assert await relay.run_once() == 0

    alerts = relay_records(caplog, logging.ERROR)
    assert len(alerts) == 1
    assert alerts[0].getMessage() == "Outbox message evt-alert not published after 5 attempts"
    assert alerts[0].kind == "COMPLETION"
    (row,) = read_rows(session_factory)
    assert row.state == OutboxState.PENDING
    assert row.attempt_count == 6


@pytest.mark.asyncio
async def test_run_logs_a_failed_pass_with_traceback_and_keeps_running(session_factory, caplog):
    seed(session_factory, "evt-run")
    publisher = FakePublisher()
    relay = OutboxRelay(session_factory, publisher, make_settings())
    real_run_once = relay.run_once
    passes: list[int] = []

    async def flaky_once() -> int:
        passes.append(1)
        if len(passes) == 1:
            raise RuntimeError("database hiccup")
        return await real_run_once()

    relay.run_once = flaky_once  # type: ignore[method-assign]
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))

    with caplog.at_level(logging.ERROR, logger=RELAY_LOGGER):
        await wait_until(lambda: publisher.published == ["evt-run"])
        stop.set()
        relay.wake()
        await asyncio.wait_for(task, timeout=5.0)

    failures = relay_records(caplog, logging.ERROR)
    assert len(failures) == 1
    assert failures[0].getMessage() == "Outbox relay pass failed"
    assert failures[0].exc_info is not None


@pytest.mark.asyncio
async def test_wake_runs_a_pass_before_the_poll_interval(session_factory):
    relay = OutboxRelay(session_factory, FakePublisher(), make_settings(outbox_poll_interval_seconds=60))
    passes: list[int] = []

    async def counting_once() -> int:
        passes.append(1)
        return 0

    relay.run_once = counting_once  # type: ignore[method-assign]
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))

    await wait_until(lambda: len(passes) == 1)
    relay.wake()
    await wait_until(lambda: len(passes) == 2, timeout=2.0)

    stop.set()
    relay.wake()
    await asyncio.wait_for(task, timeout=2.0)


@pytest.mark.asyncio
async def test_wake_issued_during_a_pass_causes_another_pass_without_waiting(session_factory):
    relay = OutboxRelay(session_factory, FakePublisher(), make_settings(outbox_poll_interval_seconds=60))
    passes: list[int] = []

    async def wakes_itself_once() -> int:
        passes.append(1)
        if len(passes) == 1:
            relay.wake()
        return 0

    relay.run_once = wakes_itself_once  # type: ignore[method-assign]
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))

    await wait_until(lambda: len(passes) == 2, timeout=2.0)

    stop.set()
    relay.wake()
    await asyncio.wait_for(task, timeout=2.0)


@pytest.mark.asyncio
async def test_without_a_wake_a_pass_still_happens_after_the_poll_interval(session_factory):
    relay = OutboxRelay(session_factory, FakePublisher(), make_settings(outbox_poll_interval_seconds=0.05))
    passes: list[int] = []

    async def counting_once() -> int:
        passes.append(1)
        return 0

    relay.run_once = counting_once  # type: ignore[method-assign]
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))

    await wait_until(lambda: len(passes) >= 2, timeout=2.0)

    stop.set()
    relay.wake()
    await asyncio.wait_for(task, timeout=2.0)


@pytest.mark.asyncio
async def test_stop_while_waiting_returns_promptly_without_a_wake(session_factory):
    relay = OutboxRelay(session_factory, FakePublisher(), make_settings(outbox_poll_interval_seconds=60))
    passes: list[int] = []

    async def counting_once() -> int:
        passes.append(1)
        return 0

    relay.run_once = counting_once  # type: ignore[method-assign]
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))

    await wait_until(lambda: len(passes) == 1)
    stop.set()

    await asyncio.wait_for(task, timeout=2.0)
    assert len(passes) == 1


def seed_completion(session_factory: Callable[[], Session], message_id: str, correlation_id: str | None) -> None:
    with session_factory() as session:
        event = CompletionEvent(event_id=message_id, certificate_id="cert-1", status="COMPLETED", retry_count=0)
        OutboxRepository(session).enqueue_completion(event, correlation_id)
        session.commit()


def seed_retry(session_factory: Callable[[], Session], message_id: str, headers: dict[str, Any]) -> None:
    with session_factory() as session:
        payload = {"event_id": message_id, "certificate_id": "cert-1", "payload": {}}
        OutboxRepository(session).enqueue_retry(payload, 5, headers)
        session.commit()


def published_lines(json_log: Callable[[], list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [line for line in json_log() if line["message"] == "Outbox message published"]


@pytest.mark.asyncio
async def test_outbox_row_timing_uses_the_application_clock(session_factory, freeze_clock):
    frozen = datetime.now(UTC) - timedelta(hours=1)
    freeze_clock(models, frozen)
    seed_completion(session_factory, "evt-clock", None)

    (row,) = read_rows(session_factory)
    assert row.available_at == row.created_at == frozen.replace(tzinfo=None)

    publisher = FakePublisher(fail_message_ids=frozenset({"evt-clock"}))
    relay = OutboxRelay(session_factory, publisher, make_settings())
    freeze_clock(relay_module, frozen)
    assert await relay.run_once() == 0
    assert publisher.attempts == ["evt-clock"], "a row stamped with the application clock is due at that time"
    (row,) = read_rows(session_factory)
    assert row.available_at == (frozen + timedelta(seconds=60)).replace(tzinfo=None)

    freeze_clock(relay_module, frozen + timedelta(seconds=59))
    assert await relay.run_once() == 0
    assert publisher.attempts == ["evt-clock"], "not due one second before the backoff ends"

    freeze_clock(relay_module, frozen + timedelta(seconds=60))
    assert await relay.run_once() == 0
    assert publisher.attempts == ["evt-clock", "evt-clock"], "due exactly when the backoff ends"


@pytest.mark.asyncio
async def test_relay_failure_logging_is_throttled_and_recovery_logged_once(
    session_factory, monkeypatch, caplog, metric_value
):
    clock = {"now": 0.0}
    monkeypatch.setattr(relay_module, "monotonic", lambda: clock["now"])
    relay = OutboxRelay(session_factory, FakePublisher(), make_settings(relay_failure_log_interval_seconds=60))
    # (seconds since the previous pass, whether the pass succeeds). Passes at 0, 10, 30, 59 and 60 fail; the passes
    # at 61 and 62 succeed; the pass at 63 fails again, after the recovery.
    pending = [(0, False), (10, False), (20, False), (29, False), (1, False), (1, True), (1, True), (1, False)]
    stop = asyncio.Event()

    async def scripted_pass() -> int:
        advance, succeeds = pending.pop(0)
        clock["now"] += advance
        if not pending:
            stop.set()
        if succeeds:
            return 0
        raise RuntimeError("database unavailable")

    relay.run_once = scripted_pass  # type: ignore[method-assign]
    failures_before = metric_value(PASS_FAILURES)

    with caplog.at_level(logging.INFO, logger=RELAY_LOGGER):
        await asyncio.wait_for(relay.run(stop), timeout=5.0)

    failed = [
        record for record in relay_records(caplog, logging.ERROR) if record.getMessage() == "Outbox relay pass failed"
    ]
    recovered = [
        record for record in relay_records(caplog, logging.INFO) if record.getMessage() == "Outbox relay recovered"
    ]
    assert len(failed) == 3, "one ERROR at the start of the outage, one past the interval, one after the recovery"
    assert all(record.exc_info is not None for record in failed)
    assert len(recovered) == 1
    assert metric_value(PASS_FAILURES) - failures_before == 6, "every failed pass is counted, logged or not"


@pytest.mark.asyncio
async def test_each_pass_sets_the_gauges_and_draining_returns_them_to_zero(session_factory, freeze_clock, metric_value):
    freeze_clock(models, T0)
    seed_completion(session_factory, "evt-g1", None)
    seed_completion(session_factory, "evt-g2", None)
    seed_retry(session_factory, "evt-g3:retry:1", {})
    failing = FakePublisher(fail_message_ids=frozenset({"evt-g1", "evt-g2", "evt-g3:retry:1"}))
    freeze_clock(relay_module, T0 + timedelta(seconds=42))

    assert await OutboxRelay(session_factory, failing, make_settings()).run_once() == 0

    assert metric_value(PENDING_GAUGE, kind="COMPLETION") == 2, "rows backing off still count as waiting"
    assert metric_value(PENDING_GAUGE, kind="RETRY") == 1
    assert metric_value(OLDEST_GAUGE) == 42.0

    freeze_clock(relay_module, T0 + timedelta(seconds=200))
    assert await OutboxRelay(session_factory, FakePublisher(), make_settings()).run_once() == 3

    assert metric_value(PENDING_GAUGE, kind="COMPLETION") == 0
    assert metric_value(PENDING_GAUGE, kind="RETRY") == 0
    assert metric_value(OLDEST_GAUGE) == 0.0


@pytest.mark.asyncio
async def test_a_failed_commit_sets_no_gauge(session_factory, freeze_clock, metric_value):
    freeze_clock(models, T0)
    seed_completion(session_factory, "evt-k1", None)
    seed_completion(session_factory, "evt-k2", None)
    freeze_clock(relay_module, T0 + timedelta(seconds=5))
    failing = FakePublisher(fail_message_ids=frozenset({"evt-k1", "evt-k2"}))
    assert await OutboxRelay(session_factory, failing, make_settings()).run_once() == 0
    assert metric_value(PENDING_GAUGE, kind="COMPLETION") == 2

    seed_completion(session_factory, "evt-k3", None)
    seed_completion(session_factory, "evt-k4", None)
    seed_completion(session_factory, "evt-k5", None)

    def failing_factory() -> Session:
        session = session_factory()
        session.commit = _raise_commit_error  # type: ignore[method-assign]
        return session

    with pytest.raises(OSError, match="commit refused"):
        await OutboxRelay(failing_factory, FakePublisher(), make_settings()).run_once()

    assert metric_value(PENDING_GAUGE, kind="COMPLETION") == 2, "the gauge still shows the last committed pass"


@pytest.mark.asyncio
async def test_backlog_read_failure_after_commit_leaves_rows_sent_and_is_a_failed_pass(
    session_factory, monkeypatch, metric_value
):
    seed_completion(session_factory, "evt-read", None)

    def broken_backlog(self: OutboxRepository, now: datetime) -> None:
        raise RuntimeError("backlog query failed")

    monkeypatch.setattr(OutboxRepository, "pending_backlog", broken_backlog)
    failures_before = metric_value(PASS_FAILURES)
    relay = OutboxRelay(session_factory, FakePublisher(), make_settings())
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))

    await wait_until(lambda: metric_value(PASS_FAILURES) > failures_before)
    stop.set()
    relay.wake()
    await asyncio.wait_for(task, timeout=5.0)

    (row,) = read_rows(session_factory)
    assert row.state == OutboxState.SENT, "the publish was committed before the read failed"


@pytest.mark.asyncio
async def test_publish_counters_move_once_per_confirmed_publish_and_once_per_failure(session_factory, metric_value):
    seed_completion(session_factory, "evt-m1", None)
    seed_completion(session_factory, "evt-m2", None)
    published_before = metric_value(PUBLISHED, kind="COMPLETION")
    failures_before = metric_value(PUBLISH_FAILURES, kind="COMPLETION")
    relay = OutboxRelay(session_factory, FakePublisher(fail_message_ids=frozenset({"evt-m2"})), make_settings())

    assert await relay.run_once() == 1

    assert metric_value(PUBLISHED, kind="COMPLETION") - published_before == 1
    assert metric_value(PUBLISH_FAILURES, kind="COMPLETION") - failures_before == 1


@pytest.mark.asyncio
async def test_completion_publish_line_carries_its_header_correlation_id_and_event_id(session_factory, json_log):
    seed_completion(session_factory, "evt-cid", "corr-row")

    await OutboxRelay(session_factory, FakePublisher(), make_settings()).run_once()

    (line,) = published_lines(json_log)
    assert (line["correlationId"], line["eventId"], line["kind"]) == ("corr-row", "evt-cid", "COMPLETION")
    assert line["logger"] == RELAY_LOGGER


@pytest.mark.asyncio
async def test_retry_row_without_an_inbound_id_carries_the_root_event_id(session_factory, json_log):
    seed_retry(session_factory, "evt-r:retry:1", {HEADER_ROOT_EVENT_ID: "root-r", HEADER_ORIGINAL_ROUTING_KEY: "x"})

    await OutboxRelay(session_factory, FakePublisher(), make_settings()).run_once()

    (line,) = published_lines(json_log)
    assert (line["correlationId"], line["kind"]) == ("root-r", "RETRY")


@pytest.mark.asyncio
async def test_retry_row_whose_stored_inbound_id_is_invalid_carries_the_root_never_the_invalid_value(
    session_factory, json_log
):
    invalid = "c" * 37
    seed_retry(session_factory, "evt-bad:retry:1", {HEADER_ROOT_EVENT_ID: "root-bad", "x-correlation-id": invalid})

    await OutboxRelay(session_factory, FakePublisher(), make_settings()).run_once()

    (line,) = published_lines(json_log)
    assert line["correlationId"] == "root-bad"
    assert invalid not in json.dumps(line)


@pytest.mark.asyncio
async def test_row_with_neither_id_carries_no_correlation_id(session_factory, json_log):
    seed_completion(session_factory, "evt-none", None)

    await OutboxRelay(session_factory, FakePublisher(), make_settings()).run_once()

    (line,) = published_lines(json_log)
    assert "correlationId" not in line


@pytest.mark.asyncio
async def test_one_published_line_per_sent_row_and_a_failed_publish_logs_its_kind_and_event_id(
    session_factory, json_log
):
    seed_completion(session_factory, "evt-p1", "corr-p")
    seed_completion(session_factory, "evt-p2", "corr-p")

    publisher = FakePublisher(fail_message_ids=frozenset({"evt-p2"}))
    await OutboxRelay(session_factory, publisher, make_settings()).run_once()

    lines = json_log()
    assert [line["eventId"] for line in lines if line["message"] == "Outbox message published"] == ["evt-p1"]
    (failure,) = [line for line in lines if line["message"] == "Outbox publish failed; will retry"]
    assert (failure["eventId"], failure["kind"], failure["level"]) == ("evt-p2", "COMPLETION", "WARN")
