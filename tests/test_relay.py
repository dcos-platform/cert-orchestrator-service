import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy.orm import Session

from cert_orchestrator.config import Settings
from cert_orchestrator.messaging.relay import LAST_ERROR_MAX_LENGTH, OutboxRelay
from cert_orchestrator.models import OutboxMessage, OutboxState
from cert_orchestrator.persistence.outbox import OutboxRepository
from cert_orchestrator.schemas import CompletionEvent

RELAY_LOGGER = "cert_orchestrator.messaging.relay"


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
    assert alerts[0].event_id == "evt-alert"
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
