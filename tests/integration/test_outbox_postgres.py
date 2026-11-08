import asyncio
import contextlib
import json
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import aio_pika
import pytest
from sqlalchemy import delete, text
from sqlalchemy.orm import Session

from cert_orchestrator.config import Settings
from cert_orchestrator.db import SessionLocal
from cert_orchestrator.messaging.handlers import LifecycleMessageHandler
from cert_orchestrator.messaging.inbound import (
    HEADER_CORRELATION_ID,
    HEADER_ORIGINAL_ROUTING_KEY,
    HEADER_ROOT_EVENT_ID,
    InboundMessage,
)
from cert_orchestrator.messaging.relay import OutboxRelay
from cert_orchestrator.models import (
    CertificateLifecycle,
    LifecycleState,
    OutboxKind,
    OutboxMessage,
    OutboxState,
    ProcessedEvent,
)
from cert_orchestrator.persistence.outbox import OutboxRepository
from cert_orchestrator.schemas import CompletionEvent


@dataclass(frozen=True)
class RunIds:
    """Identifiers unique to one test run, so rows left by other runs never match."""

    uid: str
    event_id: str
    certificate_id: str
    correlation_id: str


@pytest.fixture
def run_ids() -> Iterator[RunIds]:
    uid = uuid.uuid4().hex[:12]
    ids = RunIds(
        uid=uid,
        event_id=f"evt-{uid}",
        certificate_id=f"cert-{uid}",
        correlation_id=str(uuid.uuid4()),
    )
    yield ids
    _delete_rows(uid)


@pytest.fixture
def relay_settings(integration_settings: Settings) -> Settings:
    """Shrink the backoff so a row that failed once is due again within the test's wait."""
    return integration_settings.model_copy(
        update={"outbox_backoff_base_seconds": 0.1, "outbox_backoff_max_seconds": 1.0}
    )


class RefusingPublisher:
    """Stands in for a broker that refuses every publish."""

    async def publish_outbox_message(self, row: OutboxMessage) -> None:
        raise ConnectionError("broker refused the publish")


class GatedPublisher:
    """Holds its first publish until the other relay has claimed its rows, so both claims overlap."""

    def __init__(self, claimed_here: asyncio.Event, claimed_elsewhere: asyncio.Event) -> None:
        self.claimed_here = claimed_here
        self.claimed_elsewhere = claimed_elsewhere
        self.published: list[str] = []
        self._gated = False

    async def publish_outbox_message(self, row: OutboxMessage) -> None:
        if not self._gated:
            self._gated = True
            self.claimed_here.set()
            await asyncio.wait_for(self.claimed_elsewhere.wait(), timeout=5.0)
        self.published.append(row.message_id)


def session_with_lock_timeout() -> Session:
    """Open a session whose lock waits fail after 5 s, so a missing SKIP LOCKED fails instead of hanging the loop."""
    session = SessionLocal()
    session.execute(text("SET LOCAL lock_timeout = '5s'"))
    return session


def session_whose_first_commit_fails() -> Session:
    """Open a session whose first commit raises, simulating a crash after the publish and before the mark."""
    session = SessionLocal()
    original_commit = session.commit
    failed: list[bool] = []

    def commit_once() -> None:
        if not failed:
            failed.append(True)
            raise RuntimeError("forced commit failure")
        original_commit()

    session.commit = commit_once
    return session


def _delete_rows(uid: str) -> None:
    pattern = f"%{uid}%"
    with SessionLocal() as session:
        session.execute(delete(OutboxMessage).where(OutboxMessage.message_id.like(pattern)))
        session.execute(delete(ProcessedEvent).where(ProcessedEvent.event_id.like(pattern)))
        session.execute(delete(CertificateLifecycle).where(CertificateLifecycle.certificate_id.like(pattern)))
        session.commit()


def lifecycle_body(event_id: str, certificate_id: str, should_fail: bool = False) -> dict[str, Any]:
    return {"event_id": event_id, "certificate_id": certificate_id, "payload": {"should_fail": should_fail}}


def create_message(run: RunIds, headers: dict[str, Any] | None = None, should_fail: bool = False) -> InboundMessage:
    return InboundMessage(
        body=lifecycle_body(run.event_id, run.certificate_id, should_fail),
        routing_key="cert.created",
        headers=headers or {},
        exchange="cert.events",
    )


def lifecycle_amqp_message(event_id: str, certificate_id: str, should_fail: bool = False) -> aio_pika.Message:
    return aio_pika.Message(
        body=json.dumps(lifecycle_body(event_id, certificate_id, should_fail)).encode("utf-8"),
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )


def outbox_rows_matching(text_in_message_id: str) -> list[OutboxMessage]:
    """Read the outbox rows whose message id contains the given text, using a fresh session."""
    with SessionLocal() as session:
        query = session.query(OutboxMessage).filter(OutboxMessage.message_id.like(f"%{text_in_message_id}%"))
        return list(query.order_by(OutboxMessage.id).all())


def lifecycle_state_of(certificate_id: str) -> LifecycleState | None:
    with SessionLocal() as session:
        row = session.query(CertificateLifecycle).filter_by(certificate_id=certificate_id).one_or_none()
        return None if row is None else row.state


async def get_message_within(queue: Any, seconds: float) -> Any:
    """Return the next message from the queue within the time limit, or None."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        message = await queue.get(fail=False)
        if message is not None:
            return message
        await asyncio.sleep(0.1)
    return None


async def wait_for_lifecycle_state(certificate_id: str, state: LifecycleState, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if lifecycle_state_of(certificate_id) == state:
            return True
        await asyncio.sleep(0.1)
    return False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dod15_relay_delivers_completion_after_publish_fails_post_commit(
    client_with_cleanup, relay_settings, run_ids
):
    """DoD 15: a completion committed before any publish is delivered once the broker accepts it."""
    broker = client_with_cleanup
    await LifecycleMessageHandler().handle(
        create_message(run_ids, headers={HEADER_CORRELATION_ID: run_ids.correlation_id})
    )

    (row,) = outbox_rows_matching(run_ids.event_id)
    assert row.kind == OutboxKind.COMPLETION
    assert row.state == OutboxState.PENDING
    assert row.attempt_count == 0

    assert await OutboxRelay(SessionLocal, RefusingPublisher(), relay_settings).run_once() == 0
    (row,) = outbox_rows_matching(run_ids.event_id)
    assert row.state == OutboxState.PENDING
    assert row.attempt_count == 1
    assert row.last_error is not None

    await asyncio.sleep(0.3)  # the 0.1 s backoff after one failure has elapsed
    assert await OutboxRelay(SessionLocal, broker, relay_settings).run_once() == 1

    completions = await broker.channel.get_queue(relay_settings.completion_queue)
    message = await get_message_within(completions, 5.0)
    assert message is not None, "completion did not arrive on the completion queue"
    assert message.message_id == run_ids.event_id
    assert message.headers[HEADER_CORRELATION_ID] == run_ids.correlation_id
    assert message.body == row.body.encode("utf-8")
    await message.ack()

    (row,) = outbox_rows_matching(run_ids.event_id)
    assert row.state == OutboxState.SENT
    assert row.sent_at is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_crash_after_publish_before_mark_sent_republishes_same_message_id(
    client_with_cleanup, relay_settings, run_ids
):
    """A crash after the publish but before the mark-sent commit republishes the row with the same message id."""
    broker = client_with_cleanup
    await LifecycleMessageHandler().handle(create_message(run_ids))

    crashing = OutboxRelay(session_whose_first_commit_fails, broker, relay_settings)
    with pytest.raises(RuntimeError, match="forced commit failure"):
        await crashing.run_once()

    (row,) = outbox_rows_matching(run_ids.event_id)
    assert row.state == OutboxState.PENDING
    assert row.attempt_count == 0

    assert await OutboxRelay(SessionLocal, broker, relay_settings).run_once() == 1
    (row,) = outbox_rows_matching(run_ids.event_id)
    assert row.state == OutboxState.SENT

    completions = await broker.channel.get_queue(relay_settings.completion_queue)
    first = await get_message_within(completions, 5.0)
    second = await get_message_within(completions, 5.0)
    assert first is not None and second is not None, "expected the completion twice"
    assert first.message_id == second.message_id == run_ids.event_id
    await first.ack()
    await second.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_skip_locked_parallel_relays_publish_each_row_once(relay_settings, run_ids):
    """Two relays with overlapping open transactions claim disjoint rows, so each row is published once."""
    with SessionLocal() as session:
        assert session.query(OutboxMessage).filter_by(state=OutboxState.PENDING).count() == 0

    message_ids = [f"{run_ids.event_id}-{index}" for index in range(10)]
    with SessionLocal() as session:
        repository = OutboxRepository(session)
        for message_id in message_ids:
            repository.enqueue_completion(
                CompletionEvent(
                    event_id=message_id,
                    certificate_id=run_ids.certificate_id,
                    status="COMPLETED",
                    retry_count=0,
                ),
                None,
            )
        session.commit()

    batch_settings = relay_settings.model_copy(update={"outbox_batch_size": 5})
    a_claimed = asyncio.Event()
    b_claimed = asyncio.Event()
    publisher_a = GatedPublisher(a_claimed, b_claimed)
    publisher_b = GatedPublisher(b_claimed, a_claimed)
    relay_a = OutboxRelay(session_with_lock_timeout, publisher_a, batch_settings)
    relay_b = OutboxRelay(session_with_lock_timeout, publisher_b, batch_settings)

    sent_a, sent_b = await asyncio.gather(relay_a.run_once(), relay_b.run_once())

    assert (sent_a, sent_b) == (5, 5)
    published = publisher_a.published + publisher_b.published
    assert sorted(published) == sorted(message_ids), "each of the 10 rows is published exactly once"
    rows = outbox_rows_matching(run_ids.event_id)
    assert len(rows) == 10
    assert all(row.state == OutboxState.SENT for row in rows)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_business_retry_through_outbox_arrives_after_delay_with_headers(
    client_with_cleanup, relay_settings, run_ids
):
    """A failed create's retry leaves the outbox after its delay with the root id, routing key and correlation id."""
    broker = client_with_cleanup
    await LifecycleMessageHandler().handle(
        create_message(run_ids, headers={HEADER_CORRELATION_ID: run_ids.correlation_id}, should_fail=True)
    )

    (row,) = outbox_rows_matching(run_ids.event_id)
    assert row.kind == OutboxKind.RETRY
    assert row.message_id == f"{run_ids.event_id}:retry:1"
    delay = row.delay_seconds

    incoming = await broker.channel.get_queue(relay_settings.incoming_queue)
    start = time.monotonic()
    assert await OutboxRelay(SessionLocal, broker, relay_settings).run_once() == 1
    message = await get_message_within(incoming, delay + 3.0)
    elapsed = time.monotonic() - start

    assert message is not None, "retry did not arrive on the incoming queue"
    assert elapsed >= delay - 0.2
    assert message.message_id == f"{run_ids.event_id}:retry:1"
    assert message.headers[HEADER_ROOT_EVENT_ID] == run_ids.event_id
    assert message.headers[HEADER_ORIGINAL_ROUTING_KEY] == "cert.created"
    assert message.headers[HEADER_CORRELATION_ID] == run_ids.correlation_id
    await message.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_revoke_gets_no_completion_and_create_does(client_with_cleanup, relay_settings, run_ids):
    """Revoke is recorded with no outbox row or completion; create is completed by the relay the handler wakes."""
    broker = client_with_cleanup
    channel = broker.channel
    relay = OutboxRelay(SessionLocal, broker, relay_settings)
    handler = LifecycleMessageHandler(wake=relay.wake)
    stop = asyncio.Event()
    relay_task = asyncio.create_task(relay.run(stop))

    exchange = await channel.declare_exchange(
        f"test.topic.{run_ids.uid}",
        aio_pika.ExchangeType.TOPIC,
        durable=False,
        auto_delete=False,
    )
    incoming = await channel.get_queue(relay_settings.incoming_queue)
    completions = await channel.get_queue(relay_settings.completion_queue)
    consume_task = asyncio.create_task(broker.consume(handler.handle))
    revoke_certificate = f"{run_ids.certificate_id}-rev"
    try:
        await incoming.bind(exchange, routing_key="cert.#")
        await asyncio.sleep(0.1)

        revoke = lifecycle_amqp_message(f"{run_ids.event_id}-rev", revoke_certificate, should_fail=True)
        await exchange.publish(revoke, routing_key="cert.revoked")
        assert await wait_for_lifecycle_state(revoke_certificate, LifecycleState.COMPLETED, 5.0)
        assert outbox_rows_matching(run_ids.uid) == []
        assert await get_message_within(completions, 2.0) is None

        create = lifecycle_amqp_message(run_ids.event_id, run_ids.certificate_id)
        await exchange.publish(create, routing_key="cert.created")
        completion = await get_message_within(completions, 5.0)
        assert completion is not None, "create produced no completion"
        assert completion.message_id == run_ids.event_id
        assert json.loads(completion.body)["status"] == "COMPLETED"
        await completion.ack()
    finally:
        stop.set()
        relay.wake()
        await asyncio.wait_for(relay_task, timeout=5.0)
        consume_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consume_task
        await exchange.delete()
