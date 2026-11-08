import asyncio
import contextlib
import json
import os
import time
import uuid

import aio_pika
import pytest
import pytest_asyncio
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cert_orchestrator.config import Settings
from cert_orchestrator.messaging.handlers import LifecycleMessageHandler
from cert_orchestrator.messaging.inbound import (
    HEADER_DEAD_LETTER_REASON,
    HEADER_EXCEPTION_MESSAGE,
    HEADER_ORIGINAL_ROUTING_KEY,
    DeadLetterReason,
    InboundMessage,
)
from cert_orchestrator.messaging.rabbitmq import RabbitMQClient
from cert_orchestrator.models import Base, CertificateLifecycle, LifecycleOperation, LifecycleState


@pytest.fixture
def integration_settings() -> Settings:
    unique_id = str(uuid.uuid4())[:8]
    host = os.environ.get("CERT_ORCH_RABBITMQ_HOST", "localhost")
    backoff = int(os.environ.get("CERT_ORCH_BACKOFF_SECONDS", "5"))

    return Settings(
        rabbitmq_host=host,
        incoming_queue=f"test.events.{unique_id}",
        completion_queue=f"test.completions.{unique_id}",
        dead_letter_exchange=f"test.dlx.{unique_id}",
        dead_letter_queue=f"test.dlq.{unique_id}",
        wait_queue_prefix=f"test.wait.{unique_id}",
        backoff_seconds=backoff,
    )


@pytest_asyncio.fixture
async def client_with_cleanup(integration_settings) -> aio_pika.RobustConnection:
    client = RabbitMQClient(config=integration_settings)
    await client.connect()
    yield client

    try:
        channel = client.channel
        if not channel:
            await client.close()
            raise RuntimeError("Channel is not initialized during cleanup")

        dlq = await channel.get_queue(integration_settings.dead_letter_queue)
        await dlq.delete(if_unused=False, if_empty=False)

        dlx = await channel.get_exchange(integration_settings.dead_letter_exchange)
        await dlx.delete()

        for queue_name, queue in list(client._wait_queues.items()):
            try:
                await queue.delete(if_unused=False, if_empty=False)
            except Exception as e:
                raise RuntimeError(f"Failed to delete wait queue {queue_name}: {e}") from e

        incoming = await channel.get_queue(integration_settings.incoming_queue)
        await incoming.delete(if_unused=False, if_empty=False)

        completion = await channel.get_queue(integration_settings.completion_queue)
        await completion.delete(if_unused=False, if_empty=False)
    finally:
        await client.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invalid_utf8_body_reaches_dlq_intact(client_with_cleanup, integration_settings):
    """Invalid UTF-8 bytes are dead-lettered with body intact."""
    client = client_with_cleanup

    async def noop_handler(msg: InboundMessage) -> None:
        pass

    consume_task = asyncio.create_task(client.consume(noop_handler))
    await asyncio.sleep(0.1)

    invalid_bytes = b"\xff\xfe\xfd"
    message = aio_pika.Message(body=invalid_bytes, delivery_mode=aio_pika.DeliveryMode.PERSISTENT)

    await client.channel.default_exchange.publish(message, routing_key=integration_settings.incoming_queue)

    dlq = await client.channel.get_queue(integration_settings.dead_letter_queue)
    msg = await asyncio.wait_for(dlq.get(), timeout=5.0)

    assert msg.body == invalid_bytes
    assert msg.headers.get(HEADER_DEAD_LETTER_REASON) == DeadLetterReason.INVALID.value
    assert HEADER_ORIGINAL_ROUTING_KEY in msg.headers
    await msg.ack()

    consume_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consume_task
    await asyncio.sleep(0.1)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invalid_json_body_reaches_dlq_intact(client_with_cleanup, integration_settings):
    """Invalid JSON bytes are dead-lettered with body intact."""
    client = client_with_cleanup

    async def noop_handler(msg: InboundMessage) -> None:
        pass

    consume_task = asyncio.create_task(client.consume(noop_handler))
    await asyncio.sleep(0.1)

    invalid_json = b"not json at all"
    message = aio_pika.Message(body=invalid_json, delivery_mode=aio_pika.DeliveryMode.PERSISTENT)

    await client.channel.default_exchange.publish(message, routing_key=integration_settings.incoming_queue)

    dlq = await client.channel.get_queue(integration_settings.dead_letter_queue)
    msg = await asyncio.wait_for(dlq.get(), timeout=5.0)

    assert msg.body == invalid_json
    assert msg.headers.get(HEADER_DEAD_LETTER_REASON) == DeadLetterReason.INVALID.value
    await msg.ack()

    consume_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consume_task
    await asyncio.sleep(0.1)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transport_retry_genuinely_delays_and_dead_letters(client_with_cleanup, integration_settings):
    """Transport retries delay exponentially, then dead-letter with reason EXHAUSTED."""
    client = client_with_cleanup

    delivery_times = []

    async def failing_handler(msg: InboundMessage) -> None:
        delivery_times.append(time.monotonic())
        raise ValueError("test failure")

    consume_task = asyncio.create_task(client.consume(failing_handler))

    payload = {"event_id": "test-1", "certificate_id": "cert-1", "payload": {}}
    message = aio_pika.Message(
        body=json.dumps(payload).encode("utf-8"),
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )

    await client.channel.default_exchange.publish(message, routing_key=integration_settings.incoming_queue)

    await asyncio.sleep(integration_settings.backoff_seconds * integration_settings.max_delivery_attempts + 3)

    dlq = await client.channel.get_queue(integration_settings.dead_letter_queue)
    msg = await asyncio.wait_for(dlq.get(), timeout=5.0)

    assert msg.headers.get(HEADER_DEAD_LETTER_REASON) == DeadLetterReason.EXHAUSTED.value
    assert HEADER_EXCEPTION_MESSAGE in msg.headers
    await msg.ack()

    consume_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consume_task

    assert len(delivery_times) == integration_settings.max_delivery_attempts

    for i in range(1, len(delivery_times)):
        gap = delivery_times[i] - delivery_times[i - 1]
        expected = integration_settings.backoff_seconds * i
        tolerance = 0.2
        print(f"Gap {i}: {gap:.2f}s (expected {expected:.2f}s ± {tolerance:.2f}s)")
        assert gap >= expected - tolerance, f"Gap {gap}s less than expected {expected}s (attempt {i})"
        assert gap < expected + 5, f"Gap {gap}s more than expected + 5s (attempt {i})"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_business_retry_preserves_routing_key_and_headers(client_with_cleanup, integration_settings):
    """Business retry via publish_retry preserves routing key and headers after delay."""
    client = client_with_cleanup

    test_headers = {"x-correlation-id": "test-correlation"}
    payload = {"event_id": "test-2", "certificate_id": "cert-2", "payload": {}}

    start = time.monotonic()
    await client.publish_retry(
        payload,
        delay_seconds=integration_settings.backoff_seconds,
        routing_key="custom.routing",
        headers=test_headers,
        exchange="test.exchange",
    )

    incoming = await client.channel.get_queue(integration_settings.incoming_queue)
    deadline = time.monotonic() + integration_settings.backoff_seconds + 2
    msg = None
    while time.monotonic() < deadline:
        msg = await incoming.get(fail=False)
        if msg is not None:
            break
        await asyncio.sleep(0.1)

    if msg is None:
        raise TimeoutError("Message did not arrive in time")

    elapsed = time.monotonic() - start

    assert msg.headers.get(HEADER_ORIGINAL_ROUTING_KEY) == "custom.routing"
    assert msg.headers.get("x-correlation-id") == "test-correlation"
    assert elapsed >= integration_settings.backoff_seconds - 0.2

    await msg.ack()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_shared_queue_declarations_match_server_arguments(client_with_cleanup, integration_settings):
    """Server-side proof: shared queues redeclare with no arguments, and a conflicting declaration is refused."""
    connection = await aio_pika.connect(client_with_cleanup.url)
    try:
        channel = await connection.channel()
        await channel.declare_queue(integration_settings.incoming_queue, durable=True)
        await channel.declare_queue(integration_settings.completion_queue, durable=True)

        with pytest.raises(aio_pika.exceptions.ChannelPreconditionFailed):
            await channel.declare_queue(
                integration_settings.incoming_queue,
                durable=True,
                arguments={"x-message-ttl": 1},
            )
    finally:
        await connection.close()


def _integration_session() -> Session:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return Session(engine)


def _lifecycle_message(event_id: str, certificate_id: str, should_fail: bool = False) -> aio_pika.Message:
    body = {"event_id": event_id, "certificate_id": certificate_id, "payload": {"should_fail": should_fail}}
    return aio_pika.Message(
        body=json.dumps(body).encode("utf-8"),
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )


async def _get_message_within(queue, seconds: float):
    """Return the next message from the queue within the time limit, or None."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        message = await queue.get(fail=False)
        if message is not None:
            return message
        await asyncio.sleep(0.1)
    return None


async def _wait_for_state(session: Session, certificate_id: str, state: LifecycleState, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        row = session.query(CertificateLifecycle).filter_by(certificate_id=certificate_id).one_or_none()
        if row is not None and row.state == state:
            return True
        await asyncio.sleep(0.1)
    return False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_revoke_gets_no_completion_and_create_does(client_with_cleanup, integration_settings, monkeypatch):
    """Revoke is recorded without a completion on the real broker; create on the same setup still completes."""
    client = client_with_cleanup
    channel = client.channel
    session = _integration_session()

    @contextlib.contextmanager
    def _session_provider():
        yield session

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", _session_provider)

    exchange = await channel.declare_exchange(
        f"test.topic.{uuid.uuid4().hex[:8]}",
        aio_pika.ExchangeType.TOPIC,
        durable=False,
        auto_delete=False,
    )
    incoming = await channel.get_queue(integration_settings.incoming_queue)
    completions = await channel.get_queue(integration_settings.completion_queue)
    consume_task = asyncio.create_task(client.consume(LifecycleMessageHandler(client).handle))
    try:
        await incoming.bind(exchange, routing_key="cert.#")
        await asyncio.sleep(0.1)

        revoke = _lifecycle_message("evt-int-rev", "cert-int-rev", should_fail=True)
        await exchange.publish(revoke, routing_key="cert.revoked")
        assert await _wait_for_state(session, "cert-int-rev", LifecycleState.COMPLETED, 5.0)
        assert await _get_message_within(completions, 2.0) is None

        create = _lifecycle_message("evt-int-create", "cert-int-create")
        await exchange.publish(create, routing_key="cert.created")
        completion = await _get_message_within(completions, 5.0)
        assert completion is not None
        assert json.loads(completion.body)["status"] == "COMPLETED"
        await completion.ack()

        revoked = session.query(CertificateLifecycle).filter_by(certificate_id="cert-int-rev").one()
        assert revoked.operation == LifecycleOperation.REVOKE
    finally:
        consume_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consume_task
        await exchange.delete()
