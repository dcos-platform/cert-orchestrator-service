import asyncio
import contextlib
import json
import os
import time
import uuid

import aio_pika
import pytest
import pytest_asyncio

from cert_orchestrator.config import Settings
from cert_orchestrator.messaging.inbound import (
    HEADER_DEAD_LETTER_REASON,
    HEADER_EXCEPTION_MESSAGE,
    HEADER_ORIGINAL_ROUTING_KEY,
    DeadLetterReason,
    InboundMessage,
)
from cert_orchestrator.messaging.rabbitmq import RabbitMQClient


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
        payload, delay_seconds=integration_settings.backoff_seconds, routing_key="custom.routing", headers=test_headers
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
async def test_shared_queue_arguments_empty(client_with_cleanup, integration_settings):
    """Shared queues have no arguments."""
    client = client_with_cleanup
    channel = client.channel

    incoming_queue_obj = await channel.get_queue(integration_settings.incoming_queue)
    completion_queue_obj = await channel.get_queue(integration_settings.completion_queue)

    assert incoming_queue_obj.arguments in (None, {})
    assert completion_queue_obj.arguments in (None, {})
