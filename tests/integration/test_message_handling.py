import asyncio
import contextlib
import json
import time
from collections.abc import Callable
from typing import Any

import aio_pika
import pytest

from cert_orchestrator.config import Settings
from cert_orchestrator.messaging.inbound import (
    HEADER_CORRELATION_ID,
    HEADER_DEAD_LETTER_REASON,
    HEADER_EXCEPTION_MESSAGE,
    HEADER_ORIGINAL_ROUTING_KEY,
    DeadLetterReason,
    InboundMessage,
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invalid_utf8_body_reaches_dlq_intact(client_with_cleanup, integration_settings: Settings):
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
async def test_invalid_json_body_reaches_dlq_intact(client_with_cleanup, integration_settings: Settings):
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
async def test_transport_retry_genuinely_delays_and_dead_letters(client_with_cleanup, integration_settings: Settings):
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


SETTLED_MESSAGES = ("Transport retry scheduled", "Message dead-lettered")


async def _settled_lines_within(
    json_log: Callable[[], list[dict[str, Any]]], count: int, seconds: float
) -> list[dict[str, Any]]:
    """Poll the captured log until it holds count settled-delivery lines, or the time limit passes."""
    deadline = time.monotonic() + seconds
    lines: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        lines = [entry for entry in json_log() if entry["message"].startswith(SETTLED_MESSAGES)]
        if len(lines) >= count:
            break
        await asyncio.sleep(0.05)
    return lines


@pytest.mark.integration
@pytest.mark.asyncio
async def test_always_failing_delivery_logs_each_retry_and_one_dead_letter(
    client_with_cleanup, integration_settings: Settings, json_log
):
    """Each transport retry and the final dead letter of one failing delivery are logged with its correlation id."""
    client = client_with_cleanup
    backoff = integration_settings.backoff_seconds
    max_attempts = integration_settings.max_delivery_attempts

    async def failing_handler(msg: InboundMessage) -> None:
        raise ValueError("always failing")

    consume_task = asyncio.create_task(client.consume(failing_handler))

    payload = {"event_id": "test-always-failing", "certificate_id": "cert-always-failing", "payload": {}}
    message = aio_pika.Message(
        body=json.dumps(payload).encode("utf-8"),
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        headers={HEADER_CORRELATION_ID: "corr-always-failing"},
    )
    await client.channel.default_exchange.publish(message, routing_key=integration_settings.incoming_queue)

    await asyncio.sleep(backoff * max_attempts + 3)
    dlq = await client.channel.get_queue(integration_settings.dead_letter_queue)
    dead = await asyncio.wait_for(dlq.get(), timeout=5.0)
    await dead.ack()

    lines = await _settled_lines_within(json_log, max_attempts, seconds=5.0)

    consume_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consume_task

    assert len(lines) == max_attempts, f"expected {max_attempts} settled-delivery lines, got {lines}"
    retries = [entry["message"] for entry in lines if entry["message"].startswith("Transport retry scheduled")]
    assert retries == [
        f"Transport retry scheduled (attempt {attempt} of {max_attempts}, delay {backoff * attempt} s)"
        for attempt in range(1, max_attempts)
    ]
    dead_lines = [entry for entry in lines if entry["message"] == "Message dead-lettered"]
    assert [(entry["level"], entry["reason"]) for entry in dead_lines] == [("ERROR", "EXHAUSTED")]
    assert all(entry["correlationId"] == "corr-always-failing" for entry in lines)
    assert all("ValueError: always failing" in entry["exception"] for entry in lines)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_shared_queue_declarations_match_server_arguments(client_with_cleanup, integration_settings: Settings):
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
