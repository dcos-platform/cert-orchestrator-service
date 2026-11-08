import json
from unittest.mock import AsyncMock, MagicMock, patch

import aio_pika
import pytest
from pydantic import ValidationError

from cert_orchestrator.config import Settings
from cert_orchestrator.messaging.inbound import (
    HEADER_ATTEMPT,
    HEADER_CORRELATION_ID,
    HEADER_DEAD_LETTER_REASON,
    HEADER_EXCEPTION_MESSAGE,
    HEADER_ORIGINAL_EXCHANGE,
    HEADER_ORIGINAL_ROUTING_KEY,
    DeadLetterReason,
    InboundMessage,
    UnsupportedOperationError,
)
from cert_orchestrator.messaging.rabbitmq import RabbitMQClient
from cert_orchestrator.models import OutboxKind, OutboxMessage, OutboxState
from cert_orchestrator.schemas import CertificateLifecycleEvent


@pytest.fixture
def settings():
    return Settings(
        incoming_queue="test.events",
        completion_queue="test.completions",
        dead_letter_exchange="test.dlx",
        dead_letter_queue="test.dlq",
        wait_queue_prefix="test.wait",
        max_delivery_attempts=3,
        backoff_seconds=1,
    )


@pytest.fixture
def client(settings):
    return RabbitMQClient(config=settings)


@pytest.mark.asyncio
async def test_constructor_defaults_to_settings():
    client = RabbitMQClient()
    assert client.config is not None


@pytest.mark.asyncio
async def test_constructor_uses_provided_config(settings):
    client = RabbitMQClient(config=settings)
    assert client.config == settings


@pytest.mark.asyncio
async def test_connect_declares_queues_and_exchanges(client):
    mock_channel = AsyncMock()
    mock_dlx = AsyncMock()
    mock_dlq = AsyncMock()

    mock_channel.declare_exchange.return_value = mock_dlx
    mock_channel.declare_queue.return_value = mock_dlq

    with patch("cert_orchestrator.messaging.rabbitmq.aio_pika.connect_robust") as mock_connect:
        mock_connection = AsyncMock()
        mock_connect.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel

        await client.connect()

    assert mock_channel.declare_queue.call_count == 3
    mock_channel.declare_queue.assert_any_call("test.events", durable=True)
    mock_channel.declare_queue.assert_any_call("test.completions", durable=True)
    mock_channel.declare_queue.assert_any_call("test.dlq", durable=True)

    mock_channel.declare_exchange.assert_called_once()
    mock_dlq.bind.assert_called_once()


@pytest.mark.asyncio
async def test_on_message_invalid_utf8_dead_letters(client):
    message = MagicMock()
    message.body = b"\xff\xfe"
    message.routing_key = "test.routing"
    message.headers = {}
    message.exchange = "test.exchange"
    message.ack = AsyncMock()

    handler = AsyncMock()

    client.channel = AsyncMock()
    queue = AsyncMock()

    async def fake_consume(cb):
        await cb(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue

    with patch.object(client, "_dead_letter", new_callable=AsyncMock) as mock_dl:
        await client.consume(handler)
        mock_dl.assert_called_once()
        assert mock_dl.call_args[0][1] == DeadLetterReason.INVALID


@pytest.mark.asyncio
async def test_on_message_invalid_json_dead_letters(client):
    message = MagicMock()
    message.body = b"not json"
    message.routing_key = "test.routing"
    message.headers = {}
    message.exchange = "test.exchange"
    message.ack = AsyncMock()

    handler = AsyncMock()

    client.channel = AsyncMock()
    queue = AsyncMock()

    async def fake_consume(cb):
        await cb(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue

    with patch.object(client, "_dead_letter", new_callable=AsyncMock):
        await client.consume(handler)
        message.ack.assert_called_once()


@pytest.mark.asyncio
async def test_on_message_validation_error_dead_letters(client):
    message = MagicMock()
    message.body = b'{"key": "value"}'
    message.routing_key = "test.routing"
    message.headers = {}
    message.exchange = "test.exchange"
    message.ack = AsyncMock()

    handler = AsyncMock(side_effect=ValueError("validation"))

    client.channel = AsyncMock()
    queue = AsyncMock()

    async def fake_consume(cb):
        await cb(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue

    with patch.object(client, "_schedule_transport_retry", new_callable=AsyncMock):
        await client.consume(handler)
        message.ack.assert_called_once()


@pytest.mark.asyncio
async def test_on_message_republish_failure_nacks(client):
    message = MagicMock()
    message.body = b'{"key": "value"}'
    message.routing_key = "test.routing"
    message.headers = {}
    message.exchange = "test.exchange"
    message.nack = AsyncMock()

    handler = AsyncMock(side_effect=ValueError("test"))

    client.channel = AsyncMock()
    queue = AsyncMock()

    async def fake_consume(cb):
        await cb(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue

    with patch.object(client, "_schedule_transport_retry", side_effect=RuntimeError("broker down")):
        await client.consume(handler)
        message.nack.assert_called_once_with(requeue=True)


@pytest.mark.asyncio
async def test_transport_retry_increments_attempt(client):
    message = MagicMock()
    message.body = b'{"key": "value"}'
    message.routing_key = "test.routing"
    message.headers = {HEADER_ATTEMPT: 2}
    message.exchange = "test.exchange"
    message.ack = AsyncMock()

    handler = AsyncMock(side_effect=ValueError("test"))

    client.channel = AsyncMock()
    queue = AsyncMock()

    async def fake_consume(cb):
        await cb(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue

    retry_calls = []

    async def capture_retry(msg, inbound, attempt, exception):
        retry_calls.append(attempt)

    with patch.object(client, "_schedule_transport_retry", side_effect=capture_retry):
        await client.consume(handler)
        assert len(retry_calls) == 1
        assert retry_calls[0] == 2


@pytest.mark.asyncio
async def test_effective_routing_key_preserved_on_retry(client):
    message = MagicMock()
    message.body = b'{"key": "value"}'
    message.routing_key = "second.routing"
    message.headers = {HEADER_ORIGINAL_ROUTING_KEY: "first.routing"}
    message.exchange = "test.exchange"
    message.ack = AsyncMock()

    handler_calls = []

    async def handler(inbound):
        handler_calls.append(inbound.routing_key)

    client.channel = AsyncMock()
    queue = AsyncMock()

    async def fake_consume(cb):
        await cb(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue

    await client.consume(handler)

    assert len(handler_calls) == 1
    assert handler_calls[0] == "first.routing"


@pytest.mark.asyncio
async def test_original_routing_key_set_once_on_retry(client):
    message = MagicMock()
    message.body = b'{"key": "value"}'
    message.routing_key = "new.routing"
    message.headers = {}
    message.exchange = "test.exchange"
    message.ack = AsyncMock()
    message.content_type = "application/json"

    handler = AsyncMock(side_effect=ValueError("retry"))

    client.channel = AsyncMock()
    client.channel.default_exchange = AsyncMock()
    queue = AsyncMock()

    async def fake_consume(cb):
        await cb(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue

    published_msgs = []

    async def capture_publish(msg, routing_key):
        published_msgs.append((msg, routing_key))

    client.channel.default_exchange.publish = capture_publish

    await client.consume(handler)

    assert len(published_msgs) == 1
    msg, routing_key = published_msgs[0]
    assert msg.headers[HEADER_ORIGINAL_ROUTING_KEY] == "new.routing"


@pytest.mark.asyncio
async def test_wait_queue_name_in_retry(client):
    message = MagicMock()
    message.body = b'{"key": "value"}'
    message.routing_key = "test.routing"
    message.headers = {}
    message.exchange = "test.exchange"
    message.ack = AsyncMock()
    message.content_type = "application/json"

    handler = AsyncMock(side_effect=ValueError("retry"))

    client.channel = AsyncMock()
    client.channel.default_exchange = AsyncMock()
    queue = AsyncMock()

    async def fake_consume(cb):
        await cb(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue

    published_msgs = []

    async def capture_publish(msg, routing_key):
        published_msgs.append(routing_key)

    client.channel.default_exchange.publish = capture_publish

    await client.consume(handler)

    assert len(published_msgs) == 1
    assert published_msgs[0] == "test.wait.1000ms"  # 1 attempt × 1 second backoff


@pytest.mark.asyncio
async def test_dead_letter_preserves_body_bytes(client):
    original_body = b'{"invalid": "data'
    message = MagicMock()
    message.body = original_body
    message.routing_key = "test.routing"
    message.headers = {}
    message.exchange = "test.exchange"
    message.ack = AsyncMock()
    message.message_id = None
    message.correlation_id = None
    message.content_type = "application/json"

    client.channel = AsyncMock()
    dlx = AsyncMock()
    client.channel.get_exchange = AsyncMock(return_value=dlx)
    queue = AsyncMock()

    async def fake_consume(cb):
        await cb(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue

    published_msgs = []

    async def capture_publish(msg, routing_key):
        published_msgs.append(msg.body)

    dlx.publish = capture_publish

    await client.consume(AsyncMock())

    assert len(published_msgs) == 1
    assert published_msgs[0] == original_body


@pytest.mark.asyncio
async def test_dead_letter_includes_exception_message(client):
    message = MagicMock()
    message.body = b'{"key": "value"}'
    message.routing_key = "test.routing"
    message.headers = {HEADER_ATTEMPT: 3}
    message.exchange = "test.exchange"
    message.ack = AsyncMock()
    message.message_id = None
    message.correlation_id = None
    message.content_type = "application/json"

    exc = ValueError("test error")
    handler = AsyncMock(side_effect=exc)

    client.channel = AsyncMock()
    dlx = AsyncMock()
    client.channel.get_exchange = AsyncMock(return_value=dlx)
    queue = AsyncMock()

    async def fake_consume(cb):
        await cb(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue

    published_msgs = []

    async def capture_publish(msg, routing_key):
        published_msgs.append(msg.headers)

    dlx.publish = capture_publish

    await client.consume(handler)

    assert len(published_msgs) == 1
    headers = published_msgs[0]
    assert HEADER_EXCEPTION_MESSAGE in headers
    assert "ValueError" in headers[HEADER_EXCEPTION_MESSAGE]
    assert "test error" in headers[HEADER_EXCEPTION_MESSAGE]


@pytest.mark.asyncio
async def test_ensure_wait_queue_creates_queue(client):
    client.channel = AsyncMock()
    queue = AsyncMock()
    client.channel.declare_queue = AsyncMock(return_value=queue)

    queue_name = await client._ensure_wait_queue(delay_seconds=5)

    assert queue_name == "test.wait.5000ms"
    client.channel.declare_queue.assert_called_once()
    assert "test.wait.5000ms" in client._wait_queues


@pytest.mark.asyncio
async def test_ensure_wait_queue_reuses_existing_queue(client):
    client.channel = AsyncMock()
    queue = AsyncMock()
    client._wait_queues["test.wait.5000ms"] = queue

    queue_name = await client._ensure_wait_queue(delay_seconds=5)

    assert queue_name == "test.wait.5000ms"
    client.channel.declare_queue.assert_not_called()


@pytest.mark.asyncio
async def test_close_with_connection(client):
    client.connection = AsyncMock()
    await client.close()
    client.connection.close.assert_called_once()


@pytest.mark.asyncio
async def test_close_without_connection(client):
    client.connection = None
    await client.close()


@pytest.mark.asyncio
async def test_consume_without_channel():
    client = RabbitMQClient()
    client.channel = None
    handler = AsyncMock()

    with pytest.raises(RuntimeError, match="RabbitMQ channel is not initialized"):
        await client.consume(handler)


@pytest.mark.asyncio
async def test_dead_letter_failure_on_invalid_message(client):
    message = MagicMock()
    message.body = b"\xff\xfe"
    message.routing_key = "test.routing"
    message.headers = {}
    message.exchange = "test.exchange"
    message.nack = AsyncMock()

    handler = AsyncMock()

    client.channel = AsyncMock()
    queue = AsyncMock()

    async def fake_consume(cb):
        await cb(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue

    with patch.object(client, "_dead_letter", side_effect=RuntimeError("broker error")):
        await client.consume(handler)
        message.nack.assert_called_once_with(requeue=True)


@pytest.mark.asyncio
async def test_on_message_validation_error_acks_after_dead_letter(client):
    from pydantic import ValidationError

    message = MagicMock()
    message.body = b'{"key": "value"}'
    message.routing_key = "test.routing"
    message.headers = {}
    message.exchange = "test.exchange"
    message.ack = AsyncMock()

    exc = ValidationError.from_exception_data("test", [])
    handler = AsyncMock(side_effect=exc)

    client.channel = AsyncMock()
    queue = AsyncMock()

    async def fake_consume(cb):
        await cb(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue

    with patch.object(client, "_dead_letter", new_callable=AsyncMock):
        await client.consume(handler)
        message.ack.assert_called_once()


@pytest.mark.asyncio
async def test_dead_letter_failure_on_validation_error(client):
    from pydantic import ValidationError

    message = MagicMock()
    message.body = b'{"key": "value"}'
    message.routing_key = "test.routing"
    message.headers = {}
    message.exchange = "test.exchange"
    message.ack = AsyncMock()
    message.nack = AsyncMock()

    exc = ValidationError.from_exception_data("test", [])
    handler = AsyncMock(side_effect=exc)

    client.channel = AsyncMock()
    queue = AsyncMock()

    async def fake_consume(cb):
        await cb(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue

    with patch.object(client, "_dead_letter", side_effect=RuntimeError("broker error")):
        await client.consume(handler)
        message.nack.assert_called_once_with(requeue=True)


@pytest.mark.asyncio
async def test_dead_letter_failure_on_exhaustion(client):
    message = MagicMock()
    message.body = b'{"key": "value"}'
    message.routing_key = "test.routing"
    message.headers = {HEADER_ATTEMPT: 3}
    message.exchange = "test.exchange"
    message.ack = AsyncMock()
    message.nack = AsyncMock()

    handler = AsyncMock(side_effect=ValueError("test"))

    client.channel = AsyncMock()
    queue = AsyncMock()

    async def fake_consume(cb):
        await cb(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue

    with patch.object(client, "_dead_letter", side_effect=RuntimeError("broker error")):
        await client.consume(handler)
        message.nack.assert_called_once_with(requeue=True)


@pytest.mark.asyncio
async def test_dead_letter_without_channel():
    client = RabbitMQClient()
    client.channel = None
    message = MagicMock()

    with pytest.raises(RuntimeError, match="RabbitMQ channel is not initialized"):
        await client._dead_letter(message, DeadLetterReason.INVALID)


@pytest.mark.asyncio
async def test_schedule_transport_retry_without_channel():
    client = RabbitMQClient()
    client.channel = None
    message = MagicMock()
    inbound = MagicMock()

    with pytest.raises(RuntimeError, match="RabbitMQ channel is not initialized"):
        await client._schedule_transport_retry(message, inbound, 1, ValueError("handler failed"))


@pytest.mark.asyncio
async def test_dead_letter_header_already_set_original_routing_key(client):
    message = MagicMock()
    message.body = b'{"key": "value"}'
    message.routing_key = "new.routing"
    message.headers = {HEADER_ORIGINAL_ROUTING_KEY: "first.routing"}
    message.exchange = "test.exchange"
    message.message_id = None
    message.correlation_id = None
    message.content_type = "application/json"

    client.channel = AsyncMock()
    dlx = AsyncMock()
    client.channel.get_exchange = AsyncMock(return_value=dlx)

    published_msgs = []

    async def capture_publish(msg, routing_key):
        published_msgs.append(msg.headers)

    dlx.publish = capture_publish

    await client._dead_letter(message, DeadLetterReason.INVALID)

    assert len(published_msgs) == 1
    headers = published_msgs[0]
    assert headers[HEADER_ORIGINAL_ROUTING_KEY] == "first.routing"


@pytest.mark.asyncio
async def test_dead_letter_header_already_set_original_exchange(client):
    message = MagicMock()
    message.body = b'{"key": "value"}'
    message.routing_key = "test.routing"
    message.headers = {HEADER_ORIGINAL_EXCHANGE: "first.exchange"}
    message.exchange = "test.exchange"
    message.message_id = None
    message.correlation_id = None
    message.content_type = "application/json"

    client.channel = AsyncMock()
    dlx = AsyncMock()
    client.channel.get_exchange = AsyncMock(return_value=dlx)

    published_msgs = []

    async def capture_publish(msg, routing_key):
        published_msgs.append(msg.headers)

    dlx.publish = capture_publish

    await client._dead_letter(message, DeadLetterReason.INVALID)

    assert len(published_msgs) == 1
    headers = published_msgs[0]
    assert headers[HEADER_ORIGINAL_EXCHANGE] == "first.exchange"


@pytest.mark.asyncio
async def test_schedule_transport_retry_header_already_set_original_routing_key(client):
    message = MagicMock()
    message.body = b'{"key": "value"}'
    message.routing_key = "new.routing"
    message.headers = {HEADER_ORIGINAL_ROUTING_KEY: "first.routing"}
    message.exchange = "test.exchange"
    message.content_type = "application/json"

    inbound = MagicMock()
    inbound.routing_key = "first.routing"

    client.channel = AsyncMock()
    queue = AsyncMock()

    async def capture_declare(name, durable, arguments):
        return queue

    client.channel.declare_queue = capture_declare
    client.channel.default_exchange = AsyncMock()

    published_msgs = []

    async def capture_publish(msg, routing_key):
        published_msgs.append(msg.headers)

    client.channel.default_exchange.publish = capture_publish

    await client._schedule_transport_retry(message, inbound, 1, ValueError("handler failed"))

    assert len(published_msgs) == 1
    headers = published_msgs[0]
    assert headers[HEADER_ORIGINAL_ROUTING_KEY] == "first.routing"


@pytest.mark.asyncio
async def test_schedule_transport_retry_header_already_set_original_exchange(client):
    message = MagicMock()
    message.body = b'{"key": "value"}'
    message.routing_key = "test.routing"
    message.headers = {HEADER_ORIGINAL_EXCHANGE: "first.exchange"}
    message.exchange = "test.exchange"
    message.content_type = "application/json"

    inbound = MagicMock()
    inbound.routing_key = "test.routing"

    client.channel = AsyncMock()
    queue = AsyncMock()

    async def capture_declare(name, durable, arguments):
        return queue

    client.channel.declare_queue = capture_declare
    client.channel.default_exchange = AsyncMock()

    published_msgs = []

    async def capture_publish(msg, routing_key):
        published_msgs.append(msg.headers)

    client.channel.default_exchange.publish = capture_publish

    await client._schedule_transport_retry(message, inbound, 1, ValueError("handler failed"))

    assert len(published_msgs) == 1
    headers = published_msgs[0]
    assert headers[HEADER_ORIGINAL_EXCHANGE] == "first.exchange"


def _wire_consume(client: RabbitMQClient, message: MagicMock) -> AsyncMock:
    """Give the client a channel whose queue delivers the given message to the consume callback."""
    client.channel = AsyncMock()
    queue = AsyncMock()

    async def fake_consume(cb):
        await cb(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue
    return client.channel


def _incoming(
    body: bytes = b'{"key": "value"}',
    routing_key: str = "cert.created",
    headers: dict | None = None,
    exchange: str | None = "test.exchange",
) -> MagicMock:
    message = MagicMock()
    message.body = body
    message.routing_key = routing_key
    message.headers = headers if headers is not None else {}
    message.exchange = exchange
    message.message_id = None
    message.correlation_id = None
    message.content_type = "application/json"
    message.ack = AsyncMock()
    message.nack = AsyncMock()
    return message


def _capture_default_exchange(client: RabbitMQClient) -> list:
    """Give the client a channel whose default exchange records each (message, routing_key) published."""
    client.channel = AsyncMock()
    client.channel.declare_queue = AsyncMock(return_value=AsyncMock())
    client.channel.default_exchange = AsyncMock()
    published: list = []

    async def capture_publish(msg, routing_key):
        published.append((msg, routing_key))

    client.channel.default_exchange.publish = capture_publish
    return published


@pytest.mark.asyncio
async def test_unsupported_operation_is_dead_lettered_with_its_reason_and_acked(client):
    message = _incoming(routing_key="cert.unknown")
    channel = _wire_consume(client, message)
    dlx = AsyncMock()
    channel.get_exchange = AsyncMock(return_value=dlx)
    published = []

    async def capture_publish(msg, routing_key):
        published.append(msg)

    dlx.publish = capture_publish
    handler = AsyncMock(side_effect=UnsupportedOperationError("unsupported routing key: 'cert.unknown'"))

    await client.consume(handler)

    assert len(published) == 1
    assert published[0].headers[HEADER_DEAD_LETTER_REASON] == "UNSUPPORTED_OPERATION"
    assert published[0].body == b'{"key": "value"}'
    message.ack.assert_awaited_once()
    message.nack.assert_not_called()


@pytest.mark.asyncio
async def test_unsupported_operation_dead_letter_failure_nacks_for_redelivery(client):
    message = _incoming(routing_key="cert.unknown")
    _wire_consume(client, message)
    handler = AsyncMock(side_effect=UnsupportedOperationError("unsupported routing key: 'cert.unknown'"))

    with patch.object(client, "_dead_letter", side_effect=RuntimeError("broker error")):
        await client.consume(handler)

    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_called()


@pytest.mark.asyncio
async def test_ack_failure_after_successful_handler_propagates_without_retry_or_dead_letter(client):
    message = _incoming()
    message.ack = AsyncMock(side_effect=RuntimeError("ack failed"))
    channel = _wire_consume(client, message)
    handler = AsyncMock()

    with pytest.raises(RuntimeError, match="ack failed"):
        await client.consume(handler)

    handler.assert_awaited_once()
    channel.get_exchange.assert_not_called()
    channel.default_exchange.publish.assert_not_called()
    message.nack.assert_not_called()


@pytest.mark.asyncio
async def test_inbound_exchange_is_empty_string_when_message_has_no_exchange(client):
    message = _incoming(exchange=None)
    _wire_consume(client, message)
    handler = AsyncMock()

    await client.consume(handler)

    assert handler.call_args.args[0].exchange == ""


@pytest.mark.asyncio
async def test_inbound_exchange_prefers_original_exchange_header(client):
    message = _incoming(headers={HEADER_ORIGINAL_EXCHANGE: "first.exchange"}, exchange="second.exchange")
    _wire_consume(client, message)
    handler = AsyncMock()

    await client.consume(handler)

    assert handler.call_args.args[0].exchange == "first.exchange"


@pytest.mark.asyncio
async def test_transport_retry_keeps_original_message_id_and_correlation_id(client):
    published = _capture_default_exchange(client)
    message = _incoming()
    message.message_id = "msg-original"
    message.correlation_id = "corr-original"
    inbound = InboundMessage(body={"key": "value"}, routing_key="cert.created", headers={}, exchange="test.exchange")

    await client._schedule_transport_retry(message, inbound, 1, ValueError("handler failed"))

    published_message = published[0][0]
    assert published_message.message_id == "msg-original"
    assert published_message.correlation_id == "corr-original"


def _outbox_row(
    kind: OutboxKind,
    message_id: str,
    body: str,
    headers: dict,
    delay_seconds: int | None = None,
) -> OutboxMessage:
    return OutboxMessage(
        message_id=message_id,
        kind=kind,
        state=OutboxState.PENDING,
        body=body,
        headers=headers,
        delay_seconds=delay_seconds,
    )


@pytest.mark.asyncio
async def test_publish_outbox_completion_sends_the_stored_bytes_and_properties_to_the_completion_queue(client):
    published = _capture_default_exchange(client)
    stored_body = '{"event_id": "evt-1",  "status": "COMPLETED"}'
    row = _outbox_row(OutboxKind.COMPLETION, "evt-1", stored_body, {"x-correlation-id": "corr-1"})

    await client.publish_outbox_message(row)

    msg, routing_key = published[0]
    assert routing_key == "test.completions"
    assert msg.body == stored_body.encode("utf-8")
    assert msg.message_id == "evt-1"
    assert msg.headers == {"x-correlation-id": "corr-1"}
    assert msg.content_type == "application/json"
    assert msg.delivery_mode == aio_pika.DeliveryMode.PERSISTENT


@pytest.mark.asyncio
async def test_publish_outbox_retry_goes_via_its_wait_queue_with_stored_headers_unchanged(client):
    published = _capture_default_exchange(client)
    declared = []

    async def capture_declare(name, durable, arguments):
        declared.append((name, arguments))
        return AsyncMock()

    client.channel.declare_queue = capture_declare
    stored_headers = {HEADER_ATTEMPT: 3, "x-correlation-id": "keep-me", HEADER_ORIGINAL_ROUTING_KEY: "cert.created"}
    row = _outbox_row(
        OutboxKind.RETRY,
        "evt-1:retry:1",
        '{"event_id": "evt-1:retry:1"}',
        stored_headers,
        delay_seconds=5,
    )

    await client.publish_outbox_message(row)

    msg, routing_key = published[0]
    assert routing_key == "test.wait.5000ms"
    assert declared == [
        (
            "test.wait.5000ms",
            {"x-message-ttl": 5000, "x-dead-letter-exchange": "", "x-dead-letter-routing-key": "test.events"},
        )
    ]
    assert msg.body == b'{"event_id": "evt-1:retry:1"}'
    assert msg.message_id == "evt-1:retry:1"
    assert msg.headers == stored_headers


@pytest.mark.asyncio
async def test_publish_outbox_message_without_a_channel_raises(client):
    client.channel = None
    row = _outbox_row(OutboxKind.COMPLETION, "evt-1", "{}", {})

    with pytest.raises(RuntimeError, match="RabbitMQ channel is not initialized"):
        await client.publish_outbox_message(row)


@pytest.mark.asyncio
async def test_publish_outbox_message_propagates_a_broker_refusal(client):
    client.channel = AsyncMock()
    client.channel.default_exchange.publish = AsyncMock(side_effect=ConnectionError("broker refused"))
    row = _outbox_row(OutboxKind.COMPLETION, "evt-1", "{}", {})

    with pytest.raises(ConnectionError, match="broker refused"):
        await client.publish_outbox_message(row)


CONSUMED_TOTAL = "cert_orchestrator_events_consumed_total"
DEAD_LETTERS_TOTAL = "cert_orchestrator_dead_letters_total"


def _wire_delivery(client: RabbitMQClient, message: MagicMock) -> AsyncMock:
    """Deliver the message to the consumer, and give the channel a default exchange and a dead-letter exchange."""
    channel = _wire_consume(client, message)
    channel.default_exchange = AsyncMock()
    channel.default_exchange.publish = AsyncMock()
    dlx = AsyncMock()
    dlx.publish = AsyncMock()
    channel.get_exchange = AsyncMock(return_value=dlx)
    return channel


def _unsupported_handler() -> AsyncMock:
    return AsyncMock(side_effect=UnsupportedOperationError("unsupported routing key: 'cert.unknown'"))


@pytest.mark.asyncio
async def test_a_failed_dead_letter_logs_its_cause_with_the_delivery_context(client, json_log):
    message = _incoming(
        body=b'{"event_id": "evt-dl", "certificate_id": "cert-dl"}',
        routing_key="cert.unknown",
        headers={HEADER_CORRELATION_ID: "corr-dl"},
    )
    _wire_consume(client, message)

    with patch.object(client, "_dead_letter", side_effect=RuntimeError("broker error")):
        await client.consume(_unsupported_handler())

    (line,) = [entry for entry in json_log() if entry["message"] == "Failed to dead-letter message"]
    assert (line["level"], line["correlationId"], line["eventId"], line["reason"]) == (
        "ERROR",
        "corr-dl",
        "evt-dl",
        "UNSUPPORTED_OPERATION",
    )
    assert "RuntimeError: broker error" in line["exception"]
    message.nack.assert_awaited_once_with(requeue=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("inbound", ["", "   ", "c" * 37])
async def test_an_invalid_correlation_header_never_appears_as_correlation_id(client, json_log, inbound):
    message = _incoming(routing_key="cert.unknown", headers={HEADER_CORRELATION_ID: inbound})
    _wire_consume(client, message)

    with patch.object(client, "_dead_letter", side_effect=RuntimeError("broker error")):
        await client.consume(_unsupported_handler())

    (line,) = [entry for entry in json_log() if entry["message"] == "Failed to dead-letter message"]
    assert "correlationId" not in line


@pytest.mark.asyncio
async def test_an_undecodable_body_logs_with_the_correlation_id_and_no_event_id(client, json_log):
    message = _incoming(body=b"\xff\xfe", headers={HEADER_CORRELATION_ID: "corr-undecodable"})
    _wire_consume(client, message)

    with patch.object(client, "_dead_letter", side_effect=RuntimeError("broker error")):
        await client.consume(AsyncMock())

    (line,) = [entry for entry in json_log() if entry["message"] == "Failed to dead-letter message"]
    assert line["correlationId"] == "corr-undecodable"
    assert "eventId" not in line


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body", "expected_event_id"),
    [
        (b'{"event_id": "evt-ok", "certificate_id": "cert-ok"}', "evt-ok"),
        (b'{"event_id": "   "}', None),
        (b'{"event_id": 7}', None),
        (b"[1, 2]", None),
        (b"not json", None),
        (b"\xff", None),
    ],
)
async def test_the_event_id_context_comes_only_from_a_non_blank_string_body_event_id(
    client, json_log, body, expected_event_id
):
    message = _incoming(body=body, routing_key="cert.unknown")
    _wire_consume(client, message)

    with patch.object(client, "_dead_letter", side_effect=RuntimeError("broker error")):
        await client.consume(_unsupported_handler())

    (line,) = [entry for entry in json_log() if entry["message"] == "Failed to dead-letter message"]
    assert line.get("eventId") == expected_event_id


@pytest.mark.asyncio
async def test_a_failed_transport_retry_publish_logs_its_cause_and_moves_no_counter(client, json_log, metric_value):
    message = _incoming(headers={})
    channel = _wire_delivery(client, message)
    channel.default_exchange.publish = AsyncMock(side_effect=ConnectionError("broker refused"))
    before = metric_value(CONSUMED_TOTAL, operation="CREATE", outcome="transport_retry")

    await client.consume(AsyncMock(side_effect=ValueError("retry")))

    assert metric_value(CONSUMED_TOTAL, operation="CREATE", outcome="transport_retry") == before
    (line,) = [entry for entry in json_log() if entry["message"] == "Failed to handle transport error (RETRY)"]
    assert line["level"] == "ERROR"
    assert "ConnectionError: broker refused" in line["exception"]
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_called()


@pytest.mark.asyncio
async def test_a_failed_dead_letter_publish_moves_no_dead_letter_counter(client, metric_value):
    message = _incoming(routing_key="cert.unknown")
    channel = _wire_delivery(client, message)
    dlx = AsyncMock()
    dlx.publish = AsyncMock(side_effect=ConnectionError("broker refused"))
    channel.get_exchange = AsyncMock(return_value=dlx)
    reason_before = metric_value(DEAD_LETTERS_TOTAL, reason="UNSUPPORTED_OPERATION")
    outcome_before = metric_value(CONSUMED_TOTAL, operation="unknown", outcome="dead_lettered")

    await client.consume(_unsupported_handler())

    assert metric_value(DEAD_LETTERS_TOTAL, reason="UNSUPPORTED_OPERATION") == reason_before
    assert metric_value(CONSUMED_TOTAL, operation="unknown", outcome="dead_lettered") == outcome_before
    message.nack.assert_awaited_once_with(requeue=True)


@pytest.mark.asyncio
async def test_a_dead_letter_moves_its_reason_and_outcome_counters_once(client, metric_value):
    message = _incoming(routing_key="cert.unknown")
    _wire_delivery(client, message)
    reason_before = metric_value(DEAD_LETTERS_TOTAL, reason="UNSUPPORTED_OPERATION")
    outcome_before = metric_value(CONSUMED_TOTAL, operation="unknown", outcome="dead_lettered")

    await client.consume(_unsupported_handler())

    assert metric_value(DEAD_LETTERS_TOTAL, reason="UNSUPPORTED_OPERATION") - reason_before == 1
    assert metric_value(CONSUMED_TOTAL, operation="unknown", outcome="dead_lettered") - outcome_before == 1
    message.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_transport_retry_and_dead_letter_with_a_list_routing_header_count_and_are_acked(client, metric_value):
    retry_message = _incoming(headers={HEADER_ORIGINAL_ROUTING_KEY: ["cert.created"]})
    _wire_delivery(client, retry_message)
    retry_before = metric_value(CONSUMED_TOTAL, operation="unknown", outcome="transport_retry")

    await client.consume(AsyncMock(side_effect=ValueError("retry")))

    assert metric_value(CONSUMED_TOTAL, operation="unknown", outcome="transport_retry") - retry_before == 1
    retry_message.ack.assert_awaited_once()
    retry_message.nack.assert_not_called()

    dead_message = _incoming(routing_key="cert.unknown", headers={HEADER_ORIGINAL_ROUTING_KEY: ["cert.created"]})
    _wire_delivery(client, dead_message)
    dead_before = metric_value(CONSUMED_TOTAL, operation="unknown", outcome="dead_lettered")

    await client.consume(_unsupported_handler())

    assert metric_value(CONSUMED_TOTAL, operation="unknown", outcome="dead_lettered") - dead_before == 1
    dead_message.ack.assert_awaited_once()
    dead_message.nack.assert_not_called()


REASONS = ("INVALID", "EXHAUSTED", "UNSUPPORTED_OPERATION")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reason", "body", "routing_key", "headers", "handler_error"),
    [
        ("INVALID", b"\xff\xfe", "cert.created", {}, None),
        ("EXHAUSTED", b'{"key": "value"}', "cert.created", {HEADER_ATTEMPT: 3}, ValueError("boom")),
        (
            "UNSUPPORTED_OPERATION",
            b'{"key": "value"}',
            "cert.unknown",
            {},
            UnsupportedOperationError("unsupported routing key: 'cert.unknown'"),
        ),
    ],
)
async def test_each_dead_letter_reason_moves_only_its_own_counter(
    client, metric_value, reason, body, routing_key, headers, handler_error
):
    message = _incoming(body=body, routing_key=routing_key, headers=headers)
    _wire_delivery(client, message)
    before = {name: metric_value(DEAD_LETTERS_TOTAL, reason=name) for name in REASONS}

    await client.consume(AsyncMock(side_effect=handler_error))

    moved = {name: metric_value(DEAD_LETTERS_TOTAL, reason=name) - before[name] for name in REASONS}
    assert moved == {name: (1.0 if name == reason else 0.0) for name in REASONS}
    message.ack.assert_awaited_once()
    message.nack.assert_not_called()


def _dead_letter_lines(json_log) -> list[dict]:
    return [entry for entry in json_log() if entry["message"] == "Message dead-lettered"]


@pytest.mark.asyncio
@pytest.mark.parametrize(("attempt", "delay"), [(1, 1), (2, 2)])
async def test_transport_retry_logs_a_warning_with_its_traceback(client, json_log, attempt, delay):
    message = _incoming(headers={HEADER_ATTEMPT: attempt, HEADER_CORRELATION_ID: "corr-retry"})
    _wire_delivery(client, message)

    await client.consume(AsyncMock(side_effect=ValueError("handler exploded")))

    (line,) = [entry for entry in json_log() if entry["message"].startswith("Transport retry scheduled")]
    assert line["message"] == f"Transport retry scheduled (attempt {attempt} of 3, delay {delay} s)"
    assert (line["level"], line["correlationId"]) == ("WARN", "corr-retry")
    assert "reason" not in line
    assert "Traceback (most recent call last)" in line["exception"]
    assert "ValueError: handler exploded" in line["exception"]


@pytest.mark.asyncio
async def test_exhausted_dead_letter_logs_an_error_with_its_traceback(client, json_log):
    message = _incoming(headers={HEADER_ATTEMPT: 3, HEADER_CORRELATION_ID: "corr-exhausted"})
    _wire_delivery(client, message)

    await client.consume(AsyncMock(side_effect=ValueError("final failure")))

    (line,) = _dead_letter_lines(json_log)
    assert (line["level"], line["severity"], line["reason"], line["correlationId"]) == (
        "ERROR",
        40000,
        "EXHAUSTED",
        "corr-exhausted",
    )
    assert "Traceback (most recent call last)" in line["exception"]
    assert "ValueError: final failure" in line["exception"]


@pytest.mark.asyncio
async def test_invalid_dead_letter_logs_a_warning_without_a_traceback(client, json_log):
    message = _incoming(body=b"\xff\xfe", headers={HEADER_CORRELATION_ID: "corr-invalid"})
    _wire_delivery(client, message)

    await client.consume(AsyncMock())

    (line,) = _dead_letter_lines(json_log)
    assert (line["level"], line["reason"], line["correlationId"]) == ("WARN", "INVALID", "corr-invalid")
    assert "exception" not in line


@pytest.mark.asyncio
async def test_unsupported_operation_dead_letter_logs_a_warning_without_a_traceback(client, json_log):
    message = _incoming(routing_key="cert.unknown", headers={HEADER_CORRELATION_ID: "corr-unsupported"})
    _wire_delivery(client, message)

    await client.consume(_unsupported_handler())

    (line,) = _dead_letter_lines(json_log)
    assert (line["level"], line["reason"], line["correlationId"]) == (
        "WARN",
        "UNSUPPORTED_OPERATION",
        "corr-unsupported",
    )
    assert "exception" not in line


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", [DeadLetterReason.INVALID, DeadLetterReason.UNSUPPORTED_OPERATION])
async def test_only_an_exhausted_dead_letter_keeps_a_traceback_when_one_is_handed_over(client, json_log, reason):
    message = _incoming(routing_key="cert.unknown")
    _wire_delivery(client, message)

    try:
        raise ValueError("handed over")
    except ValueError as error:
        await client._dead_letter(message, reason, exception=error)

    (line,) = _dead_letter_lines(json_log)
    assert (line["level"], line["reason"]) == ("WARN", reason.value)
    assert "exception" not in line


SENTINEL = "BODY-SENTINEL-7731"


async def _validate_as_lifecycle_event(inbound: InboundMessage) -> None:
    CertificateLifecycleEvent.model_validate(inbound.body)


def test_the_validation_error_text_quotes_the_sentinel_so_the_leak_test_is_not_vacuous():
    with pytest.raises(ValidationError) as caught:
        CertificateLifecycleEvent.model_validate({"event_id": "evt-sentinel", "certificate_id": [SENTINEL]})

    assert SENTINEL in str(caught.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        f'{{"event_id": "evt-sentinel", "note": "{SENTINEL}"'.encode(),
        json.dumps({"event_id": "evt-sentinel", "certificate_id": [SENTINEL]}).encode(),
    ],
    ids=["undecodable", "fails-validation"],
)
async def test_invalid_dead_letter_line_never_contains_the_body(client, json_log, body):
    message = _incoming(body=body, routing_key="cert.created")
    _wire_delivery(client, message)

    await client.consume(_validate_as_lifecycle_event)

    (line,) = _dead_letter_lines(json_log)
    assert (line["level"], line["reason"]) == ("WARN", "INVALID")
    assert "exception" not in line
    assert SENTINEL not in json.dumps(json_log())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body", "routing_key", "headers", "handler_error", "expected_failure"),
    [
        (
            b'{"key": "value"}',
            "cert.created",
            {},
            ValueError("retry"),
            "Failed to handle transport error (RETRY)",
        ),
        (
            b'{"key": "value"}',
            "cert.created",
            {HEADER_ATTEMPT: 3},
            ValueError("exhausted"),
            "Failed to handle transport error (DEAD_LETTER)",
        ),
        (b"\xff\xfe", "cert.created", {}, None, "Failed to dead-letter message"),
        (
            b'{"key": "value"}',
            "cert.unknown",
            {},
            UnsupportedOperationError("unsupported routing key: 'cert.unknown'"),
            "Failed to dead-letter message",
        ),
    ],
    ids=["retry", "exhausted", "invalid", "unsupported"],
)
async def test_failed_republish_writes_no_settled_line(
    client, json_log, body, routing_key, headers, handler_error, expected_failure
):
    message = _incoming(body=body, routing_key=routing_key, headers=headers)
    channel = _wire_delivery(client, message)
    channel.default_exchange.publish = AsyncMock(side_effect=ConnectionError("broker refused"))
    failing_dlx = AsyncMock()
    failing_dlx.publish = AsyncMock(side_effect=ConnectionError("broker refused"))
    channel.get_exchange = AsyncMock(return_value=failing_dlx)

    await client.consume(AsyncMock(side_effect=handler_error))

    messages = [entry["message"] for entry in json_log()]
    assert expected_failure in messages
    assert "Message dead-lettered" not in messages
    assert not any(text.startswith("Transport retry scheduled") for text in messages)
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_called()
