from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cert_orchestrator.config import Settings
from cert_orchestrator.messaging.inbound import (
    HEADER_ATTEMPT,
    HEADER_EXCEPTION_MESSAGE,
    HEADER_ORIGINAL_EXCHANGE,
    HEADER_ORIGINAL_ROUTING_KEY,
    DeadLetterReason,
)
from cert_orchestrator.messaging.rabbitmq import RabbitMQClient
from cert_orchestrator.schemas import CompletionEvent


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

    async def capture_retry(msg, inbound, attempt):
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
async def test_publish_completion(client):
    client.channel = AsyncMock()
    published = []

    async def capture_publish(msg, routing_key):
        published.append((msg, routing_key))

    client.channel.default_exchange.publish = capture_publish

    event = CompletionEvent(
        event_id="evt-1",
        certificate_id="cert-1",
        status="COMPLETED",
        retry_count=0,
        error=None,
    )

    await client.publish_completion(event)

    assert len(published) == 1
    msg, routing_key = published[0]
    assert routing_key == "test.completions"


@pytest.mark.asyncio
async def test_publish_retry_uses_wait_queue(client):
    client.channel = AsyncMock()
    client.channel.default_exchange = AsyncMock()
    queue = AsyncMock()
    client.channel.declare_queue = AsyncMock(return_value=queue)

    published = []

    async def capture_publish(msg, routing_key):
        published.append(routing_key)

    client.channel.default_exchange.publish = capture_publish

    await client.publish_retry({"test": "payload"}, delay_seconds=5, routing_key="test.key", headers={})

    assert len(published) == 1
    assert published[0] == "test.wait.5000ms"


@pytest.mark.asyncio
async def test_publish_retry_preserves_headers(client):
    client.channel = AsyncMock()
    client.channel.default_exchange = AsyncMock()
    queue = AsyncMock()
    client.channel.declare_queue = AsyncMock(return_value=queue)

    published = []

    async def capture_publish(msg, routing_key):
        published.append(msg.headers)

    client.channel.default_exchange.publish = capture_publish

    headers = {"x-correlation-id": "test-123"}
    await client.publish_retry({"test": "payload"}, delay_seconds=5, routing_key="test.key", headers=headers)

    assert len(published) == 1
    msg_headers = published[0]
    assert msg_headers["x-correlation-id"] == "test-123"
    assert HEADER_ORIGINAL_ROUTING_KEY in msg_headers


@pytest.mark.asyncio
async def test_wait_queue_has_dead_letter_routing_key(client):
    client.channel = AsyncMock()
    declare_calls = []

    async def capture_declare(name, durable, arguments):
        declare_calls.append((name, arguments))
        return AsyncMock()

    client.channel.declare_queue = capture_declare
    client.channel.default_exchange = AsyncMock()
    client.channel.default_exchange.publish = AsyncMock()

    await client.publish_retry({"test": "payload"}, delay_seconds=5, routing_key="test.key", headers={})

    assert len(declare_calls) == 1
    name, arguments = declare_calls[0]
    assert arguments["x-dead-letter-routing-key"] == "test.events"


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
        await client._schedule_transport_retry(message, inbound, 1)


@pytest.mark.asyncio
async def test_publish_completion_without_channel():
    client = RabbitMQClient()
    client.channel = None
    event = MagicMock(spec=CompletionEvent)

    with pytest.raises(RuntimeError, match="RabbitMQ channel is not initialized"):
        await client.publish_completion(event)


@pytest.mark.asyncio
async def test_publish_retry_without_channel():
    client = RabbitMQClient()
    client.channel = None

    with pytest.raises(RuntimeError, match="RabbitMQ channel is not initialized"):
        await client.publish_retry({"test": "payload"}, delay_seconds=5, routing_key="test.key", headers={})


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

    await client._schedule_transport_retry(message, inbound, 1)

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

    await client._schedule_transport_retry(message, inbound, 1)

    assert len(published_msgs) == 1
    headers = published_msgs[0]
    assert headers[HEADER_ORIGINAL_EXCHANGE] == "first.exchange"


@pytest.mark.asyncio
async def test_publish_retry_header_already_set_original_routing_key(client):
    client.channel = AsyncMock()
    client.channel.default_exchange = AsyncMock()
    queue = AsyncMock()
    client.channel.declare_queue = AsyncMock(return_value=queue)

    published = []

    async def capture_publish(msg, routing_key):
        published.append(msg.headers)

    client.channel.default_exchange.publish = capture_publish

    headers = {HEADER_ORIGINAL_ROUTING_KEY: "first.routing"}
    await client.publish_retry({"test": "payload"}, delay_seconds=5, routing_key="new.routing", headers=headers)

    assert len(published) == 1
    msg_headers = published[0]
    assert msg_headers[HEADER_ORIGINAL_ROUTING_KEY] == "first.routing"


@pytest.mark.asyncio
async def test_publish_retry_header_already_set_original_exchange(client):
    client.channel = AsyncMock()
    client.channel.default_exchange = AsyncMock()
    queue = AsyncMock()
    client.channel.declare_queue = AsyncMock(return_value=queue)

    published = []

    async def capture_publish(msg, routing_key):
        published.append(msg.headers)

    client.channel.default_exchange.publish = capture_publish

    headers = {HEADER_ORIGINAL_EXCHANGE: "first.exchange"}
    await client.publish_retry({"test": "payload"}, delay_seconds=5, routing_key="test.key", headers=headers)

    assert len(published) == 1
    msg_headers = published[0]
    assert msg_headers[HEADER_ORIGINAL_EXCHANGE] == "first.exchange"
