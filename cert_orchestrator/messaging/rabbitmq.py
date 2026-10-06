import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from pydantic import ValidationError

from cert_orchestrator.config import Settings
from cert_orchestrator.messaging.inbound import (
    HEADER_ATTEMPT,
    HEADER_DEAD_LETTER_REASON,
    HEADER_EXCEPTION_MESSAGE,
    HEADER_ORIGINAL_EXCHANGE,
    HEADER_ORIGINAL_ROUTING_KEY,
    DeadLetterReason,
    InboundMessage,
    TransportAction,
    effective_routing_key,
    next_transport_action,
    wait_queue_name,
)
from cert_orchestrator.schemas import CompletionEvent

logger = logging.getLogger(__name__)


class RabbitMQClient:
    """RabbitMQ client with dead-lettering, transport retry, and publish-then-ack semantics."""

    def __init__(self, config: Settings | None = None):
        self.config = config or Settings()
        self.url = self.config.resolved_rabbitmq_url
        self.connection: aio_pika.RobustConnection | None = None
        self.channel: aio_pika.abc.AbstractRobustChannel | None = None
        self._wait_queues: dict[str, aio_pika.abc.AbstractRobustQueue] = {}

    async def connect(self) -> None:
        """Connect to RabbitMQ and declare queues and exchanges."""
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        await self.channel.declare_queue(self.config.incoming_queue, durable=True)
        await self.channel.declare_queue(self.config.completion_queue, durable=True)

        dlx = await self.channel.declare_exchange(
            self.config.dead_letter_exchange, aio_pika.ExchangeType.TOPIC, durable=True
        )
        dlq = await self.channel.declare_queue(self.config.dead_letter_queue, durable=True)
        await dlq.bind(dlx, routing_key="#")

    async def close(self) -> None:
        """Close the RabbitMQ connection."""
        if self.connection:
            await self.connection.close()

    async def consume(self, handler: Callable[[InboundMessage], Awaitable[None]]) -> None:
        """Consume messages from the incoming queue with dead-lettering and transport retry."""
        if not self.channel:
            raise RuntimeError("RabbitMQ channel is not initialized")

        self._message_handler = handler
        queue = await self.channel.declare_queue(self.config.incoming_queue, durable=True)
        await queue.consume(self._on_message)

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        """Process an incoming message with validation and error handling."""
        try:
            payload = json.loads(message.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            if await self._dead_letter_or_nack(message, DeadLetterReason.INVALID, original_body=message.body):
                await message.ack()
            return

        inbound = InboundMessage(
            body=payload,
            routing_key=effective_routing_key(message.routing_key or "", message.headers),
            headers=message.headers,
        )

        try:
            await self._message_handler(inbound)
            await message.ack()
        except ValidationError:
            if await self._dead_letter_or_nack(message, DeadLetterReason.INVALID, original_body=message.body):
                await message.ack()
        except Exception as e:
            if await self._handle_transport_error(message, inbound, e):
                await message.ack()

    async def _dead_letter_or_nack(
        self,
        message: AbstractIncomingMessage,
        reason: DeadLetterReason,
        original_body: bytes | None = None,
        exception: Exception | None = None,
    ) -> bool:
        """Dead-letter a message or nack if dead-lettering fails. Returns True if successful."""
        try:
            await self._dead_letter(message, reason, original_body=original_body, exception=exception)
            return True
        except Exception as e:
            logger.error(
                "Failed to dead-letter message",
                extra={"reason": reason.value, "error": str(e)},
            )
            await message.nack(requeue=True)
            return False

    async def _handle_transport_error(
        self, message: AbstractIncomingMessage, inbound: InboundMessage, exception: Exception
    ) -> bool:
        """Handle transport errors with retry or dead-lettering."""
        attempt = message.headers.get(HEADER_ATTEMPT, 1)
        action = next_transport_action(attempt, self.config.max_delivery_attempts)
        try:
            if action == TransportAction.RETRY:
                await self._schedule_transport_retry(message, inbound, attempt)
            else:
                await self._dead_letter(message, DeadLetterReason.EXHAUSTED, exception=exception)
        except Exception as e:
            logger.error("Failed to handle transport error", extra={"action": action.value, "error": str(e)})
            await message.nack(requeue=True)
            return False
        return True

    async def _dead_letter(
        self,
        message: AbstractIncomingMessage,
        reason: DeadLetterReason,
        original_body: bytes | None = None,
        exception: Exception | None = None,
    ) -> None:
        """Publish message to dead-letter exchange with reason and exception headers."""
        if not self.channel:
            raise RuntimeError("RabbitMQ channel is not initialized")

        body = original_body or message.body
        routing_key = effective_routing_key(message.routing_key or "", message.headers)

        headers = dict(message.headers) if message.headers else {}
        if HEADER_ORIGINAL_ROUTING_KEY not in headers:
            headers[HEADER_ORIGINAL_ROUTING_KEY] = routing_key
        if HEADER_ORIGINAL_EXCHANGE not in headers:
            headers[HEADER_ORIGINAL_EXCHANGE] = message.exchange or ""
        headers[HEADER_DEAD_LETTER_REASON] = reason.value

        if exception:
            exc_msg = f"{type(exception).__name__}: {str(exception)}"[:500]
            headers[HEADER_EXCEPTION_MESSAGE] = exc_msg

        message_obj = aio_pika.Message(
            body=body,
            content_type=message.content_type,
            headers=headers,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=message.message_id,
            correlation_id=message.correlation_id,
        )

        dlx = await self.channel.get_exchange(self.config.dead_letter_exchange)
        await dlx.publish(message_obj, routing_key=routing_key)

    async def _ensure_wait_queue(self, delay_seconds: int) -> str:
        """Ensure wait queue exists and return its name."""
        queue_name = wait_queue_name(self.config.wait_queue_prefix, delay_seconds)
        if queue_name not in self._wait_queues:
            queue = await self.channel.declare_queue(
                queue_name,
                durable=True,
                arguments={
                    "x-message-ttl": delay_seconds * 1000,
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": self.config.incoming_queue,
                },
            )
            self._wait_queues[queue_name] = queue
        return queue_name

    async def _schedule_transport_retry(
        self, message: AbstractIncomingMessage, inbound: InboundMessage, attempt: int
    ) -> None:
        """Schedule transport retry via wait queue with exponential backoff."""
        if not self.channel:
            raise RuntimeError("RabbitMQ channel is not initialized")

        delay_seconds = self.config.backoff_seconds * attempt
        queue_name = await self._ensure_wait_queue(delay_seconds)

        headers = dict(message.headers) if message.headers else {}
        if HEADER_ORIGINAL_ROUTING_KEY not in headers:
            headers[HEADER_ORIGINAL_ROUTING_KEY] = inbound.routing_key
        if HEADER_ORIGINAL_EXCHANGE not in headers:
            headers[HEADER_ORIGINAL_EXCHANGE] = message.exchange or ""
        headers[HEADER_ATTEMPT] = attempt + 1

        message_obj = aio_pika.Message(
            body=message.body,
            content_type=message.content_type,
            headers=headers,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

        await self.channel.default_exchange.publish(message_obj, routing_key=queue_name)

    async def publish_completion(self, event: CompletionEvent) -> None:
        """Publish a completion event to the completion queue."""
        if not self.channel:
            raise RuntimeError("RabbitMQ channel is not initialized")

        message = aio_pika.Message(
            body=event.model_dump_json().encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await self.channel.default_exchange.publish(message, routing_key=self.config.completion_queue)

    async def publish_retry(
        self, payload: dict, delay_seconds: int, routing_key: str, headers: Mapping[str, Any]
    ) -> None:
        """Publish a business retry via a wait queue with TTL-based delay."""
        if not self.channel:
            raise RuntimeError("RabbitMQ channel is not initialized")

        queue_name = await self._ensure_wait_queue(delay_seconds)

        msg_headers = dict(headers) if headers else {}
        if HEADER_ORIGINAL_ROUTING_KEY not in msg_headers:
            msg_headers[HEADER_ORIGINAL_ROUTING_KEY] = routing_key
        if HEADER_ORIGINAL_EXCHANGE not in msg_headers:
            msg_headers[HEADER_ORIGINAL_EXCHANGE] = ""

        message = aio_pika.Message(
            body=json.dumps(payload).encode("utf-8"),
            content_type="application/json",
            headers=msg_headers,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

        await self.channel.default_exchange.publish(message, routing_key=queue_name)
