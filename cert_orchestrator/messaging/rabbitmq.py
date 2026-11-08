import json
import logging
from collections.abc import Awaitable, Callable

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from pydantic import ValidationError

from cert_orchestrator.config import Settings
from cert_orchestrator.logging_context import log_context
from cert_orchestrator.messaging.inbound import (
    HEADER_ATTEMPT,
    HEADER_DEAD_LETTER_REASON,
    HEADER_EXCEPTION_MESSAGE,
    HEADER_ORIGINAL_EXCHANGE,
    HEADER_ORIGINAL_ROUTING_KEY,
    DeadLetterReason,
    InboundMessage,
    TransportAction,
    UnsupportedOperationError,
    effective_exchange,
    effective_routing_key,
    inbound_correlation_id,
    known_operation,
    next_transport_action,
    wait_queue_name,
)
from cert_orchestrator.metrics import ConsumeOutcome, record_dead_letter, record_event_consumed
from cert_orchestrator.models import OutboxKind, OutboxMessage

logger = logging.getLogger(__name__)

# The body's key for the event id; read for log context only, never for routing.
EVENT_ID_KEY = "event_id"


def _body_event_id(body: bytes) -> str | None:
    """Return the body's event_id when the body is a JSON object holding a non-blank string one, else None."""
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    event_id = payload.get(EVENT_ID_KEY) if isinstance(payload, dict) else None
    if isinstance(event_id, str) and event_id.strip():
        return event_id
    return None


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
        """Process one delivery with its log context set for the whole handling."""
        with log_context(
            correlationId=inbound_correlation_id(message.headers),
            eventId=_body_event_id(message.body),
        ):
            await self._settle_delivery(message)

    async def _settle_delivery(self, message: AbstractIncomingMessage) -> None:
        """Decode, hand to the handler, then ack, dead-letter, transport-retry or nack the delivery."""
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
            exchange=effective_exchange(message.exchange or "", message.headers),
        )

        try:
            await self._message_handler(inbound)
        except Exception as e:
            await self._handle_handler_error(message, inbound, e)
        else:
            # Outside the handler try: an ack failure after a successful handler is not a handler failure.
            await message.ack()

    async def _handle_handler_error(
        self,
        message: AbstractIncomingMessage,
        inbound: InboundMessage,
        exception: Exception,
    ) -> None:
        """Dead-letter or transport-retry a message whose handler raised, then ack it once settled."""
        if isinstance(exception, UnsupportedOperationError):
            settled = await self._dead_letter_or_nack(
                message,
                DeadLetterReason.UNSUPPORTED_OPERATION,
                original_body=message.body,
            )
        elif isinstance(exception, ValidationError):
            settled = await self._dead_letter_or_nack(
                message,
                DeadLetterReason.INVALID,
                original_body=message.body,
            )
        else:
            settled = await self._handle_transport_error(message, inbound, exception)
        if settled:
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
        except Exception:
            logger.exception("Failed to dead-letter message", extra={"reason": reason.value})
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
                await self._schedule_transport_retry(message, inbound, attempt, exception)
            else:
                await self._dead_letter(message, DeadLetterReason.EXHAUSTED, exception=exception)
        except Exception:
            logger.exception("Failed to handle transport error (%s)", action.value)
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
        record_dead_letter(reason)
        record_event_consumed(known_operation(routing_key), ConsumeOutcome.DEAD_LETTERED)
        # Only EXHAUSTED carries a traceback, whatever exception a caller passes (C-19).
        if reason == DeadLetterReason.EXHAUSTED:
            logger.error("Message dead-lettered", exc_info=exception, extra={"reason": reason.value})
        else:
            logger.warning("Message dead-lettered", extra={"reason": reason.value})

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
        self, message: AbstractIncomingMessage, inbound: InboundMessage, attempt: int, exception: Exception
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
            message_id=message.message_id,
            correlation_id=message.correlation_id,
        )

        await self.channel.default_exchange.publish(message_obj, routing_key=queue_name)
        record_event_consumed(known_operation(inbound.routing_key), ConsumeOutcome.TRANSPORT_RETRY)
        logger.warning(
            "Transport retry scheduled (attempt %d of %d, delay %d s)",
            attempt,
            self.config.max_delivery_attempts,
            delay_seconds,
            exc_info=exception,
        )

    async def publish_outbox_message(self, row: OutboxMessage) -> None:
        """Publish a stored outbox row as written: completions to the completion queue, retries via their wait queue."""
        if not self.channel:
            raise RuntimeError("RabbitMQ channel is not initialized")

        message = aio_pika.Message(
            body=row.body.encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=row.message_id,
            headers=row.headers,
        )
        if row.kind == OutboxKind.COMPLETION:
            routing_key = self.config.completion_queue
        else:
            routing_key = await self._ensure_wait_queue(row.delay_seconds)
        await self.channel.default_exchange.publish(message, routing_key=routing_key)
