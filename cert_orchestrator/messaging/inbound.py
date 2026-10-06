from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

HEADER_ORIGINAL_ROUTING_KEY = "x-original-routingKey"
HEADER_ORIGINAL_EXCHANGE = "x-original-exchange"
HEADER_EXCEPTION_MESSAGE = "x-exception-message"
HEADER_ATTEMPT = "x-cert-orch-attempt"
HEADER_DEAD_LETTER_REASON = "x-cert-orch-dead-letter-reason"


@dataclass(frozen=True)
class InboundMessage:
    """Inbound message with decoded body, routing key, and headers from RabbitMQ."""

    body: dict[str, Any]
    routing_key: str
    headers: Mapping[str, Any]


class DeadLetterReason(str, Enum):
    """Reason for dead-lettering a message."""

    INVALID = "INVALID"
    EXHAUSTED = "EXHAUSTED"


class TransportAction(str, Enum):
    """Transport action: retry or dead-letter."""

    RETRY = "RETRY"
    DEAD_LETTER = "DEAD_LETTER"


def next_transport_action(attempt: int, max_attempts: int) -> TransportAction:
    """Determine transport action based on attempt number."""
    if attempt < max_attempts:
        return TransportAction.RETRY
    return TransportAction.DEAD_LETTER


def effective_routing_key(routing_key: str, headers: Mapping[str, Any]) -> str:
    """Get effective routing key, using original if present."""
    return headers.get(HEADER_ORIGINAL_ROUTING_KEY, routing_key)


def wait_queue_name(prefix: str, delay_seconds: int) -> str:
    """Generate wait queue name from prefix and delay."""
    return f"{prefix}.{delay_seconds * 1000}ms"
