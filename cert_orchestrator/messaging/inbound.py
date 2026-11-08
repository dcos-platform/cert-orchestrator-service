from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

from cert_orchestrator.models import LifecycleOperation

HEADER_ORIGINAL_ROUTING_KEY = "x-original-routingKey"
HEADER_ORIGINAL_EXCHANGE = "x-original-exchange"
HEADER_EXCEPTION_MESSAGE = "x-exception-message"
HEADER_ATTEMPT = "x-cert-orch-attempt"
HEADER_DEAD_LETTER_REASON = "x-cert-orch-dead-letter-reason"
HEADER_ROOT_EVENT_ID = "x-cert-orch-root-event-id"
HEADER_X_DEATH = "x-death"

_OPERATION_BY_ROUTING_KEY: dict[str, LifecycleOperation] = {
    "cert.created": LifecycleOperation.CREATE,
    "cert.renewed": LifecycleOperation.RENEW,
    "cert.revoked": LifecycleOperation.REVOKE,
    "cert.expired": LifecycleOperation.EXPIRE,
}


class UnsupportedOperationError(ValueError):
    """Raised when a routing key does not name a known lifecycle operation."""


@dataclass(frozen=True)
class InboundMessage:
    """Inbound message with decoded body, routing key, headers and exchange from RabbitMQ."""

    body: dict[str, Any]
    routing_key: str
    headers: Mapping[str, Any]
    exchange: str


class DeadLetterReason(str, Enum):
    """Reason for dead-lettering a message."""

    INVALID = "INVALID"
    EXHAUSTED = "EXHAUSTED"
    UNSUPPORTED_OPERATION = "UNSUPPORTED_OPERATION"


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


def effective_exchange(exchange: str, headers: Mapping[str, Any]) -> str:
    """Get effective exchange, using original if present."""
    return headers.get(HEADER_ORIGINAL_EXCHANGE, exchange)


def operation_for_routing_key(routing_key: str) -> LifecycleOperation:
    """Map a cert-api routing key to its lifecycle operation, or raise UnsupportedOperationError."""
    operation = _OPERATION_BY_ROUTING_KEY.get(routing_key)
    if operation is None:
        raise UnsupportedOperationError(f"unsupported routing key: {routing_key!r}")
    return operation


def wait_queue_name(prefix: str, delay_seconds: int) -> str:
    """Generate wait queue name from prefix and delay."""
    return f"{prefix}.{delay_seconds * 1000}ms"
