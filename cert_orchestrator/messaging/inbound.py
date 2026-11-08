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
HEADER_CORRELATION_ID = "x-correlation-id"
# Source: cert-api LoggingContext.isValidCorrelationId. Its length limit counts UTF-16 code units,
# as Java String.length() does.
CORRELATION_ID_MAX_LENGTH = 36

# Delivery-transport headers that a business retry must not carry: it is a new event with a fresh budget.
TRANSPORT_HEADERS = (HEADER_ATTEMPT, HEADER_X_DEATH)

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


def known_operation(routing_key: Any) -> LifecycleOperation | None:
    """Return the lifecycle operation for a cert-api routing key, or None for an unknown or non-string key."""
    if not isinstance(routing_key, str):
        return None
    return _OPERATION_BY_ROUTING_KEY.get(routing_key)


def operation_for_routing_key(routing_key: str) -> LifecycleOperation:
    """Map a cert-api routing key to its lifecycle operation, or raise UnsupportedOperationError."""
    operation = known_operation(routing_key)
    if operation is None:
        raise UnsupportedOperationError(f"unsupported routing key: {routing_key!r}")
    return operation


def wait_queue_name(prefix: str, delay_seconds: int) -> str:
    """Generate wait queue name from prefix and delay."""
    return f"{prefix}.{delay_seconds * 1000}ms"


def _is_valid_correlation_id(value: Any) -> bool:
    """Return True when cert-api would accept the value as a correlation id."""
    if not isinstance(value, str) or value.strip() == "":
        return False
    # Two bytes per UTF-16 code unit; surrogatepass keeps lone surrogates countable as one unit each.
    return len(value.encode("utf-16-le", errors="surrogatepass")) // 2 <= CORRELATION_ID_MAX_LENGTH


def inbound_correlation_id(headers: Mapping[str, Any]) -> str | None:
    """Return the x-correlation-id header if cert-api would accept it, else None."""
    inbound = headers.get(HEADER_CORRELATION_ID)
    return inbound if _is_valid_correlation_id(inbound) else None


def completion_correlation_id(headers: Mapping[str, Any], root_event_id: str | None) -> str | None:
    """Return the inbound correlation id if valid, else the root event id if valid, else None."""
    inbound = inbound_correlation_id(headers)
    if inbound is not None:
        return inbound
    if _is_valid_correlation_id(root_event_id):
        return root_event_id
    return None


def business_retry_headers(headers: Mapping[str, Any], routing_key: str, exchange: str) -> dict[str, Any]:
    """Return a new header dict for a business retry: transport headers removed, x-original-* set if absent."""
    result = {name: value for name, value in headers.items() if name not in TRANSPORT_HEADERS}
    result.setdefault(HEADER_ORIGINAL_ROUTING_KEY, routing_key)
    result.setdefault(HEADER_ORIGINAL_EXCHANGE, exchange)
    return result
