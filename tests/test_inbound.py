import re

import pytest

from cert_orchestrator.messaging.inbound import (
    HEADER_ORIGINAL_EXCHANGE,
    HEADER_ORIGINAL_ROUTING_KEY,
    HEADER_ROOT_EVENT_ID,
    DeadLetterReason,
    InboundMessage,
    TransportAction,
    UnsupportedOperationError,
    effective_exchange,
    effective_routing_key,
    next_transport_action,
    operation_for_routing_key,
    wait_queue_name,
)
from cert_orchestrator.models import LifecycleOperation


def test_next_transport_action_retry_when_attempt_less_than_max():
    assert next_transport_action(1, 3) == TransportAction.RETRY
    assert next_transport_action(2, 3) == TransportAction.RETRY


def test_next_transport_action_dead_letter_at_max():
    assert next_transport_action(3, 3) == TransportAction.DEAD_LETTER


def test_next_transport_action_dead_letter_beyond_max():
    assert next_transport_action(4, 3) == TransportAction.DEAD_LETTER


def test_effective_routing_key_without_header():
    assert effective_routing_key("original.key", {}) == "original.key"


def test_effective_routing_key_with_header():
    headers = {HEADER_ORIGINAL_ROUTING_KEY: "first.key"}
    assert effective_routing_key("second.key", headers) == "first.key"


def test_effective_exchange_with_header_uses_header_value():
    headers = {HEADER_ORIGINAL_EXCHANGE: "first.exchange"}
    assert effective_exchange("second.exchange", headers) == "first.exchange"


def test_effective_exchange_without_header_uses_message_exchange():
    assert effective_exchange("cert.events", {}) == "cert.events"


def test_effective_exchange_empty_header_wins_over_message_exchange():
    headers = {HEADER_ORIGINAL_EXCHANGE: ""}
    assert effective_exchange("cert.events", headers) == ""


@pytest.mark.parametrize(
    ("routing_key", "operation"),
    [
        ("cert.created", LifecycleOperation.CREATE),
        ("cert.renewed", LifecycleOperation.RENEW),
        ("cert.revoked", LifecycleOperation.REVOKE),
        ("cert.expired", LifecycleOperation.EXPIRE),
    ],
)
def test_operation_for_routing_key_maps_each_cert_api_key(routing_key, operation):
    assert operation_for_routing_key(routing_key) is operation


@pytest.mark.parametrize(
    "routing_key",
    [
        "",
        "cert.unknown",
        "cert.create",
        "cert.created.v2",
        "cert.#",
        "CERT.CREATED",
        "cert.Created",
        "cert.created ",
        "certificate.lifecycle.events",
    ],
)
def test_operation_for_routing_key_rejects_unknown_and_near_miss_keys(routing_key):
    with pytest.raises(UnsupportedOperationError, match=re.escape(repr(routing_key))):
        operation_for_routing_key(routing_key)


def test_unsupported_operation_error_is_a_value_error():
    assert issubclass(UnsupportedOperationError, ValueError)


def test_wait_queue_name():
    assert wait_queue_name("cert.orchestrator.wait", 5) == "cert.orchestrator.wait.5000ms"
    assert wait_queue_name("cert.orchestrator.wait", 10) == "cert.orchestrator.wait.10000ms"


def test_inbound_message_frozen():
    msg = InboundMessage(body={"key": "value"}, routing_key="test.key", headers={}, exchange="test.exchange")
    with pytest.raises(AttributeError):
        msg.routing_key = "other.key"


def test_dead_letter_reason_enum():
    assert DeadLetterReason.INVALID.value == "INVALID"
    assert DeadLetterReason.EXHAUSTED.value == "EXHAUSTED"
    assert DeadLetterReason.UNSUPPORTED_OPERATION.value == "UNSUPPORTED_OPERATION"


def test_root_event_header_name():
    assert HEADER_ROOT_EVENT_ID == "x-cert-orch-root-event-id"
