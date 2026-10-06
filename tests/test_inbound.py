import pytest

from cert_orchestrator.messaging.inbound import (
    HEADER_ORIGINAL_ROUTING_KEY,
    DeadLetterReason,
    InboundMessage,
    TransportAction,
    effective_routing_key,
    next_transport_action,
    wait_queue_name,
)


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


def test_wait_queue_name():
    assert wait_queue_name("cert.orchestrator.wait", 5) == "cert.orchestrator.wait.5000ms"
    assert wait_queue_name("cert.orchestrator.wait", 10) == "cert.orchestrator.wait.10000ms"


def test_inbound_message_frozen():
    msg = InboundMessage(body={"key": "value"}, routing_key="test.key", headers={})
    with pytest.raises(AttributeError):
        msg.routing_key = "other.key"


def test_dead_letter_reason_enum():
    assert DeadLetterReason.INVALID.value == "INVALID"
    assert DeadLetterReason.EXHAUSTED.value == "EXHAUSTED"
