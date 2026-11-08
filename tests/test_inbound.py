import re

import pytest

from cert_orchestrator.messaging.inbound import (
    HEADER_ATTEMPT,
    HEADER_CORRELATION_ID,
    HEADER_ORIGINAL_EXCHANGE,
    HEADER_ORIGINAL_ROUTING_KEY,
    HEADER_ROOT_EVENT_ID,
    HEADER_X_DEATH,
    TRANSPORT_HEADERS,
    DeadLetterReason,
    InboundMessage,
    TransportAction,
    UnsupportedOperationError,
    business_retry_headers,
    completion_correlation_id,
    effective_exchange,
    effective_routing_key,
    inbound_correlation_id,
    known_operation,
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


VALID_36 = "a" * 36


@pytest.mark.parametrize("inbound", ["corr-1", " padded id ", VALID_36])
def test_completion_correlation_returns_a_valid_inbound_header_unchanged(inbound):
    assert completion_correlation_id({HEADER_CORRELATION_ID: inbound}, "root-1") == inbound


@pytest.mark.parametrize("inbound", [None, "", "   ", "a" * 37, b"corr", 42])
def test_completion_correlation_falls_back_to_a_valid_root_when_the_header_is_unusable(inbound):
    assert completion_correlation_id({HEADER_CORRELATION_ID: inbound}, "root-1") == "root-1"


def test_completion_correlation_falls_back_to_root_when_the_header_is_absent():
    assert completion_correlation_id({}, "root-1") == "root-1"


def test_completion_correlation_counts_utf16_units_not_code_points():
    astral = "\U0001f600"
    thirty_five_ascii_plus_astral = "a" * 35 + astral
    thirty_four_ascii_plus_astral = "a" * 34 + astral

    assert len(thirty_five_ascii_plus_astral) == 36
    assert completion_correlation_id({HEADER_CORRELATION_ID: thirty_five_ascii_plus_astral}, "root-1") == "root-1"
    assert completion_correlation_id({HEADER_CORRELATION_ID: thirty_four_ascii_plus_astral}, "root-1") == (
        thirty_four_ascii_plus_astral
    )


def test_completion_correlation_is_none_when_neither_header_nor_root_is_valid():
    assert completion_correlation_id({HEADER_CORRELATION_ID: ""}, "r" * 37) is None
    assert completion_correlation_id({}, "") is None


def test_business_retry_headers_strip_attempt_and_death_and_keep_other_headers():
    headers = {HEADER_ATTEMPT: 3, HEADER_X_DEATH: [{"reason": "expired"}], "x-correlation-id": "keep-me"}

    result = business_retry_headers(headers, "cert.created", "cert.events")

    assert HEADER_ATTEMPT not in result
    assert HEADER_X_DEATH not in result
    assert result["x-correlation-id"] == "keep-me"


def test_business_retry_headers_set_original_routing_key_and_exchange_when_absent():
    result = business_retry_headers({}, "cert.renewed", "cert.events")

    assert result == {HEADER_ORIGINAL_ROUTING_KEY: "cert.renewed", HEADER_ORIGINAL_EXCHANGE: "cert.events"}


def test_business_retry_headers_keep_existing_original_routing_key_and_exchange():
    headers = {HEADER_ORIGINAL_ROUTING_KEY: "first.routing", HEADER_ORIGINAL_EXCHANGE: "first.exchange"}

    result = business_retry_headers(headers, "new.routing", "new.exchange")

    assert result[HEADER_ORIGINAL_ROUTING_KEY] == "first.routing"
    assert result[HEADER_ORIGINAL_EXCHANGE] == "first.exchange"


def test_business_retry_headers_do_not_mutate_the_input():
    headers = {HEADER_ATTEMPT: 2, "x-keep": "v"}

    business_retry_headers(headers, "cert.created", "cert.events")

    assert headers == {HEADER_ATTEMPT: 2, "x-keep": "v"}


def test_transport_headers_are_the_attempt_and_death_headers():
    assert TRANSPORT_HEADERS == (HEADER_ATTEMPT, HEADER_X_DEATH)


@pytest.mark.parametrize("inbound", ["corr-1", " padded id ", VALID_36])
def test_inbound_correlation_id_returns_a_valid_header_unchanged(inbound):
    assert inbound_correlation_id({HEADER_CORRELATION_ID: inbound}) == inbound


@pytest.mark.parametrize("inbound", [None, "", "   ", "a" * 37, b"corr", 42])
def test_inbound_correlation_id_is_none_for_a_header_cert_api_would_reject(inbound):
    assert inbound_correlation_id({HEADER_CORRELATION_ID: inbound}) is None


def test_inbound_correlation_id_is_none_when_the_header_is_absent():
    assert inbound_correlation_id({}) is None


def test_completion_correlation_id_accepts_a_missing_root_and_uses_a_valid_inbound_header():
    assert completion_correlation_id({HEADER_CORRELATION_ID: "corr-1"}, None) == "corr-1"


def test_completion_correlation_id_is_none_for_an_invalid_header_and_a_missing_root():
    assert completion_correlation_id({HEADER_CORRELATION_ID: "a" * 37}, None) is None


@pytest.mark.parametrize(
    ("routing_key", "operation"),
    [
        ("cert.created", LifecycleOperation.CREATE),
        ("cert.renewed", LifecycleOperation.RENEW),
        ("cert.revoked", LifecycleOperation.REVOKE),
        ("cert.expired", LifecycleOperation.EXPIRE),
    ],
)
def test_known_operation_maps_each_cert_api_routing_key(routing_key, operation):
    assert known_operation(routing_key) is operation


@pytest.mark.parametrize("routing_key", ["cert.unknown", "", ["cert.created"], {"k": "v"}, None, 7])
def test_known_operation_is_none_for_an_unknown_or_non_string_key_and_never_raises(routing_key):
    assert known_operation(routing_key) is None
