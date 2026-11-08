import asyncio
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from prometheus_client import REGISTRY
from sqlalchemy.orm import Session

from cert_orchestrator.config import Settings, settings
from cert_orchestrator.messaging.handlers import LifecycleMessageHandler
from cert_orchestrator.messaging.inbound import HEADER_ATTEMPT, HEADER_CORRELATION_ID, DeadLetterReason
from cert_orchestrator.messaging.rabbitmq import RabbitMQClient
from cert_orchestrator.messaging.relay import OutboxRelay
from cert_orchestrator.metrics import UNKNOWN_OPERATION, ConsumeOutcome, render_metrics, time_event_processing
from cert_orchestrator.models import LifecycleOperation, LifecycleState, OutboxKind

EXPOSED_FAMILIES = {
    "cert_orchestrator_events_consumed",
    "cert_orchestrator_event_processing_seconds",
    "cert_orchestrator_lifecycle_transitions",
    "cert_orchestrator_dead_letters",
    "cert_orchestrator_outbox_published",
    "cert_orchestrator_outbox_publish_failures",
    "cert_orchestrator_outbox_pending",
    "cert_orchestrator_outbox_oldest_pending_age_seconds",
    "cert_orchestrator_relay_pass_failures",
}
OPERATIONS = [operation.value for operation in LifecycleOperation]
CLOSED_VALUES = {
    "operation": {*OPERATIONS, UNKNOWN_OPERATION},
    "state": {state.value for state in LifecycleState},
    "outcome": {outcome.value for outcome in ConsumeOutcome},
    "reason": {reason.value for reason in DeadLetterReason},
    "kind": {kind.value for kind in OutboxKind},
}
CONSUMED_TOTAL = "cert_orchestrator_events_consumed_total"
TRANSITIONS_TOTAL = "cert_orchestrator_lifecycle_transitions_total"
PROCESSING_COUNT = "cert_orchestrator_event_processing_seconds_count"
DEAD_LETTERS_TOTAL = "cert_orchestrator_dead_letters_total"
PENDING_GAUGE = "cert_orchestrator_outbox_pending"


def _cert_orchestrator_samples() -> list[Any]:
    return [
        sample
        for family in REGISTRY.collect()
        if family.name.startswith("cert_orchestrator_")
        for sample in family.samples
    ]


def _label_sets() -> set[tuple[str, frozenset[tuple[str, str]]]]:
    """Every exported cert_orchestrator_ sample as (name, labels), with the histogram's le label left out."""
    return {
        (sample.name, frozenset((key, value) for key, value in sample.labels.items() if key != "le"))
        for sample in _cert_orchestrator_samples()
    }


def _exported_labels(name: str) -> set[frozenset[tuple[str, str]]]:
    return {frozenset(sample.labels.items()) for sample in _cert_orchestrator_samples() if sample.name == name}


def test_exactly_the_nine_metrics_are_declared():
    families = {family.name for family in REGISTRY.collect() if family.name.startswith("cert_orchestrator_")}

    assert families == EXPOSED_FAMILIES


def test_every_closed_label_combination_is_exported_before_it_moves():
    assert _exported_labels(CONSUMED_TOTAL) == {
        frozenset({("operation", operation), ("outcome", outcome.value)})
        for operation in [*OPERATIONS, UNKNOWN_OPERATION]
        for outcome in ConsumeOutcome
    }
    assert _exported_labels(TRANSITIONS_TOTAL) == {
        frozenset({("operation", operation), ("state", state.value)})
        for operation in OPERATIONS
        for state in LifecycleState
    }
    assert _exported_labels(PROCESSING_COUNT) == {frozenset({("operation", op)}) for op in OPERATIONS}
    assert _exported_labels(DEAD_LETTERS_TOTAL) == {frozenset({("reason", r.value)}) for r in DeadLetterReason}
    assert _exported_labels(PENDING_GAUGE) == {frozenset({("kind", kind.value)}) for kind in OutboxKind}


def test_the_scrape_uses_prometheus_names_without_created_series():
    lines = render_metrics().decode("utf-8").splitlines()
    ours = [line for line in lines if "cert_orchestrator_" in line]

    assert ours, "the cert_orchestrator_ metrics must be in the scrape"
    assert not any("cert.orchestrator" in line for line in lines), "dotted names must not reach the registry"
    assert not any("_created" in line for line in ours), "disable_created_metrics must hold"


def test_time_event_processing_observes_even_when_the_block_raises(metric_value):
    before = metric_value(PROCESSING_COUNT, operation="RENEW")

    with pytest.raises(RuntimeError, match="handler failed"), time_event_processing(LifecycleOperation.RENEW):
        raise RuntimeError("handler failed")

    assert metric_value(PROCESSING_COUNT, operation="RENEW") - before == 1


class PartlyRefusingPublisher:
    """Accepts every outbox row except the message ids it was told to refuse."""

    def __init__(self, refuse: frozenset[str]) -> None:
        self.refuse = refuse

    async def publish_outbox_message(self, row: Any) -> None:
        if row.message_id in self.refuse:
            raise ConnectionError("broker refused")


def _probe_client() -> RabbitMQClient:
    config = Settings(
        incoming_queue="probe.in",
        completion_queue="probe.out",
        dead_letter_exchange="probe.dlx",
        dead_letter_queue="probe.dlq",
        wait_queue_prefix="probe.wait",
        max_delivery_attempts=3,
        backoff_seconds=1,
    )
    client = RabbitMQClient(config=config)
    channel = AsyncMock()
    channel.default_exchange = AsyncMock()
    channel.default_exchange.publish = AsyncMock()
    dlx = AsyncMock()
    dlx.publish = AsyncMock()
    channel.get_exchange = AsyncMock(return_value=dlx)
    client.channel = channel
    return client


def _delivery(body: bytes, routing_key: str = "cert.created", headers: dict[str, Any] | None = None) -> MagicMock:
    message = MagicMock()
    message.body = body
    message.routing_key = routing_key
    message.headers = headers if headers is not None else {}
    message.exchange = "cert.events"
    message.message_id = None
    message.correlation_id = None
    message.content_type = "application/json"
    message.ack = AsyncMock()
    message.nack = AsyncMock()
    return message


def _lifecycle_body(event_id: str, certificate_id: str, should_fail: bool = False) -> bytes:
    body = {"event_id": event_id, "certificate_id": certificate_id, "payload": {"should_fail": should_fail}}
    return json.dumps(body).encode("utf-8")


async def _deliver(client: RabbitMQClient, handler: Callable[..., Any], message: MagicMock) -> None:
    queue = AsyncMock()

    async def fake_consume(callback: Callable[..., Any]) -> None:
        await callback(message)

    queue.consume.side_effect = fake_consume
    client.channel.declare_queue.return_value = queue
    await client.consume(handler)


@pytest.fixture
def handler_database(sqlite_engine, monkeypatch) -> None:
    """Point the lifecycle handler at the test database, as the handler tests do."""

    @contextmanager
    def provider() -> Iterator[Session]:
        with Session(sqlite_engine) as session:
            yield session

    monkeypatch.setattr("cert_orchestrator.messaging.handlers.get_session", provider)


@pytest.mark.asyncio
async def test_metric_label_values_stay_within_closed_sets(handler_database, session_factory, monkeypatch):
    """Exercise every counted path with recognisable ids, then check the label sets and that no id is exported."""
    cert, event, correlation = "cert-probe-7731", "evt-probe-7731", "corr-probe-7731"
    label_sets_before = _label_sets()
    handler = LifecycleMessageHandler(wake=lambda: None)
    client = _probe_client()

    # Processed, then a duplicate of the same delivery.
    ok = _delivery(_lifecycle_body(event, cert), headers={HEADER_CORRELATION_ID: correlation})
    await _deliver(client, handler.handle, ok)
    await _deliver(client, handler.handle, ok)
    # Processing: one failed create schedules a business retry.
    await _deliver(client, handler.handle, _delivery(_lifecycle_body(f"{event}-b", cert, should_fail=True)))
    # Failed: with no retries left, a failed renew completes as FAILED.
    monkeypatch.setattr(settings, "max_retries", 1)
    await _deliver(client, handler.handle, _delivery(_lifecycle_body(f"{event}-c", cert, True), "cert.renewed"))
    # Revoke: recorded as COMPLETED with no outbox row.
    await _deliver(client, handler.handle, _delivery(_lifecycle_body(f"{event}-d", cert), "cert.revoked"))
    # Dead letters: unsupported key, undecodable body, and an exhausted transport budget.
    await _deliver(client, handler.handle, _delivery(_lifecycle_body(f"{event}-e", cert), "cert.unknown"))
    await _deliver(client, handler.handle, _delivery(b"\xff\xfe"))
    failing = AsyncMock(side_effect=ValueError("handler failed"))
    await _deliver(client, failing, _delivery(_lifecycle_body(f"{event}-g", cert), headers={HEADER_ATTEMPT: 3}))
    # Transport retry: the first failure of a delivery goes to the wait queue.
    await _deliver(client, failing, _delivery(_lifecycle_body(f"{event}-f", cert)))

    # Relay: the completions publish; the business retry is refused, so one publish failure is counted.
    refused = PartlyRefusingPublisher(frozenset({f"{event}-b:retry:1"}))
    assert await OutboxRelay(session_factory, refused, settings).run_once() == 2

    # Relay pass failure: a pass that raises is counted.
    stop = asyncio.Event()

    async def failing_pass() -> int:
        stop.set()
        raise RuntimeError("database unavailable")

    relay = OutboxRelay(session_factory, PartlyRefusingPublisher(frozenset()), settings)
    relay.run_once = failing_pass  # type: ignore[method-assign]
    await asyncio.wait_for(relay.run(stop), timeout=5.0)

    assert _label_sets() == label_sets_before, "no label set may appear that was not created at import"
    for sample in _cert_orchestrator_samples():
        for key, value in sample.labels.items():
            if key == "le":
                continue
            assert key in CLOSED_VALUES, f"{sample.name} has an unexpected label {key}"
            assert value in CLOSED_VALUES[key], f"{sample.name} {key}={value!r} is outside its closed set"
            if value == UNKNOWN_OPERATION:
                assert "events_consumed" in sample.name, "unknown is only a value of events_consumed"
    text = render_metrics().decode("utf-8")
    for forbidden in (cert, event, correlation):
        assert forbidden not in text, f"{forbidden} appears in the scrape"
