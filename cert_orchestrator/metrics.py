from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from enum import Enum
from time import perf_counter

import prometheus_client
from prometheus_client import Counter, Gauge, Histogram
from prometheus_client.exposition import CONTENT_TYPE_PLAIN_0_0_4, generate_latest

from cert_orchestrator.messaging.inbound import DeadLetterReason
from cert_orchestrator.models import LifecycleOperation, LifecycleState, OutboxKind

# Without this every counter and histogram also exports a *_created series (C-7). Must precede the declarations below.
prometheus_client.disable_created_metrics()

METRICS_CONTENT_TYPE = CONTENT_TYPE_PLAIN_0_0_4
# Operation label for a consumed delivery whose routing key is not one of the four operations.
UNKNOWN_OPERATION = "unknown"

# Dotted names as the code states them; the client receives the Prometheus form (C-8).
EVENTS_CONSUMED_METRIC = "cert.orchestrator.events.consumed"
EVENT_PROCESSING_METRIC = "cert.orchestrator.event.processing.seconds"
LIFECYCLE_TRANSITIONS_METRIC = "cert.orchestrator.lifecycle.transitions"
DEAD_LETTERS_METRIC = "cert.orchestrator.dead.letters"
OUTBOX_PUBLISHED_METRIC = "cert.orchestrator.outbox.published"
OUTBOX_PUBLISH_FAILURES_METRIC = "cert.orchestrator.outbox.publish.failures"
OUTBOX_PENDING_METRIC = "cert.orchestrator.outbox.pending"
OUTBOX_OLDEST_PENDING_AGE_METRIC = "cert.orchestrator.outbox.oldest.pending.age.seconds"
RELAY_PASS_FAILURES_METRIC = "cert.orchestrator.relay.pass.failures"


class ConsumeOutcome(str, Enum):
    """How one consumed delivery ended, as seen by the consumer."""

    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    DEAD_LETTERED = "dead_lettered"
    TRANSPORT_RETRY = "transport_retry"


def _exposed(dotted: str) -> str:
    return dotted.replace(".", "_")


_events_consumed = Counter(
    _exposed(EVENTS_CONSUMED_METRIC),
    "Deliveries consumed, by operation and outcome.",
    ["operation", "outcome"],
)
_event_processing_seconds = Histogram(
    _exposed(EVENT_PROCESSING_METRIC),
    "Seconds spent handling a validated lifecycle event, including handlings that raise.",
    ["operation"],
)
_lifecycle_transitions = Counter(
    _exposed(LIFECYCLE_TRANSITIONS_METRIC),
    "Lifecycle records moved to a state by a committed handling.",
    ["operation", "state"],
)
_dead_letters = Counter(
    _exposed(DEAD_LETTERS_METRIC),
    "Deliveries dead-lettered, by reason.",
    ["reason"],
)
_outbox_published = Counter(
    _exposed(OUTBOX_PUBLISHED_METRIC),
    "Outbox rows the broker confirmed, by kind.",
    ["kind"],
)
_outbox_publish_failures = Counter(
    _exposed(OUTBOX_PUBLISH_FAILURES_METRIC),
    "Outbox publish attempts that failed, by kind.",
    ["kind"],
)
_outbox_pending = Gauge(
    _exposed(OUTBOX_PENDING_METRIC),
    "Outbox rows waiting to be published, by kind, as of the last relay pass.",
    ["kind"],
)
_outbox_oldest_pending_age = Gauge(
    _exposed(OUTBOX_OLDEST_PENDING_AGE_METRIC),
    "Age in seconds of the oldest waiting outbox row, as of the last relay pass; 0 when none is waiting.",
)
_relay_pass_failures = Counter(
    _exposed(RELAY_PASS_FAILURES_METRIC),
    "Relay passes that raised.",
)


def _initialise_label_sets() -> None:
    """Create every label combination now, so each one is exported at zero before it first moves."""
    operations = [operation.value for operation in LifecycleOperation]
    for operation in [*operations, UNKNOWN_OPERATION]:
        for outcome in ConsumeOutcome:
            _events_consumed.labels(operation=operation, outcome=outcome.value)
    for operation in operations:
        _event_processing_seconds.labels(operation=operation)
        for state in LifecycleState:
            _lifecycle_transitions.labels(operation=operation, state=state.value)
    for reason in DeadLetterReason:
        _dead_letters.labels(reason=reason.value)
    for kind in OutboxKind:
        _outbox_published.labels(kind=kind.value)
        _outbox_publish_failures.labels(kind=kind.value)
        _outbox_pending.labels(kind=kind.value)


_initialise_label_sets()


def record_event_consumed(operation: LifecycleOperation | None, outcome: ConsumeOutcome) -> None:
    """Count one consumed delivery; a delivery whose operation is not known is labelled unknown."""
    label = UNKNOWN_OPERATION if operation is None else operation.value
    _events_consumed.labels(operation=label, outcome=outcome.value).inc()


@contextmanager
def time_event_processing(operation: LifecycleOperation) -> Iterator[None]:
    """Observe the seconds the block takes, whether it returns or raises."""
    started = perf_counter()
    try:
        yield
    finally:
        _event_processing_seconds.labels(operation=operation.value).observe(perf_counter() - started)


def record_transition(operation: LifecycleOperation, state: LifecycleState) -> None:
    """Count one lifecycle record moved to a state."""
    _lifecycle_transitions.labels(operation=operation.value, state=state.value).inc()


def record_dead_letter(reason: DeadLetterReason) -> None:
    """Count one delivery dead-lettered for the given reason."""
    _dead_letters.labels(reason=reason.value).inc()


def record_outbox_published(kind: OutboxKind) -> None:
    """Count one outbox row the broker confirmed."""
    _outbox_published.labels(kind=kind.value).inc()


def record_outbox_publish_failure(kind: OutboxKind) -> None:
    """Count one failed outbox publish attempt."""
    _outbox_publish_failures.labels(kind=kind.value).inc()


def set_outbox_backlog(pending: Mapping[OutboxKind, int], oldest_age_seconds: float) -> None:
    """Set the waiting-row gauges from one relay pass."""
    for kind, count in pending.items():
        _outbox_pending.labels(kind=kind.value).set(count)
    _outbox_oldest_pending_age.set(oldest_age_seconds)


def record_relay_pass_failure() -> None:
    """Count one relay pass that raised."""
    _relay_pass_failures.inc()


def render_metrics() -> bytes:
    """Return every registered metric in the Prometheus text format."""
    return generate_latest()
