import logging
from collections.abc import Callable

from cert_orchestrator.config import settings
from cert_orchestrator.db import get_session
from cert_orchestrator.messaging.inbound import (
    HEADER_ROOT_EVENT_ID,
    InboundMessage,
    business_retry_headers,
    completion_correlation_id,
    operation_for_routing_key,
)
from cert_orchestrator.models import CertificateLifecycle, LifecycleState
from cert_orchestrator.persistence.outbox import OutboxRepository
from cert_orchestrator.persistence.repository import LifecycleRepository
from cert_orchestrator.schemas import CertificateLifecycleEvent, CompletionEvent
from cert_orchestrator.state_machine import TransitionDecision, completion_required, decide_transition

logger = logging.getLogger(__name__)

# Test/demo hook that forces a simulated failure. cert-api never sends this key.
SIMULATE_FAILURE_KEY = "should_fail"
SIMULATED_FAILURE_ERROR = "simulated processing failure"


class LifecycleMessageHandler:
    def __init__(self, wake: Callable[[], None] | None = None) -> None:
        self.wake = wake

    async def handle(self, message: InboundMessage) -> None:
        """Record one lifecycle event and write any completion or retry to the outbox in the same transaction."""
        operation = operation_for_routing_key(message.routing_key)
        event = CertificateLifecycleEvent.model_validate(message.body)
        root_event_id = message.headers.get(HEADER_ROOT_EVENT_ID, event.event_id)

        with get_session() as session:
            repository = LifecycleRepository(session)
            is_new = repository.mark_event_if_new(event.event_id, event.certificate_id)
            if not is_new:
                logger.info(
                    "Skipping duplicate event",
                    extra={"event_id": event.event_id, "certificate_id": event.certificate_id},
                )
                return

            lifecycle = repository.get_or_create_operation(root_event_id, event.certificate_id, operation)
            outbox = OutboxRepository(session)
            outbox_written = completion_required(operation)
            if outbox_written:
                state = self._orchestrate(repository, outbox, lifecycle, event, message, root_event_id)
            else:
                repository.mark_completed(lifecycle)
                state = LifecycleState.COMPLETED
            session.commit()

        if outbox_written and self.wake is not None:
            self.wake()
        logger.info(
            "Processed lifecycle event",
            extra={
                "event_id": event.event_id,
                "certificate_id": event.certificate_id,
                "state": state.value,
            },
        )

    def _orchestrate(
        self,
        repository: LifecycleRepository,
        outbox: OutboxRepository,
        lifecycle: CertificateLifecycle,
        event: CertificateLifecycleEvent,
        message: InboundMessage,
        root_event_id: str,
    ) -> LifecycleState:
        """Record the outcome of a create or renew, then enqueue its retry or completion."""
        should_fail = bool(event.payload.get(SIMULATE_FAILURE_KEY, False))
        # retry_count includes the failure being decided, so the decision is made before any state is written.
        decision = decide_transition(
            success=not should_fail,
            retry_count=lifecycle.retry_count + 1,
            max_retries=settings.max_retries,
        )
        _record_outcome(repository, lifecycle, decision)
        if decision.should_retry:
            _enqueue_retry(outbox, event, message, lifecycle.retry_count, root_event_id)
        else:
            _enqueue_completion(outbox, event, lifecycle, decision.next_state, message, root_event_id)
        return decision.next_state


def _enqueue_retry(
    outbox: OutboxRepository,
    event: CertificateLifecycleEvent,
    message: InboundMessage,
    retry_count: int,
    root_event_id: str,
) -> None:
    retry_payload = event.model_dump(mode="json")
    retry_payload["event_id"] = f"{event.event_id}:retry:{retry_count}"
    outbox.enqueue_retry(
        retry_payload,
        delay_seconds=settings.backoff_seconds * retry_count,
        headers=business_retry_headers(
            {**message.headers, HEADER_ROOT_EVENT_ID: root_event_id},
            message.routing_key,
            message.exchange,
        ),
    )


def _enqueue_completion(
    outbox: OutboxRepository,
    event: CertificateLifecycleEvent,
    lifecycle: CertificateLifecycle,
    state: LifecycleState,
    message: InboundMessage,
    root_event_id: str,
) -> None:
    outbox.enqueue_completion(
        CompletionEvent(
            event_id=event.event_id,
            certificate_id=event.certificate_id,
            status=state.value,
            retry_count=lifecycle.retry_count,
            error=lifecycle.last_error,
        ),
        completion_correlation_id(message.headers, root_event_id),
    )


def _record_outcome(
    repository: LifecycleRepository,
    lifecycle: CertificateLifecycle,
    decision: TransitionDecision,
) -> None:
    if decision.next_state == LifecycleState.COMPLETED:
        repository.mark_completed(lifecycle)
    elif decision.should_retry:
        repository.mark_retry_scheduled(lifecycle, error=SIMULATED_FAILURE_ERROR)
    else:
        repository.mark_failed(lifecycle, error=SIMULATED_FAILURE_ERROR)
