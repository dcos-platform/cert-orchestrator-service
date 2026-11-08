import logging

from cert_orchestrator.config import settings
from cert_orchestrator.db import get_session
from cert_orchestrator.messaging.events import EventPublisher
from cert_orchestrator.messaging.inbound import HEADER_ROOT_EVENT_ID, InboundMessage, operation_for_routing_key
from cert_orchestrator.models import CertificateLifecycle, LifecycleState
from cert_orchestrator.persistence.repository import LifecycleRepository
from cert_orchestrator.schemas import CertificateLifecycleEvent, CompletionEvent
from cert_orchestrator.state_machine import TransitionDecision, completion_required, decide_transition

logger = logging.getLogger(__name__)

# Test/demo hook that forces a simulated failure. cert-api never sends this key.
SIMULATE_FAILURE_KEY = "should_fail"
SIMULATED_FAILURE_ERROR = "simulated processing failure"


class LifecycleMessageHandler:
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher

    async def handle(self, message: InboundMessage) -> None:
        """Record one lifecycle event against its operation and publish any retry or completion."""
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
            if completion_required(operation):
                state = await self._orchestrate(repository, lifecycle, event, message, root_event_id)
            else:
                repository.mark_completed(lifecycle)
                state = LifecycleState.COMPLETED
            session.commit()

        logger.info(
            "Processed lifecycle event",
            extra={
                "event_id": event.event_id,
                "certificate_id": event.certificate_id,
                "state": state.value,
            },
        )

    async def _orchestrate(
        self,
        repository: LifecycleRepository,
        lifecycle: CertificateLifecycle,
        event: CertificateLifecycleEvent,
        message: InboundMessage,
        root_event_id: str,
    ) -> LifecycleState:
        """Record the outcome of a create or renew, then publish its retry or completion."""
        should_fail = bool(event.payload.get(SIMULATE_FAILURE_KEY, False))
        # retry_count includes the failure being decided, so the decision is made before any state is written.
        decision = decide_transition(
            success=not should_fail,
            retry_count=lifecycle.retry_count + 1,
            max_retries=settings.max_retries,
        )
        _record_outcome(repository, lifecycle, decision)
        if decision.should_retry:
            await self._publish_retry(event, message, lifecycle.retry_count, root_event_id)
        else:
            await self._publish_completion(event, lifecycle, decision.next_state)
        return decision.next_state

    async def _publish_retry(
        self,
        event: CertificateLifecycleEvent,
        message: InboundMessage,
        retry_count: int,
        root_event_id: str,
    ) -> None:
        retry_payload = event.model_dump(mode="json")
        retry_payload["event_id"] = f"{event.event_id}:retry:{retry_count}"
        await self.publisher.publish_retry(
            retry_payload,
            delay_seconds=settings.backoff_seconds * retry_count,
            routing_key=message.routing_key,
            headers={**message.headers, HEADER_ROOT_EVENT_ID: root_event_id},
            exchange=message.exchange,
        )

    async def _publish_completion(
        self,
        event: CertificateLifecycleEvent,
        lifecycle: CertificateLifecycle,
        state: LifecycleState,
    ) -> None:
        await self.publisher.publish_completion(
            CompletionEvent(
                event_id=event.event_id,
                certificate_id=event.certificate_id,
                status=state.value,
                retry_count=lifecycle.retry_count,
                error=lifecycle.last_error,
            )
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
