import logging

from cert_orchestrator.config import settings
from cert_orchestrator.db import get_session
from cert_orchestrator.messaging.events import EventPublisher
from cert_orchestrator.persistence.repository import LifecycleRepository
from cert_orchestrator.schemas import CertificateLifecycleEvent, CompletionEvent
from cert_orchestrator.state_machine import decide_transition

logger = logging.getLogger(__name__)


class LifecycleMessageHandler:
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher

    async def handle(self, payload: dict) -> None:
        event = CertificateLifecycleEvent.model_validate(payload)

        with get_session() as session:
            repository = LifecycleRepository(session)
            is_new = repository.mark_event_if_new(event.event_id, event.certificate_id)
            if not is_new:
                logger.info(
                    "Skipping duplicate event",
                    extra={"event_id": event.event_id, "certificate_id": event.certificate_id},
                )
                return

            lifecycle = repository.get_or_create_lifecycle(event.certificate_id)
            repository.set_processing(lifecycle)

            should_fail = bool(event.payload.get("should_fail", False))
            error = "simulated processing failure" if should_fail else None

            if should_fail:
                repository.mark_failed(lifecycle, error=error or "processing failed")
            else:
                repository.mark_completed(lifecycle)

            decision = decide_transition(
                success=not should_fail,
                retry_count=lifecycle.retry_count,
                max_retries=settings.max_retries,
            )

            session.commit()

        logger.info(
            "Processed lifecycle event",
            extra={"event_id": event.event_id, "certificate_id": event.certificate_id, "state": decision.next_state.value},
        )

        if decision.should_retry:
            retry_payload = event.model_dump(mode="json")
            retry_payload["event_id"] = f"{event.event_id}:retry:{lifecycle.retry_count}"
            await self.publisher.publish_retry(retry_payload, delay_seconds=settings.backoff_seconds * lifecycle.retry_count)
            return

        if decision.should_publish_completion:
            await self.publisher.publish_completion(
                CompletionEvent(
                    event_id=event.event_id,
                    certificate_id=event.certificate_id,
                    status=decision.next_state.value,
                    retry_count=lifecycle.retry_count,
                    error=lifecycle.last_error,
                )
            )
