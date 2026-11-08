from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from cert_orchestrator.models import CertificateLifecycle, LifecycleOperation, LifecycleState, ProcessedEvent


class LifecycleRepository:
    def __init__(self, session: Session):
        self.session = session

    def mark_event_if_new(self, event_id: str, certificate_id: str) -> bool:
        event = ProcessedEvent(event_id=event_id, certificate_id=certificate_id)
        self.session.add(event)
        try:
            self.session.flush()
        except IntegrityError:
            self.session.rollback()
            return False
        return True

    def get_or_create_operation(
        self,
        root_event_id: str,
        certificate_id: str,
        operation: LifecycleOperation,
    ) -> CertificateLifecycle:
        """Return the lifecycle record for this root event, creating it if absent."""
        lifecycle = self.session.execute(
            select(CertificateLifecycle).where(CertificateLifecycle.root_event_id == root_event_id)
        ).scalar_one_or_none()
        if lifecycle is not None:
            return lifecycle

        lifecycle = CertificateLifecycle(
            certificate_id=certificate_id,
            root_event_id=root_event_id,
            operation=operation,
            state=LifecycleState.PENDING,
            retry_count=0,
        )
        self.session.add(lifecycle)
        self.session.flush()
        return lifecycle

    def mark_completed(self, lifecycle: CertificateLifecycle) -> None:
        lifecycle.state = LifecycleState.COMPLETED
        lifecycle.last_error = None
        self.session.flush()

    def mark_retry_scheduled(self, lifecycle: CertificateLifecycle, error: str) -> None:
        """Record that a retry is scheduled: state PROCESSING and one more retry counted."""
        lifecycle.state = LifecycleState.PROCESSING
        lifecycle.retry_count += 1
        lifecycle.last_error = error
        self.session.flush()

    def mark_failed(self, lifecycle: CertificateLifecycle, error: str) -> None:
        lifecycle.state = LifecycleState.FAILED
        lifecycle.retry_count += 1
        lifecycle.last_error = error
        self.session.flush()
