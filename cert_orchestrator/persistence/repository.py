from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from cert_orchestrator.models import CertificateLifecycle, LifecycleState, ProcessedEvent


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

    def get_or_create_lifecycle(self, certificate_id: str) -> CertificateLifecycle:
        lifecycle = self.session.execute(
            select(CertificateLifecycle).where(CertificateLifecycle.certificate_id == certificate_id)
        ).scalar_one_or_none()
        if lifecycle:
            return lifecycle

        lifecycle = CertificateLifecycle(certificate_id=certificate_id, state=LifecycleState.PENDING, retry_count=0)
        self.session.add(lifecycle)
        self.session.flush()
        return lifecycle

    def set_processing(self, lifecycle: CertificateLifecycle) -> None:
        lifecycle.state = LifecycleState.PROCESSING
        lifecycle.last_error = None
        self.session.flush()

    def mark_completed(self, lifecycle: CertificateLifecycle) -> None:
        lifecycle.state = LifecycleState.COMPLETED
        lifecycle.last_error = None
        self.session.flush()

    def mark_failed(self, lifecycle: CertificateLifecycle, error: str) -> None:
        lifecycle.state = LifecycleState.FAILED
        lifecycle.retry_count += 1
        lifecycle.last_error = error
        self.session.flush()
