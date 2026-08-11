from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cert_orchestrator.models import Base, LifecycleState
from cert_orchestrator.persistence.repository import LifecycleRepository


def build_session() -> Session:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return Session(engine)


def test_mark_event_if_new_enforces_idempotency():
    session = build_session()
    repo = LifecycleRepository(session)

    assert repo.mark_event_if_new("evt-1", "cert-1") is True
    session.commit()

    assert repo.mark_event_if_new("evt-1", "cert-1") is False


def test_lifecycle_progression_updates_state_and_retry_count():
    session = build_session()
    repo = LifecycleRepository(session)

    lifecycle = repo.get_or_create_lifecycle("cert-42")
    assert lifecycle.state == LifecycleState.PENDING

    repo.set_processing(lifecycle)
    assert lifecycle.state == LifecycleState.PROCESSING

    repo.mark_failed(lifecycle, "boom")
    assert lifecycle.state == LifecycleState.FAILED
    assert lifecycle.retry_count == 1
    assert lifecycle.last_error == "boom"

    repo.mark_completed(lifecycle)
    assert lifecycle.state == LifecycleState.COMPLETED
    assert lifecycle.last_error is None
