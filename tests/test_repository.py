from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from cert_orchestrator.models import Base, CertificateLifecycle, LifecycleOperation, LifecycleState
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


def test_new_operation_record_starts_pending_with_operation_and_root():
    session = build_session()
    repo = LifecycleRepository(session)

    lifecycle = repo.get_or_create_operation("root-1", "cert-1", LifecycleOperation.RENEW)

    assert lifecycle.state == LifecycleState.PENDING
    assert lifecycle.retry_count == 0
    assert lifecycle.operation == LifecycleOperation.RENEW
    assert lifecycle.root_event_id == "root-1"
    assert lifecycle.certificate_id == "cert-1"


def test_same_root_returns_the_same_record():
    session = build_session()
    repo = LifecycleRepository(session)

    first = repo.get_or_create_operation("root-1", "cert-1", LifecycleOperation.CREATE)
    session.commit()
    second = repo.get_or_create_operation("root-1", "cert-1", LifecycleOperation.CREATE)

    assert second.id == first.id
    assert session.query(CertificateLifecycle).count() == 1


def test_two_operations_on_one_certificate_get_independent_records():
    session = build_session()
    repo = LifecycleRepository(session)

    create = repo.get_or_create_operation("root-create", "cert-2", LifecycleOperation.CREATE)
    renew = repo.get_or_create_operation("root-renew", "cert-2", LifecycleOperation.RENEW)
    repo.mark_failed(create, "boom")

    assert create.id != renew.id
    assert create.retry_count == 1
    assert renew.retry_count == 0
    assert session.query(CertificateLifecycle).count() == 2


def test_legacy_row_without_root_is_not_reused():
    session = build_session()
    legacy = CertificateLifecycle(
        certificate_id="cert-3",
        state=LifecycleState.FAILED,
        retry_count=3,
        last_error="legacy failure",
    )
    session.add(legacy)
    session.commit()
    repo = LifecycleRepository(session)

    fresh = repo.get_or_create_operation("root-new", "cert-3", LifecycleOperation.CREATE)

    assert fresh.id != legacy.id
    assert fresh.retry_count == 0
    assert fresh.state == LifecycleState.PENDING


def test_mark_retry_scheduled_sets_processing_counts_retry_and_records_error():
    session = build_session()
    repo = LifecycleRepository(session)
    lifecycle = repo.get_or_create_operation("root-1", "cert-4", LifecycleOperation.CREATE)

    repo.mark_retry_scheduled(lifecycle, "boom")

    assert lifecycle.state == LifecycleState.PROCESSING
    assert lifecycle.retry_count == 1
    assert lifecycle.last_error == "boom"


def test_mark_failed_sets_failed_counts_retry_and_records_error():
    session = build_session()
    repo = LifecycleRepository(session)
    lifecycle = repo.get_or_create_operation("root-1", "cert-4", LifecycleOperation.CREATE)

    repo.mark_failed(lifecycle, "boom")

    assert lifecycle.state == LifecycleState.FAILED
    assert lifecycle.retry_count == 1
    assert lifecycle.last_error == "boom"


def test_mark_completed_sets_completed_and_clears_error():
    session = build_session()
    repo = LifecycleRepository(session)
    lifecycle = repo.get_or_create_operation("root-1", "cert-4", LifecycleOperation.CREATE)
    repo.mark_failed(lifecycle, "boom")

    repo.mark_completed(lifecycle)

    assert lifecycle.state == LifecycleState.COMPLETED
    assert lifecycle.last_error is None
    assert lifecycle.retry_count == 1
