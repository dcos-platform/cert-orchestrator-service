import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from cert_orchestrator.messaging.inbound import HEADER_CORRELATION_ID
from cert_orchestrator.models import OutboxKind, OutboxMessage, OutboxState
from cert_orchestrator.persistence.outbox import OutboxRepository, relay_backoff_seconds
from cert_orchestrator.schemas import CompletionEvent


def completion_event(event_id: str = "evt-1") -> CompletionEvent:
    return CompletionEvent(event_id=event_id, certificate_id="cert-1", status="COMPLETED", retry_count=0, error=None)


def enqueue_completion(session: Session, event_id: str) -> OutboxMessage:
    return OutboxRepository(session).enqueue_completion(completion_event(event_id), None)


def test_enqueue_completion_stores_the_exact_serialised_event_as_a_pending_row(db_session):
    event = completion_event()

    row = OutboxRepository(db_session).enqueue_completion(event, None)

    assert row.kind == OutboxKind.COMPLETION
    assert row.state == OutboxState.PENDING
    assert row.message_id == "evt-1"
    assert row.body == event.model_dump_json()
    assert row.headers == {}
    assert row.delay_seconds is None
    assert row.id is not None


def test_enqueue_completion_with_correlation_id_sets_the_header(db_session):
    row = OutboxRepository(db_session).enqueue_completion(completion_event(), "corr-1")

    assert row.headers == {HEADER_CORRELATION_ID: "corr-1"}


def test_enqueue_retry_stores_the_exact_json_body_delay_and_message_id(db_session):
    payload = {"event_id": "evt-1:retry:1", "certificate_id": "cert-1", "payload": {"should_fail": True}}

    row = OutboxRepository(db_session).enqueue_retry(payload, 5, {"x-test": "value"})

    assert row.kind == OutboxKind.RETRY
    assert row.state == OutboxState.PENDING
    assert row.message_id == "evt-1:retry:1"
    assert row.body == json.dumps(payload)
    assert row.delay_seconds == 5
    assert row.headers == {"x-test": "value"}


def test_enqueue_retry_converts_only_values_json_cannot_hold_and_keeps_the_rest(db_session):
    when = datetime(2026, 11, 7, 12, 0, tzinfo=UTC)
    headers = {
        "when": when,
        "raw": b"ab",
        "text": "t",
        "count": 3,
        "flag": True,
        "items": [1, "x"],
    }

    row = OutboxRepository(db_session).enqueue_retry({"event_id": "evt-h:retry:1"}, 1, headers)

    assert row.headers["when"] == str(when)
    assert row.headers["raw"] == str(b"ab")
    assert row.headers["text"] == "t"
    assert type(row.headers["count"]) is int and row.headers["count"] == 3
    assert row.headers["flag"] is True
    assert row.headers["items"] == [1, "x"]


def test_enqueue_retry_does_not_mutate_the_headers_it_was_given(db_session):
    headers = {"when": datetime(2026, 1, 1, tzinfo=UTC)}

    OutboxRepository(db_session).enqueue_retry({"event_id": "evt-m:retry:1"}, 1, headers)

    assert isinstance(headers["when"], datetime)


def test_second_row_with_the_same_kind_and_message_id_is_rejected_on_flush(db_session):
    repository = OutboxRepository(db_session)
    repository.enqueue_completion(completion_event("evt-dup"), None)

    with pytest.raises(IntegrityError):
        repository.enqueue_completion(completion_event("evt-dup"), None)

    db_session.rollback()
    assert db_session.query(OutboxMessage).count() == 0


def test_retry_and_completion_sharing_a_message_id_both_persist(db_session):
    repository = OutboxRepository(db_session)
    repository.enqueue_retry({"event_id": "evt-shared:retry:1"}, 5, {})
    repository.enqueue_completion(completion_event("evt-shared:retry:1"), None)

    db_session.flush()

    rows = db_session.query(OutboxMessage).order_by(OutboxMessage.id).all()
    assert [(row.kind, row.message_id) for row in rows] == [
        (OutboxKind.RETRY, "evt-shared:retry:1"),
        (OutboxKind.COMPLETION, "evt-shared:retry:1"),
    ]


def test_claim_returns_only_due_pending_rows_in_id_order_up_to_the_limit(db_session):
    now = datetime.now(UTC)
    enqueue_completion(db_session, "evt-a")
    future = enqueue_completion(db_session, "evt-b")
    OutboxRepository(db_session).mark_attempt_failed(future, "boom", now, 3600)
    sent = enqueue_completion(db_session, "evt-c")
    OutboxRepository(db_session).mark_sent(sent, now)
    enqueue_completion(db_session, "evt-d")
    enqueue_completion(db_session, "evt-e")
    db_session.commit()

    repository = OutboxRepository(db_session)
    everything_due = [row.message_id for row in repository.claim_pending(10, now)]
    two_at_most = [row.message_id for row in repository.claim_pending(2, now)]

    assert everything_due == ["evt-a", "evt-d", "evt-e"]
    assert two_at_most == ["evt-a", "evt-d"]


def test_mark_sent_sets_state_and_sent_at(db_session):
    now = datetime.now(UTC)
    row = enqueue_completion(db_session, "evt-s")

    OutboxRepository(db_session).mark_sent(row, now)

    assert row.state == OutboxState.SENT
    assert row.sent_at == now
    db_session.refresh(row)
    assert row.state == OutboxState.SENT
    assert row.sent_at is not None


def test_mark_attempt_failed_counts_the_attempt_and_holds_the_row_back(db_session):
    now = datetime.now(UTC)
    row = enqueue_completion(db_session, "evt-f")
    repository = OutboxRepository(db_session)

    repository.mark_attempt_failed(row, "ConnectionError: refused", now, 7)
    assert row.state == OutboxState.PENDING
    assert row.attempt_count == 1
    assert row.last_error == "ConnectionError: refused"
    assert row.available_at == now + timedelta(seconds=7)

    repository.mark_attempt_failed(row, "ConnectionError: refused again", now, 9)
    assert row.attempt_count == 2
    assert row.last_error == "ConnectionError: refused again"
    assert row.available_at == now + timedelta(seconds=9)


@pytest.mark.parametrize(
    ("attempt_count", "base", "cap", "expected"),
    [
        (1, 1, 60, 1),
        (2, 1, 60, 2),
        (3, 1, 60, 4),
        (6, 1, 60, 32),
        (7, 1, 60, 60),
        (10_000, 1, 60, 60),
        (5, 0, 60, 0),
        (1, 2.5, 60, 2.5),
    ],
)
def test_relay_backoff_doubles_from_base_and_is_capped(attempt_count, base, cap, expected):
    assert relay_backoff_seconds(attempt_count, base, cap) == expected
