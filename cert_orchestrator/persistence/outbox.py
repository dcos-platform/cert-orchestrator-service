import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from cert_orchestrator.messaging.inbound import HEADER_CORRELATION_ID
from cert_orchestrator.models import OutboxKind, OutboxMessage, OutboxState
from cert_orchestrator.schemas import CompletionEvent

# 2 ** 1000 is the largest power of two that still converts to a float; beyond it the cap already applies.
_MAX_BACKOFF_EXPONENT = 1000


def relay_backoff_seconds(attempt_count: int, base: float, cap: float) -> float:
    """Return the delay after attempt_count failed attempts: base × 2^(attempt_count − 1), capped."""
    exponent = min(attempt_count - 1, _MAX_BACKOFF_EXPONENT)
    return min(cap, base * 2**exponent)


@dataclass(frozen=True)
class OutboxBacklog:
    """Waiting (PENDING) rows per kind, every kind present, and the age in seconds of the oldest of them."""

    pending_by_kind: dict[OutboxKind, int]
    oldest_age_seconds: float


def _as_utc(moment: datetime) -> datetime:
    """Treat a naive datetime as UTC, which is what SQLite returns for timestamps stored in UTC."""
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def _json_safe_headers(headers: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copy of the headers in which every value json.dumps rejects is replaced by its str()."""
    safe: dict[str, Any] = {}
    for name, value in headers.items():
        try:
            json.dumps(value)
        except (TypeError, ValueError):
            value = str(value)
        safe[name] = value
    return safe


class OutboxRepository:
    """Writes outbox rows into the caller's transaction and claims and marks them for the relay."""

    def __init__(self, session: Session):
        self.session = session

    def enqueue_completion(self, event: CompletionEvent, correlation_id: str | None) -> OutboxMessage:
        """Add a PENDING completion row whose message id is the event id."""
        headers = {} if correlation_id is None else {HEADER_CORRELATION_ID: correlation_id}
        return self._add(
            OutboxMessage(
                message_id=event.event_id,
                kind=OutboxKind.COMPLETION,
                state=OutboxState.PENDING,
                body=event.model_dump_json(),
                headers=headers,
                delay_seconds=None,
            )
        )

    def enqueue_retry(self, payload: dict[str, Any], delay_seconds: int, headers: Mapping[str, Any]) -> OutboxMessage:
        """Add a PENDING business-retry row whose message id is the payload's event id."""
        return self._add(
            OutboxMessage(
                message_id=payload["event_id"],
                kind=OutboxKind.RETRY,
                state=OutboxState.PENDING,
                body=json.dumps(payload),
                headers=_json_safe_headers(headers),
                delay_seconds=delay_seconds,
            )
        )

    def claim_pending(self, limit: int, now: datetime) -> list[OutboxMessage]:
        """Lock up to limit PENDING rows due at now, oldest first, skipping rows another relay has locked."""
        statement = (
            select(OutboxMessage)
            .where(OutboxMessage.state == OutboxState.PENDING, OutboxMessage.available_at <= now)
            .order_by(OutboxMessage.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(self.session.execute(statement).scalars())

    def pending_backlog(self, now: datetime) -> OutboxBacklog:
        """Count PENDING rows per kind, backed-off rows included, and the age of the oldest, in one statement."""
        statement = (
            select(OutboxMessage.kind, func.count(), func.min(OutboxMessage.created_at))
            .where(OutboxMessage.state == OutboxState.PENDING)
            .group_by(OutboxMessage.kind)
        )
        counts = {kind: 0 for kind in OutboxKind}
        oldest: datetime | None = None
        for kind, count, first_created in self.session.execute(statement).all():
            counts[kind] = count
            created = _as_utc(first_created)
            if oldest is None or created < oldest:
                oldest = created
        age = 0.0 if oldest is None else max(0.0, (_as_utc(now) - oldest).total_seconds())
        return OutboxBacklog(pending_by_kind=counts, oldest_age_seconds=age)

    def mark_sent(self, row: OutboxMessage, now: datetime) -> None:
        """Record that the broker confirmed the row."""
        row.state = OutboxState.SENT
        row.sent_at = now
        self.session.flush()

    def mark_attempt_failed(self, row: OutboxMessage, error: str, now: datetime, backoff_seconds: float) -> None:
        """Count a failed publish and hold the row back for the backoff period."""
        row.attempt_count += 1
        row.last_error = error
        row.available_at = now + timedelta(seconds=backoff_seconds)
        self.session.flush()

    def _add(self, row: OutboxMessage) -> OutboxMessage:
        self.session.add(row)
        self.session.flush()
        return row
