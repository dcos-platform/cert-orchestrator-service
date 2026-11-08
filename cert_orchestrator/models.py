from datetime import UTC, datetime
from enum import Enum
from typing import Any

from sqlalchemy import JSON, DateTime, Index, Integer, String, Text, UniqueConstraint, func
from sqlalchemy import Enum as SqlEnum
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


def application_now() -> datetime:
    """Return the current UTC time from the application clock, the Python-side default of outbox timing columns."""
    return datetime.now(UTC)


class LifecycleState(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class LifecycleOperation(str, Enum):
    """Cert-api operation that a lifecycle record belongs to."""

    CREATE = "CREATE"
    RENEW = "RENEW"
    REVOKE = "REVOKE"
    EXPIRE = "EXPIRE"


class OutboxKind(str, Enum):
    """Kind of outbox message: a lifecycle completion or a business retry."""

    COMPLETION = "COMPLETION"
    RETRY = "RETRY"


class OutboxState(str, Enum):
    """Relay state of an outbox message: waiting to be published, or published and confirmed."""

    PENDING = "PENDING"
    SENT = "SENT"


class CertificateLifecycle(Base):
    __tablename__ = "certificate_lifecycles"
    __table_args__ = (UniqueConstraint("root_event_id", name="uq_certificate_lifecycles_root_event_id"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    certificate_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    operation: Mapped[LifecycleOperation | None] = mapped_column(SqlEnum(LifecycleOperation), nullable=True)
    root_event_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    state: Mapped[LifecycleState] = mapped_column(
        SqlEnum(LifecycleState), nullable=False, default=LifecycleState.PENDING
    )
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    __table_args__ = (UniqueConstraint("event_id", name="uq_processed_events_event_id"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    certificate_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class OutboxMessage(Base):
    """A message the handler wrote in its transaction and the relay publishes later."""

    __tablename__ = "outbox_messages"
    __table_args__ = (
        UniqueConstraint("kind", "message_id", name="uq_outbox_messages_kind_message_id"),
        Index("ix_outbox_messages_state_available_at", "state", "available_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    message_id: Mapped[str] = mapped_column(String(255), nullable=False)
    kind: Mapped[OutboxKind] = mapped_column(SqlEnum(OutboxKind), nullable=False)
    state: Mapped[OutboxState] = mapped_column(SqlEnum(OutboxState), nullable=False, default=OutboxState.PENDING)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    headers: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    delay_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=application_now, server_default=func.now()
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=application_now, server_default=func.now()
    )
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
