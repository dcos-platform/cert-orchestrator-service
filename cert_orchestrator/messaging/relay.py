import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from time import monotonic

from sqlalchemy.orm import Session

from cert_orchestrator.config import Settings
from cert_orchestrator.logging_context import log_context
from cert_orchestrator.messaging.events import OutboxPublisher
from cert_orchestrator.messaging.inbound import HEADER_ROOT_EVENT_ID, completion_correlation_id
from cert_orchestrator.metrics import (
    record_outbox_publish_failure,
    record_outbox_published,
    record_relay_pass_failure,
    set_outbox_backlog,
)
from cert_orchestrator.models import OutboxMessage
from cert_orchestrator.persistence.outbox import OutboxRepository, relay_backoff_seconds

logger = logging.getLogger(__name__)

# Caps the stored last_error text so one failure cannot store an unbounded message.
LAST_ERROR_MAX_LENGTH = 500


class OutboxRelay:
    """Publishes due outbox rows with broker confirms; failed rows back off and are retried, never abandoned."""

    def __init__(self, session_factory: Callable[[], Session], publisher: OutboxPublisher, config: Settings) -> None:
        self.session_factory = session_factory
        self.publisher = publisher
        self.config = config
        self._wake_event = asyncio.Event()
        # Set when a failed pass was logged and cleared by the next successful pass; None means no outage.
        self._last_failure_logged_at: float | None = None

    def wake(self) -> None:
        """Ask for a pass to run now instead of waiting for the poll interval."""
        self._wake_event.set()

    async def run_once(self) -> int:
        """Claim, publish and mark one batch in a single transaction; return how many rows were sent."""
        with self.session_factory() as session:
            outbox = OutboxRepository(session)
            rows = outbox.claim_pending(self.config.outbox_batch_size, datetime.now(UTC))
            sent = 0
            for row in rows:
                if await self._deliver(outbox, row):
                    sent += 1
            session.commit()
            # Read after the commit: a failing read must not roll back rows already published and marked SENT.
            backlog = outbox.pending_backlog(datetime.now(UTC))
        set_outbox_backlog(backlog.pending_by_kind, backlog.oldest_age_seconds)
        return sent

    async def run(self, stop: asyncio.Event) -> None:
        """Run passes until stop is set. A pass that raises is counted and logged, and the loop continues."""
        while not stop.is_set():
            self._wake_event.clear()
            try:
                await self.run_once()
            except Exception:
                record_relay_pass_failure()
                if self._take_failure_log_slot(monotonic()):
                    logger.exception("Outbox relay pass failed")
            else:
                self._record_pass_success()
            await self._wait_for_next_pass(stop)

    def _take_failure_log_slot(self, now: float) -> bool:
        """Return True when a failure is logged: the first of an outage, then once per interval. Records the time."""
        last = self._last_failure_logged_at
        if last is not None and now - last < self.config.relay_failure_log_interval_seconds:
            return False
        self._last_failure_logged_at = now
        return True

    def _record_pass_success(self) -> None:
        """End an outage with one INFO line, so the next outage starts with an immediate ERROR."""
        if self._last_failure_logged_at is not None:
            self._last_failure_logged_at = None
            logger.info("Outbox relay recovered")

    async def _deliver(self, outbox: OutboxRepository, row: OutboxMessage) -> bool:
        """Publish one row and mark it SENT only after the broker confirmed it. Returns True when sent."""
        correlation_id = completion_correlation_id(row.headers, row.headers.get(HEADER_ROOT_EVENT_ID))
        with log_context(correlationId=correlation_id, eventId=row.message_id):
            try:
                await self.publisher.publish_outbox_message(row)
            except Exception as exc:
                self._record_failure(outbox, row, exc)
                return False
            record_outbox_published(row.kind)
            outbox.mark_sent(row, datetime.now(UTC))
            logger.info("Outbox message published", extra={"kind": row.kind.value})
            return True

    def _record_failure(self, outbox: OutboxRepository, row: OutboxMessage, exc: Exception) -> None:
        error = f"{type(exc).__name__}: {exc}"[:LAST_ERROR_MAX_LENGTH]
        backoff = relay_backoff_seconds(
            row.attempt_count + 1,
            self.config.outbox_backoff_base_seconds,
            self.config.outbox_backoff_max_seconds,
        )
        outbox.mark_attempt_failed(row, error, datetime.now(UTC), backoff)
        record_outbox_publish_failure(row.kind)
        logger.warning("Outbox publish failed; will retry", extra={"kind": row.kind.value})
        if row.attempt_count == self.config.outbox_alert_after_attempts:
            logger.error(
                "Outbox message %s not published after %d attempts",
                row.message_id,
                row.attempt_count,
                extra={"kind": row.kind.value},
            )

    async def _wait_for_next_pass(self, stop: asyncio.Event) -> None:
        """Wait until woken, stopped, or the poll interval elapses, whichever comes first."""
        woken = asyncio.create_task(self._wake_event.wait())
        stopped = asyncio.create_task(stop.wait())
        try:
            await asyncio.wait(
                {woken, stopped},
                timeout=self.config.outbox_poll_interval_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            woken.cancel()
            stopped.cancel()
            await asyncio.gather(woken, stopped, return_exceptions=True)
