import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy.orm import Session

from cert_orchestrator.config import Settings
from cert_orchestrator.messaging.events import OutboxPublisher
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
        return sent

    async def run(self, stop: asyncio.Event) -> None:
        """Run passes until stop is set; an exception in a pass is logged and the loop continues."""
        while not stop.is_set():
            self._wake_event.clear()
            try:
                await self.run_once()
            except Exception:
                logger.exception("Outbox relay pass failed")
            await self._wait_for_next_pass(stop)

    async def _deliver(self, outbox: OutboxRepository, row: OutboxMessage) -> bool:
        """Publish one row and mark it SENT only after the broker confirmed it. Returns True when sent."""
        try:
            await self.publisher.publish_outbox_message(row)
        except Exception as exc:
            self._record_failure(outbox, row, exc)
            return False
        outbox.mark_sent(row, datetime.now(UTC))
        return True

    def _record_failure(self, outbox: OutboxRepository, row: OutboxMessage, exc: Exception) -> None:
        error = f"{type(exc).__name__}: {exc}"[:LAST_ERROR_MAX_LENGTH]
        backoff = relay_backoff_seconds(
            row.attempt_count + 1,
            self.config.outbox_backoff_base_seconds,
            self.config.outbox_backoff_max_seconds,
        )
        outbox.mark_attempt_failed(row, error, datetime.now(UTC), backoff)
        logger.warning("Outbox publish failed; will retry", extra={"event_id": row.message_id})
        if row.attempt_count == self.config.outbox_alert_after_attempts:
            logger.error(
                "Outbox message %s not published after %d attempts",
                row.message_id,
                row.attempt_count,
                extra={"event_id": row.message_id},
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
