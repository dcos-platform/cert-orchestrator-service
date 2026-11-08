from typing import Protocol

from cert_orchestrator.models import OutboxMessage


class OutboxPublisher(Protocol):
    """Broker seam for the outbox relay: publish one stored row, raising if the broker does not confirm it."""

    async def publish_outbox_message(self, row: OutboxMessage) -> None: ...
