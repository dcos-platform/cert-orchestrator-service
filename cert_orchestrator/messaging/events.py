from collections.abc import Mapping
from typing import Any, Protocol

from cert_orchestrator.schemas import CompletionEvent


class EventPublisher(Protocol):
    async def publish_completion(self, event: CompletionEvent) -> None: ...
    async def publish_retry(
        self,
        payload: dict,
        delay_seconds: int,
        routing_key: str,
        headers: Mapping[str, Any],
    ) -> None: ...
