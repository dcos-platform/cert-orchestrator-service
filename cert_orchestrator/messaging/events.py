from cert_orchestrator.schemas import CompletionEvent


class EventPublisher:
    async def publish_completion(self, event: CompletionEvent) -> None:  # pragma: no cover - protocol method
        raise NotImplementedError

    async def publish_retry(self, payload: dict, delay_seconds: int) -> None:  # pragma: no cover - protocol method
        raise NotImplementedError
