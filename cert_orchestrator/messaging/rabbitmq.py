import json
from typing import Awaitable, Callable

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from cert_orchestrator.config import settings
from cert_orchestrator.schemas import CompletionEvent


class RabbitMQClient:
    def __init__(self, url: str | None = None):
        self.url = url or settings.rabbitmq_url
        self.connection: aio_pika.RobustConnection | None = None
        self.channel: aio_pika.abc.AbstractRobustChannel | None = None

    async def connect(self) -> None:
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        await self.channel.declare_queue(settings.incoming_queue, durable=True)
        await self.channel.declare_queue(settings.completion_queue, durable=True)

    async def close(self) -> None:
        if self.connection:
            await self.connection.close()

    async def consume(self, handler: Callable[[dict], Awaitable[None]]) -> None:
        if not self.channel:
            raise RuntimeError("RabbitMQ channel is not initialized")

        queue = await self.channel.declare_queue(settings.incoming_queue, durable=True)

        async def _on_message(message: AbstractIncomingMessage) -> None:
            async with message.process(requeue=False):
                payload = json.loads(message.body.decode("utf-8"))
                await handler(payload)

        await queue.consume(_on_message)

    async def publish_completion(self, event: CompletionEvent) -> None:
        if not self.channel:
            raise RuntimeError("RabbitMQ channel is not initialized")

        message = aio_pika.Message(
            body=event.model_dump_json().encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await self.channel.default_exchange.publish(message, routing_key=settings.completion_queue)

    async def publish_retry(self, payload: dict, delay_seconds: int) -> None:
        if not self.channel:
            raise RuntimeError("RabbitMQ channel is not initialized")

        headers = {"x-delay": delay_seconds * 1000}
        message = aio_pika.Message(
            body=json.dumps(payload).encode("utf-8"),
            content_type="application/json",
            headers=headers,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await self.channel.default_exchange.publish(message, routing_key=settings.incoming_queue)
