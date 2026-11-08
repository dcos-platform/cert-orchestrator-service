import os
import uuid

import aio_pika
import pytest
import pytest_asyncio

from cert_orchestrator.config import Settings
from cert_orchestrator.messaging.rabbitmq import RabbitMQClient


@pytest.fixture
def integration_settings() -> Settings:
    unique_id = str(uuid.uuid4())[:8]
    host = os.environ.get("CERT_ORCH_RABBITMQ_HOST", "localhost")
    backoff = int(os.environ.get("CERT_ORCH_BACKOFF_SECONDS", "5"))

    return Settings(
        rabbitmq_host=host,
        incoming_queue=f"test.events.{unique_id}",
        completion_queue=f"test.completions.{unique_id}",
        dead_letter_exchange=f"test.dlx.{unique_id}",
        dead_letter_queue=f"test.dlq.{unique_id}",
        wait_queue_prefix=f"test.wait.{unique_id}",
        backoff_seconds=backoff,
    )


@pytest_asyncio.fixture
async def client_with_cleanup(integration_settings) -> aio_pika.RobustConnection:
    client = RabbitMQClient(config=integration_settings)
    await client.connect()
    yield client

    try:
        channel = client.channel
        if not channel:
            await client.close()
            raise RuntimeError("Channel is not initialized during cleanup")

        dlq = await channel.get_queue(integration_settings.dead_letter_queue)
        await dlq.delete(if_unused=False, if_empty=False)

        dlx = await channel.get_exchange(integration_settings.dead_letter_exchange)
        await dlx.delete()

        for queue_name, queue in list(client._wait_queues.items()):
            try:
                await queue.delete(if_unused=False, if_empty=False)
            except Exception as e:
                raise RuntimeError(f"Failed to delete wait queue {queue_name}: {e}") from e

        incoming = await channel.get_queue(integration_settings.incoming_queue)
        await incoming.delete(if_unused=False, if_empty=False)

        completion = await channel.get_queue(integration_settings.completion_queue)
        await completion.delete(if_unused=False, if_empty=False)
    finally:
        await client.close()
