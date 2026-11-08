import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response

from cert_orchestrator.config import settings
from cert_orchestrator.db import SessionLocal
from cert_orchestrator.logging_config import configure_logging
from cert_orchestrator.messaging.handlers import LifecycleMessageHandler
from cert_orchestrator.messaging.rabbitmq import RabbitMQClient
from cert_orchestrator.messaging.relay import OutboxRelay
from cert_orchestrator.metrics import METRICS_CONTENT_TYPE, render_metrics

logger = logging.getLogger(__name__)

# How long shutdown waits for the relay to finish its current pass before cancelling it.
RELAY_STOP_TIMEOUT_SECONDS = 10.0

configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Re-apply JSON logging, connect to RabbitMQ, run the outbox relay and consume lifecycle events until shutdown."""
    # Uvicorn applies its logging config when its Config is created: before this module's import under
    # `uvicorn module:app`, but after it when a program imports the app and calls uvicorn.run(app). This call covers the
    # second case.
    configure_logging()
    rabbitmq = RabbitMQClient()
    await rabbitmq.connect()
    app.state.rabbitmq = rabbitmq

    stop = asyncio.Event()
    relay = OutboxRelay(SessionLocal, rabbitmq, settings)
    relay_task = asyncio.create_task(relay.run(stop))
    try:
        handler = LifecycleMessageHandler(wake=relay.wake)
        await rabbitmq.consume(handler.handle)
        yield
    finally:
        await _stop_relay(relay, stop, relay_task)
        await rabbitmq.close()


async def _stop_relay(relay: OutboxRelay, stop: asyncio.Event, relay_task: asyncio.Task[None]) -> None:
    """Ask the relay to stop, wait a bounded time for its current pass, and cancel it if it does not finish."""
    stop.set()
    relay.wake()
    done, _ = await asyncio.wait({relay_task}, timeout=RELAY_STOP_TIMEOUT_SECONDS)
    if not done:
        logger.error("Outbox relay did not stop within %s seconds; cancelling it", RELAY_STOP_TIMEOUT_SECONDS)
        relay_task.cancel()
        await asyncio.gather(relay_task, return_exceptions=True)


app = FastAPI(title=settings.app_name, lifespan=lifespan)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
def scrape_metrics() -> Response:
    """Return the Prometheus metrics. Unauthenticated, on the internal network (platform deviation 4)."""
    return Response(content=render_metrics(), media_type=METRICS_CONTENT_TYPE)
