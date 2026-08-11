from contextlib import asynccontextmanager

from fastapi import FastAPI

from cert_orchestrator.config import settings
from cert_orchestrator.logging_config import configure_logging
from cert_orchestrator.messaging.handlers import LifecycleMessageHandler
from cert_orchestrator.messaging.rabbitmq import RabbitMQClient

configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    rabbitmq = RabbitMQClient(settings.rabbitmq_url)
    await rabbitmq.connect()
    app.state.rabbitmq = rabbitmq

    handler = LifecycleMessageHandler(rabbitmq)
    await rabbitmq.consume(handler.handle)
    try:
        yield
    finally:
        await rabbitmq.close()


app = FastAPI(title=settings.app_name, lifespan=lifespan)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}
