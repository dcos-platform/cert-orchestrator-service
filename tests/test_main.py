import asyncio
import logging
from collections.abc import Callable
from typing import Any

import pytest
from fastapi import FastAPI

from cert_orchestrator import main
from cert_orchestrator.messaging.handlers import LifecycleMessageHandler


class FakeClient:
    """Records the order of lifecycle calls made by the lifespan."""

    def __init__(self, consume_error: Exception | None = None) -> None:
        self.calls: list[str] = []
        self.consume_error = consume_error
        self.handler: Callable[..., Any] | None = None

    async def connect(self) -> None:
        self.calls.append("connect")

    async def consume(self, handler: Callable[..., Any]) -> None:
        self.calls.append("consume")
        self.handler = handler
        if self.consume_error is not None:
            raise self.consume_error

    async def close(self) -> None:
        self.calls.append("close")


class FakeRelay:
    """Runs until its stop event is set, recording whether it started and finished."""

    last: "FakeRelay | None" = None
    ignore_stop = False

    def __init__(self, session_factory: Any, publisher: Any, config: Any) -> None:
        self.publisher = publisher
        self.started = False
        self.finished = False
        self.cancelled = False
        self.wakes = 0
        FakeRelay.last = self

    def wake(self) -> None:
        self.wakes += 1

    async def run(self, stop: asyncio.Event) -> None:
        self.started = True
        try:
            if FakeRelay.ignore_stop:
                await asyncio.Event().wait()
            await stop.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        finally:
            self.finished = True


async def wait_until(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition was not reached in time")
        await asyncio.sleep(0.01)


@pytest.fixture
def client(monkeypatch) -> FakeClient:
    fake = FakeClient()
    monkeypatch.setattr(main, "RabbitMQClient", lambda: fake)
    monkeypatch.setattr(main, "OutboxRelay", FakeRelay)
    FakeRelay.last = None
    FakeRelay.ignore_stop = False
    return fake


@pytest.mark.asyncio
async def test_lifespan_connects_before_consuming_and_wires_the_handler_to_the_relay(client):
    app = FastAPI()

    async with main.lifespan(app):
        assert client.calls == ["connect", "consume"]
        assert app.state.rabbitmq is client
        relay = FakeRelay.last
        assert relay is not None
        await wait_until(lambda: relay.started)
        handler = client.handler.__self__
        assert isinstance(handler, LifecycleMessageHandler)
        assert handler.wake == relay.wake


@pytest.mark.asyncio
async def test_shutdown_stops_the_relay_and_closes_the_client(client):
    app = FastAPI()

    async with main.lifespan(app):
        relay = FakeRelay.last
        assert relay is not None
        await wait_until(lambda: relay.started)

    assert relay.finished
    assert relay.wakes >= 1
    assert not relay.cancelled
    assert client.calls == ["connect", "consume", "close"]


@pytest.mark.asyncio
async def test_relay_that_ignores_stop_is_cancelled_after_the_bound_and_the_client_still_closes(
    client, monkeypatch, caplog
):
    monkeypatch.setattr(main, "RELAY_STOP_TIMEOUT_SECONDS", 0.05)
    FakeRelay.ignore_stop = True
    app = FastAPI()

    with caplog.at_level(logging.ERROR, logger=main.logger.name):
        async with main.lifespan(app):
            relay = FakeRelay.last
            assert relay is not None
            await wait_until(lambda: relay.started)

    assert relay.cancelled
    assert relay.finished
    assert client.calls[-1] == "close"
    errors = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert len(errors) == 1
    assert "did not stop within" in errors[0].getMessage()


@pytest.mark.asyncio
async def test_consume_failure_stops_the_relay_closes_the_client_and_propagates(monkeypatch):
    fake = FakeClient(consume_error=RuntimeError("consume failed"))
    monkeypatch.setattr(main, "RabbitMQClient", lambda: fake)
    monkeypatch.setattr(main, "OutboxRelay", FakeRelay)
    FakeRelay.last = None
    app = FastAPI()

    with pytest.raises(RuntimeError, match="consume failed"):
        async with main.lifespan(app):
            pytest.fail("the lifespan body must not run when consume fails")

    relay = FakeRelay.last
    assert relay is not None
    assert relay.started
    assert relay.finished
    assert fake.calls == ["connect", "consume", "close"]


def test_healthz_reports_ok():
    assert main.healthz() == {"status": "ok"}
