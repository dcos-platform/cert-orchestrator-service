import asyncio
import logging

import pytest

from cert_orchestrator.logging_context import CONTEXT_FIELDS, ContextFilter, current_context, log_context

EMPTY = dict.fromkeys(CONTEXT_FIELDS)


def test_context_is_empty_by_default():
    assert current_context() == EMPTY


def test_values_are_visible_inside_the_block_and_restored_after_it():
    with log_context(correlationId="corr-1", certificateId="cert-1", eventId="evt-1", operation="CREATE"):
        assert current_context() == {
            "correlationId": "corr-1",
            "certificateId": "cert-1",
            "eventId": "evt-1",
            "operation": "CREATE",
        }
    assert current_context() == EMPTY


def test_nested_block_restores_the_outer_value_on_exit():
    with log_context(correlationId="outer"):
        with log_context(correlationId="inner"):
            assert current_context()["correlationId"] == "inner"
        assert current_context()["correlationId"] == "outer"
    assert current_context()["correlationId"] is None


def test_field_not_passed_keeps_its_current_value():
    with log_context(correlationId="corr-1"), log_context(eventId="evt-1"):
        assert current_context()["correlationId"] == "corr-1"
        assert current_context()["eventId"] == "evt-1"


def test_passing_none_clears_the_field_for_the_block_only():
    with log_context(correlationId="outer"):
        with log_context(correlationId=None):
            assert current_context()["correlationId"] is None
        assert current_context()["correlationId"] == "outer"


def test_values_are_restored_after_an_exception():
    with log_context(correlationId="outer"):
        with pytest.raises(RuntimeError, match="boom"), log_context(correlationId="inner", eventId="evt-1"):
            raise RuntimeError("boom")
        assert current_context() == {**EMPTY, "correlationId": "outer"}


def test_unknown_field_raises_and_leaves_the_context_untouched():
    with (
        pytest.raises(ValueError, match="unknown log context field: notAField"),
        log_context(correlationId="corr-1", notAField="x"),
    ):
        pytest.fail("the block must not run")
    assert current_context() == EMPTY


async def _worker(correlation_id: str, observed: list[str | None]) -> None:
    with log_context(correlationId=correlation_id):
        for _ in range(3):
            await asyncio.sleep(0)
            observed.append(current_context()["correlationId"])


@pytest.mark.asyncio
async def test_concurrent_tasks_never_see_each_others_context():
    first: list[str | None] = []
    second: list[str | None] = []

    await asyncio.gather(_worker("corr-first", first), _worker("corr-second", second))

    assert first == ["corr-first"] * 3
    assert second == ["corr-second"] * 3
    assert current_context() == EMPTY


def test_context_filter_copies_every_field_onto_the_record_and_passes_it():
    record = logging.LogRecord("svc", logging.INFO, __file__, 1, "hello", (), None)

    with log_context(correlationId="corr-1", operation="RENEW"):
        assert ContextFilter().filter(record) is True

    assert (record.correlationId, record.operation) == ("corr-1", "RENEW")
    assert (record.certificateId, record.eventId) == (None, None)
