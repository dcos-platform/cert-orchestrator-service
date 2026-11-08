import io
import json
import logging
import logging.config
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest
from uvicorn.config import LOGGING_CONFIG

from cert_orchestrator.logging_config import ROUTED_LOGGER_NAMES, JsonFormatter, configure_logging
from cert_orchestrator.logging_context import ContextFilter, log_context

CORE_KEYS = ["timestamp", "version", "level", "severity", "thread", "logger", "message"]


def _record(level: int = logging.INFO, **attributes: object) -> logging.LogRecord:
    record = logging.LogRecord("svc", level, __file__, 1, "hello %s", ("world",), None)
    for key, value in attributes.items():
        setattr(record, key, value)
    return record


def _format(record: logging.LogRecord) -> dict[str, Any]:
    return json.loads(JsonFormatter().format(record))


def _exc_info() -> Any:
    try:
        raise ValueError("boom")
    except ValueError:
        return sys.exc_info()


@pytest.fixture
def restore_logging() -> Iterator[None]:
    """Save and restore the root logger and every routed logger, so configure_logging cannot leak into other tests."""
    root = logging.getLogger()
    saved_root = (list(root.handlers), root.level)
    routed: dict[str, tuple[list[logging.Handler], int, bool]] = {}
    for name in ROUTED_LOGGER_NAMES:
        logger = logging.getLogger(name)
        routed[name] = (list(logger.handlers), logger.level, logger.propagate)
    yield
    root.handlers = saved_root[0]
    root.setLevel(saved_root[1])
    for name, (handlers, level, propagate) in routed.items():
        logger = logging.getLogger(name)
        logger.handlers = handlers
        logger.setLevel(level)
        logger.propagate = propagate


def _swap_root_stream() -> io.StringIO:
    """Point the single root handler at a buffer and return the buffer."""
    (handler,) = logging.getLogger().handlers
    buffer = io.StringIO()
    handler.setStream(buffer)
    return buffer


def test_bare_record_has_exactly_the_core_keys_in_order():
    payload = _format(_record())

    assert list(payload) == CORE_KEYS
    assert payload["version"] == "1"
    assert payload["message"] == "hello world"
    assert payload["logger"] == "svc"
    assert payload["thread"] == _record().threadName


def test_record_with_every_optional_field_has_all_keys_in_order():
    record = _record(
        correlationId="corr-1",
        certificateId="cert-1",
        eventId="evt-1",
        operation="CREATE",
        state="COMPLETED",
        reason="INVALID",
        kind="RETRY",
    )
    record.exc_info = _exc_info()

    payload = _format(record)

    assert list(payload) == [
        *CORE_KEYS,
        "exception",
        "correlationId",
        "certificateId",
        "eventId",
        "operation",
        "state",
        "reason",
        "kind",
    ]


@pytest.mark.parametrize(
    ("levelno", "level", "severity"),
    [
        (logging.DEBUG, "DEBUG", 10000),
        (logging.INFO, "INFO", 20000),
        (logging.WARNING, "WARN", 30000),
        (logging.ERROR, "ERROR", 40000),
        (logging.CRITICAL, "ERROR", 40000),
    ],
)
def test_level_and_severity_follow_logback_names_for_each_standard_level(levelno, level, severity):
    payload = _format(_record(levelno))

    assert payload["level"] == level
    assert payload["severity"] == severity


def test_context_keys_are_absent_when_unset_or_none():
    payload = _format(_record(correlationId=None, certificateId=None, eventId=None, operation=None))

    assert list(payload) == CORE_KEYS


def test_context_fields_from_the_filter_are_written_when_set():
    handler_stream = io.StringIO()
    handler = logging.StreamHandler(handler_stream)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(ContextFilter())
    logger = logging.getLogger("svc.context")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        with log_context(correlationId="corr-9", eventId="evt-9"):
            logger.info("inside")
        logger.info("outside")
    finally:
        logger.removeHandler(handler)
    inside, outside = (json.loads(line) for line in handler_stream.getvalue().splitlines())

    assert (inside["correlationId"], inside["eventId"]) == ("corr-9", "evt-9")
    assert "correlationId" not in outside and "eventId" not in outside


def test_exception_field_holds_the_traceback_only_when_the_record_has_exc_info():
    record = _record(logging.ERROR)
    record.exc_info = _exc_info()

    payload = _format(record)

    assert "ValueError: boom" in payload["exception"]
    assert "exception" not in _format(_record(logging.ERROR))


def test_multiline_message_and_traceback_still_produce_exactly_one_line():
    record = logging.LogRecord("svc", logging.ERROR, __file__, 1, "first\nsecond", (), None)
    record.exc_info = _exc_info()

    output = JsonFormatter().format(record)

    assert "\n" not in output
    assert json.loads(output)["message"] == "first\nsecond"


def test_timestamp_is_utc_with_milliseconds_from_the_record_creation_time():
    record = _record()
    record.created = datetime(2026, 11, 8, 16, 31, 23, 584000, tzinfo=UTC).timestamp()

    payload = _format(record)

    assert payload["timestamp"] == "2026-11-08T16:31:23.584Z"


def test_non_json_value_is_written_as_text_and_does_not_raise():
    payload = _format(_record(state=object()))

    assert payload["state"].startswith("<object object")


def test_configure_logging_installs_one_json_handler_with_the_context_filter(restore_logging):
    configure_logging()

    root = logging.getLogger()
    (handler,) = root.handlers
    assert isinstance(handler.formatter, JsonFormatter)
    assert any(isinstance(item, ContextFilter) for item in handler.filters)
    assert root.level == logging.INFO


def test_calling_configure_logging_again_keeps_one_handler_and_one_line_per_record(restore_logging):
    configure_logging()
    configure_logging()
    buffer = _swap_root_stream()

    logging.getLogger("svc").info("once")

    assert [json.loads(line)["message"] for line in buffer.getvalue().splitlines()] == ["once"]


def test_records_from_child_loggers_reach_the_formatter_with_their_context(restore_logging):
    configure_logging()
    buffer = _swap_root_stream()

    with log_context(correlationId="corr-child"):
        logging.getLogger("a.b").info("child line")

    (payload,) = [json.loads(line) for line in buffer.getvalue().splitlines()]
    assert payload["logger"] == "a.b"
    assert payload["correlationId"] == "corr-child"


def test_uvicorn_and_alembic_loggers_emit_json_after_configure_logging(restore_logging):
    logging.config.dictConfig(LOGGING_CONFIG)
    configure_logging()
    buffer = _swap_root_stream()

    names = ["uvicorn", "uvicorn.error", "uvicorn.access", "alembic"]
    for name in names:
        logging.getLogger(name).info("from %s", name)

    payloads = [json.loads(line) for line in buffer.getvalue().splitlines()]
    assert [payload["logger"] for payload in payloads] == names
    assert [payload["message"] for payload in payloads] == [f"from {name}" for name in names]


def test_routed_loggers_lose_their_handlers_and_propagate_after_configure_logging(restore_logging):
    logging.config.dictConfig(LOGGING_CONFIG)
    assert logging.getLogger("uvicorn.access").handlers, "uvicorn's own config must give its logger a handler first"

    configure_logging()

    for name in ROUTED_LOGGER_NAMES:
        logger = logging.getLogger(name)
        assert logger.handlers == []
        assert logger.propagate is True
