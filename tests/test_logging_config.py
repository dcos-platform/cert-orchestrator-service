import json
import logging
import sys

from cert_orchestrator.logging_config import JsonFormatter, configure_logging


def _record(**extra: object) -> logging.LogRecord:
    record = logging.LogRecord("svc", logging.INFO, __file__, 1, "hello %s", ("world",), None)
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_format_emits_core_fields() -> None:
    payload = json.loads(JsonFormatter().format(_record()))
    assert payload["level"] == "INFO"
    assert payload["logger"] == "svc"
    assert payload["message"] == "hello world"
    assert payload["timestamp"].endswith("+00:00")
    assert "exception" not in payload
    assert "event_id" not in payload


def test_format_includes_extra_fields() -> None:
    payload = json.loads(JsonFormatter().format(_record(event_id="e1", certificate_id="c1", state="FAILED")))
    assert (payload["event_id"], payload["certificate_id"], payload["state"]) == ("e1", "c1", "FAILED")


def test_format_includes_exception() -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        record = _record()
        record.exc_info = sys.exc_info()
    payload = json.loads(JsonFormatter().format(record))
    assert "ValueError: boom" in payload["exception"]


def test_configure_logging_installs_json_handler() -> None:
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers, root.level
    try:
        configure_logging()
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, JsonFormatter)
        assert root.level == logging.INFO
    finally:
        root.handlers, root.level = saved_handlers, saved_level
