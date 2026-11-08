import json
import logging
from datetime import UTC, datetime

from cert_orchestrator.logging_context import CONTEXT_FIELDS, ContextFilter

# Field set and fixed order of every log line (cert-api's field set, camelCase names; see context.md).
LOG_FORMAT_VERSION = "1"
OPTIONAL_FIELDS = (*CONTEXT_FIELDS, "state", "reason", "kind")
# Logback's names for the two levels that differ from Python's, so one level query matches on both services.
_LOGBACK_LEVEL_NAMES = {"WARNING": "WARN", "CRITICAL": "ERROR"}
# Loggers that keep their own handlers unless routed through the root: uvicorn's and alembic's.
ROUTED_LOGGER_NAMES = ("uvicorn", "uvicorn.error", "uvicorn.access", "alembic")


def _timestamp(created: float) -> str:
    """Return the record's creation time as UTC with millisecond precision and a Z suffix."""
    return datetime.fromtimestamp(created, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class JsonFormatter(logging.Formatter):
    """Formats each record as one JSON object with cert-api's field set, in a fixed order."""

    def format(self, record: logging.LogRecord) -> str:
        """Return the record as a single-line JSON object."""
        payload: dict[str, object] = {
            "timestamp": _timestamp(record.created),
            "version": LOG_FORMAT_VERSION,
            "level": _LOGBACK_LEVEL_NAMES.get(record.levelname, record.levelname),
            "severity": min(record.levelno, logging.ERROR) * 1000,
            "thread": record.threadName,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        for field in OPTIONAL_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        return json.dumps(payload, default=str)


def _json_handler() -> logging.Handler:
    """Return a stream handler that writes JSON with the current context attached."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.addFilter(ContextFilter())
    return handler


def _route_through_root(name: str) -> None:
    """Remove a logger's own handlers so its records propagate to the root's JSON handler."""
    logger = logging.getLogger(name)
    logger.handlers = []
    logger.propagate = True


def configure_logging() -> None:
    """Send every log line in the process through one JSON handler on the root logger, at INFO."""
    root = logging.getLogger()
    root.handlers = [_json_handler()]
    root.setLevel(logging.INFO)
    for name in ROUTED_LOGGER_NAMES:
        _route_through_root(name)
