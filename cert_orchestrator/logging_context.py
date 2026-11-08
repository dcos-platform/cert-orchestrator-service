import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

# Fields a log line can carry from the code that is handling it. Names match cert-api's MDC keys (D-S6-1).
CONTEXT_FIELDS = ("correlationId", "certificateId", "eventId", "operation")

_FIELD_NAMES = frozenset(CONTEXT_FIELDS)
_VARIABLES: dict[str, ContextVar[str | None]] = {name: ContextVar(name, default=None) for name in CONTEXT_FIELDS}


def current_context() -> dict[str, str | None]:
    """Return the value of every context field in the current context."""
    return {name: variable.get() for name, variable in _VARIABLES.items()}


@contextmanager
def log_context(**fields: str | None) -> Iterator[None]:
    """Set the given context fields for the block; fields not given keep their value. All are restored on exit."""
    unknown = sorted(set(fields) - _FIELD_NAMES)
    if unknown:
        raise ValueError(f"unknown log context field: {', '.join(unknown)}")
    tokens = [(_VARIABLES[name], _VARIABLES[name].set(value)) for name, value in fields.items()]
    try:
        yield
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)


class ContextFilter(logging.Filter):
    """Copies the current context onto every record, so the formatter can read it. Install it on a handler."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Attach the context to the record; always lets the record through."""
        for name, value in current_context().items():
            setattr(record, name, value)
        return True
