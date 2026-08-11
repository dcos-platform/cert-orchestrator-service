from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class CertificateLifecycleEvent(BaseModel):
    event_id: str
    certificate_id: str
    payload: dict[str, Any] = Field(default_factory=dict)
    occurred_at: datetime | None = None


class CompletionEvent(BaseModel):
    event_id: str
    certificate_id: str
    status: str
    retry_count: int
    error: str | None = None
