# Certificate Lifecycle Orchestrator Service — Current State

## Overview

A FastAPI service that consumes certificate lifecycle events from RabbitMQ, processes them through a state machine, persists state to PostgreSQL, and publishes completion events. **The plumbing works; the business logic is a placeholder.** `handlers.py` decides completion vs. failure from `payload["should_fail"]`; no certificate work is performed. By owner decision of 2026-10-04 (D1(c)), Story 4 will add routing-key operation awareness and keep the work a documented no-op. This service must not become a CA integration.

## Current Limitations

- **Routing key ignored:** The routing key is currently discarded (`rabbitmq.py`), so all four operations are handled identically — until Stories 3 and 4.
- **Retry backoff inert:** Retry backoff (`x-delay` on the default exchange) does not work — until Story 3.
- **No DLQ:** Invalid events are discarded without a dead-letter queue; unexpected exceptions requeue forever — until Story 3.
- **No CI:** No CI and no quality gate yet — until Story 2. Until then, coverage and tests are only as good as the last local run.

## Architecture

**Components:**

- `cert_orchestrator/main.py` — FastAPI app, lifespan management.
- `cert_orchestrator/messaging/handlers.py` — RabbitMQ message handler, orchestration flow.
- `cert_orchestrator/state_machine.py` — transition decisions (`PENDING → PROCESSING → COMPLETED/FAILED`), retry behavior.
- `cert_orchestrator/persistence/repository.py` — SQLAlchemy persistence, idempotency guard (`processed_events`).
- `alembic/` — schema migrations for PostgreSQL lifecycle tables.

**Wire contract:** Snake_case envelope in (from cert-api 1.0.0), `CompletionEvent` out. Fixed and owned by cert-api.

**Queue declarations:** `durable=True` with **no arguments**. The queues are shared with cert-api-service, and RabbitMQ requires every declaration of a queue to match. If any service adds an argument (an `x-dead-letter-exchange`, a TTL, anything else), the broker rejects the mismatched declaration with `PRECONDITION_FAILED` and whichever service declares second fails to start. A dead-letter queue (Story 3) must therefore be a separate, service-scoped queue, never an argument on a shared one.

**Database:** `expire_on_commit=False` on SessionLocal (`db.py:10`) — objects remain valid after commit, so `handlers.py` can read `retry_count` after commit without a re-query.

## Configuration

All configuration is environment-driven via `Settings` in `cert_orchestrator/config.py`. Full-URL overrides (`CERT_ORCH_DATABASE_URL`, `CERT_ORCH_RABBITMQ_URL`) are available but optional; the primary path is component-based.

| Variable | Type | Default |
|---|---|---|
| `CERT_ORCH_DATABASE_URL` | `str` (optional) | `None` (use components) |
| `CERT_ORCH_POSTGRES_HOST` | `str` | `localhost` |
| `CERT_ORCH_POSTGRES_PORT` | `int` | `5432` |
| `CERT_ORCH_POSTGRES_DB` | `str` | `cert_orchestrator` |
| `CERT_ORCH_POSTGRES_USER` | `str` | `dcos` |
| `CERT_ORCH_POSTGRES_PASSWORD` | `SecretStr` | `changeme` |
| `CERT_ORCH_RABBITMQ_URL` | `str` (optional) | `None` (use components) |
| `CERT_ORCH_RABBITMQ_HOST` | `str` | `localhost` |
| `CERT_ORCH_RABBITMQ_PORT` | `int` | `5672` |
| `CERT_ORCH_RABBITMQ_USER` | `str` | `dcos` |
| `CERT_ORCH_RABBITMQ_PASSWORD` | `SecretStr` | `changeme` |
| `CERT_ORCH_RABBITMQ_VHOST` | `str` | `/` |

## How to Run

Install dependencies, apply migrations, start the service:

```bash
pip install -e .[test]
CERT_ORCH_POSTGRES_HOST=<host> CERT_ORCH_RABBITMQ_HOST=<host> alembic upgrade head
CERT_ORCH_POSTGRES_HOST=<host> CERT_ORCH_RABBITMQ_HOST=<host> uvicorn cert_orchestrator.main:app --host 0.0.0.0 --port 8000
```

With dcos-infra: there is no image yet (the Dockerfile arrives in Story 7), so run from source in a stock Python container on `dcos-net`. Run this from PowerShell, not Git Bash, because Git Bash rewrites container paths:

```powershell
docker run --rm --network dcos-net -p 8000:8000 -v "${PWD}:/src:ro" `
  -e CERT_ORCH_POSTGRES_HOST=postgres -e CERT_ORCH_RABBITMQ_HOST=rabbitmq `
  python:3.12-slim sh -c "cp -r /src /app && cd /app && pip install -q -e . && alembic upgrade head && uvicorn cert_orchestrator.main:app --host 0.0.0.0 --port 8000"
```

## Test

```bash
pytest tests -q
pytest tests --cov=cert_orchestrator --cov-report=term-missing
```

## Story Status

| Story | Status | Notes |
|---|---|---|
| Story 1 | Complete | Run from clean, establish repo conventions. Approved 2026-10-04. Coverage baseline: 70% total (branch), `config.py` 100%. |
| Story 2 | Pending | CI workflow, SonarCloud, quality gate, coverage floor. |
| Story 3 | Pending | DLQ, redelivery, routing key, headers, retry backoff. |
| Story 4 | Pending | Operation awareness, `PROCESSING` state, `retry_count` semantics. |
| Story 5 | Pending | `message_id` / `x-correlation-id` on completions. |
| Story 6 | Pending | Metrics, logging changes. |
| Story 7 | Pending | Dockerfile, image publish. |
| Story 8 | Pending | Version bump. |

*Story numbering revised 2026-10-04 by owner decision: CI and quality gates moved forward to Story 2, and the platform coverage bar raised to 95%.*
