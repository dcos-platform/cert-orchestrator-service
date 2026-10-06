# Certificate Lifecycle Orchestrator Service — Current State

## Overview

A FastAPI service that consumes certificate lifecycle events from RabbitMQ, processes them through a state machine, persists state to PostgreSQL, and publishes completion events. **The plumbing works; the business logic is a placeholder.** `handlers.py` decides completion vs. failure from `payload["should_fail"]`; no certificate work is performed. By owner decision of 2026-10-04 (D1(c)), Story 4 will add routing-key operation awareness and keep the work a documented no-op. This service must not become a CA integration.

## Current Limitations

- **Coverage metrics:** The new-code coverage gate uses **line** coverage (via `diff-cover`), while the codebase floor uses **branch** coverage.

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

## Message Handling

Two kinds of retry exist:

| | Business retry | Transport retry |
|---|---|---|
| Trigger | handler decides failure (`should_fail` in tests) | an unexpected exception escapes the handler |
| Counter | `retry_count` in the database | header `x-cert-orch-attempt` on the message |
| Message | new event id `"<id>:retry:<n>"` | same message, same event id |
| Delay | `backoff_seconds × retry_count` | `backoff_seconds × attempt` |
| End | FAILED completion after `max_retries` | dead-letter after `max_delivery_attempts` |

**Retry backoff:** Broker-native wait queues (one per distinct delay, named `cert.orchestrator.wait.<delay_ms>ms`) with `x-message-ttl` and dead-lettering to the default exchange. A message sits in the wait queue for exactly the TTL, then the broker moves it back to the incoming queue.

**Dead-lettering:** Messages that cannot be decoded (invalid UTF-8 or JSON) or fail validation go to the dead-letter exchange (`cert.orchestrator.dlx`) and queue (`cert.orchestrator.dlq`). The body is preserved byte-for-byte; headers record the reason (`x-cert-orch-dead-letter-reason`: `INVALID` or `EXHAUSTED`) and exception message.

**Headers:** Original routing key and exchange are preserved across retries and dead-letter hops via `x-original-routingKey` and `x-original-exchange` (set only if absent, so they survive multiple hops). The attempt number is tracked in `x-cert-orch-attempt`.

**Publish-then-ack:** A message is only acknowledged after its republish (to a wait queue or dead-letter) has been confirmed by the broker.

**Publish-then-commit:** The handler publishes the completion or retry, then commits the event to the database, then the message is acknowledged (in `_on_message` after the handler returns). A crash between publish and commit leaves nothing recorded; the redelivered message is processed again, and the completion is published again with the same event_id, which cert-api's idempotency guard (`ON CONFLICT (event_id) DO NOTHING`) absorbs. This is safe only while the handler work is a no-op; Story 5 will replace this with a transactional outbox.

**Known limitation:** If a republish (to a wait queue or dead-letter) itself fails (the broker is refusing publishes), the message is logged at ERROR and nack(requeue=True) is called. This can redeliver the message in a tight loop until the broker recovers. **The completion outbox (Story 5) is a 1.0.0 release blocker and will defer republish confirmation in a durable log.** Publish-then-commit is the interim solution.

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
| `CERT_ORCH_DEAD_LETTER_EXCHANGE` | `str` | `cert.orchestrator.dlx` |
| `CERT_ORCH_DEAD_LETTER_QUEUE` | `str` | `cert.orchestrator.dlq` |
| `CERT_ORCH_WAIT_QUEUE_PREFIX` | `str` | `cert.orchestrator.wait` |
| `CERT_ORCH_MAX_DELIVERY_ATTEMPTS` | `int` | `3` |

## Quality Gates

CI runs on every pull request to `main` and every push to `main`:

1. **Lint:** `ruff check .`
2. **Format:** `ruff format --check .`
3. **Tests with coverage:** `pytest --cov=cert_orchestrator --cov-report=xml --cov-report=term` (branch coverage floor: 93%)
4. **New-code coverage (PRs only):** `diff-cover` at 95% of changed lines
5. **Migration round trip:** `alembic upgrade head`, `alembic downgrade base`, `alembic upgrade head`
6. **SonarCloud analysis:** Quality gate wait on PRs and `main` push. The org is on SonarCloud's Free plan, which allows only the built-in *Sonar way* gate (80% coverage on new code). The platform's 95% bar is enforced by step 4 (`diff-cover`), not by SonarCloud.

Branch protection is not currently applied (applied after Story 2 merges).

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
| Story 2 | Complete | CI workflow, SonarCloud, quality gate. Coverage floor: 78% (branch coverage; 78.52% measured). |
| Story 3 | Complete | DLQ, redelivery, routing key, headers, retry backoff. Wait queues with TTL. Publish-then-commit. Coverage floor: 93% (branch coverage; 93.65% measured). |
| Story 4 | Pending | Operation awareness, `PROCESSING` state, `retry_count` semantics. |
| Story 5 | Pending | Transactional outbox for completions (1.0.0 release blocker). `message_id` / `x-correlation-id` on completions. |
| Story 6 | Pending | Metrics, logging changes. |
| Story 7 | Pending | Dockerfile, image publish. |
| Story 8 | Pending | Version bump. |

*Story numbering revised 2026-10-04 by owner decision: CI and quality gates moved forward to Story 2, and the platform coverage bar raised to 95%.*
