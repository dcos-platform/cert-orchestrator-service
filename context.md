# Certificate Lifecycle Orchestrator Service — Current State

## Overview

A FastAPI service that consumes certificate lifecycle events from RabbitMQ, processes them through a state machine, persists state to PostgreSQL, and publishes completion events. **The plumbing works; the business logic is a placeholder.** `handlers.py` decides completion vs. failure from `payload["should_fail"]`; no certificate work is performed. By owner decision of 2026-10-04 (D1(c)), the work stays a documented no-op. Story 4 (complete) added routing-key operation awareness: each cert-api operation has its own lifecycle record, and only create and renew produce a completion. This service must not become a CA integration.

## Current Limitations

- **Coverage metrics:** The codebase floor (`fail_under`) and the coverage bar are **combined** coverage: coverage.py's total with `branch = true`, which counts lines and branches together (the same basis as SonarCloud's "Coverage"). The new-code gate (`diff-cover`) measures changed lines. Branch-only coverage is quoted in each story's evidence but is not enforced.

## Architecture

**Components:**

- `cert_orchestrator/main.py` — FastAPI app, lifespan management.
- `cert_orchestrator/messaging/handlers.py` — RabbitMQ message handler, orchestration flow.
- `cert_orchestrator/state_machine.py` — transition decisions (`PROCESSING` = retry scheduled, `COMPLETED`, `FAILED` = retries exhausted), and which operations produce a completion.
- `cert_orchestrator/persistence/repository.py` — SQLAlchemy persistence, idempotency guard (`processed_events`), per-operation lifecycle records.
- `alembic/` — schema migrations for PostgreSQL lifecycle tables (`0001` creates them; `0002` adds per-operation records).

**Wire contract:** Snake_case envelope in (from cert-api 1.0.0), `CompletionEvent` out. Fixed and owned by cert-api.

**Queue declarations:** `durable=True` with **no arguments**. The queues are shared with cert-api-service, and RabbitMQ requires every declaration of a queue to match. If any service adds an argument (an `x-dead-letter-exchange`, a TTL, anything else), the broker rejects the mismatched declaration with `PRECONDITION_FAILED` and whichever service declares second fails to start. A dead-letter queue (Story 3) must therefore be a separate, service-scoped queue, never an argument on a shared one.

**Database:** `expire_on_commit=False` on SessionLocal (`db.py:10`) — objects remain valid after commit, so `handlers.py` can read `retry_count` after commit without a re-query.

## Operations

cert-api sends one of four routing keys. The handler maps each to an operation (`operation_for_routing_key`) before it touches the database. A `cert.*` key that is not one of the four is dead-lettered with reason `UNSUPPORTED_OPERATION` and writes nothing.

| Operation | Routing key | Orchestrated (retries, then `FAILED`) | Completion published |
|---|---|---|---|
| `CREATE` | `cert.created` | yes | yes |
| `RENEW` | `cert.renewed` | yes | yes |
| `REVOKE` | `cert.revoked` | no | no (record is `COMPLETED`; `should_fail` is ignored) |
| `EXPIRE` | `cert.expired` | no | no (record is `COMPLETED`; `should_fail` is ignored) |

**States of a lifecycle record:**

- `PENDING`: the initial value of a new record. Current code never commits it: the record is created and moved to its next state in the same transaction.
- `PROCESSING`: a retry is scheduled. Committed together with the retry publish.
- `COMPLETED`: the operation succeeded, or a revoke/expire was recorded.
- `FAILED`: retries are exhausted. A `FAILED` completion is published.

**Per-operation records:** There is one lifecycle record per operation, keyed by `root_event_id`, the id of the event that started the operation. A business retry carries that id in the `x-cert-orch-root-event-id` header, so every retry of an operation updates the same record, and `retry_count` counts only that operation's retries. A new operation on a certificate, including a renew after an exhausted create, gets a new record starting at zero. Legacy rows (created before migration `0002`) have `NULL` `operation` and `root_event_id`, and new events never match them.

**Retry event ids:** A business retry's event id is `"<event_id>:retry:<n>"`, built from the event being retried, so a second retry's id extends the first (`evt:retry:1:retry:2`). The root is taken from the header, never parsed out of the event id.

**cert-api facts behind this design (measured 2026-10-06):**

- cert-api sets `orchestrationStatus = PENDING` on create and renew only. Revoke and expire do not touch it.
- Its completion listener overwrites `orchestrationStatus` with whichever completion arrives, without knowing which operation it answers.
- Its timeout sweep fails certificates left `PENDING` past a cutoff.

So a completion for revoke or expire would answer a question cert-api never asked, and could overwrite the status of a pending renewal. That is why those operations publish none.

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

**Dead-lettering:** Messages that cannot be decoded (invalid UTF-8 or JSON), fail validation, or carry an unsupported `cert.*` routing key go to the dead-letter exchange (`cert.orchestrator.dlx`) and queue (`cert.orchestrator.dlq`). The body is preserved byte-for-byte; headers record the reason (`x-cert-orch-dead-letter-reason`: `INVALID`, `EXHAUSTED` or `UNSUPPORTED_OPERATION`) and exception message.

**Headers:** Original routing key and exchange are preserved across retries and dead-letter hops via `x-original-routingKey` and `x-original-exchange` (set only if absent, so they survive multiple hops). The attempt number is tracked in `x-cert-orch-attempt`. The business retry adds `x-cert-orch-root-event-id` (see Operations).

**Retry carry-over rules:** A business retry is a new event with a fresh transport budget, so it strips `x-cert-orch-attempt` and `x-death` from the headers it copies; other headers are kept. A transport retry keeps the original `message_id` and `correlation_id`. A business retry's `x-original-exchange` is the inbound exchange, not an empty string.

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
3. **Tests with coverage:** `pytest --cov=cert_orchestrator --cov-report=xml --cov-report=term` (combined line and branch coverage floor: 94%)
4. **New-code coverage (PRs only):** `diff-cover` at 95% of changed lines
5. **Migration round trip:** `alembic upgrade head`, `alembic downgrade base`, `alembic upgrade head`, then `alembic check` (fails if the models and the migrated schema differ)
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
| Story 4 | Complete | Operation awareness: routing key → operation; create and renew orchestrated with a completion; revoke and expire recorded with none. Per-operation lifecycle records keyed by `root_event_id` (migration `0002`). `PROCESSING` = retry scheduled. Unsupported `cert.*` keys dead-lettered (`UNSUPPORTED_OPERATION`). `alembic check` in CI. Five Story 3 carry-overs. Coverage floor: 94% combined (94.96% measured; branch-only 68/68 = 100.00%; lines 403/428 = 94.16%). |
| Story 5 | Pending | Transactional outbox for completions (1.0.0 release blocker). `message_id` / `x-correlation-id` on completions. |
| Story 6 | Pending | Metrics, logging changes. |
| Story 7 | Pending | Dockerfile, image publish. |
| Story 8 | Pending | Version bump. |

*Story numbering revised 2026-10-04 by owner decision: CI and quality gates moved forward to Story 2, and the platform coverage bar raised to 95%.*
