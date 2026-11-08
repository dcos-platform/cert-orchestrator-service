# cert-orchestrator-service

FastAPI based orchestrator that consumes certificate lifecycle events, runs a state machine, applies retry/backoff logic, ensures idempotency, mutates certificate lifecycle state in PostgreSQL using SQLAlchemy, and publishes completion events.

## Components

- `cert_orchestrator/main.py`: FastAPI app, lifespan management, and the outbox relay task.
- `cert_orchestrator/messaging/handlers.py`: RabbitMQ message handler and orchestration flow (writes outbox rows; never touches the broker).
- `cert_orchestrator/messaging/relay.py`: outbox relay that publishes pending rows with broker confirms and retries failures with capped backoff.
- `cert_orchestrator/state_machine.py`: transition decisions (`PROCESSING` while a retry is scheduled, `COMPLETED`, `FAILED` once retries are exhausted) and which operations publish a completion.
- `cert_orchestrator/persistence/repository.py`: SQLAlchemy persistence and idempotency guard (`processed_events`).
- `cert_orchestrator/persistence/outbox.py`: outbox persistence (`outbox_messages`): enqueue, claim, and mark.
- `alembic/`: schema migrations for PostgreSQL lifecycle and outbox tables.

## Run locally

Install dependencies:

```bash
pip install -e .[test]
```

Set environment variables (see `context.md` for defaults and the full table):

```bash
export CERT_ORCH_POSTGRES_HOST=localhost
export CERT_ORCH_POSTGRES_PORT=5432
export CERT_ORCH_POSTGRES_DB=cert_orchestrator
export CERT_ORCH_POSTGRES_USER=dcos
export CERT_ORCH_POSTGRES_PASSWORD=changeme
export CERT_ORCH_RABBITMQ_HOST=localhost
export CERT_ORCH_RABBITMQ_PORT=5672
export CERT_ORCH_RABBITMQ_USER=dcos
export CERT_ORCH_RABBITMQ_PASSWORD=changeme
export CERT_ORCH_RABBITMQ_VHOST=/
```

Apply migrations and start the service:

```bash
alembic upgrade head
uvicorn cert_orchestrator.main:app --reload
```

Retry message delays are implemented using broker-native wait queues with TTL and dead-lettering to avoid message loss. A dead-letter queue (`cert.orchestrator.dlq`) collects messages that cannot be decoded, validated, or retried.

Operation handling: each cert-api routing key maps to an operation with its own lifecycle record. `cert.created` and `cert.renewed` are orchestrated and publish a completion, with retries and a `FAILED` completion once retries are exhausted. `cert.revoked` and `cert.expired` are recorded and acknowledged with no completion and no retry. Any other `cert.*` key is dead-lettered with reason `UNSUPPORTED_OPERATION`.

Outbox: the handler writes each completion and business retry to the `outbox_messages` table in the same database transaction as the lifecycle change, and never publishes from the handler itself. A relay task started with the application publishes pending rows, and marks a row sent only after the broker confirms it. A failed publish is retried with capped exponential backoff and is never abandoned; an ERROR log is written when a row has failed `CERT_ORCH_OUTBOX_ALERT_AFTER_ATTEMPTS` times. Completions carry `message_id` set to the event id and, when a valid one is available, an `x-correlation-id` header.

## Test

Integration tests need RabbitMQ **and** a PostgreSQL database migrated with `alembic upgrade head` (`CERT_ORCH_POSTGRES_*`). They fail, rather than skip, when either is unreachable.

Run tests with broker (via Docker on `dcos-net`):

```bash
docker run --rm --network dcos-net --name rabbitmq -p 5672:5672 -e RABBITMQ_DEFAULT_USER=dcos -e RABBITMQ_DEFAULT_PASS=changeme rabbitmq:3.12-alpine
CERT_ORCH_RABBITMQ_HOST=rabbitmq pytest --cov=cert_orchestrator --cov-report=term
```

Run unit tests only (no broker needed):

```bash
pytest tests -q -m "not integration"
ruff check .
ruff format --check .
```

## CI

[![CI](https://github.com/dcos-platform/cert-orchestrator-service/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/dcos-platform/cert-orchestrator-service/actions/workflows/ci.yml)
[![SonarCloud Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=dcos-platform_cert-orchestrator-service&metric=alert_status)](https://sonarcloud.io/dashboard?id=dcos-platform_cert-orchestrator-service)

Every pull request and push to `main` runs:

- Lint check (`ruff`)
- Format check (`ruff`)
- Migration round trip (upgrade, downgrade, upgrade) and `alembic check` (models match migrations), before the tests so that the integration tests run against the migrated schema
- Test suite with combined line and branch coverage (floor: 95%; branch-only is also reported)
- New-code coverage gate (95% of changed lines, PRs only)
- SonarCloud analysis with quality gate wait
