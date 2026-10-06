# cert-orchestrator-service

FastAPI based orchestrator that consumes certificate lifecycle events, runs a state machine, applies retry/backoff logic, ensures idempotency, mutates certificate lifecycle state in PostgreSQL using SQLAlchemy, and publishes completion events.

## Components

- `cert_orchestrator/main.py`: FastAPI app and lifecycle management.
- `cert_orchestrator/messaging/handlers.py`: RabbitMQ message handler and orchestration flow.
- `cert_orchestrator/state_machine.py`: transition decisions (`PENDING -> PROCESSING -> COMPLETED/FAILED`) and retry behavior.
- `cert_orchestrator/persistence/repository.py`: SQLAlchemy persistence and idempotency guard (`processed_events`).
- `alembic/`: schema migrations for PostgreSQL lifecycle tables.

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

> Retry message delay uses RabbitMQ's `x-delay` header and requires the delayed message exchange plugin to be available in the target broker setup.

## Test

```bash
ruff check .
ruff format --check .
pytest tests -q
```

## CI

[![CI](https://github.com/dcos-platform/cert-orchestrator-service/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/dcos-platform/cert-orchestrator-service/actions/workflows/ci.yml)
[![SonarCloud Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=dcos-platform_cert-orchestrator-service&metric=alert_status)](https://sonarcloud.io/dashboard?id=dcos-platform_cert-orchestrator-service)

Every pull request and push to `main` runs:

- Lint check (`ruff`)
- Format check (`ruff`)
- Test suite with branch coverage (floor: 78%)
- New-code coverage gate (95% of changed lines, PRs only)
- Migration round trip (upgrade, downgrade, upgrade)
- SonarCloud analysis with quality gate wait
