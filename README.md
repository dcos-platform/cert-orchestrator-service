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

Retry message delays are implemented using broker-native wait queues with TTL and dead-lettering to avoid message loss. A dead-letter queue (`cert.orchestrator.dlq`) collects messages that cannot be decoded, validated, or retried.

## Test

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
- Test suite with branch coverage (floor: 93%)
- New-code coverage gate (95% of changed lines, PRs only)
- Migration round trip (upgrade, downgrade, upgrade)
- SonarCloud analysis with quality gate wait
