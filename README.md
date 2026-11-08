# cert-orchestrator-service

FastAPI based orchestrator that consumes certificate lifecycle events, runs a state machine, applies retry/backoff logic, ensures idempotency, mutates certificate lifecycle state in PostgreSQL using SQLAlchemy, and publishes completion events.

## Components

- `cert_orchestrator/main.py`: FastAPI app, lifespan management, the outbox relay task, and the `/healthz` and `/metrics` routes.
- `cert_orchestrator/logging_config.py`: the JSON log formatter and `configure_logging()`.
- `cert_orchestrator/logging_context.py`: the correlation context (`log_context`) and the filter that writes it onto each log record.
- `cert_orchestrator/metrics.py`: the Prometheus metrics, their closed label sets, and the helpers that record them.
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

The plain `uvicorn` command needs no `--log-config` and no `--no-access-log`: the service writes JSON logs itself.

Retry message delays are implemented using broker-native wait queues with TTL and dead-lettering to avoid message loss. A dead-letter queue (`cert.orchestrator.dlq`) collects messages that cannot be decoded, validated, or retried.

Operation handling: each cert-api routing key maps to an operation with its own lifecycle record. `cert.created` and `cert.renewed` are orchestrated and publish a completion, with retries and a `FAILED` completion once retries are exhausted. `cert.revoked` and `cert.expired` are recorded and acknowledged with no completion and no retry. Any other `cert.*` key is dead-lettered with reason `UNSUPPORTED_OPERATION`.

Outbox: the handler writes each completion and business retry to the `outbox_messages` table in the same database transaction as the lifecycle change, and never publishes from the handler itself. A relay task started with the application publishes pending rows, and marks a row sent only after the broker confirms it. A failed publish is retried with capped exponential backoff and is never abandoned; an ERROR log is written when a row has failed `CERT_ORCH_OUTBOX_ALERT_AFTER_ATTEMPTS` times. Completions carry `message_id` set to the event id and, when a valid one is available, an `x-correlation-id` header.

## Observability

**Logs.** Every line the process writes is one JSON object with cert-api's field set, in this fixed order: `timestamp`, `version`, `level`, `severity`, `thread`, `logger`, `message`, then `exception` when there is one, then the correlation fields that are set (`correlationId`, `certificateId`, `eventId`, `operation`, `state`, `reason`, `kind`). Field names are camelCase, matching cert-api, so one query finds one id in both services. The full field table is in `context.md`.

A line from the end-to-end run, for one completed create:

```json
{"timestamp": "2026-11-08T17:04:49.307Z", "version": "1", "level": "INFO", "severity": 20000, "thread": "MainThread", "logger": "cert_orchestrator.messaging.handlers", "message": "Processed lifecycle event", "correlationId": "429b1194-3547-45bf-9b77-a45a9dec75fb", "certificateId": "1084fd79-193f-47fe-963e-569ff288832b", "eventId": "8e568c6e-f32b-41b5-b66e-e76aa44db8d1", "operation": "CREATE", "state": "COMPLETED"}
```

**Tracing one correlation id across both services.** Search both containers' logs for the id:

```bash
docker logs <orchestrator-container> 2>&1 | grep -F '<id>'
docker logs <cert-api-container> 2>&1 | grep -F '<id>'
```

The orchestrator logs to stderr, hence `2>&1`. The two services separate JSON differently (`"correlationId": "<id>"` here, `"correlationId":"<id>"` in cert-api), so search for the id rather than the quoted key.

cert-api 1.0.0 needs two settings for this, applied to its container at run time:

- `SPRING_PROFILES_ACTIVE=prod`, because it writes JSON only under that profile.
- `LOGGING_LEVEL_ORG_SPRINGFRAMEWORK_WEB_SERVLET=DEBUG`, to get its create request line. At its default levels it writes no line for a create request.

**Metrics.** `GET /metrics` on the same port as `/healthz` serves the Prometheus text format. It is unauthenticated and meant for the internal network only. It never queries the database; the outbox gauges are refreshed once per relay pass.

| Metric | Type | Labels |
|---|---|---|
| `cert_orchestrator_events_consumed_total` | counter | `operation` (`CREATE`, `RENEW`, `REVOKE`, `EXPIRE`, or `unknown`), `outcome` (`processed`, `duplicate`, `dead_lettered`, `transport_retry`) |
| `cert_orchestrator_event_processing_seconds` | histogram | `operation` |
| `cert_orchestrator_lifecycle_transitions_total` | counter | `operation`, `state` (`PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`) |
| `cert_orchestrator_dead_letters_total` | counter | `reason` (`INVALID`, `EXHAUSTED`, `UNSUPPORTED_OPERATION`) |
| `cert_orchestrator_outbox_published_total` | counter | `kind` (`COMPLETION`, `RETRY`) |
| `cert_orchestrator_outbox_publish_failures_total` | counter | `kind` |
| `cert_orchestrator_outbox_pending` | gauge | `kind` |
| `cert_orchestrator_outbox_oldest_pending_age_seconds` | gauge | — |
| `cert_orchestrator_relay_pass_failures_total` | counter | — |

Label values come only from these closed sets. No id, subject, queue name or error text is ever a label, and every combination exports `0` until it moves.

**Relay failure logging.** A failing relay pass is counted every time, but logged at ERROR only on the first failure of an outage and then at most once per `CERT_ORCH_RELAY_FAILURE_LOG_INTERVAL_SECONDS` (default `60`). The first successful pass after an outage logs one INFO line, `Outbox relay recovered`.

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
