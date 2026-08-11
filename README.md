# cert-orchestrator-service

FastAPI based orchestrator that consumes certificate lifecycle events, runs a state machine, applies retry/backoff logic, ensures idempotency, mutates certificate lifecycle state in PostgreSQL using SQLAlchemy, and publishes completion events.

## Components

- `cert_orchestrator/main.py`: FastAPI app and lifecycle management.
- `cert_orchestrator/messaging/handlers.py`: RabbitMQ message handler and orchestration flow.
- `cert_orchestrator/state_machine.py`: transition decisions (`PENDING -> PROCESSING -> COMPLETED/FAILED`) and retry behavior.
- `cert_orchestrator/persistence/repository.py`: SQLAlchemy persistence and idempotency guard (`processed_events`).
- `alembic/`: schema migrations for PostgreSQL lifecycle tables.

## Run locally

```bash
pip install -e .[test]
alembic upgrade head
uvicorn cert_orchestrator.main:app --reload
```

## Test

```bash
pytest tests -q
```
