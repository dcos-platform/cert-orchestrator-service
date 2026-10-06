# Repository Rules

## Context Maintenance

Changes that require an update to `context.md`:

- Configuration defaults (`cert_orchestrator/config.py`)
- Messaging contract (queue names, payload structure)
- Database schema (via `alembic/versions/`)
- Environment-variable naming
- Dependencies added or removed
- Story completion status

## Code Quality

**Cognitive complexity:** ≤ 15 per function (enforced by SonarCloud rule `python:S3776`).

**Fanout:** ≤ 5 as a guideline (a function calls at most 5 other functions directly).

**Imports:** No circular imports; all imports at module level (no function-local imports except rare cases approved in code review).

**Nested classes:** None in `cert_orchestrator/` (tests are exempt).

**Business logic:** All business logic must be unit-tested.

**Test coverage:** ≥ 95% branch coverage on new code (`pytest --cov` with `branch = true` in `pyproject.toml`). Enforced in CI by `diff-cover` at 95% of changed lines on every pull request, and by `fail_under` in `pyproject.toml` on the whole codebase. `fail_under` only ever rises: each story sets it to its measured total, rounded down, until 95.

**Lint:** ruff, configuration in `pyproject.toml`; `ruff check .` and `ruff format --check .` must pass. `# noqa` needs the rule code and a reason on the same line.

**Type hints:** All public functions and class methods have type hints on parameters and return values.

**Unused code:** Unused imports must be removed.

## Coverage Exclusions

Every path or line excluded from coverage must be listed here with its reason. An exclusion not listed here is a defect.

- `alembic/` — migration scripts run against a real database, not in unit tests. Each migration is proven by an `upgrade` / `downgrade` / `upgrade` round trip against an empty database. `# pragma: no cover` is not used.
- Lines consisting only of `...` — `Protocol` method bodies, never executed by design (`exclude_also` in `pyproject.toml`).

## String Literals

**Module-level constants:** Single-module literals → `UPPER_CASE` constant.

**Shared literals:** Across modules → constants module (not a class of constants that others inherit).

**Fixed-membership sets:** → `Enum` (e.g., `LifecycleState`).

**Environment-varying values:** → `Settings`, never constants.

**Exemptions:** Log templates, single-use exception messages, decorator arguments, test fixtures.

## Repository Rules

- **Git operations:** Never commit, push, tag, merge, or rebase. The owner performs all git operations. The committable surface is exactly: `alembic/`, `alembic.ini`, `cert_orchestrator/`, `tests/`, `pyproject.toml`, `.gitignore`, `LICENSE`, `.github/`, `sonar-project.properties`, `README.md`, `CLAUDE.md`, `context.md`.

## Documentation

- **Public functions/classes/methods:** Docstrings required for hand-written public items. Docstrings must be concise (one line preferred for simple cases).

## Reporting

- **Build and test results:** Never report a result that did not come from an actual run. Always quote real output verbatim. Label anything unverified.
- **No invented results:** All claims must be backed by actual execution.

## Platform Deviations

These deviations from cert-api's rules are recorded here per integration gate Check 2:

1. **Environment variable naming:** Variables carry the `CERT_ORCH_` prefix (e.g., `CERT_ORCH_POSTGRES_HOST`), unlike cert-api's unprefixed names. Reason: an unprefixed `POSTGRES_DB` shared with dcos-infra's `.env` (value `dcos`) would silently point this service at the wrong database.
2. **Rules file name:** This file is `CLAUDE.md` (uppercase), not cert-api's `claude.md`. Reason: Claude Code on Linux (used in CI) is case-sensitive.

The 95% branch coverage bar is not a deviation; it is the platform convention as of 2026-10-04 per `PLATFORM_INTEGRATION_GATE.md` Check 2.
