# File Friend core

Lifecycle, storage coordination, PostgreSQL access, tusd termination adapter and supervised `ff-worker`. Python 3.13+, Linux/local filesystem. No Flask dependency.

```python
from byteforge_mcp_file_friend_core.lifecycle import Lifecycle
```

## Install and validate locally

The virtual environment lives at the repository root. Preserve `.gitignore` when recreating it.

```bash
source bin/activate
uv pip install --require-hashes -r requirements.txt -r dev-requirements.txt -r build-requirements.txt
uv pip install --no-build-isolation 'git+https://github.com/jmazzahacks/byteforge-mcp-file-friend-models.git@9c5d7054af80be19206d32dadd7975f4e421aa0d'
uv build --wheel --no-build-isolation
uv pip install --no-deps dist/byteforge_mcp_file_friend_core-0.0.1-py3-none-any.whl
python -m pytest tests/test_database_pool.py
bash dev_scripts/test_local.sh -q
ruff check src tests
```

The models dependency is pinned to its reviewed public MIT commit. Third-party pins and hashes are preserved from validated workspace locks; refresh with the seven-day release-age gate. No sibling path dependencies or credentials belong in package metadata.

Tests run against installed wheels. The local runner creates and removes a disposable loopback PostgreSQL 18 cluster and uses a non-superuser application role, with a separate schema per test. `FF_TEST_PG_BIN` can select the PostgreSQL binaries. Never point tests at Athena. Plain pytest skips database tests unless explicitly configured for a disposable local database.

`tests/fixtures/schema.sql` is a test-only snapshot of the workspace schema at extraction. Production schema/setup tooling belongs to backend and provisioning belongs to Athena; keep the fixture synchronized when the canonical schema changes. There is no runtime migration. Core tests use simulated tusd callbacks; real Flask/tusd and process-crash integration suites remain with backend.

## Extraction status

Core behavior is unchanged; imports and the worker entry point use the independent package names. The legacy workspace implementation remains intact until consumer migration. CLI command `ff-worker`, `FF_*` configuration and lifecycle contracts are preserved.

MIT licensed; see `LICENSE`. Public repository: https://github.com/jmazzahacks/byteforge-mcp-file-friend-core.

## PgCat transaction-pooling contract

Client startup `options` are deliberately omitted: Athena's PgCat drops them. Athena must provision role defaults of `statement_timeout=5s`, `lock_timeout=2s` and `idle_in_transaction_session_timeout=10s`. Each application transaction additionally applies `SET LOCAL statement_timeout` and `lock_timeout`; this does not replace the idle role default. The local PostgreSQL runner sets the same role defaults.

Pre-ping rolls back before yielding a connection. Success and exception cleanup return or discard the slot and end the transaction. No hashing or tusd RPC occurs inside a database transaction. SQLSTATE `58000` with PgCat's pool-checkout/AllServersDown diagnostic is surfaced as `DatabaseUnavailable`, as are connection-loss errors; backend translates this to HTTP 503 with Retry-After. Pre-ping/checkout retries are bounded to three attempts before application work. Once a transaction body begins, it is never replayed automatically, including on lost commit acknowledgement. Callers may retry reads and proven-unexecuted writes; ambiguous mutations must reconcile durable state first. Workers retry by reconciling on a later pass. Capacity rejection remains distinct from database unavailability.

Terminal states `deleted`, `expired` and `failed` require `deleted_at`; `deleting` intentionally does not, because it represents cleanup still in progress. Tests cover the schema constraint, idle connections at hash/tusd boundaries and injected PgCat errors. Local tests use direct PostgreSQL and injected SQLSTATE failures, not Athena's live PgCat; routing/defaults must still be smoke-tested by Athena after separately authorized provisioning.

Admission also enforces `storage_budget` (default 200 GiB). It counts declared
sizes for all nonterminal uploads, including ready files and pending deletion,
under the same transaction advisory lock as disk reservations. The independent
free-space check subtracts unwritten reservations; published hardlinks count once.
The budget limits payload commitments, not filesystem metadata or other services.
