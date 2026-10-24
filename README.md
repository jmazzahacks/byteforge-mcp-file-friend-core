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
uv pip install --no-build-isolation 'git+https://github.com/jmazzahacks/byteforge-mcp-file-friend-models.git@1282a681a41cd144b21593ab0820a724b9a485a6'
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

MIT licensed; see `LICENSE`. Public GitHub owner `jmazzahacks` is approved. This checkpoint is local; no core remote has been created.
