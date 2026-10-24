import hashlib
import json
from contextlib import contextmanager
from unittest.mock import patch

import psycopg2
import pytest

from byteforge_mcp_file_friend_core.database import DatabaseUnavailable
from byteforge_mcp_file_friend_core.lifecycle import Lifecycle
from byteforge_mcp_file_friend_core.repository import ConflictError


def upload(tmp_path):
    store = Lifecycle(tmp_path, "https://ff.example")
    record = store.reserve({"filename": "a", "sha256": hashlib.sha256(b"abc").hexdigest()}, 3, 1000, 0, 10)
    store.source(record["id"]).write_bytes(b"abc")
    return store, record


def test_stale_update_cannot_resurrect_deletion(tmp_path):
    store, record = upload(tmp_path)
    store.request_delete(record["id"])
    record["state"] = "verifying"
    with pytest.raises(ConflictError):
        store.write(record)
    assert store.read(record["id"])["state"] == "deleting"
    assert not (tmp_path / "meta").exists()


def test_unknown_filestore_is_not_adopted(tmp_path):
    store, record = upload(tmp_path)
    store.source(record["id"]).with_suffix(".info").write_text(
        json.dumps({"Size": 3, "MetaData": {"filename": "a", "sha256": record["sha256"]}})
    )
    store.repository.delete(record)  # Models metadata missing after restore.
    assert "UnreservedUpload" in store.reconcile(lambda _: None)
    assert not store.published(record).exists()
    with pytest.raises(FileNotFoundError):
        store.read(record["id"])


def test_dead_socket_is_replaced_without_restarting_process(tmp_path):
    store, record = upload(tmp_path)
    database = store.repository.database
    with database.cursor() as cursor:
        cursor.execute("SELECT pg_backend_pid() AS pid")
        pid = cursor.fetchone()["pid"]
    connection = psycopg2.connect(**database.options)
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute("SELECT pg_terminate_backend(%s)", (pid,))
    finally:
        connection.close()
    assert store.read(record["id"])["size"] == 3


def test_commit_ack_loss_recovers_without_republishing(tmp_path):
    store, record = upload(tmp_path)
    store.verify(record["id"])
    database = store.repository.database
    original = database.connection

    class LostAck:
        def __init__(self, connection):
            self.connection = connection

        def cursor(self, **kwargs):
            return self.connection.cursor(**kwargs)

        def commit(self):
            self.connection.commit()
            raise psycopg2.OperationalError("Injected lost acknowledgement")

    @contextmanager
    def connection():
        with original() as conn:
            yield LostAck(conn)

    # Fail the write's commit after it succeeds, rather than a preceding read.
    deleting = store.read(record["id"])
    deleting.update(state="deleting", terminal="deleted")
    with patch.object(database, "connection", connection), pytest.raises(DatabaseUnavailable):
        store.write(deleting)
    assert store.read(record["id"])["state"] == "deleting"
    assert store.published(record).exists()  # Crash boundary: intent committed, unlink not yet done.
    assert store.reconcile(lambda file_id: store.source(file_id).unlink(missing_ok=True)) == []
    assert store.read(record["id"])["state"] == "deleted"
    assert not store.published(record).exists()


def test_database_constraints_and_transaction_rollback(tmp_path):
    store, record = upload(tmp_path)
    with pytest.raises(psycopg2.IntegrityError), store.repository.database.cursor() as cursor:
        cursor.execute("UPDATE uploads SET size=-1 WHERE id=%s", (record["id"],))
    assert store.read(record["id"])["size"] == 3
    with pytest.raises(ValueError), store.repository.database.cursor() as cursor:
        cursor.execute("UPDATE uploads SET size=42 WHERE id=%s", (record["id"],))
        raise ValueError("Abort transaction")
    assert store.read(record["id"])["size"] == 3


def test_one_connection_pool_can_reserve(tmp_path, monkeypatch):
    monkeypatch.setenv("FF_DB_POOL_SIZE", "1")
    store, record = upload(tmp_path)
    assert store.read(record["id"])["state"] == "uploading"




def test_legacy_sidecars_require_explicit_import(tmp_path):
    legacy = tmp_path / "meta"
    legacy.mkdir()
    sidecar = legacy / ("a" * 32 + ".json")
    sidecar.write_text('{"state":"ready"}')
    with pytest.raises(ValueError, match="Legacy lifecycle sidecars"):
        Lifecycle(tmp_path, "https://ff.example")
    assert sidecar.read_text() == '{"state":"ready"}'


def test_stale_terminal_snapshot_does_not_corrupt_delete_intent(tmp_path):
    store, record = upload(tmp_path)
    record.update(state="expired", terminal="expired", deleted_at=1)
    store.write(record)
    original = store.read
    calls = 0

    def read(file_id):
        nonlocal calls
        value = original(file_id)
        calls += 1
        if calls == 1:
            updated = dict(value)
            updated.update(state="deleting")
            store.write(updated)  # Another reconciler obtained the lock after our snapshot.
        return value

    with patch.object(store, "read", side_effect=read):
        assert store.reconcile(lambda file_id: store.source(file_id).unlink(missing_ok=True)) == []
    assert store.read(record["id"])["state"] == "expired"


def test_verifier_rejects_fifo_without_blocking(tmp_path):
    import os
    import subprocess
    import sys

    store, record = upload(tmp_path)
    source = store.source(record["id"])
    source.unlink()
    os.mkfifo(source)
    code = """from pathlib import Path
import sys
from byteforge_mcp_file_friend_core.lifecycle import Lifecycle
try:
    Lifecycle(Path(sys.argv[1]), "https://ff.example").verify(sys.argv[2])
except ValueError:
    sys.exit(0)
sys.exit(1)
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(tmp_path), record["id"]],
        env={key: value for key, value in os.environ.items() if key != "PYTHONPATH"},
        timeout=3,
        check=False,
    )
    assert result.returncode == 0
    assert store.read(record["id"])["state"] == "uploading"


@pytest.mark.parametrize("state", ["deleted", "expired", "failed"])
def test_terminal_rows_require_cleanup_timestamp(tmp_path, state):
    store, record = upload(tmp_path)
    with pytest.raises(psycopg2.IntegrityError), store.repository.database.cursor() as cursor:
        cursor.execute("UPDATE uploads SET state=%s, deleted_at=NULL WHERE id=%s", (state, record["id"]))
    assert store.read(record["id"])["state"] == "uploading"


def test_transaction_timeouts_and_idle_role_default(tmp_path):
    store, _ = upload(tmp_path)
    with store.repository.database.cursor() as cursor:
        cursor.execute("SELECT current_setting('statement_timeout') AS statement, "
                       "current_setting('lock_timeout') AS lock, "
                       "current_setting('idle_in_transaction_session_timeout') AS idle")
        assert dict(cursor.fetchone()) == {"statement": "5s", "lock": "2s", "idle": "10s"}


def test_hashing_and_tusd_callbacks_hold_no_database_transaction(tmp_path):
    from psycopg2.extensions import TRANSACTION_STATUS_IDLE

    store, record = upload(tmp_path)
    observed = []

    def assert_idle():
        pool = store.repository.database.pool
        assert not pool._used
        assert all(conn.get_transaction_status() == TRANSACTION_STATUS_IDLE for conn in pool._pool)

    def fault(point):
        if point in ("verifying", "hashed"):
            assert_idle()
            observed.append(point)

    def terminate(file_id):
        assert_idle()
        observed.append("terminate")
        store.source(file_id).unlink()

    store.fault = fault
    store.verify(record["id"])
    store.request_delete(record["id"])
    store.drain_delete(record["id"], terminate)
    assert observed == ["verifying", "hashed", "terminate"]


@pytest.mark.parametrize("message,expected", [
    ("could not get connection from the pool: AllServersDown", DatabaseUnavailable),
    ("unrelated database system failure", psycopg2.DatabaseError),
])
def test_real_sqlstate_diagnostics_classify_only_pgcat_pool_errors(tmp_path, message, expected):
    store, _ = upload(tmp_path)
    with pytest.raises(expected), store.repository.database.cursor() as cursor:
        cursor.execute("DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='58000', MESSAGE=%s; END $$", (message,))
    assert store.repository.database.healthy()
