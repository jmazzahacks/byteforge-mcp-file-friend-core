"""Recovery-path regressions for the ByteForge pool pattern (no live DB needed)."""

import os
import threading
from unittest.mock import MagicMock

import psycopg2
import pytest

from byteforge_mcp_file_friend_core.database import Database, DatabaseUnavailable


def database(*connections):
    db = Database.__new__(Database)
    db.pid = os.getpid()
    db.mutex = threading.Lock()
    db.pool = MagicMock()
    db.pool.closed = False
    db.pool.getconn.side_effect = connections
    return db


def connection():
    conn = MagicMock()
    conn.closed = 0
    return conn


@pytest.mark.parametrize("error", [psycopg2.OperationalError, psycopg2.DatabaseError])
def test_preping_discards_dead_and_recovers(error):
    dead, live = connection(), connection()
    dead.cursor.return_value.execute.side_effect = error()
    db = database(dead, live)
    with db.connection() as conn:
        assert conn is live
    assert db.pool.putconn.call_args_list[0].kwargs["close"] is True
    assert db.pool.putconn.call_args_list[1].kwargs["close"] is False


def test_preping_signal_returns_slot():
    conn = connection()
    conn.cursor.return_value.execute.side_effect = KeyboardInterrupt
    db = database(conn)
    with pytest.raises(KeyboardInterrupt), db.connection():
        pass
    db.pool.putconn.assert_called_once_with(conn, close=True)


@pytest.mark.parametrize("broken", [False, True])
def test_midflight_failure_classifies_connection_health(broken):
    conn = connection()
    db = database(conn)
    with pytest.raises(psycopg2.errors.SerializationFailure), db.connection():
        if broken:
            conn.rollback.side_effect = RuntimeError("Unknown transaction state")
        raise psycopg2.errors.SerializationFailure()
    db.pool.putconn.assert_called_once_with(conn, close=broken)


def test_retry_budget_and_pool_exhaustion():
    db = database(*[psycopg2.OperationalError() for _ in range(3)])
    with pytest.raises(DatabaseUnavailable), db.connection():
        pass
    assert db.pool.getconn.call_count == 3
    db = database(psycopg2.pool.PoolError())
    with pytest.raises(DatabaseUnavailable, match="exhausted"), db.connection():
        pass
    assert db.pool.getconn.call_count == 1


def test_preping_cleanup_cannot_swallow_shutdown_signal():
    conn = connection()
    conn.cursor.return_value.execute.side_effect = KeyboardInterrupt
    conn.cursor.return_value.close.side_effect = RuntimeError("Broken cursor cleanup")
    db = database(conn)
    with pytest.raises(KeyboardInterrupt), db.connection():
        pass
    db.pool.putconn.assert_called_once_with(conn, close=True)


class PgcatUnavailable(psycopg2.DatabaseError):
    def __init__(self, message="could not get connection from the pool: AllServersDown"):
        super().__init__(message)

    @property
    def pgcode(self):
        return "58000"


def test_startup_options_are_not_sent(monkeypatch):
    from byteforge_mcp_file_friend_core.database import connection_options

    monkeypatch.setenv("FF_DB_PASSWORD", "test")
    monkeypatch.setenv("FF_DB_SSLMODE", "disable")
    options = connection_options()
    assert "options" not in options
    assert options["sslmode"] == "disable"


def test_preping_ends_transaction_before_yield_and_success_returns_idle():
    conn = connection()
    db = database(conn)
    with db.connection():
        assert conn.rollback.call_count == 1
    assert conn.rollback.call_count == 2
    db.pool.putconn.assert_called_once_with(conn, close=False)


@pytest.mark.parametrize("error", [RuntimeError, KeyboardInterrupt])
def test_success_cleanup_failure_returns_discarded_slot(error):
    conn = connection()
    db = database(conn)
    with pytest.raises(error), db.connection():
        conn.rollback.side_effect = error()
    db.pool.putconn.assert_called_once_with(conn, close=True)


def test_pgcat_checkout_failure_retries_before_application_work():
    dead, live = connection(), connection()
    dead.cursor.return_value.execute.side_effect = PgcatUnavailable()
    db = database(dead, live)
    with db.connection() as conn:
        assert conn is live
    assert db.pool.getconn.call_count == 2
    assert db.pool.putconn.call_args_list[0].kwargs["close"] is True


@pytest.mark.parametrize("phase", ["read", "write", "commit"])
def test_pgcat_failure_is_retryable_but_never_replays_body(phase):
    conn = connection()
    db = database(conn)
    db.schema = "public"
    calls = 0
    with pytest.raises(DatabaseUnavailable), db.cursor() as cursor:
        calls += 1
        if phase == "commit":
            conn.commit.side_effect = PgcatUnavailable()
        else:
            cursor.execute.side_effect = PgcatUnavailable()
            cursor.execute("SELECT 1" if phase == "read" else "UPDATE uploads SET version=version+1")
    assert calls == 1
    assert db.pool.getconn.call_count == 1
    db.pool.putconn.assert_called_once_with(conn, close=False)


def test_nontransient_database_errors_are_not_reclassified():
    conn = connection()
    db = database(conn)
    db.schema = "public"
    with pytest.raises(psycopg2.IntegrityError), db.cursor():
        raise psycopg2.IntegrityError()


def test_unrelated_system_error_is_not_classified_as_pgcat_capacity():
    conn = connection()
    db = database(conn)
    db.schema = "public"
    error = PgcatUnavailable("unrelated database system failure")
    with pytest.raises(PgcatUnavailable) as caught, db.cursor():
        raise error
    assert caught.value is error
