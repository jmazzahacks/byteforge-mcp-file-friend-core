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
