"""ByteForge psycopg2 pool: bounded pre-ping, process-local connections, no write replay."""

import logging
import os
import threading
from contextlib import contextmanager

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from psycopg2.pool import PoolError, ThreadedConnectionPool

logger = logging.getLogger(__name__)


class DatabaseUnavailable(RuntimeError):
    """Retry by reconciling durable state, never by blindly replaying a write."""


def connection_options() -> dict:
    password = os.environ.get("FF_DB_PASSWORD")
    if not password:
        raise ValueError("FF_DB_PASSWORD is required")
    return {
        "host": os.environ.get("FF_DB_HOST", "localhost"),
        "port": int(os.environ.get("FF_DB_PORT", "5432")),
        "dbname": os.environ.get("FF_DB_NAME", "byteforge_file_friend"),
        "user": os.environ.get("FF_DB_USER", "byteforge_file_friend"),
        "password": password,
        "sslmode": os.environ.get("FF_DB_SSLMODE", "prefer"),
        "connect_timeout": 2,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        "options": "-c statement_timeout=5000 -c lock_timeout=2000 -c idle_in_transaction_session_timeout=10000",
    }


class Database:
    def __init__(self) -> None:
        self.options = connection_options()
        self.schema = os.environ.get("FF_DB_SCHEMA", "public")
        self.max_connections = int(os.environ.get("FF_DB_POOL_SIZE", "4"))
        if self.max_connections < 1:
            raise ValueError("FF_DB_POOL_SIZE must be positive")
        self.pool = None
        self.pid = os.getpid()
        self.mutex = threading.Lock()

    def get_pool(self):
        if self.pid != os.getpid():
            # Never use or close inherited libpq connections in the child.
            raise DatabaseUnavailable("Create the database pool after process creation")
        with self.mutex:
            if self.pool is None:
                self.pool = ThreadedConnectionPool(1, self.max_connections, **self.options)
        return self.pool

    @staticmethod
    def putback(pool, conn, close=False):
        try:
            pool.putconn(conn, close=close)
        except Exception:  # noqa: BLE001 -- cleanup must not mask the original error
            logger.error("Database pool cleanup failed")
            try:
                conn.close()
            except Exception:  # noqa: BLE001 -- best-effort cleanup
                logger.error("Database socket cleanup failed")

    @contextmanager
    def connection(self):
        for _ in range(3):
            try:
                pool = self.get_pool()
                conn = pool.getconn()
            except PoolError as error:
                raise DatabaseUnavailable("Database pool exhausted") from error
            except Exception:  # noqa: BLE001 -- connection setup only, per ByteForge skill
                logger.warning("Database checkout failed")
                continue
            try:
                cursor = conn.cursor()
                try:
                    cursor.execute("SELECT 1")
                finally:
                    try:
                        cursor.close()
                    except Exception:  # noqa: BLE001 -- preserve pre-ping errors and signals
                        logger.error("Database pre-ping cursor cleanup failed")
                conn.rollback()
            except BaseException as error:
                self.putback(pool, conn, close=True)
                if not isinstance(error, Exception):
                    raise
                continue
            break
        else:
            raise DatabaseUnavailable("Database unavailable")
        try:
            yield conn
        except BaseException:
            discard = True
            try:
                conn.rollback()
                discard = bool(conn.closed)
            except Exception:  # noqa: BLE001 -- unknown transaction state must be discarded
                logger.error("Database rollback failed")
            finally:
                self.putback(pool, conn, close=discard or bool(conn.closed))
            raise
        else:
            self.putback(pool, conn, close=bool(conn.closed))

    @contextmanager
    def cursor(self):
        try:
            with self.connection() as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                try:
                    cursor.execute(
                        sql.SQL("SET LOCAL search_path TO {}, pg_catalog").format(sql.Identifier(self.schema))
                    )
                    yield cursor
                    conn.commit()  # Lost acknowledgement: reconcile, never replay here.
                finally:
                    try:
                        cursor.close()
                    except Exception:  # noqa: BLE001 -- preserve the operation's failure
                        logger.error("Database cursor cleanup failed")
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as error:
            raise DatabaseUnavailable("Database operation unavailable") from error

    def healthy(self) -> bool:
        try:
            with self.cursor() as cursor:
                cursor.execute("SELECT id FROM uploads LIMIT 0")
            return True
        except (DatabaseUnavailable, psycopg2.Error):
            return False

    def close(self):
        if self.pid == os.getpid() and self.pool is not None and not self.pool.closed:
            self.pool.closeall()

    def __del__(self):
        try:
            self.close()
        except Exception:  # noqa: BLE001 -- destructor cleanup
            logger.error("Database pool close failed")
