"""Real PostgreSQL tests, isolated per test in a uniquely named schema."""

import os
import uuid
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def database_schema(monkeypatch, request):
    if request.node.path.name in {"test_database_pool.py"}:
        yield
        return
    if os.environ.get("FF_TEST_DATABASE") != "1":
        pytest.skip("Set FF_TEST_DATABASE=1 and FF_DB_* to a disposable local database")
    import psycopg2
    from psycopg2 import sql

    from byteforge_mcp_file_friend_core.database import connection_options

    options = connection_options()
    if options["host"] not in ("127.0.0.1", "localhost", "::1") or not options["dbname"].endswith("_test"):
        pytest.fail("Tests require a loopback database named *_test")
    schema = "ff_test_" + uuid.uuid4().hex
    monkeypatch.setenv("FF_DB_SCHEMA", schema)
    connection = psycopg2.connect(**options)
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            cursor.execute(sql.SQL("SET LOCAL search_path TO {}, pg_catalog").format(sql.Identifier(schema)))
            cursor.execute((Path(__file__).parent / "fixtures/schema.sql").read_text())
        yield
    finally:
        connection.close()
        connection = psycopg2.connect(**options)
        with connection, connection.cursor() as cursor:
            cursor.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        connection.close()
