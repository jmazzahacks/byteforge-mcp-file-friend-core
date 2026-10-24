#!/usr/bin/env bash
# Disposable native PostgreSQL; no existing database or infrastructure is touched.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
pg_bin="${FF_TEST_PG_BIN:-/usr/lib/postgresql/18/bin}"
pg_tmp=$(mktemp -d /tmp/ff-pg-test.XXXXXXXX)
pg_port=$(bin/python -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
cleanup() {
    "$pg_bin/pg_ctl" -D "$pg_tmp/db" -m fast stop >/dev/null 2>&1 || true
    rm -rf -- "$pg_tmp"
}
trap cleanup EXIT
"$pg_bin/initdb" -D "$pg_tmp/db" -A trust --no-locale >"$pg_tmp/init.log"
"$pg_bin/pg_ctl" -D "$pg_tmp/db" -l "$pg_tmp/server.log" -o "-h 127.0.0.1 -p $pg_port -k $pg_tmp" start
export FF_DB_HOST=127.0.0.1 FF_DB_PORT="$pg_port" FF_DB_NAME=ff_test FF_DB_USER=byteforge_file_friend
export FF_DB_PASSWORD=disposable-test FF_DB_SSLMODE=disable FF_TEST_DATABASE=1
export FF_TEST_PG_CTL="$pg_bin/pg_ctl" FF_TEST_PG_DATA="$pg_tmp/db"
unset FF_DB_SCHEMA
"$pg_bin/psql" -h 127.0.0.1 -p "$pg_port" -U "$(id -un)" -d postgres -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE byteforge_file_friend LOGIN;
ALTER ROLE byteforge_file_friend SET statement_timeout = '5s';
ALTER ROLE byteforge_file_friend SET lock_timeout = '2s';
ALTER ROLE byteforge_file_friend SET idle_in_transaction_session_timeout = '10s';
CREATE DATABASE ff_test OWNER byteforge_file_friend;
SQL
bin/python -m pytest "$@"
