"""Typed relational lifecycle records; local filesystem locks remain in Lifecycle."""

from contextlib import contextmanager

from psycopg2 import sql

from byteforge_mcp_file_friend_core.database import Database

FIELDS = (
    "id",
    "filename",
    "sha256",
    "size",
    "ttl",
    "state",
    "created_at",
    "ready_at",
    "expires_at",
    "deleted_at",
    "terminal",
    "error",
)


class ConflictError(RuntimeError):
    pass


class UploadRepository:
    def __init__(self):
        self.database = Database()

    def read(self, file_id, cursor=None):
        if cursor is None:
            with self.database.cursor() as owned:
                return self.read(file_id, owned)
        cursor.execute("SELECT * FROM uploads WHERE id = %s", (file_id,))
        row = cursor.fetchone()
        if row is None:
            raise FileNotFoundError("Unknown upload")
        return {key: value for key, value in row.items() if value is not None and key != "row_id"}

    def ids(self, active=False, cursor=None):
        if cursor is None:
            with self.database.cursor() as owned:
                return self.ids(active, owned)
        query = "SELECT id FROM uploads"
        if active:
            query += " WHERE state IN ('uploading','verifying','publishing','deleting')"
        cursor.execute(query + " ORDER BY id")
        return [row["id"] for row in cursor.fetchall()]

    def capacity_records(self, cursor):
        # One snapshot includes ready files; terminal rows retain no reservation.
        cursor.execute(
            "SELECT id, size, state FROM uploads "
            "WHERE state IN ('uploading','verifying','publishing','ready','deleting')"
        )
        return cursor.fetchall()

    def insert(self, record, cursor=None):
        if cursor is None:
            with self.database.cursor() as owned:
                self.insert(record, owned)
            return
        cursor.execute(
            sql.SQL("INSERT INTO uploads ({}) VALUES ({}) RETURNING version").format(
                sql.SQL(",").join(map(sql.Identifier, FIELDS)),
                sql.SQL(",").join(sql.Placeholder() for _ in FIELDS),
            ),
            [record.get(field) for field in FIELDS],
        )
        record["version"] = cursor.fetchone()["version"]

    def write(self, record):
        with self.database.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    "UPDATE uploads SET {}, version=version+1 WHERE id=%s AND version=%s RETURNING version"
                ).format(
                    sql.SQL(",").join(sql.SQL("{}=%s").format(sql.Identifier(field)) for field in FIELDS[1:])
                ),
                [record.get(field) for field in FIELDS[1:]] + [record["id"], record["version"]],
            )
            row = cursor.fetchone()
            if row is None:
                raise ConflictError("Upload changed; reconcile before retry")
        record["version"] = row["version"]

    def delete(self, record):
        with self.database.cursor() as cursor:
            cursor.execute(
                "DELETE FROM uploads WHERE id=%s AND version=%s", (record["id"], record["version"])
            )

    @contextmanager
    def admission(self):
        with self.database.cursor() as cursor:
            # Transaction-scoped; compatible with transaction pooling.
            cursor.execute("SELECT pg_advisory_xact_lock(1179000902)")
            yield cursor
