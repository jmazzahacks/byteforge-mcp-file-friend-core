"""Single-host lifecycle, validated with crash probes and real tusd."""

import fcntl
import hashlib
import json
import os
import secrets
import shutil
import stat
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote

from byteforge_mcp_file_friend_models import FileReference, FriendConfig, validate_id, validate_metadata

from byteforge_mcp_file_friend_core.repository import UploadRepository


def sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def no_fault(point: str) -> None:
    pass


class Lifecycle:
    """Local trusted volume only. Lock order: lifecycle lock, never tusd RPC.

    Verifiers hash outside the lock, then recheck state/inode under the lock.
    Deletion records a durable tombstone before removing publication and calls
    tusd only after releasing the lock, avoiding a pre-terminate hook deadlock.
    """

    def __init__(
        self,
        root: Path,
        public_url: str,
        fault: Callable[[str], None] = no_fault,
        config: FriendConfig | None = None,
    ) -> None:
        self.root = root
        self.public_url = public_url.rstrip("/")
        self.fault = fault
        self.config = config or FriendConfig(root, public_url)
        if any((root / "meta").glob("*.json")):
            raise ValueError(
                "Legacy lifecycle sidecars found; use a clean volume or an explicit offline import"
            )
        self.repository = UploadRepository()
        for name in ("uploads", "done", "locks"):
            (root / name).mkdir(parents=True, exist_ok=True)

    @contextmanager
    def named_lock(self, name: str, blocking: bool = True) -> Iterator[None]:
        with (self.root / "locks" / name).open("a") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
            try:
                yield
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)

    def lock(self, file_id: str) -> object:
        # Fixed stripes bound lock-file growth; never unlink lock files.
        validate_id(file_id)
        return self.named_lock(f"upload-{file_id[:2]}.lock")

    def read(self, file_id: str) -> dict:
        validate_id(file_id)
        return self.repository.read(file_id)

    def write(self, record: dict) -> None:
        """Caller holds the local per-ID lock; database version rejects stale writes."""
        self.repository.write(record)

    def register(self, file_id: str, metadata: dict, size: int, *, create: bool = True, cursor=None) -> dict:
        validate_id(file_id)
        values = validate_metadata(metadata, size)
        with self.lock(file_id):
            try:
                existing = self.repository.read(file_id, cursor)
            except FileNotFoundError:
                existing = None
            if existing is not None:
                for key, value in values.items():
                    if existing[key] != value:
                        raise ValueError("Upload identity changed")
                return existing  # Includes tombstones: never resurrect an ID.
            if not create:
                raise FileNotFoundError("Upload lacks an authoritative reservation")
            created_at = int(metadata.get("_ff_created_at", time.time()))
            record = {"id": file_id, "state": "uploading", "created_at": created_at, **values}
            self.repository.insert(record, cursor)
            return record

    def source(self, file_id: str) -> Path:
        return self.root / "uploads" / validate_id(file_id)

    def published(self, record: dict) -> Path:
        return self.root / "done" / record["id"] / record["filename"]

    def verify(self, file_id: str) -> None:
        with self.lock(file_id):
            record = self.read(file_id)
            if record["state"] not in ("uploading", "verifying", "publishing"):
                return
            try:
                fd = os.open(self.source(file_id), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            except FileNotFoundError:
                return
            stream = os.fdopen(fd, "rb")
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode):
                stream.close()
                raise ValueError("Upload source must be a regular file")
            if before.st_size != record["size"]:
                stream.close()
                return
            record["state"] = "verifying"
            try:
                self.write(record)
            except BaseException:
                stream.close()
                raise
        with stream:
            self.fault("verifying")
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
            os.fsync(stream.fileno())
            after = os.fstat(stream.fileno())
        self.fault("hashed")
        with self.lock(file_id):
            record = self.read(file_id)
            if record["state"] not in ("verifying", "publishing"):
                return
            try:
                current = self.source(file_id).stat(follow_symlinks=False)
            except FileNotFoundError:
                return
            identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            for observed in (after, current):
                if identity != (
                    observed.st_dev,
                    observed.st_ino,
                    observed.st_size,
                    observed.st_mtime_ns,
                    observed.st_ctime_ns,
                ):
                    raise RuntimeError("Upload changed during verification")
            if digest != record["sha256"]:
                record.update(state="deleting", terminal="failed", error="hash_mismatch")
                self.write(record)
                self.unpublish(record)
                return
            record["state"] = "publishing"
            self.write(record)
            self.fault("publishing")
            destination = self.published(record)
            destination.parent.mkdir(exist_ok=True)
            sync_directory(destination.parent.parent)
            try:
                os.link(self.source(file_id), destination, follow_symlinks=False)
            except FileExistsError:
                if destination.is_symlink() or not os.path.samefile(self.source(file_id), destination):
                    raise RuntimeError("Unexpected publication inode")
            sync_directory(destination.parent)
            self.fault("linked")
            now = int(time.time())
            record.update(state="ready", ready_at=now, expires_at=now + record["ttl"])
            self.write(record)
            self.fault("ready")

    def unpublish(self, record: dict) -> None:
        path = self.published(record)
        if path.parent.exists():
            path.unlink(missing_ok=True)
            sync_directory(path.parent)
            path.parent.rmdir()
            sync_directory(path.parent.parent)

    def request_delete(self, file_id: str, terminal: str = "deleted") -> None:
        if terminal not in ("deleted", "expired"):
            raise ValueError("Invalid deletion reason")
        with self.lock(file_id):
            record = self.read(file_id)
            if record["state"] in ("deleted", "expired", "failed"):
                return
            if record["state"] != "deleting":
                record.update(state="deleting", terminal=terminal)
                self.write(record)
            self.fault("deleting")
            self.unpublish(record)
            self.fault("unpublished")

    def drain_delete(self, file_id: str, terminate: Callable[[str], None]) -> None:
        with self.lock(file_id):
            record = self.read(file_id)
            if record["state"] != "deleting":
                return
            self.unpublish(record)
        # Never hold our lock across tusd DELETE: its hook takes the same lock.
        terminate(file_id)
        self.fault("terminated")
        with self.lock(file_id):
            record = self.read(file_id)
            if self.source(file_id).exists() or self.source(file_id).with_suffix(".info").exists():
                raise RuntimeError("tusd has not finished termination")
            record.update(state=record["terminal"], deleted_at=int(time.time()))
            self.write(record)
            self.fault("deleted")

    def expiry_due(self, record: dict, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        if record["state"] != "uploading":
            return False
        age = now - record["created_at"]
        source = self.source(record["id"])
        try:
            info = source.stat()
        except FileNotFoundError:
            return age >= self.config.reservation_ttl
        if age >= self.config.incomplete_max_age:
            return True
        if info.st_size == record["size"]:
            return False  # Fully received files go to verification, not inactivity expiry.
        return now - max(info.st_mtime, record["created_at"]) >= self.config.incomplete_ttl

    def authorize_expiry(self, file_id: str) -> bool:
        """Called by pre-terminate while tusd owns its upload lock."""
        with self.lock(file_id):
            record = self.read(file_id)
            if record["state"] == "deleting":
                return True
            if not self.expiry_due(record):
                return False
            record.update(state="deleting", terminal="expired")
            self.write(record)
            self.unpublish(record)
            return True

    def cleanup(self) -> None:
        now = time.time()
        for file_id in self.repository.ids():
            with self.lock(file_id):
                record = self.read(file_id)
                if (
                    record["state"] in ("deleted", "expired", "failed")
                    and now - record.get("deleted_at", now) >= self.config.retention
                    and not self.source(file_id).exists()
                    and not self.source(file_id).with_suffix(".info").exists()
                    and not self.published(record).exists()
                ):
                    self.repository.delete(record)

    def reconcile(
        self, terminate: Callable[[str], None], expire: Callable[[str], bool] | None = None
    ) -> list[str]:
        # A corrupt or concurrently removed record must not starve unrelated work.
        errors = []
        for info_path in sorted((self.root / "uploads").glob("*.info")):
            try:
                info = json.loads(info_path.read_text())
                if info.get("SizeIsDeferred") or info.get("IsPartial") or info.get("IsFinal"):
                    continue
                self.register(info_path.stem, info["MetaData"], info["Size"], create=False)
            except FileNotFoundError:
                if info_path.exists():
                    errors.append("UnreservedUpload")
                continue  # Never adopt unknown filestore artifacts after DB loss.
            except (OSError, ValueError, KeyError, TypeError) as error:
                errors.append(type(error).__name__)
        for file_id in self.repository.ids():
            try:
                record = self.read(file_id)
                if record["state"] == "ready" and record["expires_at"] <= int(time.time()):
                    self.request_delete(file_id, "expired")
                if record["state"] in ("deleted", "expired", "failed") and (
                    self.source(file_id).exists() or self.source(file_id).with_suffix(".info").exists()
                ):
                    # Delayed creation/notifications never resurrect a tombstoned upload.
                    with self.lock(file_id):
                        record = self.read(file_id)
                        if record["state"] in ("deleted", "expired", "failed"):
                            record.update(terminal=record["state"], state="deleting")
                            self.write(record)
                if expire is not None and self.expiry_due(record):
                    if expire(file_id):
                        # 404 means an orphan reservation: create its tombstone locally.
                        self.request_delete(file_id, "expired")
                    else:
                        continue  # Activity changed after the sweep snapshot.
                self.verify(file_id)
                self.drain_delete(file_id, terminate)
            except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                errors.append(type(error).__name__)
        try:
            self.cleanup()
        except (OSError, ValueError, KeyError, TypeError) as error:
            errors.append(type(error).__name__)
        return errors

    def available_bytes(self) -> int:
        return shutil.disk_usage(self.root).free

    def reserve(self, metadata: dict, size: int, max_size: int, min_free: int, max_active: int) -> dict:
        values = validate_metadata(metadata, size)
        if size > max_size:
            raise ValueError("Upload exceeds maximum size")
        # Database transaction serializes admission across API processes.
        with self.repository.admission() as cursor:
            active = 0
            remaining = 0
            for file_id in self.repository.ids(active=True, cursor=cursor):
                record = self.repository.read(file_id, cursor)
                if record["state"] in ("uploading", "verifying", "publishing", "deleting"):
                    active += 1
                    try:
                        present = self.source(file_id).stat().st_size
                    except FileNotFoundError:
                        present = 0
                    remaining += max(0, record["size"] - present)
            if active >= max_active or self.available_bytes() - remaining - size < min_free:
                raise CapacityError("Insufficient capacity for upload")
            return self.register(secrets.token_hex(16), values, size, cursor=cursor)

    def status(self, file_id: str) -> dict:
        record = self.read(file_id)
        status = {"id": file_id, "state": record["state"]}
        if "error" in record:
            status["error"] = record["error"]
        if record["state"] == "ready":
            reference = FileReference(
                **{
                    key: record[key]
                    for key in ("id", "filename", "size", "sha256", "created_at", "expires_at")
                },
                url=f"{self.public_url}/f/{file_id}/{quote(record['filename'], safe='')}",
            )
            status["reference"] = reference.to_dict()
        return status


class CapacityError(Exception):
    """Admission limits are exhausted; the caller can retry later."""
