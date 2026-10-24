import hashlib
import json
import os
import time
from dataclasses import replace
from pathlib import Path

import pytest
from byteforge_mcp_file_friend_models import FriendConfig

from byteforge_mcp_file_friend_core.lifecycle import Lifecycle
from byteforge_mcp_file_friend_core.worker import Supervisor, healthy


def config(root: Path) -> FriendConfig:
    return FriendConfig(
        root,
        "https://ff.example",
        hook_token="test",
        min_free=0,
        reservation_ttl=2,
        incomplete_ttl=2,
        incomplete_max_age=100,
        retention=2,
        worker_timeout=0.5,
        worker_grace=0.1,
    )


def reserve(store: Lifecycle, size: int = 100) -> str:
    record = store.reserve(
        {"filename": "a", "sha256": hashlib.sha256(b"a" * size).hexdigest()}, size, 1000, 0, 16
    )
    with store.lock(record["id"]):
        record["created_at"] -= 10
        store.write(record)
    return record["id"]


def test_orphan_reservation_and_bounded_cleanup(tmp_path: Path) -> None:
    store = Lifecycle(tmp_path, "https://ff.example", config=config(tmp_path))
    file_id = reserve(store)

    def gone(target: str) -> None:
        assert target == file_id

    def expire(target: str) -> bool:
        return True

    assert store.reconcile(gone, expire) == []
    assert store.read(file_id)["state"] == "expired"
    assert store.reserve({"filename": "b", "sha256": "b" * 64}, 100, 1000, 0, 1)
    with store.lock(file_id):
        record = store.read(file_id)
        record["deleted_at"] -= 10
        store.write(record)
    store.cleanup()
    assert not (tmp_path / "meta").exists()
    with pytest.raises(FileNotFoundError):
        store.read(file_id)
    assert len(list((tmp_path / "locks").iterdir())) <= 257


def test_stale_sweep_cannot_delete_new_activity(tmp_path: Path) -> None:
    store = Lifecycle(tmp_path, "https://ff.example", config=config(tmp_path))
    file_id = reserve(store)
    store.source(file_id).write_bytes(b"a")
    os.utime(store.source(file_id), (0, 0))

    def expire(target: str) -> bool:
        store.source(target).write_bytes(b"aa")  # New PATCH won before tusd's pre-terminate hook.
        return store.authorize_expiry(target)

    def never(target: str) -> None:
        raise AssertionError("Active upload must not be deleted")

    assert store.reconcile(never, expire) == []
    assert store.read(file_id)["state"] == "uploading"
    assert store.source(file_id).read_bytes() == b"aa"


def test_absolute_expiry_and_late_creation_do_not_resurrect(tmp_path: Path) -> None:
    settings = replace(config(tmp_path), incomplete_max_age=3)
    store = Lifecycle(tmp_path, settings.public_url, config=settings)
    file_id = reserve(store)

    def terminate(target: str) -> None:
        store.source(target).unlink(missing_ok=True)
        store.source(target).with_suffix(".info").unlink(missing_ok=True)

    def expire(target: str) -> bool:
        assert store.authorize_expiry(target)
        terminate(target)
        return True

    store.source(file_id).write_bytes(b"a")
    assert store.reconcile(terminate, expire) == []
    assert store.read(file_id)["state"] == "expired"
    store.source(file_id).write_bytes(b"a" * 100)  # Delayed creation after a tombstone.
    assert store.reconcile(terminate, expire) == []
    assert store.read(file_id)["state"] == "expired"
    assert not store.source(file_id).exists()


def sleeping_pass(settings: FriendConfig) -> None:
    time.sleep(10)


def success_pass(settings: FriendConfig) -> None:
    (settings.root / "ran").write_text("yes")


def test_worker_timeout_health_and_singleton(tmp_path: Path) -> None:
    settings = config(tmp_path)
    supervisor = Supervisor(settings, task=sleeping_pass)
    start = time.monotonic()
    assert supervisor.run(once=True) == 1
    assert time.monotonic() - start < 5
    assert not healthy(tmp_path)
    assert json.loads((tmp_path / "worker.json").read_text())["failures"] == 1
    store = Lifecycle(tmp_path, settings.public_url)
    with store.named_lock("worker.lock"), pytest.raises(BlockingIOError):
        Supervisor(settings).run(once=True)
    assert Supervisor(replace(settings, worker_timeout=5), task=success_pass).run(once=True) == 0
    assert (tmp_path / "ran").read_text() == "yes"


def test_supervisor_hard_death_stops_child(tmp_path):
    import subprocess
    import sys

    # Guarded module entry is needed by multiprocessing's spawn start method.
    runner = tmp_path / "run_supervisor.py"
    root = tmp_path / "worker-data"
    runner.write_text("""from pathlib import Path
import sys, time
from byteforge_mcp_file_friend_core.worker import Supervisor
from byteforge_mcp_file_friend_models import FriendConfig
def task(config):
    while True:
        (config.root / "child-pulse").write_text(str(time.monotonic()))
        time.sleep(0.02)
if __name__ == "__main__":
    Supervisor(FriendConfig(Path(sys.argv[1]), "https://ff.example", worker_timeout=30), task=task).run()
""")
    supervisor = subprocess.Popen(
        [sys.executable, str(runner), str(root)],
        env={key: value for key, value in os.environ.items() if key != "PYTHONPATH"},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    child_pid = None
    try:
        for _ in range(100):
            if (root / "child-pulse").exists():
                child_pid = json.loads((root / "worker.json").read_text())["child_pid"]
                break
            time.sleep(0.03)
        assert child_pid is not None, (
            supervisor.communicate(timeout=1)[0]
            if supervisor.poll() is not None
            else "Supervisor running without child"
        )
        supervisor.kill()
        supervisor.wait(3)
        for _ in range(100):
            proc = Path(f"/proc/{child_pid}/stat")
            if not proc.exists() or proc.read_text().split()[2] == "Z":
                break
            time.sleep(0.02)
        else:
            pytest.fail("Worker child survived supervisor SIGKILL")
        assert not healthy(root, max_age=0)
    finally:
        if supervisor.poll() is None:
            supervisor.kill()
            supervisor.wait(3)
        if child_pid:
            try:
                os.kill(child_pid, 9)
            except ProcessLookupError:
                pass
