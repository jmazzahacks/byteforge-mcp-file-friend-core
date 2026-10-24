from unittest.mock import patch

from byteforge_mcp_file_friend_models import FriendConfig
from prometheus_client import CollectorRegistry, generate_latest

from byteforge_mcp_file_friend_core.lifecycle import Lifecycle
from byteforge_mcp_file_friend_core.observability import LifecycleCollector


def test_lifecycle_snapshot_and_database_outage(tmp_path):
    store = Lifecycle(tmp_path, "https://example.invalid")
    record = store.reserve({"filename": "PRIVATE_FILENAME", "sha256": "a" * 64}, 7, 100, 0, 10)
    config = FriendConfig(tmp_path, "https://example.invalid")
    collector = LifecycleCollector(config)
    registry = CollectorRegistry()
    registry.register(collector)
    metrics = generate_latest(registry).decode()
    assert 'ff_uploads{state="uploading"} 1.0' in metrics
    assert "ff_storage_committed_bytes 7.0" in metrics
    assert record["id"] not in metrics
    assert "PRIVATE_FILENAME" not in metrics
    assert "ff_worker_healthy 0.0" in metrics
    with patch.object(collector.database, "cursor", side_effect=RuntimeError("PRIVATE_DSN")):
        metrics = generate_latest(registry).decode()
    assert "ff_database_up 0.0" in metrics
    assert "ff_storage_free_bytes" in metrics
    assert "PRIVATE_DSN" not in metrics
    assert "ff_uploads{" not in metrics


def test_supervisor_exports_metrics_from_its_own_process(tmp_path):
    import os
    import socket
    import subprocess
    import sys
    import time
    from urllib.request import urlopen

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = dict(os.environ, DEBUG_LOCAL="true", FF_DATA_DIR=str(tmp_path / "data"),
               FF_HOOK_TOKEN="h" * 32, FF_WORKER_METRICS_PORT=str(port))
    env.pop("PROMETHEUS_MULTIPROC_DIR", None)
    with (tmp_path / "worker.log").open("w") as log:
        process = subprocess.Popen([sys.executable, "-m", "byteforge_mcp_file_friend_core.worker",
                                    "--interval", "0.1"], env=env, stdout=log, stderr=log)
        try:
            metrics = ""
            for _ in range(150):
                try:
                    with urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2) as response:
                        metrics = response.read().decode()
                    if 'ff_worker_passes_total{result="success"}' in metrics:
                        break
                except OSError:
                    pass
                time.sleep(0.03)
            assert 'ff_worker_passes_total{result="success"}' in metrics
            assert "ff_database_up 1.0" in metrics
            assert "ff_worker_healthy 1.0" in metrics
        finally:
            process.terminate()
            try:
                process.wait(5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def test_future_heartbeat_is_not_healthy(tmp_path):
    import json
    import time

    (tmp_path / "worker.json").write_text(json.dumps({
        "heartbeat": time.time() + 1000, "state": "running", "failures": 0,
    }))
    collector = LifecycleCollector(FriendConfig(tmp_path, "https://example.invalid"))
    registry = CollectorRegistry()
    registry.register(collector)
    assert "ff_worker_healthy 0.0" in generate_latest(registry).decode()
