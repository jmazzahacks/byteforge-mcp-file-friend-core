"""Single-instance supervisor for bounded, restartable lifecycle passes."""

import argparse
import json
import logging
import math
import multiprocessing
import os
import signal
import tempfile
import threading
import time
from collections.abc import Callable
from multiprocessing.connection import wait
from pathlib import Path

from byteforge_mcp_file_friend_models import FriendConfig

from byteforge_mcp_file_friend_core.lifecycle import Lifecycle, sync_directory
from byteforge_mcp_file_friend_core.tusd import Tusd

logger = logging.getLogger(__name__)


def run_pass(config: FriendConfig) -> None:
    store = Lifecycle(config.root, config.public_url, config=config)
    tusd = Tusd(config.tusd_url)
    errors = store.reconcile(tusd.terminate, tusd.expire)
    if errors:
        raise RuntimeError(f"Reconciliation incomplete: {len(errors)} file errors")


def run_child(config: FriendConfig, task: Callable[[FriendConfig], None]) -> None:
    """Exit on parent death, including SIGKILL of a native (non-container) supervisor."""
    parent = multiprocessing.parent_process()
    if parent is None:
        raise RuntimeError("Worker child requires a supervisor")

    def watch_parent() -> None:
        wait([parent.sentinel])
        os._exit(70)  # Abrupt parent death: release all local locks and database sockets.

    threading.Thread(target=watch_parent, daemon=True).start()
    task(config)


def healthy(root: Path, max_age: float = 15) -> bool:
    try:
        state = json.loads((root / "worker.json").read_text())
        return (
            state["state"] == "running"
            and 0 <= time.time() - state["heartbeat"] < max_age
            and state["failures"] < 3
        )
    except (OSError, ValueError, KeyError, TypeError):
        return False


class Supervisor:
    def __init__(self, config: FriendConfig, interval: float = 5, task: object = run_pass) -> None:
        self.config = config
        self.interval = interval
        self.task = task
        self.stop = threading.Event()
        self.failures = 0
        self.last_success = None

    def heartbeat(self, child_pid: int | None = None, state: str = "running") -> None:
        payload = {
            "pid": os.getpid(),
            "child_pid": child_pid,
            "state": state,
            "heartbeat": time.time(),
            "last_success": self.last_success,
            "failures": self.failures,
        }
        fd, path = tempfile.mkstemp(prefix=".worker-", dir=self.config.root)
        try:
            with os.fdopen(fd, "w") as stream:
                json.dump(payload, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(path, self.config.root / "worker.json")
            sync_directory(self.config.root)
        finally:
            Path(path).unlink(missing_ok=True)

    def shutdown(self, child: multiprocessing.Process) -> None:
        if self.stop.is_set():
            child.join(self.config.worker_grace)
        if child.is_alive():
            child.terminate()
            child.join(min(1, self.config.worker_grace))
        if child.is_alive():
            child.kill()
        child.join()

    def run(self, once: bool = False) -> int:
        store = Lifecycle(self.config.root, self.config.public_url, config=self.config)
        with store.named_lock("worker.lock", blocking=False):
            for path in self.config.root.glob(".worker-*"):
                path.unlink(missing_ok=True)
            try:
                while not self.stop.is_set():
                    child = multiprocessing.get_context("spawn").Process(
                        target=run_child, args=(self.config, self.task)
                    )
                    child.start()
                    started = time.monotonic()
                    timed_out = False
                    try:
                        while child.is_alive() and not self.stop.is_set():
                            self.heartbeat(child.pid)
                            if time.monotonic() - started >= self.config.worker_timeout:
                                timed_out = True
                                break
                            self.stop.wait(0.2)
                    finally:
                        self.shutdown(child)
                    if self.stop.is_set():
                        return 0
                    success = not timed_out and child.exitcode == 0
                    self.failures = 0 if success else self.failures + 1
                    if success:
                        self.last_success = time.time()
                    else:
                        logger.error("Worker pass failed or timed out; retry %d", self.failures)
                    self.heartbeat()
                    if once:
                        return 0 if success else 1
                    delay = self.interval if success else min(60, self.interval * 2 ** min(self.failures, 6))
                    until = time.monotonic() + delay
                    while not self.stop.is_set() and time.monotonic() < until:
                        self.heartbeat()
                        self.stop.wait(min(1, max(0, until - time.monotonic())))
                return 0
            finally:
                self.heartbeat(state="stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--healthcheck", action="store_true")
    parser.add_argument("--interval", type=float, default=5)
    args = parser.parse_args()
    config = FriendConfig.from_env()
    if args.healthcheck:
        from byteforge_mcp_file_friend_core.database import Database

        database = Database()
        try:
            raise SystemExit(0 if healthy(config.root) and database.healthy() else 1)
        finally:
            database.close()
    config.validate()
    if not math.isfinite(args.interval) or args.interval <= 0:
        parser.error("--interval must be positive")
    logging.basicConfig(level=logging.INFO)
    supervisor = Supervisor(config, args.interval)

    def stop(signum: int, frame: object) -> None:
        supervisor.stop.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        raise SystemExit(supervisor.run(args.once))
    except BlockingIOError:
        parser.exit(1, "A worker supervisor already owns this storage directory\n")


if __name__ == "__main__":
    main()
