import multiprocessing
from pathlib import Path
from unittest.mock import patch

import pytest

from byteforge_mcp_file_friend_core.lifecycle import CapacityError, Lifecycle

METADATA = {"filename": "fixture.bin", "sha256": "a" * 64}


class FixedCapacity(Lifecycle):
    def available_bytes(self) -> int:
        return 100


def reserve_in_process(root: str, results: object) -> None:
    store = FixedCapacity(Path(root), "https://example.invalid")
    try:
        store.reserve(METADATA, 60, 100, 0, 10)
        results.put("accepted")
    except CapacityError:
        results.put("rejected")


def test_concurrent_reservations_cannot_overbook(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    processes = [context.Process(target=reserve_in_process, args=(str(tmp_path), results)) for _ in range(4)]
    try:
        for process in processes:
            process.start()
        outcomes = [results.get(timeout=5) for _ in processes]
        for process in processes:
            process.join(5)
            assert process.exitcode == 0
        assert outcomes.count("accepted") == 1
        assert outcomes.count("rejected") == 3
    finally:
        for process in processes:
            if process.is_alive():
                process.kill()
                process.join()
        results.close()


def test_failed_reservation_transaction_rolls_back(tmp_path: Path) -> None:
    store = FixedCapacity(tmp_path, "https://example.invalid")
    original = store.repository.insert

    def insert_then_fail(record, cursor=None):
        original(record, cursor)
        raise RuntimeError("Injected failure before admission commit")

    with patch.object(store.repository, "insert", side_effect=insert_then_fail), pytest.raises(RuntimeError):
        store.reserve(METADATA, 60, 100, 0, 10)
    assert store.repository.ids() == []
    assert not (tmp_path / "meta").exists()
    assert store.reserve(METADATA, 60, 100, 0, 10)["state"] == "uploading"


def test_pending_deletion_keeps_capacity_reserved(tmp_path: Path) -> None:
    store = FixedCapacity(tmp_path, "https://example.invalid")
    record = store.reserve(METADATA, 60, 100, 0, 10)
    store.request_delete(record["id"])
    with pytest.raises(CapacityError):
        store.reserve(METADATA, 60, 100, 0, 10)


def test_corrupt_record_does_not_starve_other_uploads(tmp_path: Path) -> None:
    import hashlib

    store = Lifecycle(tmp_path, "https://example.invalid")
    file_id = "d" * 32
    record = store.register(file_id, {"filename": "file", "sha256": hashlib.sha256(b"abc").hexdigest()}, 3)
    store.source(file_id).write_bytes(b"abc")
    (tmp_path / "uploads" / ("a" * 32 + ".info")).write_text("{broken")

    def unused_terminate(target: str) -> None:
        raise AssertionError("No deletion expected")

    assert store.reconcile(unused_terminate) == ["JSONDecodeError"]
    assert store.published(record).read_bytes() == b"abc"


def reserve_budget_in_process(root: str, results: object) -> None:
    store = Lifecycle(Path(root), "https://example.invalid")
    try:
        store.reserve(METADATA, 60, 100, 0, 10, storage_budget=100)
        results.put("accepted")
    except CapacityError:
        results.put("rejected")


def test_concurrent_storage_budget_cannot_overbook(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    processes = [context.Process(target=reserve_budget_in_process, args=(str(tmp_path), results)) for _ in range(4)]
    try:
        for process in processes:
            process.start()
        outcomes = [results.get(timeout=10) for _ in processes]
        for process in processes:
            process.join(10)
            assert process.exitcode == 0
        assert outcomes.count("accepted") == 1
        assert outcomes.count("rejected") == 3
    finally:
        for process in processes:
            if process.is_alive():
                process.kill()
                process.join()
        results.close()


def test_budget_counts_ready_hardlinks_once_and_releases_after_cleanup(tmp_path: Path) -> None:
    import hashlib

    store = Lifecycle(tmp_path, "https://example.invalid")
    metadata = {"filename": "a", "sha256": hashlib.sha256(b"abc").hexdigest()}
    record = store.reserve(metadata, 3, 100, 0, 10, storage_budget=6)
    file_id = record["id"]
    store.source(file_id).write_bytes(b"abc")
    store.verify(file_id)
    assert store.read(file_id)["state"] == "ready"
    # A source and its published hardlink consume one reservation, not two.
    store.reserve(metadata, 3, 100, 0, 10, storage_budget=6)
    with pytest.raises(CapacityError):
        store.reserve(metadata, 1, 100, 0, 10, storage_budget=6)
    store.request_delete(file_id)
    with pytest.raises(CapacityError):
        store.reserve(metadata, 1, 100, 0, 10, storage_budget=6)

    def terminate(target: str) -> None:
        store.source(target).unlink()

    store.drain_delete(file_id, terminate)
    assert store.reserve(metadata, 3, 100, 0, 10, storage_budget=6)


def test_free_space_reserve_accounts_for_unwritten_bytes(tmp_path: Path) -> None:
    store = FixedCapacity(tmp_path, "https://example.invalid")
    record = store.reserve(METADATA, 60, 100, 40, 10)
    with pytest.raises(CapacityError):
        store.reserve(METADATA, 1, 100, 40, 10)
    store.source(record["id"]).write_bytes(b"x" * 20)
    # Disk free fell by the 20 bytes already written: no double reservation.
    with patch.object(store, "available_bytes", return_value=80):
        assert store.reserve(METADATA, 0, 100, 40, 10)
        with pytest.raises(CapacityError):
            store.reserve(METADATA, 1, 100, 40, 10)
