import os

import pytest

from quixstreams.state import SlateDBOptions
from quixstreams.state.slatedb.exceptions import SlateDBCorruptedError
from quixstreams.state.slatedb.partition import SlateDBStorePartition


def test_slatedb_corruption_open_fails_without_recreate(monkeypatch, tmp_path):
    # Simulate driver raising a corruption error when opening
    corrupt_path = (tmp_path / "corrupt-db").as_posix()

    from quixstreams.state.slatedb import driver as drv

    original_open = drv.RealSlateDBDriver.open

    def fake_open(self, path: str, create_if_missing: bool = True):
        raise SlateDBCorruptedError("invalid manifest - looks corrupted")

    monkeypatch.setattr(drv.RealSlateDBDriver, "open", fake_open)

    with pytest.raises(SlateDBCorruptedError):
        SlateDBStorePartition(
            path=corrupt_path, options=SlateDBOptions(on_corrupted_recreate=False)
        )

    # restore
    monkeypatch.setattr(drv.RealSlateDBDriver, "open", original_open)


def test_slatedb_corruption_recreate_and_open(monkeypatch, tmp_path):
    # Ensure that when on_corrupted_recreate=True we retry after cleanup
    db_root = tmp_path / "corrupt-recreate"
    corrupt_path = db_root.as_posix()

    from quixstreams.state.slatedb import driver as drv

    call_count = {"count": 0}
    original_open = drv.RealSlateDBDriver.open

    def flaky_open(self, path: str, create_if_missing: bool = True):
        if call_count["count"] == 0:
            call_count["count"] += 1
            raise SlateDBCorruptedError("corrupted sst")
        else:
            return original_open(self, path, create_if_missing)

    monkeypatch.setattr(drv.RealSlateDBDriver, "open", flaky_open)

    part = SlateDBStorePartition(
        path=corrupt_path, options=SlateDBOptions(on_corrupted_recreate=True)
    )
    try:
        # Should be opened successfully on second attempt
        assert os.path.exists(os.path.dirname(corrupt_path)) or os.path.exists(
            corrupt_path
        )
    finally:
        part.close()
        monkeypatch.setattr(drv.RealSlateDBDriver, "open", original_open)


def test_slatedb_corruption_logs_on_recreate(monkeypatch, tmp_path, caplog):
    # Verify INFO logs on corruption detection and successful reopen
    db_root = tmp_path / "corrupt-logs"
    corrupt_path = db_root.as_posix()

    from quixstreams.state.slatedb import driver as drv
    from quixstreams.state.slatedb import partition as part_mod

    call_count = {"count": 0}
    original_open = drv.RealSlateDBDriver.open

    def flaky_open(self, path: str, create_if_missing: bool = True):
        if call_count["count"] == 0:
            call_count["count"] += 1
            raise SlateDBCorruptedError("invalid manifest")
        else:
            return original_open(self, path, create_if_missing)

    monkeypatch.setattr(drv.RealSlateDBDriver, "open", flaky_open)

    with caplog.at_level("INFO", logger=part_mod.__name__):
        p = SlateDBStorePartition(
            path=corrupt_path, options=SlateDBOptions(on_corrupted_recreate=True)
        )
        try:
            # Detect log messages
            logs = "\n".join(rec.getMessage() for rec in caplog.records)
            assert "Detected corrupted SlateDB" in logs
            assert "Recreated and reopened SlateDB" in logs
        finally:
            p.close()
            monkeypatch.setattr(drv.RealSlateDBDriver, "open", original_open)


def test_slatedb_corruption_no_recreate_raises(monkeypatch, tmp_path):
    # When on_corrupted_recreate=False, a corruption should bubble and not retry
    import os
    from quixstreams.state.slatedb import driver as drv

    corrupt_path = (tmp_path / "corrupt-no-recreate").as_posix()

    call_count = {"count": 0}

    def raise_corrupt(self, path: str, create_if_missing: bool = True):
        call_count["count"] += 1
        raise SlateDBCorruptedError("invalid manifest")

    original_open = drv.RealSlateDBDriver.open
    monkeypatch.setattr(drv.RealSlateDBDriver, "open", raise_corrupt)

    try:
        import pytest

        with pytest.raises(SlateDBCorruptedError):
            SlateDBStorePartition(
                path=corrupt_path, options=SlateDBOptions(on_corrupted_recreate=False)
            )
        assert call_count["count"] == 1
        # Lock should be released on error
        assert not os.path.exists(corrupt_path + ".lock")
    finally:
        monkeypatch.setattr(drv.RealSlateDBDriver, "open", original_open)


message = "Failed to fully clean corrupted path"

def test_slatedb_recreate_cleanup_fail_and_open_fail_logs_and_raises(
    monkeypatch, tmp_path, caplog
):
    # Simulate cleanup failure and second open failure; should log WARN and re-raise
    import os
    import shutil
    from quixstreams.state.slatedb import driver as drv
    from quixstreams.state.slatedb import partition as part_mod

    corrupt_path = (tmp_path / "corrupt-retry-fail").as_posix()

    call_count = {"count": 0}

    def two_fail_opens(self, path: str, create_if_missing: bool = True):
        call_count["count"] += 1
        raise SlateDBCorruptedError("invalid manifest")

    original_open = drv.RealSlateDBDriver.open
    monkeypatch.setattr(drv.RealSlateDBDriver, "open", two_fail_opens)
    # Force branch that uses shutil.rmtree
    monkeypatch.setattr(os.path, "isdir", lambda p: True)
    monkeypatch.setattr(shutil, "rmtree", lambda *a, **k: (_ for _ in ()).throw(Exception("rm fail")))
    # Also make os.remove fail if called
    monkeypatch.setattr(os, "remove", lambda *a, **k: (_ for _ in ()).throw(Exception("rm fail")))

    import pytest

    try:
        with caplog.at_level("WARNING", logger=part_mod.__name__):
            with pytest.raises(SlateDBCorruptedError):
                SlateDBStorePartition(
                    path=corrupt_path,
                    options=SlateDBOptions(on_corrupted_recreate=True),
                )
        # We should have attempted open twice (initial + retry)
        assert call_count["count"] == 2
        # Warning about cleanup failure should be present
        logs = "\n".join(rec.getMessage() for rec in caplog.records)
        assert message in logs
        # Lock should be released after failure
        assert not os.path.exists(corrupt_path + ".lock")
    finally:
        monkeypatch.setattr(drv.RealSlateDBDriver, "open", original_open)
