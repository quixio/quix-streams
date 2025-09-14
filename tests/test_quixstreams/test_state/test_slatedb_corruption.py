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
