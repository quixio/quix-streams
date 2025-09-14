import pytest

from quixstreams.state import SlateDBOptions
from quixstreams.state.slatedb.exceptions import SlateDBLockError
from quixstreams.state.slatedb.partition import SlateDBStorePartition


def test_slatedb_lock_exclusive_and_retry(tmp_path):
    path = tmp_path / "lock-db"
    opts = SlateDBOptions(open_max_retries=2, open_retry_backoff=0.01)

    p1 = SlateDBStorePartition(path=str(path), options=opts)
    try:
        with pytest.raises(SlateDBLockError):
            SlateDBStorePartition(path=str(path), options=opts)
    finally:
        p1.close()
        # After closing p1, we should be able to open again
        p2 = SlateDBStorePartition(path=str(path), options=opts)
        p2.close()
