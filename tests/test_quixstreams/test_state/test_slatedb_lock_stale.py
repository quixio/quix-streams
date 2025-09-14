import json
import os

from quixstreams.state.slatedb.partition import SlateDBStorePartition


def test_stale_lock_is_cleaned_and_db_opens(tmp_path):
    path = (tmp_path / "stale").as_posix()
    lock_path = path + ".lock"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Write a stale lock with a non-existent PID
    with open(lock_path, "w") as f:
        json.dump({"pid": 999999, "ts": 0}, f)

    p = SlateDBStorePartition(path=path)
    try:
        # Lock should be re-acquired by this process
        with open(lock_path, "r") as f:
            data = json.load(f)
        assert data.get("pid") == os.getpid()
    finally:
        p.close()
        # Lock should be released on close
        assert not os.path.exists(lock_path)