import time
from pathlib import Path
from unittest.mock import patch

import pytest
from rocksdict import Rdict

from quixstreams.state.rocksdb import (
    RocksDBOptions,
    RocksDBStorePartition,
)
from quixstreams.state.rocksdb.exceptions import RocksDBCorruptedError
from quixstreams.state.rocksdb.windowed.serialization import append_integer


class TestRocksDBStorePartition:
    def test_open_db_locked_retries(self, store_partition_factory, executor):
        db1 = store_partition_factory("db")

        def _close_db():
            time.sleep(3)
            db1.close()

        executor.submit(_close_db)

        store_partition_factory(
            "db", options=RocksDBOptions(open_max_retries=10, open_retry_backoff=1)
        )

    def test_open_io_error_retries(self, store_partition_factory, executor):
        err = Exception("io error")
        patcher = patch.object(Rdict, "__init__", side_effect=err)
        patcher.start()

        def _stop_raising_on_db_open():
            time.sleep(3)
            patcher.stop()

        executor.submit(_stop_raising_on_db_open)

        store_partition_factory(
            "db", options=RocksDBOptions(open_max_retries=10, open_retry_backoff=1)
        )

    def test_open_db_locked_no_retries_fails(self, store_partition_factory, executor):
        _ = store_partition_factory("db")

        with pytest.raises(Exception):
            store_partition_factory("db", options=RocksDBOptions(open_max_retries=0))

    def test_open_db_locked_retries_exhausted_fails(
        self, store_partition_factory, executor
    ):
        _ = store_partition_factory("db")

        with pytest.raises(Exception):
            store_partition_factory(
                "db", options=RocksDBOptions(open_max_retries=3, open_retry_backoff=1)
            )

    def test_open_arbitrary_exception_fails(self, store_partition_factory):
        err = Exception("some exception")
        with patch.object(Rdict, "__init__", side_effect=err):
            with pytest.raises(Exception) as raised:
                store_partition_factory()

        assert str(raised.value) == "some exception"

    def test_db_corrupted_fails_with_no_changelog(
        self, store_partition_factory, tmp_path
    ):
        # Initialize and corrupt the database by messing with the MANIFEST
        path = tmp_path.as_posix()
        Rdict(path=path)
        next(tmp_path.glob("MANIFEST*")).write_bytes(b"")

        with pytest.raises(
            RocksDBCorruptedError,
            match=f'State store at "{path}" is corrupted and cannot be recovered '
            f"from the changelog topic",
        ):
            store_partition_factory(changelog_producer=None)

    def test_db_corrupted_fails_with_on_corrupted_recreate_false(
        self, store_partition_factory, tmp_path
    ):
        # Initialize and corrupt the database by messing with the MANIFEST
        path = tmp_path.as_posix()
        Rdict(path=path)
        next(tmp_path.glob("MANIFEST*")).write_bytes(b"")

        with pytest.raises(
            RocksDBCorruptedError,
            match=f'State store at "{path}" is corrupted but may be recovered '
            f"from the changelog topic",
        ):
            store_partition_factory()

    def test_db_corrupted_manifest_file(self, store_partition_factory, tmp_path):
        Rdict(path=tmp_path.as_posix())  # initialize db
        next(tmp_path.glob("MANIFEST*")).write_bytes(b"")  # write random bytes

        store_partition_factory(options=RocksDBOptions(on_corrupted_recreate=True))

    def test_db_corrupted_sst_file(self, store_partition_factory, tmp_path):
        rdict = Rdict(path=tmp_path.as_posix())  # initialize db
        rdict[b"key"] = b"value"  # write something
        rdict.flush()  # flush creates .sst file
        rdict.close()  # required to release the lock
        next(tmp_path.glob("*.sst")).unlink()  # delete the .sst file

        store_partition_factory(options=RocksDBOptions(on_corrupted_recreate=True))

    def test_get_column_family(self, store_partition: RocksDBStorePartition):
        assert store_partition.get_column_family("cf")

    def test_get_column_family_cached(self, store_partition: RocksDBStorePartition):
        cf1 = store_partition.get_column_family("cf")
        cf2 = store_partition.get_column_family("cf")
        assert cf1 is cf2

    def test_list_column_families(self, store_partition: RocksDBStorePartition):
        store_partition.get_column_family("cf1")
        store_partition.get_column_family("cf2")
        cfs = store_partition.list_column_families()
        assert "cf1" in cfs
        assert "cf2" in cfs

    def test_destroy(self, store_partition_factory):
        with store_partition_factory() as storage:
            path = storage.path

        RocksDBStorePartition.destroy(path)

    def test_custom_options(self, store_partition_factory, tmp_path):
        """
        Pass custom "logs_dir" to Rdict and ensure it exists and has some files
        """

        logs_dir = Path(tmp_path / "logs")
        options = RocksDBOptions(db_log_dir=logs_dir.as_posix())
        with store_partition_factory(options=options):
            assert logs_dir.is_dir()
            assert len(list(logs_dir.rglob("*"))) == 1

    def test_list_column_families_defaults(
        self, store_partition: RocksDBStorePartition
    ):
        cfs = store_partition.list_column_families()
        assert cfs == [
            # "default" CF is always present in RocksDB
            "default",
            # "__metadata__" CF is created by the RocksDBStorePartition
            "__metadata__",
        ]

    def test_ensure_metadata_cf(self, store_partition: RocksDBStorePartition):
        assert store_partition.get_column_family("__metadata__")

    @pytest.mark.parametrize(
        ["backwards", "expected"],
        [
            (
                False,
                [
                    (append_integer(b"prefix", 1), b"value1"),
                    (append_integer(b"prefix", 2), b"value2"),
                    (append_integer(b"prefix", 10), b"value10"),
                ],
            ),
            (
                True,
                [
                    (append_integer(b"prefix", 10), b"value10"),
                    (append_integer(b"prefix", 2), b"value2"),
                    (append_integer(b"prefix", 1), b"value1"),
                ],
            ),
        ],
    )
    def test_iter_items_returns_ordered_items(
        self, store_partition: RocksDBStorePartition, cache, backwards, expected
    ):
        for key, value in expected:
            cache.set(key=key, value=value, prefix=b"prefix")

        key_too_low = b"prefi"
        key_too_high = append_integer(b"prefix", 11)
        cache.set(key=key_too_low, value=b"too-low", prefix=b"prefix")
        cache.set(key=key_too_high, value=b"too-high", prefix=b"prefix")
        store_partition.write(cache=cache, changelog_offset=None)

        assert (
            list(
                store_partition.iter_items(
                    lower_bound=b"prefix",
                    upper_bound=append_integer(b"prefix", 11),
                    backwards=backwards,
                )
            )
            == expected
        )

    def test_iter_items_exclusive_upper_bound(
        self, store_partition: RocksDBStorePartition, cache
    ):
        cache.set(key=b"prefix|1", value=b"value1", prefix=b"prefix")
        cache.set(key=b"prefix|2", value=b"value2", prefix=b"prefix")
        store_partition.write(cache=cache, changelog_offset=None)

        assert list(
            store_partition.iter_items(
                lower_bound=b"prefix",
                upper_bound=b"prefix|2",
            )
        ) == [(b"prefix|1", b"value1")]

    def test_iter_items_backwards_lower_bound(
        self, store_partition: RocksDBStorePartition, cache
    ):
        """
        Test that keys below the lower bound are filtered
        """
        prefix = b"2"
        lower_bound = b"3"
        upper_bound = b"4"

        cache.set(key=prefix + b"|" + b"test1", value=b"", prefix=prefix)
        cache.set(key=prefix + b"|" + b"test2", value=b"", prefix=prefix)
        store_partition.write(cache=cache, changelog_offset=None)

        assert (
            list(
                store_partition.iter_items(
                    lower_bound=lower_bound,
                    upper_bound=upper_bound,
                    backwards=True,
                )
            )
            == []
        )

    def test_iter_items_backwards_upper_bound(
        self, store_partition: RocksDBStorePartition, cache
    ):
        """
        Test that keys above the upper bound are filtered
        """
        prefix = b"4"
        lower_bound = b"3"
        upper_bound = b"4"

        cache.set(key=prefix + b"|" + b"test1", value=b"", prefix=prefix)
        cache.set(key=prefix + b"|" + b"test2", value=b"", prefix=prefix)
        store_partition.write(cache=cache, changelog_offset=None)

        assert (
            list(
                store_partition.iter_items(
                    lower_bound=lower_bound,
                    upper_bound=upper_bound,
                    backwards=True,
                )
            )
            == []
        )
