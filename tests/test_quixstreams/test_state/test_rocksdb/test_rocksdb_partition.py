import re
import time
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from rocksdict import Rdict

from quixstreams.state.rocksdb import (
    RocksDBOptions,
    RocksDBStorePartition,
)
from quixstreams.state.rocksdb.exceptions import RocksDBCorruptedError
from quixstreams.state.rocksdb.metadata import TTL_INDEX_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import encode_index_key
from quixstreams.state.serialization import append_integer


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
            store_partition_factory(options=RocksDBOptions(on_corrupted_recreate=False))

    def test_db_corrupted_recreated_by_default(self, store_partition_factory, tmp_path):
        # Initialize and corrupt the database by messing with the MANIFEST
        Rdict(path=tmp_path.as_posix())
        next(tmp_path.glob("MANIFEST*")).write_bytes(b"")

        # Default `on_corrupted_recreate=True` should recreate the DB
        store_partition_factory()

    def test_db_corrupted_manifest_file(self, store_partition_factory, tmp_path):
        Rdict(path=tmp_path.as_posix())  # initialize db
        next(tmp_path.glob("MANIFEST*")).write_bytes(b"")  # write random bytes

        store_partition_factory(options=RocksDBOptions(on_corrupted_recreate=True))

    def test_db_corrupted_sst_file(self, store_partition_factory, tmp_path):
        # Initialize the db the way quixstreams does, so the CFs carry the same
        # comparator the store partition will open them with. A bare
        # `Rdict(path=...)` defaults to `raw_mode=False` and is not a store any
        # quixstreams deployment can produce; opening it would fail on the
        # comparator before the corruption is ever reached.
        rdict = Rdict(path=tmp_path.as_posix(), options=RocksDBOptions().to_options())
        rdict[b"key"] = b"value"  # write something
        rdict.flush()  # flush creates .sst file
        rdict.close()  # required to release the lock
        next(tmp_path.glob("*.sst")).unlink()  # delete the .sst file

        store_partition_factory(options=RocksDBOptions(on_corrupted_recreate=True))

    def test_configured_table_options_survive_reopen(
        self, store_partition_factory, tmp_path
    ):
        """
        Reopening an existing store must apply the configured block cache and
        bloom filter to the ALREADY-EXISTING column families, not just to the
        ones created during this open.

        rocksdict takes per-CF table options from its ``column_families``
        mapping; when the argument is omitted, existing CFs are opened with
        rocksdict's defaults (8 MiB cache, no filter policy) and every
        ``RocksDBOptions`` tuning is silently discarded on every restart.

        Asserted against RocksDB's own ``LOG``, which prints the effective
        per-CF table options at open. RocksDB rotates ``LOG`` to
        ``LOG.old.<ts>`` on each open, so after the reopen ``LOG`` describes
        the reopen alone.
        """
        cache_size = 96 * 1024 * 1024
        options = RocksDBOptions(
            block_cache_size=cache_size, bloom_filter_bits_per_key=10
        )

        partition = store_partition_factory("db", options=options)
        cf_names = partition.list_column_families()
        with partition.begin() as tx:
            tx.set("key", "value", prefix=b"__key__")
        partition.close()

        # Every CF now pre-exists, so this open exercises the reopen path only.
        partition = store_partition_factory("db", options=options)
        partition.close()

        # errors="replace": the LOG embeds the absolute store path, which on a
        # non-ASCII temp dir is undecodable under the Windows locale codepage.
        log = (tmp_path / "db" / "LOG").read_text(encoding="utf-8", errors="replace")
        # Require whitespace around the colon: that is the block-cache line in
        # the block_based_table_factory section. An unrelated "capacity: 128"
        # (no leading space) appears elsewhere in RocksDB's log.
        capacities = [int(c) for c in re.findall(r"capacity\s+:\s+(\d+)", log)]
        filter_policies = set(re.findall(r"filter_policy:\s*(\S+)", log))
        # Anchored: an unanchored `block_cache:` also matches `no_block_cache: 0`
        # and `prepopulate_block_cache: 0`, which happen to be 0 today — enabling
        # either would fail this assertion for the wrong reason.
        cache_ids = set(re.findall(r"^\s*block_cache:\s*(\w+)\s*$", log, re.M))

        # One block-cache line per CF, all at the configured size.
        assert len(capacities) >= len(cf_names)
        assert set(capacities) == {cache_size}
        assert filter_policies == {"bloomfilter"}
        # All CFs must share ONE cache instance, not one each of the same size:
        # the difference between a 96 MiB ceiling and len(cf_names) x 96 MiB.
        # `to_options()` builds a single `Cache` and every CF is handed that same
        # options object, so a refactor to per-CF options would still satisfy the
        # assertions above while multiplying the real memory ceiling.
        assert len(cache_ids - {"0"}) == 1

    def test_block_cache_is_shared_across_partitions(
        self, store_partition_factory, tmp_path
    ):
        """
        ``block_cache_size`` is an aggregate ceiling, not a per-partition budget.

        One cache per partition would make the setting a multiplier: a
        32-partition, 3-store application would reserve 96x the configured size,
        which is a multi-gigabyte ceiling charged lazily — so it surfaces as an
        OOM hours after an upgrade rather than at startup.

        Asserted on the ``block_cache`` pointer RocksDB prints per column family:
        two partitions sharing one cache report the SAME address.
        """
        cache_size = 96 * 1024 * 1024
        options = RocksDBOptions(
            block_cache_size=cache_size, bloom_filter_bits_per_key=10
        )

        pointers = set()
        for name in ("p0", "p1"):
            partition = store_partition_factory(name, options=options)
            partition.close()
            log = (tmp_path / name / "LOG").read_text(
                encoding="utf-8", errors="replace"
            )
            assert set(re.findall(r"capacity\s+:\s+(\d+)", log)) == {str(cache_size)}
            pointers |= set(re.findall(r"^\s*block_cache:\s*(\w+)\s*$", log, re.M))

        assert len(pointers) == 1, (
            f"each partition built its own cache ({pointers}), so "
            f"block_cache_size is a per-partition multiplier"
        )

    def test_listing_failure_on_existing_store_is_retried_not_degraded(
        self, store_partition_factory, tmp_path
    ):
        """
        A store that exists must never be opened without its column families.

        Opening without them lets rocksdict derive the CFs from the persisted
        OPTIONS file and apply its OWN defaults -- an 8 MiB block cache and no
        bloom filters on every existing CF, for the life of the process. That is
        the original bug, and it is invisible once the open succeeds, so a listing
        failure must be propagated for the existing retry budget to handle rather
        than papered over with a degraded open.
        """
        cache_size = 96 * 1024 * 1024
        options = RocksDBOptions(
            block_cache_size=cache_size,
            bloom_filter_bits_per_key=10,
            open_max_retries=2,
            open_retry_backoff=0.0,
        )
        partition = store_partition_factory("db", options=options)
        with partition.begin() as tx:
            tx.set("key", "value", prefix=b"__key__")
        partition.close()

        real_list_cf = Rdict.list_cf
        calls = []

        def _fails_once(path, *args, **kwargs):
            calls.append(path)
            if len(calls) == 1:
                raise Exception("IO error: transient")
            return real_list_cf(path, *args, **kwargs)

        with patch.object(Rdict, "list_cf", staticmethod(_fails_once)):
            reopened = store_partition_factory("db", options=options)
        try:
            with reopened.begin() as tx:
                assert tx.get("key", prefix=b"__key__") == "value"
        finally:
            reopened.close()

        # The point of the retry: the store came up with its configured options,
        # not rocksdict's 8 MiB default.
        log = (tmp_path / "db" / "LOG").read_text(encoding="utf-8", errors="replace")
        assert set(re.findall(r"capacity\s+:\s+(\d+)", log)) == {str(cache_size)}
        assert set(re.findall(r"filter_policy:\s*(\S+)", log)) == {"bloomfilter"}

    def test_reopen_applies_new_options_to_persisted_column_families(
        self, store_partition_factory, tmp_path
    ):
        """
        The upgrade path: a store whose CFs were persisted under one
        ``block_cache_size`` must adopt a different one on reopen.

        This is the case where ``_open`` hands each column family options that
        disagree with the store's own persisted OPTIONS file, and it is what an
        operator actually does when they change the setting and restart.
        """
        small, large = 32 * 1024 * 1024, 96 * 1024 * 1024

        partition = store_partition_factory(
            "db", options=RocksDBOptions(block_cache_size=small)
        )
        with partition.begin() as tx:
            tx.set("key", "value", prefix=b"__key__")
        partition.close()

        reopened = store_partition_factory(
            "db", options=RocksDBOptions(block_cache_size=large)
        )
        try:
            with reopened.begin() as tx:
                assert tx.get("key", prefix=b"__key__") == "value"
        finally:
            reopened.close()

        log = (tmp_path / "db" / "LOG").read_text(encoding="utf-8", errors="replace")
        assert set(re.findall(r"capacity\s+:\s+(\d+)", log)) == {str(large)}

    def test_open_recovers_when_persisted_options_are_unreadable(
        self, store_partition_factory, tmp_path
    ):
        """
        A store whose persisted OPTIONS file is unreadable must still open.

        Without the column-family argument rocksdict derives the CFs from that
        file, and on failure silently sees only the default CF -- so the open
        dies with ``Column families not opened``, which matches no recovery path.
        Supplying the list explicitly survives it, so a listing that fails on the
        first attempt and succeeds on the re-read must recover rather than crash.
        """
        partition = store_partition_factory("db")
        with partition.begin() as tx:
            tx.set("key", "value", prefix=b"__key__")
        partition.close()

        for options_file in tmp_path.glob("db/OPTIONS-*"):
            options_file.write_text("garbage")

        real_list_cf = Rdict.list_cf
        calls = []

        def _fails_once(path, *args, **kwargs):
            calls.append(path)
            if len(calls) == 1:
                raise Exception("IO error: transient")
            return real_list_cf(path, *args, **kwargs)

        # Retries enabled: a listing failure on an existing store propagates so
        # this budget can absorb it, rather than being papered over with a
        # degraded open.
        with patch.object(Rdict, "list_cf", staticmethod(_fails_once)):
            reopened = store_partition_factory(
                "db",
                options=RocksDBOptions(open_max_retries=2, open_retry_backoff=0.0),
            )
        try:
            # >= 2: the listing was retried. Not an equality — a TTL-enabled
            # partition probes its column families again after opening, so the
            # total call count is not a measure of the open path.
            assert len(calls) >= 2, "the failed listing should have been retried"
            with reopened.begin() as tx:
                assert tx.get("key", prefix=b"__key__") == "value"
        finally:
            reopened.close()

    def test_open_refuses_to_fall_back_to_a_degraded_open(
        self, store_partition_factory, tmp_path
    ):
        """
        A persistently unlistable store must fail, never open degraded.

        An argument-less open of a store that HAS column families succeeds while
        applying rocksdict's 8 MiB / no-bloom-filter defaults to every one of
        them — the original bug, invisible once the open returns. So when the
        listing cannot be obtained, the open must fail rather than fall back to
        it. A slip that reinstated that fallback would leave a store quietly
        serving traffic with its configured cache and filters dropped, and the
        only way to notice is that this test stops raising.
        """
        partition = store_partition_factory("db")
        with partition.begin() as tx:
            tx.set("key", "value", prefix=b"__key__")
        partition.close()

        calls = []

        def _never_lists(path, *args, **kwargs):
            calls.append(path)
            raise Exception("IO error: persistent")

        with patch.object(Rdict, "list_cf", staticmethod(_never_lists)):
            with pytest.raises(Exception, match="IO error: persistent"):
                store_partition_factory(
                    "db",
                    options=RocksDBOptions(open_max_retries=2, open_retry_backoff=0.0),
                )
        # Every attempt tried to list, and none resorted to opening without the
        # column families.
        assert len(calls) == 2

    def test_open_retries_a_stale_column_family_list(
        self, store_partition_factory, tmp_path
    ):
        """
        A CF list that is merely STALE must be re-read, not fatal.

        ``Rdict.list_cf`` takes no LOCK and column families are created lazily at
        runtime, so a process sharing the store can add one between the listing
        and the open. The open then fails with ``Column families not opened:
        <cf>`` — a message matching neither the corruption nor the io-error retry
        path, so without a re-read it is an un-retried fatal open, exactly the
        failure the None-instead-of-empty-map fix was meant to remove but reached
        through a different door.
        """
        partition = store_partition_factory("db")
        with partition.begin() as tx:
            tx.set("key", "value", prefix=b"__key__")
        partition.close()

        real_list_cf = Rdict.list_cf
        calls = []

        def _stale_once(path, *args, **kwargs):
            full = real_list_cf(path, *args, **kwargs)
            calls.append(full)
            # First call omits a CF that really exists, mimicking the race; the
            # re-read returns the truth.
            return ["default"] if len(calls) == 1 else full

        with patch.object(Rdict, "list_cf", staticmethod(_stale_once)):
            reopened = store_partition_factory("db")
        try:
            # >= 2 for the same reason as above: post-open TTL probes also
            # call list_cf, so only the lower bound describes the open.
            assert len(calls) >= 2, "the stale list should have been re-read"
            with reopened.begin() as tx:
                assert tx.get("key", prefix=b"__key__") == "value"
        finally:
            reopened.close()

    def test_get_or_create_column_family(self, store_partition: RocksDBStorePartition):
        assert store_partition.get_or_create_column_family("cf")

    def test_get_or_create_column_family_cached(
        self, store_partition: RocksDBStorePartition
    ):
        cf1 = store_partition.get_or_create_column_family("cf")
        cf2 = store_partition.get_or_create_column_family("cf")
        assert cf1 is cf2

    def test_list_column_families(self, store_partition: RocksDBStorePartition):
        store_partition.get_or_create_column_family("cf1")
        store_partition.get_or_create_column_family("cf2")
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
        # Order can vary depending on creation sequence, so compare as sets.
        # "default" is always present in RocksDB. "__metadata__" is created
        # by RocksDBStorePartition. The ``__ttl_index__`` CF is **lazy** in
        # v3: it only exists after a partition flips into TTL mode on first
        # detection of a ``state.set(..., ttl=...)`` write. A fresh, never-
        # used partition stays byte-identical to v3.23.6.
        assert set(cfs) == {"default", "__metadata__"}

    def test_ensure_metadata_cf(self, store_partition: RocksDBStorePartition):
        assert store_partition.get_or_create_column_family("__metadata__")

    def test_ttl_sweep_preserves_rewritten_same_stamp_index(
        self, store_partition_factory
    ):
        prefix = b"__key__"
        ttl = timedelta(milliseconds=100)

        with store_partition_factory() as partition:
            tx1 = partition.begin()
            tx1.set(key="k", value="v1", prefix=prefix, timestamp=1000, ttl=ttl)
            user_key = tx1._serialize_key(key="k", prefix=prefix)
            index_key = encode_index_key(1100, user_key)
            tx1.prepare(processed_offsets={"topic": 1})
            tx1.flush(changelog_offset=1)

            tx2 = partition.begin()
            # Advance high-water while re-writing k with the same expired stamp.
            # The sweep reads the old committed index key while the fresh index
            # key is staged in the same WriteBatch.
            tx2.set(
                key="advance",
                value="tick",
                prefix=prefix,
                timestamp=2000,
                ttl=ttl,
            )
            tx2.set(key="k", value="v2", prefix=prefix, timestamp=1000, ttl=ttl)
            tx2.prepare(processed_offsets={"topic": 2})
            tx2.flush(changelog_offset=2)

            index_cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
            main_cf = partition.get_or_create_column_family("default")
            assert index_cf.get(index_key, default=None) == b""
            assert main_cf.get(user_key, default=None) is not None

            tx3 = partition.begin()
            tx3.set(
                key="advance2",
                value="tick",
                prefix=prefix,
                timestamp=2001,
                ttl=ttl,
            )
            tx3.prepare(processed_offsets={"topic": 3})
            tx3.flush(changelog_offset=3)

            assert index_cf.get(index_key, default=None) is None
            assert main_cf.get(user_key, default=None) is None

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
