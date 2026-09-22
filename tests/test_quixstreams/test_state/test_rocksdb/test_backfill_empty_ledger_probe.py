"""
#10a (review batch 3): the first-flip backfill census tests ``key not in
stamped_ledger`` for every default-CF key — a per-key point-get on the ledger CF.
On the common first-flip path the ledger is empty, so that per-key check is a pure
(bloom) negative for every key. The fix probes the ledger ONCE up front and drops
the per-key membership term when the ledger is empty; a resume over a non-empty
ledger still excludes ledgered keys.
"""

from datetime import timedelta
from unittest.mock import MagicMock, PropertyMock

import pytest

from quixstreams.state.metadata import TTL_BACKFILL_STAMPED_CF_NAME
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.ttl_codec import decode_ttl_value

BASE_TS = 1_000_000_000_000


class _ContainsCountingProxy:
    """Wraps a rocksdict CF and counts ``__contains__`` (per-key membership)
    calls, while forwarding everything else (``keys``, ``get``, ...)."""

    def __init__(self, wrapped):
        self._wrapped = wrapped
        self.contains_calls = 0

    def __contains__(self, key):
        self.contains_calls += 1
        return key in self._wrapped

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


def _producer_mock():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="cl")
    type(producer).partition = PropertyMock(return_value=0)
    return producer


@pytest.fixture()
def partition(tmp_path):
    part = RocksDBStorePartition(
        (tmp_path / "db").as_posix(),
        options=RocksDBOptions(
            open_max_retries=0,
            open_retry_backoff=3.0,
            legacy_records_ttl=timedelta(days=7),
        ),
        changelog_producer=_producer_mock(),
    )
    yield part
    part.close()


def _install_ledger_spy(partition):
    """Wrap the stamped-ledger CF so the census's per-key ``in stamped_ledger``
    checks are counted. Returns the proxy (``.contains_calls``)."""
    real = partition.get_or_create_column_family(TTL_BACKFILL_STAMPED_CF_NAME)
    proxy = _ContainsCountingProxy(real)
    original = partition.get_or_create_column_family

    def _wrapped(cf_name):
        if cf_name == TTL_BACKFILL_STAMPED_CF_NAME:
            return proxy
        return original(cf_name)

    partition.get_or_create_column_family = _wrapped
    return proxy


class TestEmptyLedgerProbeSkip:
    def test_first_flip_empty_ledger_zero_per_key_point_gets(self, partition):
        # Seed a populated legacy store (no ttl → legacy).
        with partition.begin() as tx:
            for i in range(6):
                tx.set(key=f"k{i}", value=f"v{i}", prefix=b"pfx")

        proxy = _install_ledger_spy(partition)

        # A ttl= write flips the populated store and triggers the backfill.
        with partition.begin() as tx:
            tx.set(
                key="trigger",
                value="t",
                prefix=b"pfx",
                timestamp=BASE_TS,
                ttl=timedelta(days=1),
            )

        assert partition.uses_ttl_stamps is True
        assert proxy.contains_calls == 0, (
            "first-flip backfill with an EMPTY ledger must perform zero per-key "
            f"ledger point-gets, but did {proxy.contains_calls}"
        )

    def test_resume_nonempty_ledger_excludes_ledgered_keys(self, partition):
        # Populate the default CF with legacy records, then pre-mark k0/k1 as
        # already-stamped in the ledger (as an interrupted run would). A direct
        # backfill must exclude them and re-stamp only the remaining keys.
        with partition.begin() as tx:
            for i in range(5):
                tx.set(key=f"k{i}", value=f"v{i}", prefix=b"pfx")

        ser = partition.begin()
        ledgered = {ser._serialize_key(f"k{i}", prefix=b"pfx") for i in range(2)}
        from rocksdict import WriteBatch

        batch = WriteBatch(raw_mode=True)
        handle = partition.get_column_family_handle(TTL_BACKFILL_STAMPED_CF_NAME)
        for ks in ledgered:
            batch.put(ks, b"", handle)
        partition._write(batch)

        expires = BASE_TS + 7 * 86_400_000
        restamped = partition.backfill_legacy_records(
            expires_at_ms=expires,
            changelog_producer=partition._changelog_producer,
            processed_offsets=None,
            staged_default_keys=set(),
            chunk_size=100,
        )
        assert restamped == 3, "ledgered keys k0/k1 must be excluded from the resume"

        # The two ledgered keys remain raw (un-stamped); the other three are stamped.
        default_cf = partition.get_or_create_column_family("default")
        for i in range(2):
            raw = default_cf.get(ser._serialize_key(f"k{i}", prefix=b"pfx"))
            assert raw == f'"v{i}"'.encode(), "ledgered key must be left raw"
        for i in range(2, 5):
            raw = default_cf.get(ser._serialize_key(f"k{i}", prefix=b"pfx"))
            stamp, payload = decode_ttl_value(raw)
            assert stamp == expires and payload == f'"v{i}"'.encode()
