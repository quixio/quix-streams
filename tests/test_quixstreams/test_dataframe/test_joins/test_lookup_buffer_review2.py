"""
Red-first reproduction tests for findings 10 and 11 of the external review of
PR 1110.

10. `join_lookup(..., buffer=...)` returned a clone while the unbuffered path
    mutated in place, and `Stream._add` attaches the new node to its parent
    before returning it. A caller who discarded the return therefore kept
    building on the un-enriched parent while `compose()` composed the buffer
    node anyway: the main branch lost the enrichment entirely, and every record
    was still copied into the store and the changelog behind a leaf nothing
    reads.
11. The sweep repaired a queue entry the marker no longer claims by deleting
    it and writing no replacement, so a prefix whose marker survived above the
    cutoff dropped out of the deadline queue. `due()` and `earliest()` scan
    that namespace only, so neither the record sweep nor the deadline tick
    could reach the prefix again.
"""

from contextlib import contextmanager
from datetime import timedelta
from typing import Any, Union

import pytest

from quixstreams.dataframe.joins.lookups.buffer_bookkeeping import BufferBookkeeping
from quixstreams.dataframe.joins.lookups.buffer_node import BufferTransformFunction
from quixstreams.dataframe.joins.lookups.buffer_state import (
    QUEUE_PREFIX,
    PendingIndex,
    encode_prefix,
)
from quixstreams.dataframe.joins.lookups.buffer_sweep import BufferSweeper
from quixstreams.dataframe.utils import ensure_milliseconds
from quixstreams.state.metadata import SEPARATOR
from quixstreams.state.rocksdb.timestamped import TimestampedStore
from quixstreams.state.serialization import int_to_bytes
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    FakeLookup,
    make_buffer,
    make_fields,
)


class TestDiscardedReturnValueLosesNoEnrichment:
    """Finding 10."""

    @staticmethod
    def _discarded_buffered_join(topic, create_sdf):
        """
        Call `join_lookup(..., buffer=...)` the way the docstring's example
        does not: without keeping the return value.

        :return: the dataframe, and the stream node the call was made on.
        """
        sdf = create_sdf(topic)
        lookup = FakeLookup()
        lookup.configs["D"] = {"threshold": 7, "region": "eu"}
        tail = sdf.stream

        sdf.join_lookup(lookup, make_fields(), buffer=make_buffer())

        return sdf, tail

    def test_the_buffer_node_is_installed_on_the_calling_instance(
        self, topic_manager_topic_factory: Any, create_sdf: Any
    ) -> None:
        topic = topic_manager_topic_factory()
        sdf, tail = self._discarded_buffered_join(topic, create_sdf)

        assert isinstance(sdf.stream.func, BufferTransformFunction), (
            f"`join_lookup(..., buffer=...)` left the dataframe pointing at "
            f"{type(sdf.stream.func).__name__}, so everything added next is a "
            f"sibling of the buffer node rather than downstream of it."
        )
        assert tail.children == [sdf.stream]

    def test_continuing_the_chain_adds_no_second_branch(
        self, topic_manager_topic_factory: Any, create_sdf: Any
    ) -> None:
        topic = topic_manager_topic_factory()
        sdf, tail = self._discarded_buffered_join(topic, create_sdf)

        sdf["passed"] = True

        assert len(tail.children) == 1, (
            f"The node `join_lookup()` was called on has "
            f"{len(tail.children)} children: the buffer branch is composed "
            f"alongside the main one, withholding records in the store behind "
            f"a leaf executor nothing reads."
        )

    def test_a_record_is_enriched_once_on_the_main_branch(
        self,
        topic_manager_topic_factory: Any,
        create_sdf: Any,
        assign_partition: Any,
        publish: Any,
    ) -> None:
        topic = topic_manager_topic_factory()
        sdf, _ = self._discarded_buffered_join(topic, create_sdf)
        sdf["passed"] = True
        assign_partition(sdf)

        results = publish(sdf, topic, {}, "D", 0)

        assert len(results) == 1, (
            f"One record produced {len(results)} outputs: {results}. The "
            f"enrichment and the rest of the pipeline are on separate branches."
        )
        value = results[0][0]
        assert value["threshold"] == 7
        assert value["region"] == "eu"
        assert value["passed"] is True


class TestStaleQueueEntryIsRepairedNotDropped:
    """Finding 11."""

    PREFIX = b"device-1"
    KEY = "device-1"
    MARKER_MS = 500
    STALE_MS = 100
    CUTOFF = 200

    @pytest.fixture
    def store_type(self):
        # Overrides the default (`RocksDBStore`) from
        # `tests/test_quixstreams/test_state/fixtures.py` for this class only.
        return TimestampedStore

    @pytest.fixture
    def transaction(self, store: TimestampedStore):
        @contextmanager
        def _transaction(
            grace_ms: Union[int, timedelta] = timedelta(days=7),
            keep_duplicates: bool = True,
        ):
            store._grace_ms = ensure_milliseconds(grace_ms)
            store._keep_duplicates = keep_duplicates
            store.assign_partition(0)
            with store.start_partition_transaction(0) as tx:
                yield tx

        return _transaction

    @staticmethod
    def _queue_key(receive_ms: int, encoded: str) -> bytes:
        # `PendingIndex._queue_key`'s layout, written here directly because no
        # public call can leave the two namespaces out of step.
        return int_to_bytes(receive_ms) + SEPARATOR + encoded.encode()

    @classmethod
    def _sweeper(cls) -> BufferSweeper:
        return BufferSweeper(emit_on_timeout=True, bookkeeping=BufferBookkeeping())

    def test_a_repaired_prefix_stays_visible_to_due_and_earliest(
        self, transaction: Any
    ) -> None:
        """
        The prefix's marker sits above the cutoff and its only queue entry
        below it - the partially applied checkpoint `requeue()` exists for.
        Each block ends in a real flush, so every read crosses RocksDB rather
        than the transaction's own update cache.
        """
        encoded = encode_prefix(self.PREFIX)

        with transaction() as tx:
            index = PendingIndex(tx)
            index.ensure(self.PREFIX, self.KEY, receive_ms=self.MARKER_MS)
            index.flush()
            tx.delete(self._queue_key(self.MARKER_MS, encoded), prefix=QUEUE_PREFIX)
            tx.set(
                self._queue_key(self.STALE_MS, encoded),
                [encoded, self.STALE_MS],
                prefix=QUEUE_PREFIX,
            )

        with transaction() as tx:
            sweeping = PendingIndex(tx)
            assert sweeping.due(self.CUTOFF, limit=8) == [[encoded, self.STALE_MS]], (
                "test setup assumption failed: the sweep must find the stale "
                "entry due"
            )
            result = self._sweeper().sweep(tx, sweeping, 0, self.CUTOFF, skip=None)
            sweeping.flush()

        assert result.swept == 0

        with transaction() as tx:
            fresh = PendingIndex(tx)
            assert fresh.earliest() == self.MARKER_MS, (
                f"After the repair the deadline queue holds "
                f"{fresh.earliest()!r} for a prefix whose marker still claims "
                f"{self.MARKER_MS}. Nothing but a new record for that message "
                f"key can make the sweep or the tick look at it again."
            )
            assert fresh.due(self.MARKER_MS, limit=8) == [[encoded, self.MARKER_MS]]
            assert fresh.entry(encoded) == [self.MARKER_MS, "s"]

    def test_a_repair_does_not_resurrect_a_dropped_marker(
        self, transaction: Any
    ) -> None:
        """The other half of the repair: no marker, so nothing to re-queue."""
        encoded = encode_prefix(self.PREFIX)

        with transaction() as tx:
            index = PendingIndex(tx)
            index.ensure(self.PREFIX, self.KEY, receive_ms=self.MARKER_MS)
            index.flush()
            index.drop(self.PREFIX)
            index.flush()
            tx.set(
                self._queue_key(self.STALE_MS, encoded),
                [encoded, self.STALE_MS],
                prefix=QUEUE_PREFIX,
            )

        with transaction() as tx:
            sweeping = PendingIndex(tx)
            self._sweeper().sweep(tx, sweeping, 0, self.CUTOFF, skip=None)
            sweeping.flush()

        with transaction() as tx:
            fresh = PendingIndex(tx)
            assert fresh.earliest() is None
            assert fresh.entry(encoded) is None
