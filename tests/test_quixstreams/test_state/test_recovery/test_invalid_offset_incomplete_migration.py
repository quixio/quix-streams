"""
Bug #5 (review batch 4): invalid-offset guard bypassed by an incomplete TTL
migration.

``RecoveryManager.assign_partition`` used
``if rp.needs_recovery_check: ... elif rp.has_invalid_offset: raise``. The
finding-2 hoist in ``RecoveryPartition.needs_recovery_check`` force-returns True
whenever ``has_incomplete_ttl_migration()`` is True, which is no longer mutually
exclusive with an invalid offset (before the hoist, "behind" and "offset >=
highwater" could never both hold). So a store mid-TTL-migration whose changelog was
deleted+recreated (``highwater <= stored offset`` with ``highwater > 0``) took the
recovery branch instead of raising ``InvalidStoreChangelogOffset`` -- it would
silently rebuild from a changelog that cannot reconstruct its state and then cement
the inconsistency with a done-marker.

The fix checks ``has_invalid_offset`` FIRST. This test is Docker-free: it builds
the ``RecoveryPartition`` directly and patches ``_generate_recovery_partitions``,
exercising only the ``assign_partition`` if/elif ordering where the bug lives
(``test_recovery_manager.test_assign_partition_invalid_offset`` covers the same
raise via the full broker-backed path, but that path needs a Kafka container).

RED before the fix: ``needs_recovery_check`` was checked first, so
``assign_partition`` added a recovery check and did NOT raise.
"""

import uuid
from unittest.mock import MagicMock

import pytest
from confluent_kafka import TopicPartition

from quixstreams.internal_consumer import InternalConsumer
from quixstreams.state.base import StorePartition
from quixstreams.state.exceptions import InvalidStoreChangelogOffset
from quixstreams.state.recovery import RecoveryManager, RecoveryPartition


class TestInvalidOffsetWithIncompleteMigration:
    def test_invalid_offset_raises_even_with_incomplete_migration(self):
        """offset (22) >= highwater (20) with an incomplete TTL migration must
        still raise InvalidStoreChangelogOffset, not be masked into recovery."""
        changelog_name = f"changelog__{uuid.uuid4()}"
        partition_num = 0

        # Store offset AHEAD of highwater (deleted+recreated changelog) AND an
        # incomplete TTL migration (which force-Trues needs_recovery_check).
        store_partition = MagicMock(spec_set=StorePartition)
        store_partition.get_changelog_offset.return_value = 22
        store_partition.has_incomplete_ttl_migration.return_value = True

        rp = RecoveryPartition(
            changelog_name=changelog_name,
            partition_num=partition_num,
            store_partition=store_partition,
            committed_offsets={},
            lowwater=0,
            highwater=20,
        )
        # The exact masking precondition: both are True at once (only possible
        # because the finding-2 hoist forces needs_recovery_check True).
        assert rp.has_invalid_offset is True
        assert rp.needs_recovery_check is True

        consumer = MagicMock(spec_set=InternalConsumer)
        consumer.assignment.return_value = [
            TopicPartition(topic=changelog_name, partition=partition_num)
        ]
        recovery_manager = RecoveryManager(
            consumer=consumer,
            topic_manager=MagicMock(),
        )
        # Bypass the broker-backed partition generation; feed our hand-built rp
        # straight into the assign_partition if/elif under test.
        recovery_manager._generate_recovery_partitions = MagicMock(return_value=[rp])

        with pytest.raises(InvalidStoreChangelogOffset):
            recovery_manager.assign_partition(
                topic="source-topic",
                partition=partition_num,
                store_partitions={"default": store_partition},
                committed_offsets={},
            )
