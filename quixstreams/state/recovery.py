import logging
from typing import Optional, Dict, List

from confluent_kafka import TopicPartition as ConfluentPartition

from quixstreams.kafka import Consumer
from quixstreams.models import ConfluentKafkaMessageProto
from quixstreams.rowproducer import RowProducer
from quixstreams.state.types import StorePartition
from quixstreams.models.topics import TopicManager, Topic
from quixstreams.models.types import MessageHeadersMapping
from quixstreams.utils.dicts import dict_values

logger = logging.getLogger(__name__)


__all__ = ("ChangelogManager", "ChangelogWriter", "RecoveryManager")


class OffsetUpdate(ConfluentKafkaMessageProto):
    def __init__(self, offset):
        self._offset = offset

    def offset(self):
        return self._offset


class RecoveryPartition:
    """
    A changelog partition mapped to a respective `StorePartition` with helper methods
    to determine its current recovery status.

    Since `StorePartition`s do recovery directly, it also handles recovery transactions.
    """

    def __init__(
        self,
        topic_name: str,
        changelog_name: str,
        partition_num: int,
        store_partition: StorePartition,
    ):
        self.topic_name = topic_name
        self.changelog_name = changelog_name
        self.partition_num = partition_num
        self.store_partition = store_partition
        self._changelog_lowwater: Optional[int] = None
        self._changelog_highwater: Optional[int] = None

    @property
    def offset(self) -> int:
        return self.store_partition.get_changelog_offset() or 0

    @property
    def needs_recovery(self):
        has_consumable_offsets = self._changelog_lowwater != self._changelog_highwater
        state_is_behind = (self._changelog_highwater - self.offset) > 0
        return has_consumable_offsets and state_is_behind

    @property
    def needs_offset_update(self):
        return self._changelog_highwater and (self.offset != self._changelog_highwater)

    def update_offset(self):
        logger.info(
            f"changelog partition {self.changelog_name}[{self.partition_num}] "
            f"requires an offset update"
        )
        if self.offset > self._changelog_highwater:
            logger.warning(
                f"The changelog offset in state for "
                f"{self.changelog_name}[{self.partition_num}] was larger than actual "
                f"offset available on that topic-partition, possibly due to some "
                f"Kafka or network issues. State may be inaccurate for any affected "
                f"keys. The offset will now be fixed."
            )
        self.store_partition.set_changelog_offset(
            changelog_message=OffsetUpdate(self.offset)
        )

    def recover(self, changelog_message: ConfluentKafkaMessageProto):
        self.store_partition.recover(changelog_message=changelog_message)

    def set_watermarks(self, lowwater: int, highwater: int):
        self._changelog_lowwater = lowwater
        self._changelog_highwater = highwater


class ChangelogWriter:
    """
    Created and handed to `PartitionTransaction`s to produce state changes to
    a `StorePartition`s respective kafka changelog partition.
    """

    def __init__(self, changelog: Topic, partition_num: int, producer: RowProducer):
        self._changelog = changelog
        self._partition_num = partition_num
        self._producer = producer

    def produce(
        self,
        key: bytes,
        value: Optional[bytes] = None,
        headers: Optional[MessageHeadersMapping] = None,
    ):
        self._producer.produce(
            key=key,
            value=value,
            topic=self._changelog.name,
            partition=self._partition_num,
            headers=headers,
        )


class ChangelogManager:
    """
    A simple interface for managing all things related to changelog topics (including
    recovery) and is primarily used by the StateStoreManager and Application.

    Pretty much facilitates the other recovery-related components.
    """

    def __init__(
        self,
        topic_manager: TopicManager,
        consumer: Consumer,
        producer: RowProducer,
    ):
        self._topic_manager = topic_manager
        self._producer = producer
        self._recovery_manager = RecoveryManager(consumer)
        self._changelog_writers: Dict[int, Dict[str, ChangelogWriter]] = {}

    def register_changelog(self, topic_name: str, store_name: str, consumer_group: str):
        """
        Register a changelog Topic with the TopicManager.
        """
        self._topic_manager.changelog_topic(
            topic_name=topic_name,
            store_name=store_name,
            consumer_group=consumer_group,
        )

    def assign_partition(
        self,
        topic_name: str,
        partition_num: int,
        store_partitions: Dict[str, StorePartition],
    ):
        changelog_stores = {
            self._topic_manager.changelog_topics[topic_name][
                store_name
            ].name: store_partition
            for store_name, store_partition in store_partitions.items()
        }
        self._recovery_manager.assign_partitions(
            topic_name=topic_name,
            partition_num=partition_num,
            store_partitions=changelog_stores,
        )

    def revoke_partition(self, partition_num: int):
        self._recovery_manager.revoke_partitions(partition_num=partition_num)
        self._changelog_writers.pop(partition_num, None)

    def get_writer(
        self, topic_name: str, store_name: str, partition_num: int
    ) -> ChangelogWriter:
        changelog = self._topic_manager.changelog_topics[topic_name][store_name]
        return self._changelog_writers.setdefault(partition_num, {}).setdefault(
            changelog.name,
            ChangelogWriter(
                changelog=changelog,
                partition_num=partition_num,
                producer=self._producer,
            ),
        )

    def do_recovery(self):
        self._recovery_manager.do_recovery()


class RecoveryManager:
    """
    Manages all consumer-related aspects of recovery, including:
        - assigning/revoking, pausing/resuming topic partitions (especially changelogs)
        - consuming changelog messages until state is updated fully.

    Also tracks/manages `RecoveryPartitions`, which are assigned/tracked when
    recovery from a given changelog partition is actually required.

    Recovery is always triggered from any rebalance via partition assign, from which
    `Application` will call `RecoveryManager.do_recovery()` during its process loop.
    """

    _poll_attempts = 2

    def __init__(self, consumer: Consumer):
        self._consumer = consumer
        self._pending_assigns: List[RecoveryPartition] = []
        self._recovery_partitions: Dict[int, Dict[str, RecoveryPartition]] = {}
        self._polls_remaining: int = self._poll_attempts

    @property
    def in_recovery_mode(self) -> bool:
        return bool(self._recovery_partitions)

    def assign_partitions(
        self,
        topic_name: str,
        partition_num: int,
        store_partitions: Dict[str, StorePartition],
    ):
        """
        Creates `RecoveryPartitions` for all provided `StorePartitions` (which should
        be `StorePartitions` for a given partition number from every `Store`).

        Actual assignments are decided by `_handle_pending_assigns`.

        If not `in_recovery_mode`, we wait on assigning them until the Application
        kicks off recovery.
        """
        for changelog_name, store_partition in store_partitions.items():
            self._pending_assigns.append(
                RecoveryPartition(
                    topic_name=topic_name,
                    changelog_name=changelog_name,
                    partition_num=partition_num,
                    store_partition=store_partition,
                )
            )
        self._consumer.pause([ConfluentPartition(topic_name, partition_num)])
        if self.in_recovery_mode:
            # we can immediately assign changelogs since we're already recovering
            self._handle_pending_assigns()

    def revoke_partitions(self, partition_num: int):
        """
        Revoke EVERY stores' changelog partitions (if assigned) for the given partition

        :param partition_num: the partition number
        """
        if changelogs := self._recovery_partitions.get(partition_num, {}):
            self._revoke_recovery_partitions(
                [changelogs.pop(changelog) for changelog in list(changelogs.keys())],
                partition_num,
            )

    def do_recovery(self):
        """
        Perform a full recovery process.
        """
        self._handle_pending_assigns()
        self._recovery_loop()
        self._finalize_recovery()

    def _handle_pending_assigns(self):
        """ """
        assigns = []
        while self._pending_assigns:
            p = self._pending_assigns.pop()
            p.set_watermarks(
                *self._consumer.get_watermark_offsets(
                    ConfluentPartition(p.changelog_name, p.partition_num), timeout=10
                )
            )
            if p.needs_recovery:
                logger.info(
                    f"Recovery required for changelog partition "
                    f"{p.changelog_name}[{p.partition_num}]"
                )
                assigns.append(p)
                self._recovery_partitions.setdefault(p.partition_num, {})[
                    p.changelog_name
                ] = p
            elif p.needs_offset_update:
                # >0 changelog offset, but none are actually consumable
                # this is unlikely to happen with At Least Once, but just in case...
                p.update_offset()
        if assigns:
            self._consumer.incremental_assign(
                [
                    ConfluentPartition(p.changelog_name, p.partition_num, p.offset)
                    for p in assigns
                ]
            )
        self._polls_remaining = self._poll_attempts

    def _revoke_recovery_partitions(
        self,
        recovery_partitions: List[RecoveryPartition],
        partition_num: int,
    ):
        """
        For revoking a specific set of RecoveryPartitions.
        Also cleans up any remnant empty dictionary references if given a partition num.

        :param recovery_partitions: a list of `RecoveryPartition`
        :param partition_num: if given, will attempt cleanup of recovery partition
        dictionary for provided partition number.
        """
        self._consumer.incremental_unassign(
            [
                ConfluentPartition(p.changelog_name, p.partition_num)
                for p in recovery_partitions
            ]
        )
        if not self._recovery_partitions[partition_num]:
            del self._recovery_partitions[partition_num]

    def _update_partition_offsets(self):
        """
        update the offsets for assigned `RecoveryPartition`s, and then revoke them.

        This is a safety measure for when the highwater and changelog consumable
        offsets don't align after recovery (AKA stored offset < changelog highwater).
        Most likely only happens with Exactly Once Semantics?
        """
        for p in (recovery_partitions := dict_values(self._recovery_partitions)):
            if p.needs_offset_update:
                p.update_offset()
        self._consumer.incremental_unassign(
            [
                ConfluentPartition(p.changelog_name, p.partition_num)
                for p in recovery_partitions
            ]
        )
        self._recovery_partitions = {}

    def _finalize_recovery(self):
        """
        Cleanup before resuming normal processing in the Application.
        """
        if self._recovery_partitions:
            self._update_partition_offsets()
        self._consumer.resume(self._consumer.assignment())
        self._polls_remaining = self._poll_attempts
        logger.info("Recovery: finalized; resuming normal processing...")

    def _recovery_loop(self):
        """
        Perform the recovery loop, which continues updating state with changelog
        messages until recovery is "complete" (i.e. no assigned `RecoveryPartition`s).

        A RecoveryPartition is unassigned immediately once fully updated.
        """
        while self.in_recovery_mode:
            if (msg := self._consumer.poll(5)) is None:
                # offsets could be inaccurate for various reasons (which may cause
                # them not to unassign as expected), or polling could fail for various
                # reasons. So, we confirm that we have indeed consumed
                # all possible messages by polling a few times as a safety net.
                self._polls_remaining -= 1
                if not self._polls_remaining:
                    logger.debug(
                        f"Recovery: poll attempts exhausted; finalizing recovery..."
                    )
                    return
                continue

            changelog_name = msg.topic()
            partition_num = msg.partition()

            partition = self._recovery_partitions[partition_num][changelog_name]
            partition.recover(changelog_message=msg)

            if not partition.needs_recovery:
                logger.info(f"Recovery for {changelog_name}[{partition_num}] complete!")
                self._revoke_recovery_partitions(
                    [self._recovery_partitions[partition_num].pop(changelog_name)],
                    partition_num,
                )
