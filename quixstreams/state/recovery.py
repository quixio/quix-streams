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
        changelog_name: str,
        partition_num: int,
        store_partition: StorePartition,
    ):
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
                f"{self.changelog_name}[{self.partition_num}] - the changelog offset "
                f"{self.offset} in state was larger than its actual highwater "
                f"{self._changelog_highwater}, possibly due to previous Kafka or "
                f"network issues. State may be inaccurate for any affected keys. "
                f"The offset will now be set to {self._changelog_highwater}."
            )
        self.store_partition.set_changelog_offset(
            changelog_message=OffsetUpdate(self._changelog_highwater - 1)
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
        """
        Add a RecoveryPartition to the recovery manager for each StorePartition assigned
        during a rebalance.

        The `RecoveryPartition` basically matches a changelog topic partition with its
        corresponding `StorePartition`.

        :param topic_name: source topic name
        :param partition_num: partition number of source topic
        :param store_partitions: mapping of store_names to `StorePartition`s (for the
            given partition_num)
        """
        self._recovery_manager.assign_partitions(
            topic_name=topic_name,
            partition_num=partition_num,
            recovery_partitions=[
                RecoveryPartition(
                    changelog_name=self._topic_manager.changelog_topics[topic_name][
                        store_name
                    ].name,
                    partition_num=partition_num,
                    store_partition=store_partition,
                )
                for store_name, store_partition in store_partitions.items()
            ],
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
        """
        Perform a recovery only if the recovery manager has active RecoveryPartitions
        """
        # only performs recovery if changelog partitions are assigned
        if self._recovery_manager.recovering:
            self._recovery_manager.do_recovery()


class RecoveryManager:
    """
    Manages all consumer-related aspects of recovery, including:
        - assigning/revoking, pausing/resuming topic partitions (especially changelogs)
        - consuming changelog messages until state is updated fully.

    Also tracks/manages `RecoveryPartitions`, which are assigned/tracked when
    recovery from a given changelog partition is actually required.

    Recovery is attempted from the `Application` after a new partition assignment.
    """

    _poll_attempts = 2

    def __init__(self, consumer: Consumer):
        self._consumer = consumer
        self._recovery_partitions: Dict[int, Dict[str, RecoveryPartition]] = {}
        self._polls_remaining: int = self._poll_attempts

    @property
    def recovering(self) -> bool:
        return bool(self._recovery_partitions)

    def assign_partitions(
        self,
        topic_name: str,
        partition_num: int,
        recovery_partitions: List[RecoveryPartition],
    ):
        """
        Assigns `RecoveryPartitions` ONLY IF they require recovery.

        Also pauses any active consumer partitions as needed.
        """
        previously_recovering = self.recovering
        for rp in recovery_partitions:
            c_name, p_num = rp.changelog_name, rp.partition_num
            rp.set_watermarks(
                *self._consumer.get_watermark_offsets(
                    ConfluentPartition(c_name, p_num), timeout=10
                )
            )
            if rp.needs_recovery:
                logger.info(f"Recovery required for {c_name}[{p_num}]")
                self._recovery_partitions.setdefault(p_num, {})[c_name] = rp
                self._consumer.incremental_assign(
                    [ConfluentPartition(c_name, p_num, rp.offset)]
                )
            elif rp.needs_offset_update:
                # nothing to recover, but offset appears off...likely that offset >
                # highwater due to At Least Once processing issues.
                rp.update_offset()

        self._polls_remaining = self._poll_attempts
        # figure out if any pausing is required
        if self.recovering:
            if previously_recovering:
                # was already recovering, so pause source topic only
                self._consumer.pause([ConfluentPartition(topic_name, partition_num)])
            else:
                # pause ALL partitions while we wait for Application to start recovery
                # (all newly assigned partitions are available on `.assignment`).
                self._consumer.pause(self._consumer.assignment())

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
        Begin a full recovery process and resume normal `Application` processing after.
        """
        self._consumer.resume(
            [
                ConfluentPartition(rp.changelog_name, rp.partition_num)
                for rp in dict_values(self._recovery_partitions)
            ]
        )
        self._recovery_loop()
        self._polls_remaining = self._poll_attempts
        self._consumer.resume(self._consumer.assignment())

    def _revoke_recovery_partitions(
        self,
        recovery_partitions: List[RecoveryPartition],
        partition_num: int,
    ):
        """
        For revoking a specific set of RecoveryPartitions.
        Also cleans up any remnant empty dictionary references if given a partition num.

        :param recovery_partitions: a list of `RecoveryPartition`
        :param partition_num: will attempt cleanup of recovery partition
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

    def _recovery_loop(self):
        """
        Perform the recovery loop, which continues updating state with changelog
        messages until recovery is "complete" (i.e. no assigned `RecoveryPartition`s).

        A RecoveryPartition is unassigned immediately once fully updated.
        """
        while self.recovering:
            if (msg := self._consumer.poll(5)) is None:
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
