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


class RecoveryPartition:
    """
    A changelog partition mapped to a respective StorePartition with helper methods
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

    class OffsetUpdate(ConfluentKafkaMessageProto):
        def __init__(self, offset):
            self._offset = offset

        def offset(self):
            return self._offset

    @property
    def offset(self) -> int:
        return self.store_partition.get_changelog_offset() or 0

    @property
    def topic_partition(self) -> ConfluentPartition:
        return ConfluentPartition(self.topic_name, self.partition_num)

    @property
    def changelog_partition(self) -> ConfluentPartition:
        return ConfluentPartition(self.changelog_name, self.partition_num)

    @property
    def changelog_assignable_partition(self):
        return ConfluentPartition(self.changelog_name, self.partition_num, self.offset)

    @property
    def needs_recovery(self):
        has_consumable_offsets = self._changelog_lowwater != self._changelog_highwater
        state_is_behind = (self._changelog_highwater - self.offset) > 0
        return has_consumable_offsets and state_is_behind

    @property
    def needs_offset_update(self):
        return self._changelog_highwater and (self.offset != self._changelog_highwater)

    def _warn_bad_offset(self):
        logger.warning(
            f"The recorded changelog offset in state for "
            f"{self.changelog_name}: p{self.partition_num} was larger than the actual "
            f"offset available on that topic-partition, likely as a result of some "
            f"sort of error (mostly likely Kafka or network related). "
            f"It is possible that the state of any affected message keys may end "
            f"up inaccurate due to potential double processing. This is an "
            f"unfortunate possibility with 'at least once' processing guarantees. "
            f"The offset will now be corrected."
        )

    def update_offset(self):
        logger.info(
            f"changelog partition {self.changelog_name}[{self.partition_num}] "
            f"requires an offset update"
        )
        if self.offset > self._changelog_highwater:
            self._warn_bad_offset()
        self.store_partition.set_changelog_offset(
            changelog_message=self.OffsetUpdate(self.offset)
        )

    def recover(self, changelog_message: ConfluentKafkaMessageProto):
        self.store_partition.recover(changelog_message=changelog_message)

    def set_watermarks(self, lowwater: int, highwater: int):
        self._changelog_lowwater = lowwater
        self._changelog_highwater = highwater


class ChangelogWriter:
    """
    Typically created and handed to `PartitionTransaction`s to produce state changes to
    a changelog topic.
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
        msg = self._changelog.serialize(key=key, value=value)
        self._producer.produce(
            key=msg.key,
            value=msg.value,
            topic=self._changelog.name,
            partition=self._partition_num,
            headers=headers,
        )


class ChangelogManager:
    """
    A simple interface for managing all things related to changelog topics and is
    primarily used by the StateStoreManager.

    Facilitates creation of changelog topics and assigning their partitions during
    rebalances, and handles recovery process loop calls from `Application`.
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

    def add_changelog(self, topic_name: str, store_name: str, consumer_group: str):
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
    Manages all aspects of recovery, including managing all topic partition assignments
    (both source topic and changelogs), generating `RecoveryPartition`s when recovery
    is required for a given changelog partition, and stopping/revoking said partitions
    when it determines recovery is complete.

    Important to note that a RecoveryPartition is only generated (and assigned to the
    consumer) when recovery is necessary, else the partition in question is ignored.

    It will revoke these partitions throughout recovery, which will NOT kick off
    a rebalance of the consumer since this is done outside the consumer group protocol.

    Recovery is always triggered from any rebalance via topic assign, which then the
    `Application` switches its processing loop over to the `RecoveryManager`. The
    `RecoveryManager` throws the `RecoveryComplete` exception when finished,
     which is gracefully caught by the `Application`, resuming normal processing.
    """

    def __init__(self, consumer: Consumer):
        self._consumer = consumer
        self._pending_assigns: List[RecoveryPartition] = []
        self._recovery_partitions: Dict[int, Dict[str, RecoveryPartition]] = {}
        self._recovery_process = self._recover
        self._poll_attempts: int = 2
        self._polls_remaining: int = self._poll_attempts

    class RecoveryComplete(Exception):
        ...

    @property
    def in_recovery_mode(self) -> bool:
        return bool(self._recovery_partitions)

    def do_recovery(self):
        self._recovery_process()

    def assign_partitions(
        self,
        topic_name: str,
        partition_num: int,
        store_partitions: Dict[str, StorePartition],
    ):
        p = None
        for changelog_name, store_partition in store_partitions.items():
            p = RecoveryPartition(
                topic_name=topic_name,
                changelog_name=changelog_name,
                partition_num=partition_num,
                store_partition=store_partition,
            )
            self._pending_assigns.append(p)
        topic_p = [p.topic_partition]
        self._consumer.pause(topic_p)
        self._recovery_process = self._handle_pending_assigns

    def revoke_partitions(self, partition_num: int):
        if changelogs := self._recovery_partitions.get(partition_num, {}):
            self._revoke_recovery_partitions(
                [changelogs.pop(changelog) for changelog in list(changelogs.keys())],
                partition_num,
            )

    def _handle_pending_assigns(self):
        assigns = []
        while self._pending_assigns:
            p = self._pending_assigns.pop()
            p.set_watermarks(
                *self._consumer.get_watermark_offsets(p.changelog_partition, timeout=10)
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
                [p.changelog_assignable_partition for p in assigns]
            )
        self._recovery_process = self._recover
        self._polls_remaining = self._poll_attempts

    def _revoke_recovery_partitions(
        self,
        recovery_partitions: List[RecoveryPartition],
        partition_num: Optional[int] = None,
    ):
        self._consumer.incremental_unassign(
            [partition.changelog_partition for partition in recovery_partitions]
        )
        if partition_num is not None and not self._recovery_partitions[partition_num]:
            del self._recovery_partitions[partition_num]

    def _update_partition_offsets(self):
        """
        update the offsets for assigned partitions, and then revoke them.

        This is a safety measure for when, while recovering, the highwater and
        changelog consumable offsets don't align: in this case, from failed
        transactions with Exactly Once processing (stored offset < changelog highwater).
        """
        for p in (recovery_partitions := dict_values(self._recovery_partitions)):
            if p.needs_offset_update:
                p.update_offset()
        self._revoke_recovery_partitions(recovery_partitions)
        self._recovery_partitions = {}

    def _finalize_recovery(self):
        if self._recovery_partitions:
            self._update_partition_offsets()
        self._consumer.resume(self._consumer.assignment())
        self._polls_remaining = self._poll_attempts
        logger.info("Recovery: finalized; resuming normal processing...")
        raise self.RecoveryComplete

    def _recover(self):
        if not self.in_recovery_mode:
            return self._finalize_recovery()

        if (msg := self._consumer.poll(5)) is None:
            self._polls_remaining -= 1
            if not self._polls_remaining:
                logger.debug(
                    f"Recovery: all poll attempts exhausted; finalizing recovery..."
                )
                return self._finalize_recovery()
            return

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
