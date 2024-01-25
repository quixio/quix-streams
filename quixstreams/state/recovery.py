import logging
from typing import Optional, Dict, List

from confluent_kafka import TopicPartition as ConfluentPartition

from quixstreams.kafka import Consumer
from quixstreams.models import ConfluentKafkaMessageProto
from quixstreams.rowproducer import RowProducer
from quixstreams.state.types import StorePartition
from quixstreams.topic_manager import TopicManagerType, BytesTopic
from quixstreams.types import Headers
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
        topic: str,
        changelog: str,
        partition: int,
        store_partition: StorePartition,
    ):
        self.topic = topic
        self.changelog = changelog
        self.partition = partition
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
        return ConfluentPartition(self.topic, self.partition)

    @property
    def changelog_partition(self) -> ConfluentPartition:
        return ConfluentPartition(self.changelog, self.partition)

    @property
    def changelog_assignable_partition(self):
        return ConfluentPartition(self.changelog, self.partition, self.offset)

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
            f"{self.changelog}: p{self.partition} was larger than the actual offset "
            f"available on that topic-partition, likely as a result of some "
            f"sort of error (mostly likely Kafka or network related). "
            f"It is possible that the state of any affected message keys may end "
            f"up inaccurate due to potential double processing. This is an "
            f"unfortunate possibility with 'at least once' processing guarantees. "
            f"The offset will now be corrected."
        )

    def update_offset(self):
        logger.info(
            f"changelog partition {self.changelog}[{self.partition}] "
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

    def __init__(self, topic: BytesTopic, partition_num: int, producer: RowProducer):
        self._topic = topic
        self._partition_num = partition_num
        self._producer = producer

    def produce(
        self,
        key: bytes,
        value: Optional[bytes] = None,
        headers: Optional[Headers] = None,
    ):
        msg = self._topic.serialize(key=key, value=value)
        self._producer.produce(
            key=msg.key,
            value=msg.value,
            topic=self._topic.name,
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
        topic_manager: TopicManagerType,
        consumer: Consumer,
        producer: RowProducer,
    ):
        self._topic_manager = topic_manager
        self._producer = producer
        self._recovery_manager = RecoveryManager(consumer)

    def add_changelog(self, source_topic_name: str, suffix: str, consumer_group: str):
        self._topic_manager.changelog_topic(
            source_topic_name=source_topic_name,
            suffix=suffix,
            consumer_group=consumer_group,
        )

    def assign_partition(
        self,
        source_topic_name: str,
        partition: int,
        store_partitions: Dict[str, StorePartition],
    ):
        self._recovery_manager.assign_partitions(
            source_topic_name=source_topic_name,
            partition=partition,
            store_partitions={
                self._topic_manager.changelog_topics[source_topic_name][
                    suffix
                ].name: store_partition
                for suffix, store_partition in store_partitions.items()
            },
        )

    def revoke_partition(self, source_topic_name, partition):
        self._recovery_manager.revoke_partitions(
            topic=source_topic_name, partition=partition
        )

    def get_writer(
        self, source_topic_name: str, suffix: str, partition_num: int
    ) -> ChangelogWriter:
        return ChangelogWriter(
            topic=self._topic_manager.changelog_topics[source_topic_name][suffix],
            partition_num=partition_num,
            producer=self._producer,
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

    Recovery is always triggered from any source topic rebalance, which then the
    `Application` switches its processing loop over to the `RecoveryManager`. The
    `RecoveryManager` throws the `RecoveryComplete` exception when finished,
     which is gracefully caught by the `Application`, resuming normal processing.
    """

    def __init__(self, consumer: Consumer):
        self._consumer = consumer
        self._pending_assigns: List[RecoveryPartition] = []
        self._pending_revokes: List[RecoveryPartition] = []
        self._partitions: Dict[int, Dict[str, RecoveryPartition]] = {}
        self._recovery_method = self._recover
        self._poll_attempts: int = 2
        self._polls_remaining: int = self._poll_attempts

    class RecoveryComplete(Exception):
        ...

    @property
    def in_recovery_mode(self) -> bool:
        return bool(self._partitions)

    def do_recovery(self):
        self._recovery_method()

    def assign_partitions(
        self,
        source_topic_name: str,
        partition: int,
        store_partitions: Dict[str, StorePartition],
    ):
        p = None
        for changelog, store_partition in store_partitions.items():
            p = RecoveryPartition(
                topic=source_topic_name,
                changelog=changelog,
                partition=partition,
                store_partition=store_partition,
            )
            self._pending_assigns.append(p)
        # Assign manually to immediately pause it (would assign unpaused automatically)
        # TODO: consider doing all topic (not changelog) assign(s) during the
        #  Application on_assign call (rather than one at a time here)
        topic_p = [p.topic_partition]
        self._consumer.incremental_assign(topic_p)
        self._consumer.pause(topic_p)
        self._recovery_method = self._rebalance

    def _partition_cleanup(self, partition: int):
        if not self._partitions[partition]:
            del self._partitions[partition]

    def revoke_partitions(self, topic: str, partition: int):
        # TODO: consider doing all topic (not changelog) unassign(s) during the
        #  Application on_revoke call (rather than one at a time here)
        self._consumer.incremental_unassign([ConfluentPartition(topic, partition)])
        if changelogs := self._partitions.get(partition, {}):
            for changelog in list(changelogs.keys()):
                self._pending_revokes.append(changelogs.pop(changelog))
            self._consumer.pause([p.changelog_partition for p in self._pending_revokes])
            self._partition_cleanup(partition)
            self._recovery_method = self._rebalance

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
                    f"{p.changelog}[{p.partition}]"
                )
                assigns.append(p)
                self._partitions.setdefault(p.partition, {})[p.changelog] = p
            elif p.needs_offset_update:
                # >0 changelog offset, but none are actually consumable
                # this is unlikely to happen with At Least Once, but just in case...
                p.update_offset()
        if assigns:
            self._consumer.incremental_assign(
                [p.changelog_assignable_partition for p in assigns]
            )

    def _handle_pending_revokes(self):
        self._consumer.incremental_unassign(
            [p.changelog_partition for p in self._pending_revokes]
        )
        self._pending_revokes = []

    def _rebalance(self):
        """ """
        logger.debug("Recovery: rebalancing (if required)...")
        if self._pending_revokes:
            self._handle_pending_revokes()
        if self._pending_assigns:
            self._handle_pending_assigns()
        self._recovery_method = self._recover
        self._polls_remaining = self._poll_attempts

    def _update_partition_offsets(self):
        """
        update the offsets for assigned partitions, and then revoke them.

        This is a safety measure for when, while recovering, the highwater and
        changelog consumable offsets don't align: in this case, from failed
        transactions with Exactly Once processing (stored offset < changelog highwater).
        """
        for p in (p_out := dict_values(self._partitions)):
            if p.needs_offset_update:
                p.update_offset()
        self._pending_revokes.extend(p_out)
        self._partitions = {}
        self._handle_pending_revokes()

    def _finalize_recovery(self):
        if self._partitions:
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

        changelog = msg.topic()
        p_num = msg.partition()

        partition = self._partitions[p_num][changelog]
        partition.recover(changelog_message=msg)

        if not partition.needs_recovery:
            logger.info(f"Recovery for {msg.topic()}[{msg.partition()}] complete!")
            self._pending_revokes.append(self._partitions[p_num].pop(changelog))
            self._partition_cleanup(p_num)
            self._handle_pending_revokes()
