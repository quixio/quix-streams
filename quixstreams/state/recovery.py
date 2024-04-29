import logging
from typing import Optional, Dict, List

from confluent_kafka import TopicPartition as ConfluentPartition

from quixstreams.kafka import Consumer
from quixstreams.models import ConfluentKafkaMessageProto, Topic
from quixstreams.models.topics import TopicManager
from quixstreams.models.types import MessageHeadersMapping
from quixstreams.rowproducer import RowProducer
from quixstreams.state.types import StorePartition
from quixstreams.utils.dicts import dict_values

logger = logging.getLogger(__name__)

__all__ = (
    "ChangelogProducer",
    "ChangelogProducerFactory",
    "RecoveryManager",
    "RecoveryPartition",
)


class RecoveryPartition:
    """
    A changelog topic partition mapped to a respective `StorePartition` with helper
    methods to determine its current recovery status.

    Since `StorePartition`s do recovery directly, it also handles recovery transactions.
    """

    def __init__(
        self,
        changelog_name: str,
        partition_num: int,
        store_partition: StorePartition,
        committed_offset: int,
    ):
        self._changelog_name = changelog_name
        self._partition_num = partition_num
        self._store_partition = store_partition
        self._changelog_lowwater: Optional[int] = None
        self._changelog_highwater: Optional[int] = None
        self._committed_offset = committed_offset

    @property
    def changelog_name(self) -> str:
        return self._changelog_name

    @property
    def partition_num(self) -> int:
        return self._partition_num

    @property
    def changelog_highwater(self) -> Optional[int]:
        return self._changelog_highwater

    @property
    def changelog_lowwater(self) -> Optional[int]:
        return self._changelog_lowwater

    @property
    def offset(self) -> int:
        """
        Get the changelog offset from the underlying `StorePartition`.

        :return: changelog offset (int)
        """
        return self._store_partition.get_changelog_offset() or 0

    @property
    def needs_recovery(self):
        """
        Determine whether recovery is necessary for underlying `StorePartition`.
        """
        has_consumable_offsets = self._changelog_lowwater != self._changelog_highwater
        state_is_behind = self._changelog_highwater > self.offset
        return has_consumable_offsets and state_is_behind

    @property
    def needs_offset_update(self):
        """
        Determine if an offset update is required.

        Usually checked during assign if recovery was not required.
        """
        return self._changelog_highwater and (self._changelog_highwater < self.offset)

    def update_offset(self):
        """
        Update only the changelog offset of a StorePartition.
        """
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
        self._store_partition.set_changelog_offset(
            changelog_offset=self._changelog_highwater - 1
        )

    def recover_from_changelog_message(
        self, changelog_message: ConfluentKafkaMessageProto
    ):
        """
        Recover the StorePartition using a message read from its respective changelog.

        :param changelog_message: A confluent kafka message (everything as bytes)
        """
        self._store_partition.recover_from_changelog_message(
            changelog_message=changelog_message, committed_offset=self._committed_offset
        )

    def set_watermarks(self, lowwater: int, highwater: int):
        """
        Set the changelog watermarks as gathered from Consumer.get_watermark_offsets()

        :param lowwater: topic partition lowwater
        :param highwater: topic partition highwater
        """
        self._changelog_lowwater = lowwater
        self._changelog_highwater = highwater


class ChangelogProducerFactory:
    """
    Generates ChangelogProducers, which produce changelog messages to a StorePartition.
    """

    def __init__(
        self, changelog_name: str, source_topic_name: str, producer: RowProducer
    ):
        """
        :param changelog_name: changelog topic name
        :param producer: a RowProducer (not shared with `Application` instance)

        :return: a ChangelogWriter instance
        """
        self._changelog_name = changelog_name
        self._source_topic_name = source_topic_name
        self._producer = producer

    def get_partition_producer(self, partition_num) -> "ChangelogProducer":
        """
        Generate a ChangelogProducer for producing to a specific partition number
        (and thus StorePartition).

        :param partition_num: source topic partition number
        """
        return ChangelogProducer(
            changelog_name=self._changelog_name,
            source_topic_name=self._source_topic_name,
            partition=partition_num,
            producer=self._producer,
        )


class ChangelogProducer:
    """
    Generated for a `StorePartition` to produce state changes to its respective
    kafka changelog partition.
    """

    def __init__(
        self,
        changelog_name: str,
        source_topic_name: str,
        partition: int,
        producer: RowProducer,
    ):
        """
        :param changelog_name: A changelog topic name
        :param partition: source topic partition number
        :param producer: a RowProducer (not shared with `Application` instance)
        """
        self._changelog_name = changelog_name
        self._source_topic_name = source_topic_name
        self._partition = partition
        self._producer = producer

    @property
    def source_topic_name(self) -> str:
        return self._source_topic_name

    @property
    def changelog_name(self) -> str:
        return self._changelog_name

    @property
    def partition(self) -> int:
        return self._partition

    def produce(
        self,
        key: bytes,
        value: Optional[bytes] = None,
        headers: Optional[MessageHeadersMapping] = None,
    ):
        """
        Produce a message to a changelog topic partition.

        :param key: message key (same as state key, including prefixes)
        :param value: message value (same as state value)
        :param headers: message headers (includes column family info)
        """
        self._producer.produce(
            key=key,
            value=value,
            headers=headers,
            partition=self._partition,
            topic=self._changelog_name,
        )

    def flush(self, timeout: Optional[float] = None) -> int:
        return self._producer.flush(timeout=timeout)


class RecoveryManager:
    """
    Manages all consumer-related aspects of recovery, including:
        - assigning/revoking, pausing/resuming topic partitions (especially changelogs)
        - consuming changelog messages until state is updated fully.

    Also tracks/manages `RecoveryPartitions`, which are assigned/tracked only if
    recovery for that changelog partition is required.

    Recovery is attempted from the `Application` after any new partition assignment.
    """

    def __init__(self, consumer: Consumer, topic_manager: TopicManager):
        self._running = False
        self._consumer = consumer
        self._topic_manager = topic_manager
        self._recovery_partitions: Dict[int, Dict[str, RecoveryPartition]] = {}

    @property
    def partitions(self) -> Dict[int, Dict[str, RecoveryPartition]]:
        """
        Returns a mapping of assigned RecoveryPartitions in the following format:
        {<partition>: {<store_name>: <RecoveryPartition>}}
        """
        return self._recovery_partitions

    @property
    def has_assignments(self) -> bool:
        """
        Whether the Application has assigned RecoveryPartitions

        :return: has assignments, as bool
        """
        return bool(self._recovery_partitions)

    @property
    def recovering(self) -> bool:
        """
        Whether the Application is currently recovering

        :return: is recovering, as bool
        """
        return self.has_assignments and self._running

    def register_changelog(
        self, topic_name: str, store_name: str, consumer_group: str
    ) -> Topic:
        """
        Register a changelog Topic with the TopicManager.

        :param topic_name: source topic name
        :param store_name: name of the store
        :param consumer_group: name of the consumer group
        """
        return self._topic_manager.changelog_topic(
            topic_name=topic_name,
            store_name=store_name,
            consumer_group=consumer_group,
        )

    def do_recovery(self):
        """
        If there are any active RecoveryPartitions, do a recovery procedure.

        After, will resume normal `Application` processing.
        """
        logger.info("Beginning the recovery process...")
        self._running = True
        self._consumer.resume(
            [
                ConfluentPartition(rp.changelog_name, rp.partition_num)
                for rp in dict_values(self._recovery_partitions)
            ]
        )
        self._recovery_loop()
        if self._running:
            logger.info("Recovery process complete! Resuming normal processing...")
            self._running = False
            self._consumer.resume(self._consumer.assignment())
        else:
            logger.debug("Recovery interrupted; stopping.")

    def _generate_recovery_partitions(
        self,
        topic_name: str,
        partition_num: int,
        store_partitions: Dict[str, StorePartition],
        committed_offset: int,
    ) -> List[RecoveryPartition]:
        partitions = []
        for store_name, store_partition in store_partitions.items():
            changelog_topic = self._topic_manager.changelog_topics[topic_name][
                store_name
            ]
            recovery_partition = RecoveryPartition(
                changelog_name=changelog_topic.name,
                partition_num=partition_num,
                store_partition=store_partition,
                committed_offset=committed_offset,
            )

            lowwater, highwater = self._consumer.get_watermark_offsets(
                ConfluentPartition(
                    topic=recovery_partition.changelog_name,
                    partition=recovery_partition.partition_num,
                ),
                timeout=10,
            )
            recovery_partition.set_watermarks(lowwater=lowwater, highwater=highwater)

            partitions.append(recovery_partition)
        return partitions

    def assign_partition(
        self,
        topic: str,
        partition: int,
        committed_offset: int,
        store_partitions: Dict[str, StorePartition],
    ):
        """
        Assigns `StorePartition`s (as `RecoveryPartition`s) ONLY IF recovery required.

        Pauses active consumer partitions as needed.
        """
        recovery_partitions = self._generate_recovery_partitions(
            topic_name=topic,
            partition_num=partition,
            store_partitions=store_partitions,
            committed_offset=committed_offset,
        )
        for rp in recovery_partitions:
            changelog_name, partition = rp.changelog_name, rp.partition_num
            if rp.needs_recovery:
                logger.info(f"Recovery required for {changelog_name}[{partition}]")
                self._recovery_partitions.setdefault(partition, {})[changelog_name] = rp
                self._consumer.incremental_assign(
                    [ConfluentPartition(changelog_name, partition, rp.offset)]
                )
            elif rp.needs_offset_update:
                # nothing to recover, but offset is off...likely that offset >
                # highwater due to At Least Once processing behavior.
                rp.update_offset()

        # Figure out if we need to pause any topic partitions
        if self._recovery_partitions:
            if self._running:
                # Some partitions are already recovering,
                # pausing only the source topic partition
                self._consumer.pause(
                    [ConfluentPartition(topic=topic, partition=partition)]
                )
            else:
                # Recovery hasn't started yet, so pause ALL partitions
                # and wait for Application to start recovery
                self._consumer.pause(self._consumer.assignment())

    def _revoke_recovery_partitions(
        self,
        recovery_partitions: List[RecoveryPartition],
        partition_num: int,
    ):
        """
        For revoking a specific set of RecoveryPartitions.
        Also cleans up any remnant empty dictionary references for its partition number.

        :param recovery_partitions: a list of `RecoveryPartition`
        :param partition_num: partition number
        """
        self._consumer.incremental_unassign(
            [
                ConfluentPartition(p.changelog_name, p.partition_num)
                for p in recovery_partitions
            ]
        )
        if not self._recovery_partitions[partition_num]:
            del self._recovery_partitions[partition_num]
        if self.recovering:
            logger.debug("Resuming recovery...")

    def revoke_partition(self, partition_num: int):
        """
        revoke ALL StorePartitions (across all Stores) for a given partition number

        :param partition_num: partition number of source topic
        """
        if changelogs := self._recovery_partitions.get(partition_num, {}):
            logger.debug(f"Stopping recovery for {changelogs}")
            self._revoke_recovery_partitions(
                [changelogs.pop(changelog) for changelog in list(changelogs.keys())],
                partition_num,
            )

    def _recovery_loop(self):
        """
        Perform the recovery loop, which continues updating state with changelog
        messages until recovery is "complete" (i.e. no assigned `RecoveryPartition`s).

        A RecoveryPartition is unassigned immediately once fully updated.
        """
        while self.recovering:
            if (msg := self._consumer.poll(1)) is None:
                continue

            changelog_name = msg.topic()
            partition_num = msg.partition()

            partition = self._recovery_partitions[partition_num][changelog_name]
            partition.recover_from_changelog_message(changelog_message=msg)

            if not partition.needs_recovery:
                logger.info(f"Finished recovering {changelog_name}[{partition_num}]")
                self._revoke_recovery_partitions(
                    [self._recovery_partitions[partition_num].pop(changelog_name)],
                    partition_num,
                )

    def stop_recovery(self):
        self._running = False
