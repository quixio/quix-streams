import logging
from typing import Dict, List, Optional

from confluent_kafka import OFFSET_BEGINNING
from confluent_kafka import TopicPartition as ConfluentPartition

from quixstreams.internal_producer import InternalProducer
from quixstreams.kafka import BaseConsumer
from quixstreams.kafka.consumer import raise_for_msg_error
from quixstreams.models import SuccessfulConfluentKafkaMessageProto, Topic
from quixstreams.models.topics import TopicConfig, TopicManager
from quixstreams.models.types import Headers
from quixstreams.state.base import StorePartition
from quixstreams.utils.dicts import dict_values
from quixstreams.utils.json import loads as json_loads

from .exceptions import (
    ChangelogTopicPartitionNotAssigned,
    ColumnFamilyHeaderMissing,
    InvalidStoreChangelogOffset,
)
from .metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER,
)

logger = logging.getLogger(__name__)

__all__ = (
    "ChangelogProducer",
    "ChangelogProducerFactory",
    "RecoveryManager",
    "RecoveryPartition",
)

_NoneType = type(None)


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
        committed_offsets: dict[str, int],
        lowwater: int,
        highwater: int,
    ):
        self._changelog_name = changelog_name
        self._partition_num = partition_num
        self._store_partition = store_partition
        self._changelog_lowwater = lowwater
        self._changelog_highwater = highwater
        self._committed_offsets = committed_offsets
        self._recovery_consume_position: Optional[int] = None
        self._initial_offset: Optional[int] = None

    def __repr__(self):
        return (
            f'<{self.__class__.__name__} "{self.changelog_name}[{self.partition_num}]">'
        )

    @property
    def changelog_name(self) -> str:
        return self._changelog_name

    @property
    def partition_num(self) -> int:
        return self._partition_num

    @property
    def offset(self) -> int:
        """
        Get the changelog offset from the underlying `StorePartition`.

        :return: changelog offset (int)
        """
        offset = self._store_partition.get_changelog_offset()
        if offset is None:
            offset = OFFSET_BEGINNING

        if self._initial_offset is None:
            self._initial_offset = offset
        return offset

    @property
    def finished_recovery_check(self) -> bool:
        return self._recovery_consume_position == self._changelog_highwater

    @property
    def needs_recovery_check(self) -> bool:
        """
        Determine whether to attempt recovery for underlying `StorePartition`.

        This does NOT mean that anything actually requires recovering.
        """
        has_consumable_offsets = self._changelog_lowwater != self._changelog_highwater
        state_potentially_behind = self._changelog_highwater - 1 > self.offset
        return has_consumable_offsets and state_potentially_behind

    @property
    def has_invalid_offset(self) -> bool:
        """
        Determine if the current changelog offset stored in state is invalid.
        """
        if self._changelog_highwater == 0:
            return False

        return self._changelog_highwater <= self.offset

    @property
    def recovery_consume_position(self) -> Optional[int]:
        return self._recovery_consume_position

    @property
    def had_recovery_changes(self) -> bool:
        return self._initial_offset != self.offset

    def recover_from_changelog_message(
        self, changelog_message: SuccessfulConfluentKafkaMessageProto
    ):
        """
        Recover the StorePartition using a message read from its respective changelog.

        The actual update may be skipped when both conditions are met:

        - The changelog message has headers with the processed message offset.
        - This processed offsets are larger than the latest committed offsets
            for the same topic-partitions.

        This way the state does not apply the state changes for not-yet-committed
        messages and improves the state consistency guarantees.

        :param changelog_message: An instance of `confluent_kafka.Message`
        """
        headers = dict(changelog_message.headers() or ())

        # Parse the column family name from message headers
        cf_name = headers.get(CHANGELOG_CF_MESSAGE_HEADER, b"").decode()
        if not cf_name:
            raise ColumnFamilyHeaderMissing(
                f"Header '{CHANGELOG_CF_MESSAGE_HEADER}' missing from changelog message"
            )

        # Parse the processed topic-partition-offset info from the changelog message
        # headers to determine whether the update should be applied or skipped.
        # It can be empty if the message was produced by the older version of the lib.
        processed_offsets = json_loads(
            headers.get(CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER, b"null")
        )
        if processed_offsets is None or self._should_apply_changelog(
            processed_offsets=processed_offsets
        ):
            key = changelog_message.key()
            if not isinstance(key, bytes):
                raise TypeError(
                    f'Invalid changelog key type {type(key)}, expected "bytes"'
                )

            value = changelog_message.value()
            if not isinstance(value, (bytes, _NoneType)):
                raise TypeError(
                    f'Invalid changelog value type {type(value)}, expected "bytes"'
                )

            self._store_partition.recover_from_changelog_message(
                cf_name=cf_name,
                key=key,
                value=value,
                offset=changelog_message.offset(),
            )
        else:
            # Even if the changelog update is skipped, roll the changelog offset
            # to move forward within the changelog topic
            self._store_partition.write_changelog_offset(
                offset=changelog_message.offset(),
            )

    def set_recovery_consume_position(self, offset: int):
        """
        Update the recovery partition with the consumer's position (whenever
        an empty poll is returned during recovery).

        It is possible that it may be set more than once.

        :param offset: the consumer's current read position of the changelog
        """
        self._recovery_consume_position = offset

    def _should_apply_changelog(self, processed_offsets: dict[str, int]) -> bool:
        """
        Determine whether the changelog update should be skipped.

        :param processed_offsets: a dict with processed offsets
            from the changelog message header processed offset.

        :return: True if update should be applied, else False.
        """
        committed_offsets = self._committed_offsets
        for topic, processed_offset in processed_offsets.items():
            # Skip recovering from the message if its processed offset is ahead of the
            # current committed offset.
            # This is a best-effort to recover to a consistent state
            # if the checkpointing code produced the changelog messages
            # but failed to commit the source topic offset.
            if processed_offset >= committed_offsets[topic]:
                return False
        return True


class ChangelogProducerFactory:
    """
    Generates ChangelogProducers, which produce changelog messages to a StorePartition.
    """

    def __init__(self, changelog_name: str, producer: InternalProducer):
        """
        :param changelog_name: changelog topic name
        :param producer: a InternalProducer (not shared with `Application` instance)

        :return: a ChangelogWriter instance
        """
        self._changelog_name = changelog_name
        self._producer = producer

    def get_partition_producer(self, partition_num) -> "ChangelogProducer":
        """
        Generate a ChangelogProducer for producing to a specific partition number
        (and thus StorePartition).

        :param partition_num: source topic partition number
        """
        return ChangelogProducer(
            changelog_name=self._changelog_name,
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
        partition: int,
        producer: InternalProducer,
    ):
        """
        :param changelog_name: A changelog topic name
        :param partition: source topic partition number
        :param producer: an InternalProducer (not shared with `Application` instance)
        """
        self._changelog_name = changelog_name
        self._partition = partition
        self._producer = producer

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
        headers: Optional[Headers] = None,
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

    def __init__(self, consumer: BaseConsumer, topic_manager: TopicManager):
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
        self,
        stream_id: Optional[str],
        store_name: str,
        topic_config: TopicConfig,
    ) -> Topic:
        """
        Register a changelog Topic with the TopicManager.

        :param stream_id: stream id
        :param store_name: name of the store
        :param topic_config: a TopicConfig to use
        """
        return self._topic_manager.changelog_topic(
            stream_id=stream_id,
            store_name=store_name,
            config=topic_config,
        )

    def do_recovery(self):
        """
        If there are any active RecoveryPartitions, do a recovery procedure.

        After, will resume normal `Application` processing.
        """
        logger.info("Beginning recovery check...")
        self._running = True
        # note: technically it should be rp.offset + 1, but to remain backwards
        # compatible with <v2.7 +1 ALOS offsetting, it remains rp.offset.
        # This means we will always re-write the "first" recovery message.
        # More specifically, this is only covering for a very edge case:
        # when first upgrading from <v2.7 AND a recovery was actually needed.
        # Once on >=v2.7, this is no longer an issue...so we could eventually
        # remove this, potentially.

        # Seek the changelog partitions to the previously saved position and resume them
        for rp in dict_values(self._recovery_partitions):
            tp = ConfluentPartition(
                topic=rp.changelog_name, partition=rp.partition_num, offset=rp.offset
            )
            self._consumer.seek(tp)
            self._consumer.resume([tp])

        self._recovery_loop()
        if self._running:
            logger.info("Recovery process complete! Resuming normal processing...")
            self._running = False

            # When recovery is finished, resume only data partitions
            non_changelog_tps = [
                tp
                for tp in self._consumer.assignment()
                if tp.topic in self._topic_manager.non_changelog_topics
            ]
            self._consumer.resume(non_changelog_tps)
        else:
            logger.debug("Recovery process interrupted; stopping.")

    def _generate_recovery_partitions(
        self,
        topic_name: Optional[str],
        partition_num: int,
        store_partitions: Dict[str, StorePartition],
        committed_offsets: dict[str, int],
    ) -> List[RecoveryPartition]:
        partitions = []
        for store_name, store_partition in store_partitions.items():
            changelog_topic = self._topic_manager.changelog_topics[topic_name][
                store_name
            ]

            lowwater, highwater = self._consumer.get_watermark_offsets(
                ConfluentPartition(
                    topic=changelog_topic.name,
                    partition=partition_num,
                ),
                timeout=10,
            )

            partitions.append(
                RecoveryPartition(
                    changelog_name=changelog_topic.name,
                    partition_num=partition_num,
                    store_partition=store_partition,
                    committed_offsets=committed_offsets,
                    lowwater=lowwater,
                    highwater=highwater,
                )
            )
        return partitions

    def assign_partition(
        self,
        topic: Optional[str],
        partition: int,
        committed_offsets: dict[str, int],
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
            committed_offsets=committed_offsets,
        )

        assigned_tps = set(
            (tp.topic, tp.partition) for tp in self._consumer.assignment()
        )

        for rp in recovery_partitions:
            changelog_name, partition = rp.changelog_name, rp.partition_num
            # Validate that the changelog topic-partition is assigned to consumer before
            # adding a recovery check
            if (changelog_name, partition) not in assigned_tps:
                raise ChangelogTopicPartitionNotAssigned(
                    f'Changelog topic partition "{changelog_name}[{partition}]" '
                    f"must be assigned to recover from it"
                )

            if rp.needs_recovery_check:
                logger.debug(f"Adding a recovery check for {rp}")
                self._recovery_partitions.setdefault(partition, {})[changelog_name] = rp
            elif rp.has_invalid_offset:
                raise InvalidStoreChangelogOffset(
                    "The offset in the state store is greater than or equal to its "
                    "respective changelog highwater. This can happen if the changelog "
                    "was deleted (and recreated) but the state store was not. The "
                    "invalid state store can be deleted by manually calling "
                    "Application.clear_state() before running the application again."
                )

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

    def _revoke_recovery_partitions(self, recovery_partitions: List[RecoveryPartition]):
        """
        Pauses all provided RecoveryPartitions and cleans up any remaining
        empty dictionary references.

        The actual unassignment is done by Consumer.

        :param recovery_partitions: a list of `RecoveryPartition`
        """
        partition_nums = {rp.partition_num for rp in recovery_partitions}
        self._consumer.pause(
            [
                ConfluentPartition(rp.changelog_name, rp.partition_num)
                for rp in recovery_partitions
            ]
        )
        for rp in recovery_partitions:
            del self._recovery_partitions[rp.partition_num][rp.changelog_name]
        for partition_num in partition_nums:
            if not self._recovery_partitions[partition_num]:
                del self._recovery_partitions[partition_num]
        if self.recovering:
            logger.debug("Resuming recovery process...")

    def revoke_partition(self, partition_num: int):
        """
        revoke ALL StorePartitions (across all Stores) for a given partition number

        :param partition_num: partition number of source topic
        """
        if changelogs := self._recovery_partitions.get(partition_num, {}):
            recovery_partitions = list(changelogs.values())
            logger.debug(f"Stopping recovery for {list(map(str, recovery_partitions))}")
            self._revoke_recovery_partitions(recovery_partitions)

    def _update_recovery_status(self):
        rp_revokes = []
        for rp in dict_values(self._recovery_partitions):
            position = self._consumer.position(
                [ConfluentPartition(rp.changelog_name, rp.partition_num)]
            )[0].offset
            rp.set_recovery_consume_position(position)
            if rp.finished_recovery_check:
                rp_revokes.append(rp)
                if rp.had_recovery_changes:
                    logger.info(f"Recovery successful for {rp}")
                else:
                    logger.debug(f"No recovery was required for {rp}")
        self._revoke_recovery_partitions(rp_revokes)

    def _recovery_loop(self) -> None:
        """
        Perform the recovery loop, which continues updating state with changelog
        messages until recovery is "complete" (i.e. no assigned `RecoveryPartition`s).

        A RecoveryPartition is unassigned immediately once fully updated.
        """
        while self.recovering:
            if (msg := self._consumer.poll(1)) is None:
                self._update_recovery_status()
            else:
                msg = raise_for_msg_error(msg)
                rp = self._recovery_partitions[msg.partition()][msg.topic()]
                rp.recover_from_changelog_message(changelog_message=msg)

    def stop_recovery(self):
        self._running = False
