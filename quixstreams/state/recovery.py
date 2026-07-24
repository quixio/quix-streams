import inspect
import logging
import time
from typing import Callable, Dict, List, Optional, Tuple

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
    CHANGELOG_TTL_STAMPED_HEADER,
)

logger = logging.getLogger(__name__)


def _accepts_ttl_stamped(method: Callable) -> bool:
    """
    Return whether ``method`` accepts a ``ttl_stamped`` keyword argument.

    ``StorePartition.recover_from_changelog_message`` declares ``ttl_stamped``
    (default ``False``), but a third-party subclass may override it with a rigid
    signature that predates the parameter. Introspecting once lets the recovery
    loop omit the kwarg for such subclasses instead of failing with
    a ``TypeError`` — the dropped bit is a no-op for a non-TTL store. An
    un-introspectable callable (e.g. a C implementation) is assumed to honor the
    base contract and accept it.
    """
    try:
        params = inspect.signature(method).parameters
    except (TypeError, ValueError):
        return True
    if "ttl_stamped" in params:
        return True
    return any(p.kind is p.VAR_KEYWORD for p in params.values())


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
        self._invalid_offset_count = 0  # Track consecutive invalid offset attempts
        self._last_valid_position_time: Optional[float] = None
        # Whether the store partition's recovery hook accepts ``ttl_stamped``.
        # Computed once so per-message dispatch stays cheap and a
        # third-party subclass with a rigid override signature does not raise.
        self._partition_accepts_ttl_stamped = _accepts_ttl_stamped(
            store_partition.recover_from_changelog_message
        )

    def __repr__(self):
        return (
            f'<{self.__class__.__name__} "{self.changelog_name}[{self.partition_num}]">'
        )

    @property
    def changelog_name(self) -> str:
        return self._changelog_name

    @property
    def changelog_highwater(self) -> int:
        return self._changelog_highwater

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
        # Also force the check when the store has a
        # flipped-but-unfinished legacy-TTL migration (durable pending census, no
        # done-marker). Without this, an offset-caught-up restart
        # (``highwater-1 == offset`` → ``state_potentially_behind`` False) would
        # skip recovery and never run ``complete_recovery``, permanently stranding
        # the leftover legacy records. The ``has_consumable_offsets`` guard is
        # kept so an empty changelog still short-circuits (a flipped store with a
        # non-empty pending census implies a non-empty changelog by construction,
        # so the guard is satisfied in the real scenario). ``or`` short-circuits,
        # so the store scan runs only when the offset check is already False.
        return has_consumable_offsets and (
            state_potentially_behind
            or self._store_partition.has_incomplete_ttl_migration()
        )

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

    def increment_invalid_offset_count(self) -> int:
        """
        Increment the counter for consecutive invalid offset attempts.
        Returns the new count.
        """
        self._invalid_offset_count += 1
        return self._invalid_offset_count

    def reset_invalid_offset_count(self):
        """
        Reset the invalid offset counter when a valid position is obtained.
        """
        self._invalid_offset_count = 0
        self._last_valid_position_time = time.monotonic()

    @property
    def invalid_offset_count(self) -> int:
        """
        Get the number of consecutive invalid offset attempts.
        """
        return self._invalid_offset_count

    @property
    def last_valid_position_time(self) -> Optional[float]:
        """
        Get the time when a valid position was last obtained.
        """
        return self._last_valid_position_time

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

        # Stamped-vs-legacy bit. Out-of-band, never inferred from
        # value content. Absent header → legacy / un-stamped (default False, also
        # covers pre-header changelog messages for back-compat).
        ttl_stamped = bool(headers.get(CHANGELOG_TTL_STAMPED_HEADER))

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

            if self._partition_accepts_ttl_stamped:
                self._store_partition.recover_from_changelog_message(
                    cf_name=cf_name,
                    key=key,
                    value=value,
                    offset=changelog_message.offset(),
                    ttl_stamped=ttl_stamped,
                )
            else:
                # Third-party StorePartition subclass whose override predates the
                # ``ttl_stamped`` parameter: omit the kwarg so the
                # rigid signature still works. Such stores are non-TTL, so the
                # dropped stamped bit is a no-op (it would default to False).
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

    def complete_recovery(self):
        """
        Finalize recovery for the underlying `StorePartition`.

        Called once by the recovery manager after this partition has reached its
        changelog high-watermark and before it is unassigned / handed to live
        processing. Delegates to ``StorePartition.complete_recovery`` (a no-op on
        every backend except RocksDB, which uses it to complete an interrupted
        legacy-TTL migration).
        """
        self._store_partition.complete_recovery()

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

    def __init__(
        self,
        changelog_name: str,
        producer: InternalProducer,
        migration_producer: Optional[InternalProducer] = None,
    ):
        """
        :param changelog_name: changelog topic name
        :param producer: a InternalProducer (not shared with `Application` instance)
        :param migration_producer: an optional dedicated NON-transactional
            InternalProducer for legacy-TTL migration / backfill records only.
            Supplied only when the app runs
            exactly-once — see :class:`ChangelogProducer`. ``None`` otherwise.

        :return: a ChangelogWriter instance
        """
        self._changelog_name = changelog_name
        self._producer = producer
        self._migration_producer = migration_producer

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
            migration_producer=self._migration_producer,
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
        migration_producer: Optional[InternalProducer] = None,
    ):
        """
        :param changelog_name: A changelog topic name
        :param partition: source topic partition number
        :param producer: an InternalProducer (not shared with `Application` instance)
        :param migration_producer: an optional dedicated NON-transactional
            InternalProducer used ONLY for legacy-TTL migration / backfill records.
            It is set only when the app runs
            exactly-once: the main ``producer`` is then transactional, so a
            ``flush()`` does NOT make records durable until the checkpoint
            transaction commits — but the migration paths write local RocksDB
            state immediately after producing each chunk and rely on the
            changelog-first invariant (produced+flushed == durable BEFORE the
            local write). Routing migration records through a non-transactional
            producer restores ``flush()==durable`` so a crash before the
            transaction commits cannot leave local stamps + resume ledger ahead of
            an aborted (never-republished) changelog record. When ``None``
            (non-exactly-once) migration records fall back to the main producer,
            which is already non-transactional. NORMAL changelog production is
            never routed here.
        """
        self._changelog_name = changelog_name
        self._partition = partition
        self._producer = producer
        self._migration_producer = migration_producer

    @property
    def changelog_name(self) -> str:
        return self._changelog_name

    @property
    def partition(self) -> int:
        return self._partition

    def _producer_for(self, migration: bool) -> InternalProducer:
        """Pick the underlying producer: the dedicated non-transactional
        migration producer for migration/backfill records when one is configured
        (exactly-once), else the main producer."""
        if migration and self._migration_producer is not None:
            return self._migration_producer
        return self._producer

    def produce(
        self,
        key: bytes,
        value: Optional[bytes] = None,
        headers: Optional[Headers] = None,
        migration: bool = False,
        on_delivery: Optional[Callable] = None,
    ):
        """
        Produce a message to a changelog topic partition.

        :param key: message key (same as state key, including prefixes)
        :param value: message value (same as state value)
        :param headers: message headers (includes column family info)
        :param migration: route through the dedicated non-transactional migration
            producer when configured. Set only by the legacy-TTL
            backfill / recovery-completion / done-marker sites; normal changelog
            production leaves it ``False``.
        :param on_delivery: optional per-record delivery callback, chained with the
            producer's internal callback (review batch 3 #5). The legacy-TTL
            migration sites pass a per-partition ack counter so the backfill flush
            stall detector measures THIS partition's outstanding records rather
            than the shared producer's global queue depth.
        """
        self._producer_for(migration).produce(
            key=key,
            value=value,
            headers=headers,
            partition=self._partition,
            topic=self._changelog_name,
            on_delivery=on_delivery,
        )

    def flush(self, timeout: Optional[float] = None, migration: bool = False) -> int:
        return self._producer_for(migration).flush(timeout=timeout)


class RecoveryManager:
    """
    Manages all consumer-related aspects of recovery, including:
        - assigning/revoking, pausing/resuming topic partitions (especially changelogs)
        - consuming changelog messages until state is updated fully.

    Also tracks/manages `RecoveryPartitions`, which are assigned/tracked only if
    recovery for that changelog partition is required.

    Recovery is attempted from the `Application` after any new partition assignment.
    """

    # Maximum number of consecutive invalid offset attempts before failing loudly
    # At 10-second progress logging intervals, 60 attempts = ~10 minutes
    MAX_INVALID_OFFSET_ATTEMPTS = 60

    def __init__(
        self,
        consumer: BaseConsumer,
        topic_manager: TopicManager,
        broker_availability_timeout: float = 0,
    ):
        self._running = False
        self._consumer = consumer
        self._topic_manager = topic_manager
        self._broker_availability_timeout = broker_availability_timeout
        self._recovery_partitions: Dict[int, Dict[str, RecoveryPartition]] = {}
        self._last_progress_logged_time = time.monotonic()
        # Cache position results to avoid double calls in same iteration
        self._position_cache: Dict[str, Tuple[float, ConfluentPartition]] = {}
        self._recovery_paused_data_tps: set[tuple[str, int]] = set()

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
            self._forget_recovery_paused_data_partitions(non_changelog_tps)
        else:
            logger.debug("Recovery process interrupted; stopping.")

    def _pause_for_recovery(self, partitions: List[ConfluentPartition]) -> None:
        self._track_recovery_paused_data_partitions(partitions)
        self._consumer.pause(partitions)

    def _track_recovery_paused_data_partitions(
        self, partitions: List[ConfluentPartition]
    ) -> None:
        non_changelog_topics = self._topic_manager.non_changelog_topics
        self._recovery_paused_data_tps.update(
            (tp.topic, tp.partition)
            for tp in partitions
            if tp.topic in non_changelog_topics
        )

    def _forget_recovery_paused_data_partitions(
        self, partitions: List[ConfluentPartition]
    ) -> None:
        for tp in partitions:
            self._recovery_paused_data_tps.discard((tp.topic, tp.partition))

    def _resume_recovery_paused_data_partitions(
        self, partitions: List[ConfluentPartition]
    ) -> None:
        if not self._recovery_paused_data_tps:
            return

        non_changelog_topics = self._topic_manager.non_changelog_topics
        to_resume = [
            tp
            for tp in partitions
            if tp.topic in non_changelog_topics
            and (tp.topic, tp.partition) in self._recovery_paused_data_tps
        ]
        if not to_resume:
            return

        logger.info(
            f"Resuming data partitions paused for recovery: "
            f"{[(tp.topic, tp.partition) for tp in to_resume]}"
        )
        self._consumer.resume(to_resume)
        self._forget_recovery_paused_data_partitions(to_resume)

    def resume_reassigned_data_partitions(
        self, partitions: List[ConfluentPartition]
    ) -> None:
        """
        Resume data partitions still paused from a previous recovery generation.

        `assign_partition` resumes recovery-paused data partitions, but it only runs
        when a rebalance assigns at least one stateful store partition. A partition
        paused by recovery can be revoked and later reassigned in a rebalance that
        brings no stateful partition to this consumer (e.g. only stateless topics),
        in which case `assign_partition` never runs and the partition would stay
        paused indefinitely. Application calls this on every assignment to cover it.

        Skipped while a recovery is pending/active (`has_assignments`): those pauses
        are intentional and get resumed by `do_recovery`/`assign_partition`.
        """
        if not self._recovery_paused_data_tps or self.has_assignments:
            return
        self._resume_recovery_paused_data_partitions(partitions)

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

        current_assignment = self._consumer.assignment()
        assigned_tps = set((tp.topic, tp.partition) for tp in current_assignment)

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
                self._pause_for_recovery(
                    [ConfluentPartition(topic=topic, partition=partition)]
                )
            else:
                # Recovery hasn't started yet, so pause ALL partitions
                # and wait for Application to start recovery
                self._pause_for_recovery(self._consumer.assignment())
        else:
            self._resume_recovery_paused_data_partitions(current_assignment)

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
            # Clean up position cache for revoked partition
            cache_key = f"{rp.changelog_name}:{rp.partition_num}"
            self._position_cache.pop(cache_key, None)
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
            position = self._get_changelog_offset(rp)
            if position is None:
                # Skip status update if position is not yet valid (e.g., during rebalancing)
                # Will retry on next poll cycle
                logger.debug(
                    f"Skipping recovery status update for {rp}: position not available"
                )
                continue

            rp.set_recovery_consume_position(position)
            if rp.finished_recovery_check:
                # Recovery-finalize seam: the partition has reached
                # its changelog high-watermark. Complete any interrupted legacy-
                # TTL migration before the partition is unassigned and handed to
                # live processing. A no-op on every shape except a MIXED changelog
                # restored with ``legacy_records_ttl`` set.
                rp.complete_recovery()
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
            self._log_recovery_progress()
            if (msg := self._consumer.poll(1)) is None:
                self._update_recovery_status()
            else:
                msg = raise_for_msg_error(msg)
                rp = self._recovery_partitions[msg.partition()][msg.topic()]
                rp.recover_from_changelog_message(changelog_message=msg)
                self._consumer._broker_available()  # noqa: SLF001
            if self._broker_availability_timeout:
                self._consumer.raise_if_broker_unavailable(
                    self._broker_availability_timeout
                )

    def _get_position_with_cache(self, rp: RecoveryPartition) -> ConfluentPartition:
        """
        Get the consumer position for a RecoveryPartition, using cache to avoid
        multiple calls in the same iteration.

        :param rp: RecoveryPartition to get position for
        :return: ConfluentPartition with offset and error information
        """
        cache_key = f"{rp.changelog_name}:{rp.partition_num}"
        current_time = time.monotonic()

        # Check if we have a fresh cached value (within last second)
        if cache_key in self._position_cache:
            cached_time, cached_position = self._position_cache[cache_key]
            if current_time - cached_time < 1.0:
                return cached_position

        # Query position and cache it
        position_tp = self._consumer.position(
            [ConfluentPartition(rp.changelog_name, rp.partition_num)]
        )[0]
        self._position_cache[cache_key] = (current_time, position_tp)
        return position_tp

    def _log_recovery_progress(self) -> None:
        """
        Periodically log the recovery progress of all RecoveryPartitions.
        """
        if self._last_progress_logged_time < time.monotonic() - 10:
            for rp in dict_values(self._recovery_partitions):
                # Use cached position to avoid redundant network calls
                position_tp = self._get_position_with_cache(rp)

                if position_tp.error:
                    count = rp.invalid_offset_count
                    log_level = logger.warning if count > 30 else logger.info
                    log_level(
                        f"Recovery progress for {rp}: position unavailable "
                        f"(error: {position_tp.error}, attempts: {count})"
                    )
                elif position_tp.offset < 0:
                    count = rp.invalid_offset_count
                    log_level = logger.warning if count > 30 else logger.info
                    log_level(
                        f"Recovery progress for {rp}: position not yet established "
                        f"(offset: {position_tp.offset}, attempts: {count})"
                    )
                else:
                    last_consumed_offset = position_tp.offset - 1
                    logger.info(
                        f"Recovery progress for {rp}: {last_consumed_offset} / {rp.changelog_highwater}"
                    )
            self._last_progress_logged_time = time.monotonic()

    def _get_changelog_offset(self, rp: RecoveryPartition) -> Optional[int]:
        """
        Get the current offset of the changelog partition.

        Returns None if the position is not yet established (e.g., during rebalancing)
        or if there's an error querying the position.

        Tracks consecutive invalid offset attempts and raises an exception if the
        threshold is exceeded.

        :return: The current offset, or None if position is invalid/unavailable
        :raises RuntimeError: If position remains invalid beyond MAX_INVALID_OFFSET_ATTEMPTS
        """
        # Use cached position to avoid redundant network calls
        position_tp = self._get_position_with_cache(rp)

        # Check for Kafka errors (e.g., during rebalancing)
        if position_tp.error:
            count = rp.increment_invalid_offset_count()
            logger.debug(
                f"Cannot get position for {rp} due to Kafka error: {position_tp.error}. "
                f"This is expected during rebalancing (attempt {count}/{self.MAX_INVALID_OFFSET_ATTEMPTS})."
            )
            self._check_invalid_offset_threshold(rp, f"error: {position_tp.error}")
            return None

        # Check for special Kafka offset values (OFFSET_INVALID=-1001, OFFSET_STORED=-1000, etc.)
        offset = position_tp.offset
        if offset < 0:
            count = rp.increment_invalid_offset_count()
            logger.debug(
                f"Position not yet established for {rp}: offset={offset}. "
                f"This is expected during rebalancing (attempt {count}/{self.MAX_INVALID_OFFSET_ATTEMPTS})."
            )
            self._check_invalid_offset_threshold(rp, f"offset={offset}")
            return None

        # Valid offset obtained - reset the counter
        rp.reset_invalid_offset_count()
        return offset

    def _check_invalid_offset_threshold(self, rp: RecoveryPartition, reason: str):
        """
        Check if the invalid offset count exceeds the threshold and fail loudly if so.

        :param rp: RecoveryPartition being checked
        :param reason: Description of why the offset is invalid
        :raises RuntimeError: If threshold is exceeded
        """
        if rp.invalid_offset_count > self.MAX_INVALID_OFFSET_ATTEMPTS:
            error_msg = (
                f"Recovery stuck for {rp}: position has been invalid for "
                f"{rp.invalid_offset_count} consecutive attempts ({reason}). "
                f"This indicates a serious issue with the Kafka consumer or broker. "
                f"Last valid position was at {rp.last_valid_position_time or 'never'}."
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def stop_recovery(self):
        self._running = False
