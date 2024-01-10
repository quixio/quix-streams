from quixstreams.topic_manager import TopicManagerType
from quixstreams.rowconsumer import RowConsumer
from quixstreams.rowproducer import RowProducer
from quixstreams.models import Topic, MessageHeadersTuples
from quixstreams.state.types import StorePartition

from confluent_kafka import TopicPartition as ConfluentPartition

from typing import Optional, Dict, List


class RecoveryPartition:
    def __init__(
        self, topic: str, changelog: str, partition: int, state_store: StorePartition
    ):
        self.topic = topic
        self.changelog = changelog
        self.partition = partition
        self.state_store = state_store
        self._changelog_lowwater: Optional[int] = None
        self._changelog_highwater: Optional[int] = None

    @property
    def offset(self) -> int:
        return self.state_store.get_changelog_offset()

    @property
    def as_partition(self) -> ConfluentPartition:
        return ConfluentPartition(self.topic, self.partition)

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
        state_is_behind = self._changelog_highwater - self.offset
        return has_consumable_offsets and state_is_behind

    def set_watermarks(self, lowwater: int, highwater: int):
        self._changelog_lowwater = lowwater
        self._changelog_highwater = highwater


_COLUMN_FAMILY_HEADER = "__column_family__"


class ChangelogWriter:
    """
    Typically created and handed to `PartitionTransaction`s to produce state changes to
    a changelog topic.
    """

    def __init__(self, topic: Topic, partition_num: int, producer: RowProducer):
        self._topic = topic
        self._partition_num = partition_num
        self._producer = producer

    def produce(
        self, key: bytes, cf_name: str = "default", value: Optional[bytes] = None
    ):
        # TODO-CF: remove default cf_name to ensure its passed?
        msg = self._topic.serialize(key=key, value=value)
        self._producer.produce(
            key=msg.key,
            value=msg.value,
            topic=self._topic.name,
            partition=self._partition_num,
            headers={_COLUMN_FAMILY_HEADER: cf_name},
        )


class ChangelogManager:
    def __init__(
        self,
        topic_manager: TopicManagerType,
        consumer: RowConsumer,
        producer: RowProducer,
    ):
        self._topic_manager = topic_manager
        self._consumer = consumer
        self._producer = producer
        self._recovery_manager = RecoveryManager(consumer)

    """
    A simple interface for adding changelog topics during store init and
    generating changelog writers (generally for each new `Store` transaction).
    """

    def _get_changelog(self, source_topic: str, suffix: str):
        return self._topic_manager.changelog_topics[source_topic][suffix]

    def add_changelog(self, source_topic_name: str, suffix: str, consumer_group: str):
        self._topic_manager.changelog_topic(
            source_topic_name=source_topic_name,
            suffix=suffix,
            consumer_group=consumer_group,
        )

    def assign_partition(
        self, source_topic_name: str, partition: int, state_store: StorePartition
    ):
        for topic in self._topic_manager.changelog_topics[source_topic_name].values():
            print(f"CHANGELOG: ADDING PARTITION {topic.name}: {partition}")
            self._recovery_manager.assign_partition(
                source_topic_name=source_topic_name,
                changelog=topic.name,
                partition=partition,
                state_store=state_store,
            )

    def revoke_partition(self, source_topic_name, partition):
        for topic in self._topic_manager.changelog_topics[source_topic_name].values():
            print(f"CHANGELOG: REVOKING PARTITION {topic.name}: {partition}")
            self._recovery_manager.revoke_partition(
                topic=source_topic_name, changelog=topic.name, partition=partition
            )

    def get_writer(
        self, source_topic_name: str, suffix: str, partition_num: int
    ) -> ChangelogWriter:
        return ChangelogWriter(
            topic=self._get_changelog(source_topic=source_topic_name, suffix=suffix),
            partition_num=partition_num,
            producer=self._producer,
        )

    def do_recovery(self):
        print("DO RECOVERY")
        self._recovery_manager.do_recovery()


class RecoveryManager:
    def __init__(self, consumer: RowConsumer):
        self._consumer = consumer
        self._pending_assigns: List[RecoveryPartition] = []
        self._pending_revokes: List[RecoveryPartition] = []
        self._partitions: Dict[int, Dict[str, RecoveryPartition]] = {}
        self._recovery_method = self._recover
        self._topic_changelog_map: Dict[str, str] = {}

    class RecoveryComplete(Exception):
        ...

    class MissingColumnFamilyHeader(Exception):
        ...

    @property
    def in_recovery_mode(self) -> bool:
        return bool(self._partitions)

    def do_recovery(self):
        return self._recovery_method()

    def assign_partition(
        self,
        source_topic_name: str,
        changelog: str,
        partition: int,
        state_store: StorePartition,
    ):
        print("RECOVERY: ADD PARTITION")
        p = RecoveryPartition(
            topic=source_topic_name,
            changelog=changelog,
            partition=partition,
            state_store=state_store,
        )
        topic_p = p.topic_partition
        # Assign manually to immediately pause it (would assign unpaused automatically)
        # TODO: consider doing all topic (not changelog) assign(s) during the
        #  Application on_assign call (rather than one at a time here)
        self._consumer.incremental_assign([topic_p])
        self._consumer.pause([topic_p])
        self._pending_assigns.append(p)
        self._recovery_method = self._rebalance

    def _partition_cleanup(self, partition: int):
        if not self._partitions[partition]:
            del self._partitions[partition]

    def revoke_partition(self, topic: str, changelog: str, partition: int):
        print("RECOVERY: REVOKE PARTITION")
        # TODO: consider doing all topic (not changelog) unassign(s) during the
        #  Application on_revoke call (rather than one at a time here)
        self._consumer.incremental_unassign([ConfluentPartition(topic, partition)])
        print(f"CURRENT PARTITIONS: {self._partitions}")
        if recovery_p := self._partitions.get(partition, {}).pop(changelog, None):
            # pause for later revoke (will revoke all partitions at the same time)
            self._consumer.pause([recovery_p.changelog_partition])
            self._pending_revokes.append(recovery_p)
            print(f"PENDING REVOKES: {self._pending_revokes}")
            if self._pending_revokes:
                self._partition_cleanup(partition)
                self._recovery_method = self._rebalance

    def _handle_pending_assigns(self):
        print("RECOVERY: HANDLE PENDING ASSIGNS")
        if self._pending_assigns:
            assigns = []
            self._consumer.pause([p.as_partition for p in self._pending_assigns])
            while self._pending_assigns:
                p = self._pending_assigns.pop()
                p.set_watermarks(
                    *self._consumer.get_watermark_offsets(
                        p.changelog_partition, timeout=10
                    )
                )
                if p.needs_recovery:
                    print(f"ADDING PARTITION {p.changelog}: {p.partition}")
                    assigns.append(p)
                    self._partitions.setdefault(p.partition, {})[p.changelog] = p
            if assigns:
                self._consumer.incremental_assign(
                    [p.changelog_assignable_partition for p in assigns]
                )
        self._recovery_method = self._recover

    def _handle_pending_revokes(self):
        print("RECOVERY: HANDLE PENDING REVOKES")
        if self._pending_revokes:
            self._consumer.incremental_unassign(
                [p.changelog_partition for p in self._pending_revokes]
            )
            print(
                f"REVOKED PARTITIONS {[(p.changelog, p.partition) for p in self._pending_revokes]}"
            )
            self._pending_revokes = []

    def _rebalance(self):
        """ """
        print("RECOVERY: REBALANCE")
        self._handle_pending_revokes()
        self._handle_pending_assigns()
        self._recovery_method = self._recover

    def _finalize_recovery(self):
        print("RECOVERY: FINALIZE RECOVERY")
        self._consumer.resume(self._consumer.assignment())
        raise self.RecoveryComplete

    def _get_column_family_header(self, headers: MessageHeadersTuples) -> str:
        for t in headers:
            if t[0] == _COLUMN_FAMILY_HEADER:
                return t[1].decode()
        raise self.MissingColumnFamilyHeader(
            f"Header '{_COLUMN_FAMILY_HEADER}' was missing from the changelog message!"
        )

    def _recover(self):
        print("RECOVERY: RECOVER")
        if not self.in_recovery_mode:
            self._finalize_recovery()

        print("POLLING")
        if not (msg := self._consumer.poll(5)):
            print("NO MESSAGES")
            return

        print(self._consumer.assignment())
        print(
            f"RECOVERY MESSAGE: {msg.key()}, {msg.value()}, {msg.topic()}: {msg.partition()}"
        )
        changelog = msg.topic()
        p_num = msg.partition()

        partition = self._partitions[p_num][changelog]
        with partition.state_store.recover(msg.offset()) as transaction:
            # TODO-CF: UNCOMMENT AND REPLACE BELOW
            # cf_name = self._get_column_family_header(msg.headers())
            # if msg.value():
            #     transaction.set(cf_name=cf_name, key=msg.key(), value=msg.value())
            # else:
            #     transaction.delete(cf_name=cfg_name, key=msg.key())
            if msg.value():
                transaction.set(key=msg.key(), value=msg.value())
            else:
                transaction.delete(key=msg.key())

        if not partition.needs_recovery:
            print(f"RECOVERY FOR {msg.topic()}: {msg.partition()} finished!")
            self._pending_revokes.append(self._partitions[p_num].pop(changelog))
            self._partition_cleanup(p_num)
            self._handle_pending_revokes()
