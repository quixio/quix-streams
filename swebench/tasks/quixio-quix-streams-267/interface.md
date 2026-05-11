Type: Class
Name: RecoveryPartition
Location: quixstreams/state/changelog.py
Description: Represents a partition that needs to be recovered from changelog messages. Tracks watermarks and recovery state for a single partition.
Signature: __init__(self, topic: str, changelog: str, partition: int, store_partition: StorePartition)

Methods:
- set_watermarks(lowwater: int, highwater: int) -> None
- needs_recovery: property -> bool
- needs_offset_update: property -> bool
- recover(msg) -> None
- update_offset() -> None
- offset: property -> int
- topic_partition: property -> ConfluentPartition

Inner Classes:
- OffsetUpdate

Internal:
- _changelog_lowwater: int
- _changelog_highwater: int
- _warn_bad_offset() -> None

---

Type: Class
Name: RecoveryManager
Location: quixstreams/state/changelog.py
Description: Manages the recovery process for stateful partitions by coordinating changelog consumption and state restoration.
Signature: __init__(self, consumer: Consumer)

Methods:
- assign_partitions(source_topic_name: str, partition: int, store_partitions: dict) -> None
- revoke_partitions(topic: str, partition: int) -> None
- do_recovery() -> None
- _handle_pending_assigns() -> None
- _handle_pending_revokes() -> None
- _update_partition_offsets() -> None
- _finalize_recovery() -> None
- _rebalance() -> None
- _recover() -> None

Inner Classes:
- RecoveryComplete (exception)

Internal:
- _consumer: Consumer
- _partitions: dict
- _pending_assigns: list
- _pending_revokes: list
- _recovery_method: callable
- _poll_attempts: int
- _polls_remaining: int

---

Type: Method
Name: recover
Location: quixstreams/state/rocksdb/partition.py (RocksDBStorePartition class)
Signature: recover(self, changelog_message) -> None
Description: Recovers state from a changelog message. Reads the key, value, and column family from the message and applies it to the local store. Sets the changelog offset to message.offset() + 1.

---

Type: Method
Name: get_changelog_offset
Location: quixstreams/state/rocksdb/partition.py (RocksDBStorePartition class)
Signature: get_changelog_offset(self) -> Optional[int]
Description: Returns the current changelog offset, or None if no offset has been set (previously returned 0).

---

Type: Constant
Name: ConfluentPartition
Location: quixstreams/state/changelog.py (imported from confluent_kafka)
Description: The confluent_kafka TopicPartition class, imported as ConfluentPartition for recovery partition management.
