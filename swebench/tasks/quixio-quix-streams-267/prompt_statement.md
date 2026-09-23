I'm working on a stateful streaming application with Quix Streams and I need to implement state recovery from changelog topics. Right now, my application writes state changes to changelog topics correctly, but when the application restarts or when partitions get rebalanced, there's no way to replay those changelog messages to restore the local state.

The problem is that after a restart, my application loses all its accumulated state because there's no recovery mechanism. I need the system to be able to detect which partitions need recovery by comparing the locally stored offset against the changelog topic's watermarks, then replay the changelog messages to restore state before resuming normal processing.

The recovery process should handle the full lifecycle - tracking which partitions need recovery, subscribing to changelog topics, pausing source partitions during recovery, polling and applying changelog messages to the local stores, and then resuming normal consumption once everything is caught up. It also needs to handle edge cases like when there are no messages to recover, or when the local offset is already ahead of the changelog.

Additionally, the RocksDB store partitions need a way to apply individual changelog messages during recovery, updating both the state data and the changelog offset tracking.
