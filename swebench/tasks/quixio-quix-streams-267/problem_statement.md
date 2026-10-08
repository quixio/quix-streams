## State Recovery from Changelog Topics

### Description

Quix Streams needs the ability to recover stateful application state from changelog topics when a consumer is assigned new partitions. Currently, the changelog system can write state changes to changelog topics, but there's no mechanism to replay those changelog messages to restore state after a rebalance or application restart.

### Background

When a stateful Kafka Streams application restarts or rebalances, it needs to recover its local state stores from changelog topics. The changelog topics contain a log of all state changes, and by replaying these messages, the application can rebuild its state to the point where it left off.

### Expected Behavior

**Recovery Partition Tracking:**
- A partition should track whether it needs recovery based on comparing its stored changelog offset against the changelog topic's high watermark
- When the stored offset is less than the high watermark, recovery is needed
- When the stored offset equals or exceeds the high watermark, no recovery is needed
- When the changelog has no valid offsets (low watermark equals high watermark), only an offset update is needed

**Recovery Manager:**
- When partitions are assigned, the recovery manager should subscribe to their changelog topics and pause the source topic partitions
- The manager should handle partition assignment and revocation during rebalances
- During recovery, the manager should poll changelog messages and apply them to the local state stores
- Recovery should complete when all partitions have caught up to their high watermarks
- After recovery completes, the source partitions should be resumed so normal processing can continue

**Store Partition Recovery:**
- RocksDB partitions should be able to recover state from changelog messages
- The recover operation should apply the key/value data from the message to the appropriate column family
- After recovery, the changelog offset should be set to the message offset plus one

