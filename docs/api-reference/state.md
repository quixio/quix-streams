<a id="quixstreams.state.base.state"></a>

## quixstreams.state.base.state

<a id="quixstreams.state.base.state.State"></a>

### State

```python
class State(ABC, Generic[K, V])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L18)

Primary interface for working with key-value state data from `StreamingDataFrame`

<a id="quixstreams.state.base.state.State.get"></a>

<br><br>

#### State.get

```python
@abstractmethod
def get(key: K, default: Optional[V] = None) -> Optional[V]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L30)

Get the value for key if key is present in the state, else default


<br>
***Arguments:***

- `key`: key
- `default`: default value to return if the key is not found


<br>
***Returns:***

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.base.state.State.get_bytes"></a>

<br><br>

#### State.get\_bytes

```python
def get_bytes(key: K, default: Optional[bytes] = None) -> Optional[bytes]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L46)

Get the value for key if key is present in the state, else default


<br>
***Arguments:***

- `key`: key
- `default`: default value to return if the key is not found


<br>
***Returns:***

value as bytes or None if the key is not found and `default` is not provided

<a id="quixstreams.state.base.state.State.set"></a>

<br><br>

#### State.set

```python
@abstractmethod
def set(key: K, value: V, ttl: Optional[timedelta] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L56)

Set value for the key, optionally with a per-write expiry.


<br>
***Arguments:***

- `key`: key
- `value`: value
- `ttl`: optional event-time TTL. When set, the entry expires
``ttl`` after the current record's event-time and is filtered
from subsequent reads. ``None`` (default) writes a sentinel
stamp meaning "never expires", overwriting any prior TTL on
the same key.

<a id="quixstreams.state.base.state.State.set_bytes"></a>

<br><br>

#### State.set\_bytes

```python
@abstractmethod
def set_bytes(key: K, value: bytes, ttl: Optional[timedelta] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L71)

Set bytes value for the key, optionally with a per-write expiry.


<br>
***Arguments:***

- `key`: key
- `value`: value as bytes
- `ttl`: see :meth:`set`.

<a id="quixstreams.state.base.state.State.delete"></a>

<br><br>

#### State.delete

```python
@abstractmethod
def delete(key: K)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L82)

Delete value for the key.

This function always returns `None`, even if value is not found.


<br>
***Arguments:***

- `key`: key

<a id="quixstreams.state.base.state.State.exists"></a>

<br><br>

#### State.exists

```python
@abstractmethod
def exists(key: K) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L92)

Check if the key exists in state.


<br>
***Arguments:***

- `key`: key


<br>
***Returns:***

True if key exists, False otherwise

<a id="quixstreams.state.base.state.TransactionState"></a>

### TransactionState

```python
class TransactionState(State)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L101)

<a id="quixstreams.state.base.state.TransactionState.__init__"></a>

<br><br>

#### TransactionState.\_\_init\_\_

```python
def __init__(prefix: bytes,
             transaction: "PartitionTransaction",
             timestamp: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L108)

Simple key-value state to be provided into `StreamingDataFrame` functions


<br>
***Arguments:***

- `transaction`: instance of `PartitionTransaction`
- `prefix`: serialized key prefix shared across calls
- `timestamp`: optional event-time of the current record (ms).
Used by TTL-aware partitions to stamp values on ``set()`` with
``record.timestamp + ttl`` and to filter expired entries on
``get()``. The framework injects this on every record via the
``StreamingDataFrame`` stateful wrapper.

<a id="quixstreams.state.base.state.TransactionState.get"></a>

<br><br>

#### TransactionState.get

```python
def get(key: K, default: Optional[V] = None) -> Optional[V]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L135)

Get the value for key if key is present in the state, else default


<br>
***Arguments:***

- `key`: key
- `default`: default value to return if the key is not found


<br>
***Returns:***

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.base.state.TransactionState.get_bytes"></a>

<br><br>

#### TransactionState.get\_bytes

```python
def get_bytes(key: K, default: Optional[bytes] = None) -> Optional[bytes]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L156)

Get the bytes value for key if key is present in the state, else default


<br>
***Arguments:***

- `key`: key
- `default`: default value to return if the key is not found


<br>
***Returns:***

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.base.state.TransactionState.set"></a>

<br><br>

#### TransactionState.set

```python
def set(key: K, value: V, ttl: Optional[timedelta] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L171)

Set value for the key, optionally with a per-write expiry.


<br>
***Arguments:***

- `key`: key
- `value`: value
- `ttl`: optional event-time TTL. See :class:`State.set`.

<a id="quixstreams.state.base.state.TransactionState.set_bytes"></a>

<br><br>

#### TransactionState.set\_bytes

```python
def set_bytes(key: K, value: bytes, ttl: Optional[timedelta] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L187)

Set bytes value for the key, optionally with a per-write expiry.


<br>
***Arguments:***

- `key`: key
- `value`: value as bytes
- `ttl`: optional event-time TTL. See :class:`State.set`.

<a id="quixstreams.state.base.state.TransactionState.delete"></a>

<br><br>

#### TransactionState.delete

```python
def delete(key: K)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L203)

Delete value for the key.

This function always returns `None`, even if value is not found.


<br>
***Arguments:***

- `key`: key

<a id="quixstreams.state.base.state.TransactionState.exists"></a>

<br><br>

#### TransactionState.exists

```python
def exists(key: K) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L212)

Check if the key exists in state.


<br>
***Arguments:***

- `key`: key


<br>
***Returns:***

True if key exists, False otherwise

<a id="quixstreams.state.rocksdb.options"></a>

## quixstreams.state.rocksdb.options

<a id="quixstreams.state.rocksdb.options.RocksDBOptions"></a>

### RocksDBOptions

```python
@dataclasses.dataclass(frozen=True)
class RocksDBOptions(RocksDBOptionsType)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/options.py#L60)

RocksDB database options.


<br>
***Arguments:***

- `dumps`: function to dump data to JSON
- `loads`: function to load data from JSON
- `open_max_retries`: number of times to retry opening the database
if it's locked by another process. To disable retrying, pass 0
- `open_retry_backoff`: number of seconds to wait between each retry.
- `on_corrupted_recreate`: when True, the corrupted DB will be destroyed
if the `use_changelog_topics=True` is also set on the Application.
If this option is True, but `use_changelog_topics=False`,
the DB won't be destroyed.
Note: risk of data loss! Make sure that the changelog topics are up-to-date before disabling it in production.
Default - `True`.
- `block_cache_size`: size of the RocksDB block cache, in bytes.
This is an **aggregate ceiling for the whole process**: one cache of this
size is shared by every store partition, following RocksDB's own guidance
that a single block cache be shared across databases. It is not a
per-partition budget, so a 32-partition application does not reserve 32
times this value.
A cache is filled lazily, so a small deployment never realizes the full
capacity; size it against total available memory rather than per store.
Note that index and bloom-filter blocks are held outside this budget
(``cache_index_and_filter_blocks`` is not enabled), so real usage is this
value plus roughly ``bloom_filter_bits_per_key / 8`` bytes per key.
Default - ``1 GiB``.
- `max_evictions_per_flush`: cap on TTL-driven evictions performed
during a single ``flush()`` for stores with TTL enabled. Larger values
increase per-flush latency but let the sweep keep up with higher
steady-state expiration rates. Only meaningful for TTL-enabled
stores; ignored otherwise.

This is the sweep's throughput dial, and it is the one to reach for:
the drain rate is ``max_evictions_per_flush / commit_interval``, but a
checkpoint's cost is mostly *fixed* (a producer flush barrier plus an
offset commit, milliseconds against a remote broker), so shrinking
``commit_interval`` buys no drain speed and starves message processing.
Raise this instead.

**Interaction with the producer queue.** Each eviction produces one
changelog tombstone (see ``ttl_changelog_tombstones``), so a sweep can
enqueue far more records than librdkafka's
``queue.buffering.max.messages`` (default ``100_000``) would appear to
allow. In practice it does not, because ``Producer.produce()`` polls
after every produce, so against a live broker the queue is a rolling
window rather than an accumulator: enqueueing ``150_000`` tombstones was
measured to peak at a queue depth of **~6,800 (6.8% of the default)**,
with the checkpoint's producer flush completing in ~4ms. The drain rate
governs, not the queue depth.

Two situations still bound it: the peak depth scales with
partitions-per-process (roughly ~15 partitions sweeping concurrently at
that depth would approach the default cap, so raise
``queue.buffering.max.messages`` for high partition counts), and a broker
that is not draining at all will raise ``BufferError`` once the queue
genuinely fills — though by then the application has larger problems.
``queue.buffering.max.kbytes`` (~1 GB default) is a second cap that binds
first for multi-KB records.
Default - ``150_000``. Measured on one partition against a live broker: a
full ``150_000``-eviction sweep completed in **1.12s** (index scan 0.31s,
produces 0.53s, flush 0.004s, commit 0.27s) — a ~268x margin under a 300s
``max.poll.interval.ms``. The original ``10_000`` sustained only ~300
evictions/s once checkpoints grew, which loses the race against expiry
and lets the store grow without bound.
- `legacy_records_ttl`: expiry for pre-existing records when enabling
TTL on a **populated** legacy store that already holds un-stamped
records. When ``None`` (the default), the migration still completes:
the pre-existing records are backfilled using the ttl the service
itself uses (the max ``ttl=`` in the triggering flush) and a WARNING
names the implicit value. When set to a strictly positive
``timedelta``, that value is used instead: the partition **backfills**
its pre-existing un-stamped records with a uniform expiry of
``high_water + legacy_records_ttl`` (event-time high-water at the
enable moment) and flips into TTL mode in place — no state deletion.
New records keep getting their true event-time expiry. The backfill
runs exactly once; a redeploy / restart never re-runs it. Ignored for
windowed / timestamped stores (they opt out of the TTL stamp
machinery at the class level). Must be strictly positive if set;
``<= 0`` raises ``ValueError`` at construction.
Default - ``None``.
- `legacy_backfill_chunk_size`: number of pre-existing records re-stamped
per write-batch during the one-time legacy backfill (see
``legacy_records_ttl``). The backfill iterates the populated default CF
in chunks of this size; each chunk is re-stamped, produced to the
changelog, flushed, and committed before the next chunk is read, so peak
transient memory is bounded to one chunk regardless of total store size.
Lower it on memory-constrained deployments. Only meaningful on the single
backfilling flush; ignored otherwise and on windowed / timestamped
stores. Must be strictly positive; ``<= 0`` raises ``ValueError`` at
construction.
Default - ``150_000``. Raising it mainly reduces the number of confirming
flushes (one broker round-trip each), whose fixed cost otherwise dominates
a large backfill; the producer-queue note under
``max_evictions_per_flush`` applies here too.
- `ttl_changelog_tombstones`: when ``True`` (the default), TTL-driven
evictions are also produced to the changelog as tombstones
(``value=None``) so log compaction physically reclaims expired keys in
step with the local store — ``cleanup.policy=compact`` alone then shrinks
the changelog as keys expire (no ``delete`` policy / retention tuning
needed to reclaim). When ``False``, evictions are local-only (the
pre-change behavior): the changelog keeps each expired key's last record
until compacted by other means, and rebuilds rely on the read-time
expiry filter. Read-time consistency is identical either way. Only
meaningful for TTL-enabled stores; ignored for windowed / timestamped
stores and for no-``ttl=`` workloads.
Default - ``True``.

Please see `rocksdict.Options` for a complete description of other options.

<a id="quixstreams.state.rocksdb.options.RocksDBOptions.to_options"></a>

<br><br>

#### RocksDBOptions.to\_options

```python
def to_options() -> rocksdict.Options
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/options.py#L237)

Convert parameters to `rocksdict.Options`


<br>
***Returns:***

instance of `rocksdict.Options`

