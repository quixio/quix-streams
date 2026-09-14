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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/options.py#L26)

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
- `max_evictions_per_flush`: cap on TTL-driven evictions performed
during a single ``flush()`` for stores with TTL enabled. Larger values
increase per-flush latency but let the sweep keep up with higher
steady-state expiration rates. Only meaningful for TTL-enabled
stores; ignored otherwise.
Default - ``10_000``.

Please see `rocksdict.Options` for a complete description of other options.

<a id="quixstreams.state.rocksdb.options.RocksDBOptions.to_options"></a>

<br><br>

#### RocksDBOptions.to\_options

```python
def to_options() -> rocksdict.Options
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/options.py#L69)

Convert parameters to `rocksdict.Options`


<br>
***Returns:***

instance of `rocksdict.Options`

