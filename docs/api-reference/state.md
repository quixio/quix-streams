<a id="quixstreams.state.base.state"></a>

## quixstreams.state.base.state

<a id="quixstreams.state.base.state.State"></a>

### State

```python
class State(ABC, Generic[K, V])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\base\state.py#L17)

Primary interface for working with key-value state data from `StreamingDataFrame`

<a id="quixstreams.state.base.state.State.get"></a>

<br><br>

#### State.get

```python
@abstractmethod
def get(key: K, default: Optional[V] = None) -> Optional[V]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\base\state.py#L29)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\base\state.py#L45)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\base\state.py#L55)

Set value for the key.

Pass `ttl=timedelta(...)` to expire the entry after that duration of event time. Omit `ttl` (or pass `None`) for an entry that never expires.

```python
# Expires 7 days after the event timestamp of the record that writes it.
state.set("msg_id", 1, ttl=timedelta(days=7))

# Never expires (default behavior).
state.set("config_key", "value")
```

See [State TTL](../advanced/stateful-processing.md#state-ttl) for full usage, patterns, and troubleshooting.


<br>
***Arguments:***

- `key`: key
- `value`: value
- `ttl`: optional expiry duration as a `timedelta`. Must be greater than zero if provided. Expiry is event-time-based, not wall-clock. Default `None` — never expires.

<a id="quixstreams.state.base.state.State.set_bytes"></a>

<br><br>

#### State.set\_bytes

```python
@abstractmethod
def set_bytes(key: K, value: bytes, ttl: Optional[timedelta] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\base\state.py#L64)

Set value for the key. Raw-bytes variant of [`State.set()`](#stateset) — use this when you handle serialization yourself.

Pass `ttl=timedelta(...)` to expire the entry after that duration of event time. See [`State.set()`](#stateset) for TTL semantics.


<br>
***Arguments:***

- `key`: key
- `value`: value as raw bytes
- `ttl`: optional expiry duration as a `timedelta`. Must be greater than zero if provided. Default `None` — never expires.

<a id="quixstreams.state.base.state.State.delete"></a>

<br><br>

#### State.delete

```python
@abstractmethod
def delete(key: K)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\base\state.py#L73)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\base\state.py#L83)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\base\state.py#L92)

<a id="quixstreams.state.base.state.TransactionState.__init__"></a>

<br><br>

#### TransactionState.\_\_init\_\_

```python
def __init__(prefix: bytes,
             transaction: "PartitionTransaction",
             timestamp: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\base\state.py#L99)

Simple key-value state to be provided into `StreamingDataFrame` functions


<br>
***Arguments:***

- `transaction`: instance of `PartitionTransaction`
- `prefix`: serialized key prefix shared across calls
- `timestamp`: optional event-time of the current record (ms).
TTL-enabled stores use this to stamp values on ``set()`` and to
filter expired entries on ``get()``. Non-TTL stores ignore it.

<a id="quixstreams.state.base.state.TransactionState.get"></a>

<br><br>

#### TransactionState.get

```python
def get(key: K, default: Optional[V] = None) -> Optional[V]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\base\state.py#L124)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\base\state.py#L145)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\base\state.py#L160)

Set value for the key. Pass `ttl=timedelta(...)` to expire the entry after that duration of event time. See [`State.set()`](#stateset) for full TTL semantics and examples.


<br>
***Arguments:***

- `key`: key
- `value`: value
- `ttl`: optional expiry duration as a `timedelta`. Must be greater than zero if provided. Default `None` — never expires.

<a id="quixstreams.state.base.state.TransactionState.set_bytes"></a>

<br><br>

#### TransactionState.set\_bytes

```python
def set_bytes(key: K, value: bytes, ttl: Optional[timedelta] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\base\state.py#L170)

Set value for the key. Raw-bytes variant — use this when you handle serialization yourself. Pass `ttl=timedelta(...)` to expire the entry. See [`State.set()`](#stateset) for full TTL semantics.


<br>
***Arguments:***

- `key`: key
- `value`: value as raw bytes
- `ttl`: optional expiry duration as a `timedelta`. Must be greater than zero if provided. Default `None` — never expires.

<a id="quixstreams.state.base.state.TransactionState.delete"></a>

<br><br>

#### TransactionState.delete

```python
def delete(key: K)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\base\state.py#L180)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\base\state.py#L189)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\rocksdb\options.py#L26)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/feature/sc-72538/adding-ttl-to-state/quixstreams\state\rocksdb\options.py#L69)

Convert parameters to `rocksdict.Options`


<br>
***Returns:***

instance of `rocksdict.Options`

