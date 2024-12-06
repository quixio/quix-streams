<a id="quixstreams.state.base.state"></a>

## quixstreams.state.base.state

<a id="quixstreams.state.base.state.State"></a>

### State

```python
class State(ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L13)

Primary interface for working with key-value state data from `StreamingDataFrame`

<a id="quixstreams.state.base.state.State.get"></a>

<br><br>

#### State.get

```python
@abstractmethod
def get(key: Any, default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L19)

Get the value for key if key is present in the state, else default


<br>
***Arguments:***

- `key`: key
- `default`: default value to return if the key is not found


<br>
***Returns:***

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.base.state.State.set"></a>

<br><br>

#### State.set

```python
@abstractmethod
def set(key: Any, value: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L30)

Set value for the key.


<br>
***Arguments:***

- `key`: key
- `value`: value

<a id="quixstreams.state.base.state.State.delete"></a>

<br><br>

#### State.delete

```python
@abstractmethod
def delete(key: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L39)

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
def exists(key: Any) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L49)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L58)

<a id="quixstreams.state.base.state.TransactionState.__init__"></a>

<br><br>

#### TransactionState.\_\_init\_\_

```python
def __init__(prefix: bytes, transaction: "PartitionTransaction")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L64)

Simple key-value state to be provided into `StreamingDataFrame` functions


<br>
***Arguments:***

- `transaction`: instance of `PartitionTransaction`

<a id="quixstreams.state.base.state.TransactionState.get"></a>

<br><br>

#### TransactionState.get

```python
def get(key: Any, default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L73)

Get the value for key if key is present in the state, else default


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
def set(key: Any, value: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L83)

Set value for the key.


<br>
***Arguments:***

- `key`: key
- `value`: value

<a id="quixstreams.state.base.state.TransactionState.delete"></a>

<br><br>

#### TransactionState.delete

```python
def delete(key: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L91)

Delete value for the key.

This function always returns `None`, even if value is not found.


<br>
***Arguments:***

- `key`: key

<a id="quixstreams.state.base.state.TransactionState.exists"></a>

<br><br>

#### TransactionState.exists

```python
def exists(key: Any) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L100)

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
Please see `rocksdict.Options` for a complete description of other options.

<a id="quixstreams.state.rocksdb.options.RocksDBOptions.to_options"></a>

<br><br>

#### RocksDBOptions.to\_options

```python
def to_options() -> rocksdict.Options
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/options.py#L54)

Convert parameters to `rocksdict.Options`


<br>
***Returns:***

instance of `rocksdict.Options`

