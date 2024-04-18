<a id="quixstreams.state.types"></a>

## quixstreams.state.types

<a id="quixstreams.state.types.State"></a>

### State

```python
class State(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/fbc7096491954c22a9772fa6916b154d1621bf26/quixstreams/state/types.py#L151)

Primary interface for working with key-value state data from `StreamingDataFrame`

<a id="quixstreams.state.types.State.get"></a>

<br><br>

#### State.get

```python
def get(key: Any, default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/fbc7096491954c22a9772fa6916b154d1621bf26/quixstreams/state/types.py#L156)

Get the value for key if key is present in the state, else default


<br>
***Arguments:***

- `key`: key
- `default`: default value to return if the key is not found


<br>
***Returns:***

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.types.State.set"></a>

<br><br>

#### State.set

```python
def set(key: Any, value: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/fbc7096491954c22a9772fa6916b154d1621bf26/quixstreams/state/types.py#L166)

Set value for the key.


<br>
***Arguments:***

- `key`: key
- `value`: value

<a id="quixstreams.state.types.State.delete"></a>

<br><br>

#### State.delete

```python
def delete(key: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/fbc7096491954c22a9772fa6916b154d1621bf26/quixstreams/state/types.py#L174)

Delete value for the key.

This function always returns `None`, even if value is not found.


<br>
***Arguments:***

- `key`: key

<a id="quixstreams.state.types.State.exists"></a>

<br><br>

#### State.exists

```python
def exists(key: Any) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/fbc7096491954c22a9772fa6916b154d1621bf26/quixstreams/state/types.py#L183)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/fbc7096491954c22a9772fa6916b154d1621bf26/quixstreams/state/rocksdb/options.py#L25)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/fbc7096491954c22a9772fa6916b154d1621bf26/quixstreams/state/rocksdb/options.py#L53)

Convert parameters to `rocksdict.Options`


<br>
***Returns:***

instance of `rocksdict.Options`

