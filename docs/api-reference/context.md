<a id="quixstreams.context"></a>

## quixstreams.context

<a id="quixstreams.context.set_message_context"></a>

<br><br>

#### set\_message\_context

```python
def set_message_context(context: Optional[MessageContext])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/356d83c8caf613065f333dcd470e004443c12544/quixstreams/context.py#L21)

Set a MessageContext for the current message in the given `contextvars.Context`

>***NOTE:*** This is for advanced usage only. If you need to change the message key,
`StreamingDataFrame.to_topic()` has an argument for it.



<br>
***Example Snippet:***

```python
from quixstreams import Application, set_message_context, message_context

# Changes the current sdf value based on what the message partition is.
def alter_context(value):
    context = message_context()
    if value > 1:
        context.headers = context.headers + (b"cool_new_header", value.encode())
        set_message_context(context)

app = Application()
sdf = app.dataframe()
sdf = sdf.update(lambda value: alter_context(value))
```


<br>
***Arguments:***

- `context`: instance of `MessageContext`

<a id="quixstreams.context.message_context"></a>

<br><br>

#### message\_context

```python
def message_context() -> MessageContext
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/356d83c8caf613065f333dcd470e004443c12544/quixstreams/context.py#L52)

Get a MessageContext for the current message, which houses most of the message

metadata, like:
    - key
    - timestamp
    - partition
    - offset



<br>
***Example Snippet:***

```python
from quixstreams import Application, message_context

# Changes the current sdf value based on what the message partition is.

app = Application()
sdf = app.dataframe()
sdf = sdf.apply(lambda value: 1 if message_context().partition == 2 else 0)
```


<br>
***Returns:***

instance of `MessageContext`

<a id="quixstreams.context.message_key"></a>

<br><br>

#### message\_key

```python
def message_key() -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/356d83c8caf613065f333dcd470e004443c12544/quixstreams/context.py#L83)

Get the current message's key.


<br>
***Example Snippet:***

```python
from quixstreams import Application, message_key

# Changes the current sdf value based on what the message key is.

app = Application()
sdf = app.dataframe()
sdf = sdf.apply(lambda value: 1 if message_key() == b'1' else 0)
```


<br>
***Returns:***

a deserialized message key

