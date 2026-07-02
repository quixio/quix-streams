# State TTL — Cookbook

Practical patterns for per-write expiry in Quix Streams state stores.

For the full reference — API semantics, how TTL activates on a store, upgrade
guide, configuration knobs, and troubleshooting — see
[Stateful Processing — State TTL](stateful-processing.md#state-ttl).


## Quick-start patterns

Any stateful callback receives a `state` handle. Pass `stateful=True` (and
`metadata=True` if you need the message key, timestamp, or headers) so the
callback signature is `(value, key, timestamp, headers, state)`.

### Dedup filter — expire by field value

```python
from datetime import timedelta

def dedup(value, key, timestamp, headers, state):
    if state.get("status") == value["status"]:
        return False  # same status, still within TTL — drop
    state.set("status", value["status"], ttl=timedelta(minutes=5))
    return True

sdf = sdf.filter(dedup, stateful=True, metadata=True)
```

### Short-lived lookup cache (enrich)

```python
from datetime import timedelta

def enrich(value, key, timestamp, headers, state):
    cached = state.get("profile")
    if cached is None:
        cached = fetch_profile(value["user_id"])
        state.set("profile", cached, ttl=timedelta(hours=1))
    return {**value, "profile": cached}

sdf = sdf.apply(enrich, stateful=True, metadata=True)
```

### Rolling counter with expiry

```python
from datetime import timedelta

def bump(value, key, timestamp, headers, state):
    count = (state.get("count") or 0) + 1
    state.set("count", count, ttl=timedelta(minutes=10))
    value["count"] = count

sdf = sdf.update(bump, stateful=True, metadata=True)
```


## Same TTL for every entry

Hoist the TTL into a single constant. Every entry written by the callback
expires after the same duration.

```python
from datetime import timedelta

DEFAULT_TTL = timedelta(minutes=5)

def dedup(value, key, timestamp, headers, state):
    if state.get("last_status") == value["status"]:
        return False
    state.set("last_status", value["status"], ttl=DEFAULT_TTL)
    state.set("last_seen_at", value["timestamp"], ttl=DEFAULT_TTL)
    return True

sdf = sdf.filter(dedup, stateful=True, metadata=True)
```

Both `last_status` and `last_seen_at` expire five minutes after the most
recent write.

You can reuse the same constant across multiple callbacks:

```python
from datetime import timedelta

DEFAULT_TTL = timedelta(minutes=5)

def dedup(value, key, timestamp, headers, state):
    if state.get("status") == value["status"]:
        return False
    state.set("status", value["status"], ttl=DEFAULT_TTL)
    return True

def bump(value, key, timestamp, headers, state):
    count = (state.get("count") or 0) + 1
    state.set("count", count, ttl=DEFAULT_TTL)
    value["count"] = count

sdf = sdf.filter(dedup, stateful=True, metadata=True)
sdf = sdf.update(bump, stateful=True, metadata=True)
```

To tune the window without a code change, read it from an environment variable:

```python
import os
from datetime import timedelta

DEFAULT_TTL = timedelta(seconds=int(os.environ.get("STATE_TTL_SECONDS", "300")))
```


## Refreshing a key (keep-alive)

Re-writing a key resets its TTL clock — each `state.set(key, value, ttl=...)`
starts a fresh countdown from that write, replacing whatever expiry the entry
had before:

```python
state.set("session", data, ttl=timedelta(minutes=5))
# ... 4 minutes later, another message arrives for the same key:
state.set("session", data, ttl=timedelta(minutes=5))  # clock restarts at 5:00
```

This is safe regardless of timing: a refresh always wins, even when it lands in
the exact moment the previous, already-expired entry is being cleaned up. The
refreshed value is never lost and keeps expiring normally on its new schedule.
You never need to `get` first, delete, or otherwise guard a refresh.

A refresh can also change the duration (`ttl=timedelta(hours=1)` on a key
originally written with minutes), or make the key permanent by writing it
without `ttl` — a plain `state.set(key, value)` never expires.


## Emit only on toggle or after TTL expiry

A window sensor emits readings like `window_open_ON` and `window_open_OFF`. You
want the output topic to receive an event only when:

1. **The value toggles** — e.g. `ON` → `OFF`, or `OFF` → `ON`.
2. **The TTL has expired** — no event was seen for five minutes, so the next
   reading (even if identical to the previous one) is treated as fresh and
   forwarded.

A single stateful key with a TTL handles both cases:

```python
from datetime import timedelta

TTL = timedelta(minutes=5)

def emit_on_change_or_expiry(value, key, timestamp, headers, state):
    # value["event"] is e.g. "window_open_ON" or "window_open_OFF"
    cached = state.get("last_window_event")

    if cached == value["event"]:
        # Same value, still within TTL — drop
        return False

    # Either the value toggled, or the cached entry expired (cached is None)
    state.set("last_window_event", value["event"], ttl=TTL)
    return True

sdf = sdf.filter(emit_on_change_or_expiry, stateful=True, metadata=True)
```

Behavior summary:

| Incoming event | Cached state         | Result            |
|----------------|----------------------|-------------------|
| `ON`           | `None` (expired/new) | **emit**          |
| `ON`           | `ON`                 | drop              |
| `OFF`          | `ON`                 | **emit** (toggle) |
| `OFF`          | `None` (expired)     | **emit** (TTL)    |


## Status encoded in the Kafka message key

If the status is encoded in the Kafka key itself (e.g. `window_open_ON` /
`window_open_OFF`) rather than in the value, the previous pattern breaks. Quix
Streams partitions state by message key, so `window_open_ON` and
`window_open_OFF` each get their **own** state scope and never see each other's
`last_status` — the filter would always emit.

Fix: parse the key into `entity` + `status`, then use `group_by` on the entity
so both variants share one state scope.

```python
import os
from datetime import timedelta
from quixstreams import Application

TTL = timedelta(seconds=int(os.environ.get("STATE_TTL_SECONDS", "300")))

app = Application(consumer_group="window-dedup", auto_offset_reset="earliest")
input_topic  = app.topic(os.environ["input"])   # keyed as window_open_ON / window_open_OFF
output_topic = app.topic(os.environ["output"])

sdf = app.dataframe(input_topic)

# 1. Split the key into entity + status, stash both on the value.
def parse_key(value, key, timestamp, headers):
    key_str = key.decode() if isinstance(key, (bytes, bytearray)) else key
    entity, _, status = key_str.rpartition("_")   # "window_open_ON" → ("window_open", "_", "ON")
    return {**(value or {}), "_entity": entity, "_status": status}

sdf = sdf.apply(parse_key, metadata=True)

# 2. Repartition by entity so ON and OFF share one state scope.
sdf = sdf.group_by(lambda v: v["_entity"], name="by_entity")

# 3. Emit only on toggle or after TTL expiry.
def emit_on_change_or_expiry(value, key, timestamp, headers, state):
    status = value["_status"]
    if state.get("last_status") == status:
        return False                              # same status, still within TTL — drop
    state.set("last_status", status, ttl=TTL)     # toggled OR expired → emit + reset clock
    return True

sdf = sdf.filter(emit_on_change_or_expiry, stateful=True, metadata=True)

sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()
```

`state.get("last_status")` returns `None` once the TTL expires, so the next
event (even with identical status) is treated as fresh and forwarded.


## Upgrading an existing store

If your pipeline was already running before TTL was introduced, see
[Upgrading an existing (legacy) store](stateful-processing.md#upgrading-an-existing-legacy-store--legacy_records_ttl)
for the full guide, including the before/after worked example,
`legacy_records_ttl` configuration, and what happens during a cold restore.
