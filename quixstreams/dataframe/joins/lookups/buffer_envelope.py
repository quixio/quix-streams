import base64
from typing import Any, Callable, Iterable, Mapping, NamedTuple, Optional, Union

from quixstreams.state.exceptions import StateSerializationError

__all__ = (
    "ENVELOPE_BYTES",
    "ENVELOPE_CONTAINERS",
    "ENVELOPE_HEADERS",
    "ENVELOPE_OFFSET",
    "ENVELOPE_RECEIVED",
    "ENVELOPE_TIMESTAMP",
    "ENVELOPE_TOPIC",
    "ENVELOPE_VALUE",
    "NO_OFFSET",
    "Emission",
    "decode_headers",
    "describe_unstorable",
    "emit_tuple",
    "encode_envelope",
    "envelope_value",
)

ENVELOPE_VALUE = "v"
ENVELOPE_TIMESTAMP = "t"
ENVELOPE_RECEIVED = "r"
ENVELOPE_HEADERS = "h"
ENVELOPE_HEADERS_MAPPING = "m"
ENVELOPE_BYTES = "b"
ENVELOPE_CONTAINERS = "c"
ENVELOPE_TOPIC = "n"
ENVELOPE_OFFSET = "o"

NO_OFFSET = -1

_HEADER_BYTES = "b"
_HEADER_STR = "s"
_HEADER_NONE = "n"

_CONTAINER_TUPLE = "t"
_CONTAINER_SET = "s"
_CONTAINER_FROZENSET = "f"

_BINARY = (bytes, bytearray, memoryview)

_Step = Union[str, int]

_DESCRIBE_MAX_DEPTH = 20

_DESCRIBE_MAX_KEYS = 3

_DESCRIBE_FIELDS = {ENVELOPE_VALUE: "value", ENVELOPE_HEADERS: "headers"}

_INT_MIN = -(2**63)
_INT_MAX = 2**64 - 1


class Emission(NamedTuple):
    value: Any
    key: Any
    timestamp: int
    headers: Any
    topic: Optional[str]
    offset: int


def encode_envelope(
    value: Any,
    timestamp: int,
    receive_ms: int,
    headers: Any,
    topic: Optional[str] = None,
    offset: int = NO_OFFSET,
) -> dict[str, Any]:
    encoded_headers, is_mapping = _encode_headers(headers)
    stored_value, byte_paths, container_paths = _lift_value(value)
    return {
        ENVELOPE_VALUE: stored_value,
        ENVELOPE_TIMESTAMP: timestamp,
        ENVELOPE_RECEIVED: receive_ms,
        ENVELOPE_HEADERS: encoded_headers,
        ENVELOPE_HEADERS_MAPPING: is_mapping,
        ENVELOPE_BYTES: byte_paths,
        ENVELOPE_CONTAINERS: container_paths,
        ENVELOPE_TOPIC: topic,
        ENVELOPE_OFFSET: offset,
    }


def envelope_value(envelope: Mapping[str, Any]) -> Any:
    value = _restore_bytes(envelope[ENVELOPE_VALUE], envelope.get(ENVELOPE_BYTES))
    return _restore_containers(value, envelope.get(ENVELOPE_CONTAINERS))


def emit_tuple(envelope: Mapping[str, Any], key: Any) -> Emission:
    return Emission(
        envelope_value(envelope),
        key,
        envelope[ENVELOPE_TIMESTAMP],
        decode_headers(envelope),
        envelope.get(ENVELOPE_TOPIC),
        envelope.get(ENVELOPE_OFFSET, NO_OFFSET),
    )


def decode_headers(envelope: Mapping[str, Any]) -> Any:
    encoded = envelope[ENVELOPE_HEADERS]
    if encoded is None:
        return None

    items: list[tuple[Any, Any]] = []
    for name, kind, payload in encoded:
        if kind == _HEADER_BYTES:
            items.append((name, base64.b64decode(payload)))
        elif kind == _HEADER_NONE:
            items.append((name, None))
        else:
            items.append((name, payload))

    if envelope[ENVELOPE_HEADERS_MAPPING]:
        return dict(items)
    return items


def describe_unstorable(
    envelope: Mapping[str, Any],
    probe: Callable[[Any], Any],
) -> str:
    path: list[_Step] = []
    node: Any = envelope
    seen: set[int] = set()
    for _ in range(_DESCRIBE_MAX_DEPTH):
        if id(node) in seen:
            return f"{_render_path(path)}: a reference cycle"
        seen.add(id(node))
        refused = _refused_child(node, probe)
        if refused is None:
            break
        path.append(refused[0])
        node = refused[1]
    return f"{_render_path(path)}: {_refusal_reason(node)}"


def _refused_child(
    node: Any,
    probe: Callable[[Any], Any],
) -> Optional[tuple[_Step, Any]]:
    items: Iterable[tuple[Any, Any]]
    if isinstance(node, dict):
        items = node.items()
    elif isinstance(node, list):
        items = enumerate(node)
    else:
        return None

    for step, child in items:
        try:
            probe(child)
        except StateSerializationError:
            return step, child
    return None


def _refusal_reason(node: Any) -> str:
    if isinstance(node, dict):
        keys = [key for key in node if not isinstance(key, str)]
        if keys:
            named = ", ".join(
                f"{key!r} ({type(key).__name__})" for key in keys[:_DESCRIBE_MAX_KEYS]
            )
            return f"a dict whose keys are not strings: {named}"
    if (
        isinstance(node, int)
        and not isinstance(node, bool)
        and not _INT_MIN <= node <= _INT_MAX
    ):
        return "an integer outside the 64-bit range"
    return f"a value of type {type(node).__name__}"


def _render_path(path: list[_Step]) -> str:
    if not path:
        return "the envelope itself"
    head, rest = path[0], path[1:]
    name = _DESCRIBE_FIELDS.get(head, "") if isinstance(head, str) else ""
    rendered = name or f"envelope[{head!r}]"
    return rendered + "".join(f"[{step!r}]" for step in rest)


def _encode_headers(headers: Any) -> tuple[Optional[list[list[Any]]], bool]:
    if headers is None:
        return None, False

    if isinstance(headers, Mapping):
        pairs: Any = headers.items()
        is_mapping = True
    else:
        pairs = headers
        is_mapping = False

    encoded: list[list[Any]] = []
    for name, value in pairs:
        if isinstance(value, _BINARY):
            encoded.append(
                [name, _HEADER_BYTES, base64.b64encode(bytes(value)).decode()]
            )
        elif value is None:
            encoded.append([name, _HEADER_NONE, None])
        else:
            encoded.append([name, _HEADER_STR, value])
    return encoded, is_mapping


def _lift_value(
    value: Any,
) -> tuple[Any, Optional[list[list[Any]]], Optional[list[list[Any]]]]:
    if not _needs_lift(value):
        return value, None, None

    byte_paths: list[list[Any]] = []
    container_paths: list[list[Any]] = []
    body = _strip(value, [], byte_paths, container_paths)
    return body, byte_paths or None, container_paths or None


def _needs_lift(value: Any) -> bool:
    if isinstance(value, _BINARY) or _container_kind(value) is not None:
        return True
    if isinstance(value, dict):
        return any(_needs_lift(item) for item in value.values())
    if isinstance(value, list):
        return any(_needs_lift(item) for item in value)
    return False


def _container_kind(value: Any) -> Optional[str]:
    if isinstance(value, tuple):
        return _CONTAINER_TUPLE
    if isinstance(value, frozenset):
        return _CONTAINER_FROZENSET
    if isinstance(value, set):
        return _CONTAINER_SET
    return None


def _strip(
    value: Any,
    path: list[_Step],
    byte_paths: list[list[Any]],
    container_paths: list[list[Any]],
) -> Any:
    if isinstance(value, _BINARY):
        byte_paths.append([list(path), base64.b64encode(bytes(value)).decode()])
        return None

    if isinstance(value, dict):
        copied: dict[Any, Any] = {}
        for name, item in value.items():
            path.append(name)
            copied[name] = _strip(item, path, byte_paths, container_paths)
            path.pop()
        return copied

    kind = _container_kind(value)
    if kind is not None or isinstance(value, list):
        if kind is not None:
            container_paths.append([list(path), kind])
        items: list[Any] = []
        for index, item in enumerate(value):
            path.append(index)
            items.append(_strip(item, path, byte_paths, container_paths))
            path.pop()
        return items

    return value


def _restore_bytes(value: Any, paths: Optional[list[list[Any]]]) -> Any:
    if not paths:
        return value

    for path, payload in paths:
        decoded = base64.b64decode(payload)
        if not path:
            return decoded
        target = value
        for step in path[:-1]:
            target = target[step]
        target[path[-1]] = decoded
    return value


def _restore_containers(value: Any, paths: Optional[list[list[Any]]]) -> Any:
    if not paths:
        return value

    for path, kind in sorted(paths, key=lambda entry: len(entry[0]), reverse=True):
        if not path:
            return _rebuild_container(value, kind)
        parent = value
        for step in path[:-1]:
            parent = parent[step]
        parent[path[-1]] = _rebuild_container(parent[path[-1]], kind)
    return value


def _rebuild_container(items: Any, kind: str) -> Any:
    if kind == _CONTAINER_TUPLE:
        return tuple(items)
    if kind == _CONTAINER_SET:
        return set(items)
    if kind == _CONTAINER_FROZENSET:
        return frozenset(items)
    return items
