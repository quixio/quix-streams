from contextlib import contextmanager

import pytest

from quixstreams.state.rocksdb.windowed.metadata import VALUES_CF_NAME
from quixstreams.state.serialization import encode_integer_pair


@pytest.fixture
def store(windowed_rocksdb_store_factory):
    store = windowed_rocksdb_store_factory()
    store.assign_partition(0)
    return store


@pytest.fixture
def transaction_state(store):
    @contextmanager
    def _transaction_state():
        with store.start_partition_transaction(0) as tx:
            yield tx.as_state(prefix=b"__key__")

    return _transaction_state


@pytest.fixture
def get_value(transaction_state):
    # This helper function retrieves a value directly from RocksDB.
    # Note: It will not check the update cache.

    def _get_value(timestamp_ms: int, counter: int = 0):
        with transaction_state() as state:
            return state._transaction.get(
                key=encode_integer_pair(timestamp_ms, counter),
                prefix=state._prefix,
                cf_name=VALUES_CF_NAME,
            )

    return _get_value


@pytest.mark.parametrize("value", [1, [3, 1], [3, None]])
def test_update_window(transaction_state, value):
    with transaction_state() as state:
        state.update_window(start_ms=0, end_ms=10, value=value, timestamp_ms=2)
        assert state.get_window(start_ms=0, end_ms=10) == value

    with transaction_state() as state:
        assert state.get_window(start_ms=0, end_ms=10) == value


@pytest.mark.parametrize(
    "db_windows, cached_windows, deleted_windows, get_windows_args, expected_windows",
    [
        pytest.param(
            [
                dict(start_ms=1, end_ms=11, value=1, timestamp_ms=1),
                dict(start_ms=2, end_ms=12, value=2, timestamp_ms=2),
                dict(start_ms=3, end_ms=13, value=3, timestamp_ms=3),
            ],
            [],
            [],
            dict(start_from_ms=1, start_to_ms=2),
            [((2, 12), 2, b"__key__")],
            id="start-from-exclusive-start-to-inclusive",
        ),
        pytest.param(
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
            ],
            [],
            [],
            dict(start_from_ms=-1, start_to_ms=2),
            [
                ((0, 10), 1, b"__key__"),
                ((1, 11), 2, b"__key__"),
                ((2, 12), 3, b"__key__"),
            ],
            id="messages-in-db",
        ),
        pytest.param(
            [],
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
            ],
            [],
            dict(start_from_ms=-1, start_to_ms=2),
            [
                ((0, 10), 1, b"__key__"),
                ((1, 11), 2, b"__key__"),
                ((2, 12), 3, b"__key__"),
            ],
            id="messages-in-cache",
        ),
        pytest.param(
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
            ],
            [
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
            ],
            [],
            dict(start_from_ms=-1, start_to_ms=2),
            [
                ((0, 10), 1, b"__key__"),
                ((1, 11), 2, b"__key__"),
                ((2, 12), 3, b"__key__"),
            ],
            id="messages-both-in-db-and-in-cache",
        ),
        pytest.param(
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=3, end_ms=13, value=4, timestamp_ms=4),
            ],
            [
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
                dict(start_ms=0, end_ms=10, value=5, timestamp_ms=1),
            ],
            [],
            dict(start_from_ms=-1, start_to_ms=3),
            [
                ((0, 10), 5, b"__key__"),
                ((1, 11), 2, b"__key__"),
                ((2, 12), 3, b"__key__"),
                ((3, 13), 4, b"__key__"),
            ],
            id="cache-message-overrides-db-message",
        ),
        pytest.param(
            [
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
            ],
            [],
            [dict(start_ms=0, end_ms=10)],
            dict(start_from_ms=-1, start_to_ms=2),
            [((1, 11), 2, b"__key__"), ((2, 12), 3, b"__key__")],
            id="ignore-deleted-windows",
        ),
        pytest.param(
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
            ],
            [],
            [],
            dict(start_from_ms=-1, start_to_ms=2, backwards=True),
            [
                ((2, 12), 3, b"__key__"),
                ((1, 11), 2, b"__key__"),
                ((0, 10), 1, b"__key__"),
            ],
            id="messages-in-db-backwards",
        ),
        pytest.param(
            [],
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
            ],
            [],
            dict(start_from_ms=-1, start_to_ms=2, backwards=True),
            [
                ((2, 12), 3, b"__key__"),
                ((1, 11), 2, b"__key__"),
                ((0, 10), 1, b"__key__"),
            ],
            id="messages-in-cache-backwards",
        ),
        pytest.param(
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
            ],
            [
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
            ],
            [],
            dict(start_from_ms=-1, start_to_ms=2, backwards=True),
            [
                ((2, 12), 3, b"__key__"),
                ((1, 11), 2, b"__key__"),
                ((0, 10), 1, b"__key__"),
            ],
            id="messages-both-in-db-and-in-cache-backwards",
        ),
        pytest.param(
            [
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=3, end_ms=13, value=4, timestamp_ms=4),
            ],
            [
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
                dict(start_ms=0, end_ms=10, value=5, timestamp_ms=1),
            ],
            [],
            dict(start_from_ms=-1, start_to_ms=3, backwards=True),
            [
                ((3, 13), 4, b"__key__"),
                ((2, 12), 3, b"__key__"),
                ((1, 11), 2, b"__key__"),
                ((0, 10), 5, b"__key__"),
            ],
            id="cache-message-overrides-db-message",
        ),
        pytest.param(
            [
                dict(start_ms=0, end_ms=10, value=1, timestamp_ms=1),
                dict(start_ms=1, end_ms=11, value=2, timestamp_ms=2),
                dict(start_ms=2, end_ms=12, value=3, timestamp_ms=3),
            ],
            [],
            [dict(start_ms=0, end_ms=10)],
            dict(start_from_ms=-1, start_to_ms=2, backwards=True),
            [((2, 12), 3, b"__key__"), ((1, 11), 2, b"__key__")],
            id="ignore-deleted-windows",
        ),
    ],
)
def test_get_windows(
    db_windows,
    cached_windows,
    deleted_windows,
    get_windows_args,
    expected_windows,
    transaction_state,
):
    with transaction_state() as state:
        for window in db_windows:
            state.update_window(**window)

    with transaction_state() as state:
        for window in cached_windows:
            state.update_window(**window)
        for window in deleted_windows:
            state._transaction.delete_window(**window, prefix=state._prefix)

        windows = state.get_windows(**get_windows_args)
        assert list(windows) == expected_windows


@pytest.mark.parametrize("value", [1, "string", None, ["list"], {"dict": "dict"}])
def test_add_to_collection(transaction_state, get_value, value):
    with transaction_state() as state:
        state.add_to_collection(value=value, id=11)
        state.add_to_collection(value=value, id=22)
        state.add_to_collection(value=value, id=33)

    assert get_value(timestamp_ms=11, counter=0) == value
    assert get_value(timestamp_ms=22, counter=1) == value
    assert get_value(timestamp_ms=33, counter=2) == value
