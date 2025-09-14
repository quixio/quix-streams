from quixstreams.state.manager import StateStoreManager
from quixstreams.state.slatedb.store import SlateDBStore
from quixstreams.state.slatedb.timestamped import TimestampedSlateDBStore
from quixstreams.state.slatedb.windowed.store import WindowedSlateDBStore


def test_state_manager_registers_windowed_slatedb_when_default_is_slatedb():
    ssm = StateStoreManager(
        default_store_type=SlateDBStore, state_dir="/tmp/state-smoke"
    )
    ssm.register_windowed_store(stream_id="topic-x", store_name="win")
    store = ssm.stores.get("topic-x", {}).get("win")
    assert isinstance(store, WindowedSlateDBStore)


def test_state_manager_registers_timestamped_slatedb_when_default_is_slatedb():
    ssm = StateStoreManager(
        default_store_type=SlateDBStore, state_dir="/tmp/state-smoke"
    )
    ssm.register_timestamped_store(
        stream_id="topic-y",
        store_name="ts",
        grace_ms=0,
        keep_duplicates=True,
    )
    store = ssm.stores.get("topic-y", {}).get("ts")
    assert isinstance(store, TimestampedSlateDBStore)
