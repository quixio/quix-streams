from typing import Literal

import pytest

from quixstreams.app import Application
from quixstreams.state.manager import StateStoreManager
from quixstreams.state.slatedb.options import SlateDBOptions
from quixstreams.state.slatedb.store import SlateDBStore


@pytest.mark.parametrize("backend", ["rocksdb", "slatedb"])
def test_application_accepts_state_backend_param(
    backend: Literal["rocksdb", "slatedb"],
):
    # Should not raise on init; we're not running the app, only checking config acceptance
    app = Application(
        broker_address="localhost:9092",
        consumer_group="g",
        state_dir="state-test",
        state_backend=backend,
        slatedb_options=SlateDBOptions(),
    )
    assert app.config.state_backend == backend


def test_state_manager_accepts_slatedb_store_type_and_options(monkeypatch):
    ssm = StateStoreManager(group_id="g", state_dir="state-test")

    # Should not raise when registering a store with SlateDBStore type
    ssm.register_store(stream_id="s", store_name="default", store_type=SlateDBStore)
    assert "default" in ssm.stores.get("s", {})
