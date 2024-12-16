import uuid
from typing import Optional

import pytest

from quixstreams.rowproducer import RowProducer
from quixstreams.state import StateStoreManager
from quixstreams.state.manager import StoreTypes
from quixstreams.state.recovery import RecoveryManager


@pytest.fixture()
def state_manager_factory(store_type, tmp_path):
    def factory(
        group_id: Optional[str] = None,
        state_dir: Optional[str] = None,
        producer: Optional[RowProducer] = None,
        recovery_manager: Optional[RecoveryManager] = None,
        default_store_type: StoreTypes = store_type,
    ) -> StateStoreManager:
        group_id = group_id or str(uuid.uuid4())
        state_dir = state_dir or str(uuid.uuid4())
        return StateStoreManager(
            group_id=group_id,
            state_dir=str(tmp_path / state_dir),
            producer=producer,
            recovery_manager=recovery_manager,
            default_store_type=default_store_type,
        )

    return factory


@pytest.fixture()
def state_manager(state_manager_factory) -> StateStoreManager:
    manager = state_manager_factory()
    manager.init()
    yield manager
    manager.close()
