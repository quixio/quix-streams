import uuid
from typing import Optional

import pytest

from streamingdataframes.state import StateStoreManager


@pytest.fixture()
def state_manager_factory(tmp_path):
    def factory(
        group_id: Optional[str] = None, state_dir: Optional[str] = None
    ) -> StateStoreManager:
        group_id = group_id or str(uuid.uuid4())
        state_dir = state_dir or str(uuid.uuid4())
        return StateStoreManager(group_id=group_id, state_dir=str(tmp_path / state_dir))

    return factory


@pytest.fixture()
def state_manager(state_manager_factory) -> StateStoreManager:
    manager = state_manager_factory()
    manager.init()
    yield manager
    manager.close()
