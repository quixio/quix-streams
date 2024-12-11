import pytest

from quixstreams.dataframe.windows import HoppingWindowDefinition


@pytest.fixture()
def hopping_window_definition_factory(state_manager, dataframe_factory):
    def factory(
        duration_ms: int, step_ms: int, grace_ms: int = 0
    ) -> HoppingWindowDefinition:
        sdf = dataframe_factory(state_manager=state_manager)
        window_def = HoppingWindowDefinition(
            duration_ms=duration_ms, step_ms=step_ms, grace_ms=grace_ms, dataframe=sdf
        )
        return window_def

    return factory
