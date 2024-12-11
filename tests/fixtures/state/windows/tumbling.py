import pytest

from quixstreams.dataframe.windows import TumblingWindowDefinition


@pytest.fixture()
def tumbling_window_definition_factory(state_manager, dataframe_factory):
    def factory(duration_ms: int, grace_ms: int = 0) -> TumblingWindowDefinition:
        sdf = dataframe_factory(state_manager=state_manager)
        window_def = TumblingWindowDefinition(
            duration_ms=duration_ms, grace_ms=grace_ms, dataframe=sdf
        )
        return window_def

    return factory
