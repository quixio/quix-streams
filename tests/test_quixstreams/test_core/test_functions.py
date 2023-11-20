import pytest

from quixstreams.core.stream.functions import (
    ApplyFunction,
    ApplyExpandFunction,
    FilterFunction,
    UpdateFunction,
    Filtered,
)


class TestFunctions:
    def test_apply_function(self):
        func = ApplyFunction(lambda v: v)
        assert func.get_executor()(1) == 1

    def test_apply_expand_function(self):
        func = ApplyExpandFunction(lambda v: [v, v])
        result = func.get_executor()(1)
        assert isinstance(result, list)
        assert result == [1, 1]

    def test_update_function(self):
        value = [0]
        expected = [0, 1]
        func = UpdateFunction(lambda v: v.append(1))
        assert func.get_executor()(value) == expected

    @pytest.mark.parametrize(
        "value, filtered",
        [
            (1, True),
            (0, False),
        ],
    )
    def test_filter_function(self, value, filtered):
        func = FilterFunction(lambda v: v == 0)

        if filtered:
            with pytest.raises(Filtered):
                func.get_executor()(value)
        else:
            assert func.get_executor()(value) == value
