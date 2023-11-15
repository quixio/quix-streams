import pytest

from quixstreams.core.stream.functions import (
    Apply,
    Filter,
    Update,
    Filtered,
    is_apply_function,
    is_filter_function,
    is_update_function,
)


class TestFunctions:
    def test_apply_function(self):
        func = Apply(lambda v: v)
        assert func(1) == 1
        assert is_apply_function(func)

    def test_update_function(self):
        value = [0]
        expected = [0, 1]
        func = Update(lambda v: v.append(1))
        assert func(value) == expected
        assert is_update_function(func)

    @pytest.mark.parametrize(
        "value, filtered",
        [
            (1, True),
            (0, False),
        ],
    )
    def test_filter_function(self, value, filtered):
        func = Filter(lambda v: v == 0)
        assert is_filter_function(func)

        if filtered:
            with pytest.raises(Filtered):
                func(value)
        else:
            assert func(value) == value
