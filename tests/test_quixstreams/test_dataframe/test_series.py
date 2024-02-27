import pytest

from quixstreams.dataframe.exceptions import InvalidOperation
from quixstreams.dataframe.series import StreamingSeries


class TestStreamingSeries:
    def test_apply(self):
        value = {"x": 5, "y": 20, "z": 110}
        expected = {"x": 6}
        result = StreamingSeries("x").apply(lambda v: {"x": v + 1})
        assert isinstance(result, StreamingSeries)
        assert result.test(value) == expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": 5, "y": 20}, StreamingSeries("x"), StreamingSeries("y"), 25),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 10, 15),
        ],
    )
    def test_add(self, value, series, other, expected):
        result = series + other
        assert result.test(value) == expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": 5, "y": 20}, StreamingSeries("y"), StreamingSeries("x"), 15),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 10, -5),
        ],
    )
    def test_subtract(self, value, series, other, expected):
        result = series - other
        assert result.test(value) == expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": 5, "y": 20}, StreamingSeries("y"), StreamingSeries("x"), 100),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 10, 50),
        ],
    )
    def test_multiply(self, value, series, other, expected):
        result = series * other
        assert result.test(value) == expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": 5, "y": 20}, StreamingSeries("x"), StreamingSeries("x"), 1),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 2, 2.5),
        ],
    )
    def test_div(self, value, series, other, expected):
        result = series / other
        assert result.test(value) == expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": 5, "y": 2}, StreamingSeries("x"), StreamingSeries("y"), 1),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 3, 2),
        ],
    )
    def test_mod(self, value, series, other, expected):
        result = series % other
        assert result.test(value) == expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": 5, "y": 2}, StreamingSeries("x"), StreamingSeries("x"), True),
            ({"x": 5, "y": 2}, StreamingSeries("x"), StreamingSeries("y"), False),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 5, True),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 6, False),
        ],
    )
    def test_equal(self, value, series, other, expected):
        result = series == other
        assert result.test(value) is expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": 5, "y": 2}, StreamingSeries("x"), StreamingSeries("x"), False),
            ({"x": 5, "y": 2}, StreamingSeries("x"), StreamingSeries("y"), True),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 5, False),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 6, True),
        ],
    )
    def test_not_equal(self, value, series, other, expected):
        result = series != other
        assert result.test(value) is expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": 5, "y": 20}, StreamingSeries("x"), StreamingSeries("x"), False),
            ({"x": 5, "y": 20}, StreamingSeries("x"), StreamingSeries("y"), True),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 5, False),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 6, True),
        ],
    )
    def test_less_than(self, value, series, other, expected):
        result = series < other
        assert result.test(value) is expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": 5, "y": 20}, StreamingSeries("x"), StreamingSeries("x"), True),
            ({"x": 5, "y": 20}, StreamingSeries("x"), StreamingSeries("y"), True),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 4, False),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 5, True),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 6, True),
        ],
    )
    def test_less_than_equal(self, value, series, other, expected):
        result = series <= other
        assert result.test(value) is expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": 5, "y": 20}, StreamingSeries("x"), StreamingSeries("x"), False),
            ({"x": 5, "y": 4}, StreamingSeries("x"), StreamingSeries("y"), True),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 4, True),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 5, False),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 6, False),
        ],
    )
    def test_greater_than(self, value, series, other, expected):
        result = series > other
        assert result.test(value) is expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": 5, "y": 20}, StreamingSeries("x"), StreamingSeries("x"), True),
            ({"x": 5, "y": 4}, StreamingSeries("x"), StreamingSeries("y"), True),
            ({"x": 5, "y": 6}, StreamingSeries("x"), StreamingSeries("y"), False),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 4, True),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 5, True),
            ({"x": 5, "y": 20}, StreamingSeries("x"), 6, False),
        ],
    )
    def test_greater_than_equal(self, value, series, other, expected):
        result = series >= other
        assert result.test(value) is expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": True, "y": False}, StreamingSeries("x"), StreamingSeries("x"), True),
            (
                {"x": True, "y": False},
                StreamingSeries("x"),
                StreamingSeries("y"),
                False,
            ),
            ({"x": True, "y": False}, StreamingSeries("x"), True, True),
            ({"x": True, "y": False}, StreamingSeries("x"), False, False),
            ({"x": True, "y": False}, StreamingSeries("x"), 0, 0),
        ],
    )
    def test_and(self, value, series, other, expected):
        result = series & other
        assert result.test(value) is expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": True, "y": False}, StreamingSeries("x"), StreamingSeries("y"), True),
            (
                {"x": False},
                StreamingSeries("x"),
                StreamingSeries("x"),
                False,
            ),
            (
                {
                    "x": True,
                },
                StreamingSeries("x"),
                0,
                True,
            ),
            ({"x": False}, StreamingSeries("x"), 0, 0),
            ({"x": False}, StreamingSeries("x"), True, True),
        ],
    )
    def test_or(self, value, series, other, expected):
        result = series | other
        assert result.test(value) is expected

    def test_multiple_conditions(self):
        value = {"x": 5, "y": 20, "z": 110}
        expected = True
        result = (StreamingSeries("x") <= StreamingSeries("y")) & (
            StreamingSeries("x") <= StreamingSeries("z")
        )

        assert result.test(value) is expected

    @pytest.mark.parametrize(
        "value, series, expected",
        [
            ({"x": True, "y": False}, StreamingSeries("x"), False),
            ({"x": 1, "y": False}, StreamingSeries("x"), False),
        ],
    )
    def test_invert(self, value, series, expected):
        result = ~series

        assert result.test(value) == expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": 1}, StreamingSeries("x"), [1, 2, 3], True),
            ({"x": 1}, StreamingSeries("x"), [], False),
            ({"x": 1}, StreamingSeries("x"), {1: 456}, True),
        ],
    )
    def test_isin(self, value, series, other, expected):
        assert series.isin(other).test(value) == expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": [1, 2, 3]}, StreamingSeries("x"), 1, True),
            ({"x": [1, 2, 3]}, StreamingSeries("x"), 5, False),
            ({"x": "abc"}, StreamingSeries("x"), "a", True),
            ({"x": {"y": "z"}}, StreamingSeries("x"), "y", True),
        ],
    )
    def test_contains(self, series, value, other, expected):
        assert series.contains(other).test(value) == expected

    @pytest.mark.parametrize(
        "value, series, expected",
        [
            ({"x": None}, StreamingSeries("x"), True),
            ({"x": [1, 2, 3]}, StreamingSeries("x"), False),
        ],
    )
    def test_isnull(self, value, series, expected):
        assert series.isnull().test(value) == expected

    @pytest.mark.parametrize(
        "value, series, expected",
        [
            ({"x": None}, StreamingSeries("x"), False),
            ({"x": [1, 2, 3]}, StreamingSeries("x"), True),
        ],
    )
    def test_notnull(self, value, series, expected):
        assert series.notnull().test(value) == expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": [1, 2, 3]}, StreamingSeries("x"), None, False),
            ({"x": None}, StreamingSeries("x"), None, True),
            ({"x": 1}, StreamingSeries("x"), 1, True),
        ],
    )
    def test_is_(self, value, series, other, expected):
        assert series.is_(other).test(value) == expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": [1, 2, 3]}, StreamingSeries("x"), None, True),
            ({"x": None}, StreamingSeries("x"), None, False),
            ({"x": 1}, StreamingSeries("x"), 1, False),
        ],
    )
    def test_isnot(self, value, series, other, expected):
        assert series.isnot(other).test(value) == expected

    @pytest.mark.parametrize(
        "value, item, expected",
        [
            ({"x": {"y": 1}}, "y", 1),
            ({"x": [0, 1, 2, 3]}, 1, 1),
        ],
    )
    def test_getitem(self, value, item, expected):
        result = StreamingSeries("x")[item]
        assert result.test(value) == expected

    def test_getitem_with_apply(self):
        value = {"x": {"y": {"z": 110}}, "k": 0}
        result = StreamingSeries("x")["y"]["z"].apply(lambda v: v + 10)

        assert result.test(value) == 120

    @pytest.mark.parametrize("value, expected", [(10, 10), (-10, 10), (10.0, 10.0)])
    def test_abs_success(
        self,
        value,
        expected,
    ):
        result = StreamingSeries("x").abs()

        assert result.test({"x": value}) == expected

    def test_abs_not_a_number_fails(self):
        result = StreamingSeries("x").abs()

        with pytest.raises(TypeError, match="bad operand type for abs()"):
            assert result.test({"x": "string"})

    def test_and_is_lazy(self):
        series = StreamingSeries("x") & StreamingSeries("y")
        # Ensure it doesn't fail with KeyError ("y" is not present in value)
        series.test({"x": False})

    def test_or_is_lazy(self):
        series = StreamingSeries("x") | StreamingSeries("y")
        # Ensure it doesn't fail with KeyError ("y" is not present in value)
        series.test({"x": True})

    def test_cannot_use_logical_and(self):
        with pytest.raises(InvalidOperation):
            StreamingSeries("x") and StreamingSeries("y")

    def test_cannot_use_logical_or(self):
        with pytest.raises(InvalidOperation):
            StreamingSeries("x") or StreamingSeries("y")
