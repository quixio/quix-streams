import pytest

from quixstreams.dataframe.exceptions import (
    ColumnDoesNotExist,
    InvalidColumnReference,
    InvalidOperation,
)
from quixstreams.dataframe.series import StreamingSeries


class TestStreamingSeries:
    def test_apply(self):
        value = {"x": 5, "y": 20, "z": 110}
        key, timestamp, headers = "key", 0, []
        expected = ({"x": 6}, key, timestamp, headers)
        result = StreamingSeries(sdf_id=1, name="x").apply(lambda v: {"x": v + 1})
        assert isinstance(result, StreamingSeries)
        assert result.test(value, key, timestamp, headers)[0] == expected

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            (
                {"x": 5, "y": 20},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="y"),
                25,
            ),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 10, 15),
        ],
    )
    def test_add(self, value, series, other, expected):
        result = series + other
        key, timestamp, headers = "key", 0, []
        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            (
                {"x": 5, "y": 20},
                StreamingSeries(sdf_id=1, name="y"),
                StreamingSeries(sdf_id=1, name="x"),
                15,
            ),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 10, -5),
        ],
    )
    def test_subtract(self, value, series, other, expected):
        result = series - other
        key, timestamp, headers = "key", 0, None
        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            (
                {"x": 5, "y": 20},
                StreamingSeries(sdf_id=1, name="y"),
                StreamingSeries(sdf_id=1, name="x"),
                100,
            ),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 10, 50),
        ],
    )
    def test_multiply(self, value, series, other, expected):
        result = series * other
        key, timestamp, headers = "key", 0, None
        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            (
                {"x": 5, "y": 20},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="x"),
                1,
            ),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 2, 2.5),
        ],
    )
    def test_div(self, value, series, other, expected):
        result = series / other
        key, timestamp, headers = "key", 0, []
        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            (
                {"x": 5, "y": 2},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="y"),
                1,
            ),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 3, 2),
        ],
    )
    def test_mod(self, value, series, other, expected):
        result = series % other
        key, timestamp, headers = "key", 0, None
        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            (
                {"x": 5, "y": 2},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="x"),
                True,
            ),
            (
                {"x": 5, "y": 2},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="y"),
                False,
            ),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 5, True),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 6, False),
        ],
    )
    def test_equal(self, value, series, other, expected):
        result = series == other
        key, timestamp, headers = "key", 0, []
        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            (
                {"x": 5, "y": 2},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="x"),
                False,
            ),
            (
                {"x": 5, "y": 2},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="y"),
                True,
            ),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 5, False),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 6, True),
        ],
    )
    def test_not_equal(self, value, series, other, expected):
        result = series != other
        key, timestamp, headers = "key", 0, 1
        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            (
                {"x": 5, "y": 20},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="x"),
                False,
            ),
            (
                {"x": 5, "y": 20},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="y"),
                True,
            ),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 5, False),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 6, True),
        ],
    )
    def test_less_than(self, value, series, other, expected):
        result = series < other
        key, timestamp, headers = "key", 0, []
        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            (
                {"x": 5, "y": 20},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="x"),
                True,
            ),
            (
                {"x": 5, "y": 20},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="y"),
                True,
            ),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 4, False),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 5, True),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 6, True),
        ],
    )
    def test_less_than_equal(self, value, series, other, expected):
        result = series <= other
        key, timestamp, headers = "key", 0, None
        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            (
                {"x": 5, "y": 20},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="x"),
                False,
            ),
            (
                {"x": 5, "y": 4},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="y"),
                True,
            ),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 4, True),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 5, False),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 6, False),
        ],
    )
    def test_greater_than(self, value, series, other, expected):
        result = series > other
        key, timestamp, headers = "key", 0, None
        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            (
                {"x": 5, "y": 20},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="x"),
                True,
            ),
            (
                {"x": 5, "y": 4},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="y"),
                True,
            ),
            (
                {"x": 5, "y": 6},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="y"),
                False,
            ),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 4, True),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 5, True),
            ({"x": 5, "y": 20}, StreamingSeries(sdf_id=1, name="x"), 6, False),
        ],
    )
    def test_greater_than_equal(self, value, series, other, expected):
        result = series >= other
        key, timestamp, headers = "key", 0, []
        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            (
                {"x": True, "y": False},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="x"),
                True,
            ),
            (
                {"x": True, "y": False},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="y"),
                False,
            ),
            ({"x": True, "y": False}, StreamingSeries(sdf_id=1, name="x"), True, True),
            (
                {"x": True, "y": False},
                StreamingSeries(sdf_id=1, name="x"),
                False,
                False,
            ),
            ({"x": True, "y": False}, StreamingSeries(sdf_id=1, name="x"), 0, 0),
        ],
    )
    def test_and(self, value, series, other, expected):
        result = series & other
        key, timestamp, headers = "key", 0, []
        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            (
                {"x": True, "y": False},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="y"),
                True,
            ),
            (
                {"x": False},
                StreamingSeries(sdf_id=1, name="x"),
                StreamingSeries(sdf_id=1, name="x"),
                False,
            ),
            (
                {
                    "x": True,
                },
                StreamingSeries(sdf_id=1, name="x"),
                0,
                True,
            ),
            ({"x": False}, StreamingSeries(sdf_id=1, name="x"), 0, 0),
            ({"x": False}, StreamingSeries(sdf_id=1, name="x"), True, True),
        ],
    )
    def test_or(self, value, series, other, expected):
        result = series | other
        key, timestamp, headers = "key", 0, []
        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    def test_multiple_conditions(self):
        value = {"x": 5, "y": 20, "z": 110}
        key, timestamp, headers = "key", 0, []
        expected = True

        result = (
            StreamingSeries(sdf_id=1, name="x") <= StreamingSeries(sdf_id=1, name="y")
        ) & (StreamingSeries(sdf_id=1, name="x") <= StreamingSeries(sdf_id=1, name="z"))

        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, expected",
        [
            ({"x": True, "y": False}, StreamingSeries(sdf_id=1, name="x"), False),
            ({"x": 1, "y": False}, StreamingSeries(sdf_id=1, name="x"), False),
        ],
    )
    def test_invert(self, value, series, expected):
        result = ~series
        key, timestamp, headers = "key", 0, []

        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": 1}, StreamingSeries(sdf_id=1, name="x"), [1, 2, 3], True),
            ({"x": 1}, StreamingSeries(sdf_id=1, name="x"), [], False),
            ({"x": 1}, StreamingSeries(sdf_id=1, name="x"), {1: 456}, True),
        ],
    )
    def test_isin(self, value, series, other, expected):
        key, timestamp, headers = "key", 0, []
        assert series.isin(other).test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": [1, 2, 3]}, StreamingSeries(sdf_id=1, name="x"), 1, True),
            ({"x": [1, 2, 3]}, StreamingSeries(sdf_id=1, name="x"), 5, False),
            ({"x": "abc"}, StreamingSeries(sdf_id=1, name="x"), "a", True),
            ({"x": {"y": "z"}}, StreamingSeries(sdf_id=1, name="x"), "y", True),
        ],
    )
    def test_contains(self, series, value, other, expected):
        key, timestamp, headers = "key", 0, []
        assert series.contains(other).test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, expected",
        [
            ({"x": None}, StreamingSeries(sdf_id=1, name="x"), True),
            ({"x": [1, 2, 3]}, StreamingSeries(sdf_id=1, name="x"), False),
        ],
    )
    def test_isnull(self, value, series, expected):
        key, timestamp, headers = "key", 0, []
        assert series.isnull().test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, expected",
        [
            ({"x": None}, StreamingSeries(sdf_id=1, name="x"), False),
            ({"x": [1, 2, 3]}, StreamingSeries(sdf_id=1, name="x"), True),
        ],
    )
    def test_notnull(self, value, series, expected):
        key, timestamp, headers = "key", 0, []
        assert series.notnull().test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": [1, 2, 3]}, StreamingSeries(sdf_id=1, name="x"), None, False),
            ({"x": None}, StreamingSeries(sdf_id=1, name="x"), None, True),
            ({"x": 1}, StreamingSeries(sdf_id=1, name="x"), 1, True),
        ],
    )
    def test_is_(self, value, series, other, expected):
        key, timestamp, headers = "key", 0, []
        assert series.is_(other).test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, series, other, expected",
        [
            ({"x": [1, 2, 3]}, StreamingSeries(sdf_id=1, name="x"), None, True),
            ({"x": None}, StreamingSeries(sdf_id=1, name="x"), None, False),
            ({"x": 1}, StreamingSeries(sdf_id=1, name="x"), 1, False),
        ],
    )
    def test_isnot(self, value, series, other, expected):
        key, timestamp, headers = "key", 0, []
        assert series.isnot(other).test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize(
        "value, item, expected",
        [
            ({"x": {"y": 1}}, "y", 1),
            ({"x": [0, 1, 2, 3]}, 1, 1),
        ],
    )
    def test_getitem(self, value, item, expected):
        result = StreamingSeries(sdf_id=1, name="x")[item]
        key, timestamp, headers = "key", 0, []
        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    def test_getitem_with_apply(self):
        value = {"x": {"y": {"z": 110}}, "k": 0}
        key, timestamp, headers = "key", 0, []
        expected = 120
        result = StreamingSeries(sdf_id=1, name="x")["y"]["z"].apply(lambda v: v + 10)

        assert result.test(value, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    @pytest.mark.parametrize("value, expected", [(10, 10), (-10, 10), (10.0, 10.0)])
    def test_abs_success(
        self,
        value,
        expected,
    ):
        result = StreamingSeries(sdf_id=1, name="x").abs()
        key, timestamp, headers = "key", 0, []

        assert result.test({"x": value}, key, timestamp, headers)[0] == (
            expected,
            key,
            timestamp,
            headers,
        )

    def test_abs_not_a_number_fails(self):
        result = StreamingSeries(sdf_id=1, name="x").abs()
        key, timestamp = "key", 0

        with pytest.raises(TypeError, match="bad operand type for abs()"):
            assert result.test({"x": "string"}, key, timestamp)

    def test_and_is_lazy(self):
        series = StreamingSeries(sdf_id=1, name="x") & StreamingSeries(
            sdf_id=1, name="y"
        )
        key, timestamp, headers = "key", 0, []

        # Ensure it doesn't fail with KeyError ("y" is not present in value)
        series.test({"x": False}, key, timestamp, headers)

    def test_or_is_lazy(self):
        series = StreamingSeries(sdf_id=1, name="x") | StreamingSeries(
            sdf_id=1, name="y"
        )
        key, timestamp, headers = "key", 0, []
        # Ensure it doesn't fail with KeyError ("y" is not present in value)
        series.test({"x": True}, key, timestamp, headers)

    def test_cannot_use_logical_and(self):
        with pytest.raises(InvalidOperation):
            StreamingSeries(sdf_id=1, name="x") and StreamingSeries(sdf_id=1, name="y")

    def test_cannot_use_logical_or(self):
        with pytest.raises(InvalidOperation):
            StreamingSeries(sdf_id=1, name="x") or StreamingSeries(sdf_id=1, name="y")

    def test_sdf_column_missing(self):
        """
        Throw exception when user attempts an initial (SDF) column reference
        and key is missing.
        """
        key, timestamp, headers = "key", 0, []

        with pytest.raises(ColumnDoesNotExist):
            StreamingSeries(sdf_id=1, name="x").test({"y": 2}, key, timestamp, headers)

    def test_sdf_value_invalid_type(self):
        """
        Raise special TypeError when the initial (SDF) data is not a dict and user
        attempts a column reference.
        """
        key, timestamp, headers = "key", 0, []
        with pytest.raises(InvalidColumnReference):
            StreamingSeries(sdf_id=1, name="x").test(2, key, timestamp, headers)

    @pytest.mark.parametrize(
        "operation",
        [
            lambda s1, s2: s1 + s2,
            lambda s1, s2: s1 & s2,
            lambda s1, s2: s1 | s2,
        ],
    )
    def test_series_origin_mismatch_fails(self, operation):
        """
        Attempting to do operations that originate from two different SDF's fails (
        arises from SDF branching).
        """
        with pytest.raises(
            InvalidOperation,
            match="All column operations must originate from one `StreamingDataFrame`",
        ):
            operation(
                StreamingSeries(name="x", sdf_id=1), StreamingSeries(name="x", sdf_id=2)
            )
