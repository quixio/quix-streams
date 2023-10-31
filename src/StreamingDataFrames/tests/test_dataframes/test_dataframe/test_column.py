import pytest

from streamingdataframes.dataframe.column import Column


class TestColumn:
    def test_apply(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110}, key=123)
        result = Column("x").apply(lambda v, context: v + context.key)
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 128

    def test_addition(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("x") + Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 25

    def test_multi_op(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("x") + Column("y") + Column("z")
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 135

    def test_scaler_op(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("x") + 2
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 7

    def test_subtraction(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("y") - Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 15

    def test_multiplication(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("x") * Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 100

    def test_div(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("z") / Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 5.5

    def test_mod(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("y") % Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 0

    def test_equality_true(self, row_factory):
        msg_value = row_factory({"x": 5, "x2": 5})
        result = Column("x") == Column("x2")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_equality_false(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("x") == Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_inequality_true(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("x") != Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_inequality_false(self, row_factory):
        msg_value = row_factory({"x": 5, "x2": 5})
        result = Column("x") != Column("x2")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_less_than_true(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("x") < Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_less_than_false(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("y") < Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_less_than_or_equal_equal_true(self, row_factory):
        msg_value = row_factory({"x": 5, "x2": 5})
        result = Column("x") <= Column("x2")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_less_than_or_equal_less_true(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("x") <= Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_less_than_or_equal_false(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("y") <= Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_greater_than_true(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("y") > Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_greater_than_false(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("x") > Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_greater_than_or_equal_equal_true(self, row_factory):
        msg_value = row_factory({"x": 5, "x2": 5})
        result = Column("x") >= Column("x2")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_greater_than_or_equal_greater_true(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("y") >= Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_greater_than_or_equal_greater_false(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = Column("x") >= Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_and_true(self, row_factory):
        msg_value = row_factory({"x": True, "y": True, "z": False})
        result = Column("x") & Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_and_true_multiple(self, row_factory):
        msg_value = row_factory({"x": True, "y": True, "z": False})
        result = Column("x") & Column("y") & Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_and_false_multiple(self, row_factory):
        msg_value = row_factory({"x": True, "y": True, "z": False})
        result = Column("x") & Column("y") & Column("z")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_and_false(self, row_factory):
        msg_value = row_factory({"x": True, "y": True, "z": False})
        result = Column("x") & Column("z")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_and_true_bool(self, row_factory):
        msg_value = row_factory({"x": True, "y": True, "z": False})
        result = Column("x") & True
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_and_false_bool(self, row_factory):
        msg_value = row_factory({"x": True, "y": True, "z": False})
        result = Column("x") & False
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_or_true(self, row_factory):
        msg_value = row_factory({"x": True, "y": True, "z": False})
        result = Column("x") | Column("z")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_or_false(self, row_factory):
        msg_value = row_factory({"x": True, "y": True, "z": False})
        result = Column("z") | Column("z")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_and_inequalities_true(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = (Column("x") <= Column("y")) & (Column("x") <= Column("z"))
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_and_inequalities_false(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = (Column("x") <= Column("y")) & (Column("x") > Column("z"))
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_invert_int(self, row_factory):
        msg_value = row_factory({"x": 1})
        result = ~Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value) == -2

    def test_invert_bool(self, row_factory):
        msg_value = row_factory({"x": True, "y": True, "z": False})
        result = ~Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_invert_bool_from_inequalities(self, row_factory):
        msg_value = row_factory({"x": 5, "y": 20, "z": 110})
        result = ~(Column("x") <= Column("y"))
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    @pytest.mark.parametrize(
        "value, other, expected",
        [
            ({"x": 1}, [1, 2, 3], True),
            ({"x": 1}, [], False),
            ({"x": 1}, {1: 456}, True),
        ],
    )
    def test_isin(self, row_factory, value, other, expected):
        row = row_factory(value)
        assert Column("x").isin(other).eval(row) == expected

    @pytest.mark.parametrize(
        "value, other, expected",
        [
            ({"x": [1, 2, 3]}, 1, True),
            ({"x": [1, 2, 3]}, 5, False),
            ({"x": "abc"}, "a", True),
            ({"x": {"y": "z"}}, "y", True),
        ],
    )
    def test_contains(self, row_factory, value, other, expected):
        row = row_factory(value)
        assert Column("x").contains(other).eval(row) == expected

    @pytest.mark.parametrize(
        "value, expected",
        [
            ({"x": None}, True),
            ({"x": [1, 2, 3]}, False),
        ],
    )
    def test_isnull(self, row_factory, value, expected):
        row = row_factory(value)
        assert Column("x").isnull().eval(row) == expected

    @pytest.mark.parametrize(
        "value, expected",
        [
            ({"x": None}, False),
            ({"x": [1, 2, 3]}, True),
        ],
    )
    def test_notnull(self, row_factory, value, expected):
        row = row_factory(value)
        assert Column("x").notnull().eval(row) == expected

    @pytest.mark.parametrize(
        "value, other, expected",
        [
            ({"x": [1, 2, 3]}, None, False),
            ({"x": None}, None, True),
            ({"x": 1}, 1, True),
        ],
    )
    def test_is_(self, row_factory, value, other, expected):
        row = row_factory(value)
        assert Column("x").is_(other).eval(row) == expected

    @pytest.mark.parametrize(
        "value, other, expected",
        [
            ({"x": [1, 2, 3]}, None, True),
            ({"x": None}, None, False),
            ({"x": 1}, 1, False),
        ],
    )
    def test_isnot(self, row_factory, value, other, expected):
        row = row_factory(value)
        assert Column("x").isnot(other).eval(row) == expected

    @pytest.mark.parametrize(
        "nested_item",
        [
            {2: 110},
            {2: "a_string"},
            {2: {"another": "dict"}},
            {2: ["a", "list"]},
            ["item", "in", "this", "list"],
        ],
    )
    def test__get_item__(self, row_factory, nested_item):
        msg_value = row_factory({"x": {"y": nested_item}, "k": 0})
        result = Column("x")["y"][2]
        assert isinstance(result, Column)
        assert result.eval(msg_value) == nested_item[2]

    def test_get_item_with_apply(self, row_factory):
        msg_value = row_factory({"x": {"y": {"z": 110}}, "k": 0})
        result = Column("x")["y"]["z"].apply(lambda v, ctx: v + 10)
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 120

    def test_get_item_with_op(self, row_factory):
        msg_value = row_factory({"x": {"y": 10}, "j": {"k": 5}})
        result = Column("x")["y"] + Column("j")["k"]
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 15
