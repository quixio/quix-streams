from src.quixstreams.dataframes.dataframe.column import Column


class TestColumn:
    def test_apply(self, msg_value_with_ints):
        result = Column("x").apply(lambda v: v + 22)
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) == 27

    def test_addition(self, msg_value_with_ints):
        result = Column("x") + Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) == 25

    def test_multi_op(self, msg_value_with_ints):
        result = Column("x") + Column("y") + Column("z")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) == 135

    def test_scaler_op(self, msg_value_with_ints):
        result = Column("x") + 2
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) == 7

    def test_subtraction(self, msg_value_with_ints):
        result = Column("y") - Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) == 15

    def test_multiplication(self, msg_value_with_ints):
        result = Column("x") * Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) == 100

    def test_div(self, msg_value_with_ints):
        result = Column("z") / Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) == 5.5

    def test_mod(self, msg_value_with_ints):
        result = Column("y") % Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) == 0

    def test_equality_true(self, msg_value_with_ints):
        result = Column("x") == Column("x2")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is True

    def test_equality_false(self, msg_value_with_ints):
        result = Column("x") == Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is False

    def test_inequality_true(self, msg_value_with_ints):
        result = Column("x") != Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is True

    def test_inequality_false(self, msg_value_with_ints):
        result = Column("x") != Column("x2")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is False

    def test_less_than_true(self, msg_value_with_ints):
        result = Column("x") < Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is True

    def test_less_than_false(self, msg_value_with_ints):
        result = Column("y") < Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is False

    def test_less_than_or_equal_equal_true(self, msg_value_with_ints):
        result = Column("x") <= Column("x2")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is True

    def test_less_than_or_equal_less_true(self, msg_value_with_ints):
        result = Column("x") <= Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is True

    def test_less_than_or_equal_false(self, msg_value_with_ints):
        result = Column("y") <= Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is False

    def test_greater_than_true(self, msg_value_with_ints):
        result = Column("y") > Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is True

    def test_greater_than_false(self, msg_value_with_ints):
        result = Column("x") > Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is False

    def test_greater_than_or_equal_equal_true(self, msg_value_with_ints):
        result = Column("x") >= Column("x2")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is True

    def test_greater_than_or_equal_greater_true(self, msg_value_with_ints):
        result = Column("y") >= Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is True

    def test_greater_than_or_equal_greater_false(self, msg_value_with_ints):
        result = Column("x") >= Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is False

    def test_and_true(self, msg_value_with_bools):
        result = Column("x") & Column("y")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_bools) is True

    def test_and_true_multiple(self, msg_value_with_bools):
        result = Column("x") & Column("y") & Column("x")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_bools) is True

    def test_and_false_multiple(self, msg_value_with_bools):
        result = Column("x") & Column("y") & Column("z")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_bools) is False

    def test_and_false(self, msg_value_with_bools):
        result = Column("x") & Column("z")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_bools) is False

    def test_and_true_bool(self, msg_value_with_bools):
        result = Column("x") & True
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_bools) is True

    def test_and_false_bool(self, msg_value_with_bools):
        result = Column("x") & False
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_bools) is False

    def test_or_true(self, msg_value_with_bools):
        result = Column("x") | Column("z")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_bools) is True

    def test_or_false(self, msg_value_with_bools):
        result = Column("z") | Column("z")
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_bools) is False

    def test_and_inequalities_true(self, msg_value_with_ints):
        result = (Column("x") <= Column("y")) & (Column("x") <= Column("z"))
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is True

    def test_and_inequalities_false(self, msg_value_with_ints):
        result = (Column("x") <= Column("y")) & (Column("x") > Column("z"))
        assert isinstance(result, Column)
        assert result.eval(msg_value_with_ints) is False
