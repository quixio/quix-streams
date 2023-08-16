from quixstreams.dataframes.dataframe.column import Column


class TestColumn:
    def test_apply(self, message_value):
        result = Column('x').apply(lambda v: v + 22)
        assert isinstance(result, Column)
        assert result.eval(message_value) == 27

    def test_addition(self, message_value):
        result = Column('x') + Column('y')
        assert isinstance(result, Column)
        assert result.eval(message_value) == 25

    def test_multi_op(self, message_value):
        result = Column('x') + Column('y') + Column('z')
        assert isinstance(result, Column)
        assert result.eval(message_value) == 135

    def test_scaler_op(self, message_value):
        result = Column('x') + 2
        assert isinstance(result, Column)
        assert result.eval(message_value) == 7

    def test_subtraction(self, message_value):
        result = Column('y') - Column('x')
        assert isinstance(result, Column)
        assert result.eval(message_value) == 15

    def test_multiplication(self, message_value):
        result = Column('x') * Column('y')
        assert isinstance(result, Column)
        assert result.eval(message_value) == 100

    def test_div(self, message_value):
        result = Column('z') / Column('y')
        assert isinstance(result, Column)
        assert result.eval(message_value) == 5.5

    def test_mod(self, message_value):
        result = Column('y') % Column('x')
        assert isinstance(result, Column)
        assert result.eval(message_value) == 0

    def test_equality_true(self, message_value):
        result = Column('x') == Column('x2')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test_equality_false(self, message_value):
        result = Column('x') == Column('y')
        assert isinstance(result, Column)
        assert not result.eval(message_value)

    def test_inequality_true(self, message_value):
        result = Column('x') != Column('y')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test_inequality_false(self, message_value):
        result = Column('x') != Column('x2')
        assert isinstance(result, Column)
        assert not result.eval(message_value)

    def test_less_than_true(self, message_value):
        result = Column('x') < Column('y')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test_less_than_false(self, message_value):
        result = Column('y') < Column('x')
        assert isinstance(result, Column)
        assert not result.eval(message_value)

    def test_less_than_or_equal_equal_true(self, message_value):
        result = Column('x') <= Column('x2')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test_less_than_or_equal_less_true(self, message_value):
        result = Column('x') <= Column('y')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test_less_than_or_equal_false(self, message_value):
        result = Column('y') <= Column('x')
        assert isinstance(result, Column)
        assert not result.eval(message_value)

    def test_greater_than_true(self, message_value):
        result = Column('y') > Column('x')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test_greater_than_false(self, message_value):
        result = Column('x') > Column('y')
        assert isinstance(result, Column)
        assert not result.eval(message_value)

    def test_greater_than_or_equal_equal_true(self, message_value):
        result = Column('x') >= Column('x2')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test_greater_than_or_equal_greater_true(self, message_value):
        result = Column('y') >= Column('x')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test_greater_than_or_equal_greater_false(self, message_value):
        result = Column('x') >= Column('y')
        assert isinstance(result, Column)
        assert not result.eval(message_value)

    def test_and_true(self, message_value):
        result = Column('x') & Column('y')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test_and_false_int(self, message_value):
        result = Column('x') & False
        assert isinstance(result, Column)
        assert not result.eval(message_value)

    def test_and_false_0(self, message_value):
        result = Column('x') & Column('k')
        assert isinstance(result, Column)
        assert not result.eval(message_value)

    def test_or_true(self, message_value):
        result = Column('x') | 0
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test_or_false(self, message_value):
        result = Column('k') | 0
        assert isinstance(result, Column)
        assert not result.eval(message_value)

    def test_and_inequalities_true(self, message_value):
        result = (Column('x') <= Column('y')) & Column('z')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test_and_inequalities_false(self, message_value):
        result = (Column('x') <= Column('y')) & Column('k')
        assert isinstance(result, Column)
        assert not result.eval(message_value)
