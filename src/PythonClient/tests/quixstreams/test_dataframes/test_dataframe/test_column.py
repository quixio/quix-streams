from src.quixstreams.dataframes.dataframe.column import Column


class TestColumn:
    def test_apply(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('x').apply(lambda v: v + 22)
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 27

    def test_addition(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('x') + Column('y')
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 25

    def test_multi_op(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('x') + Column('y') + Column('z')
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 135

    def test_scaler_op(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('x') + 2
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 7

    def test_subtraction(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('y') - Column('x')
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 15

    def test_multiplication(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('x') * Column('y')
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 100

    def test_div(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('z') / Column('y')
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 5.5

    def test_mod(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('y') % Column('x')
        assert isinstance(result, Column)
        assert result.eval(msg_value) == 0

    def test_equality_true(self, row_factory):
        msg_value = row_factory({'x': 5, 'x2': 5})
        result = Column('x') == Column('x2')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_equality_false(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('x') == Column('y')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_inequality_true(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('x') != Column('y')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_inequality_false(self, row_factory):
        msg_value = row_factory({'x': 5, 'x2': 5})
        result = Column('x') != Column('x2')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_less_than_true(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('x') < Column('y')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_less_than_false(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('y') < Column('x')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_less_than_or_equal_equal_true(self, row_factory):
        msg_value = row_factory({'x': 5, 'x2': 5})
        result = Column('x') <= Column('x2')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_less_than_or_equal_less_true(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('x') <= Column('y')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_less_than_or_equal_false(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('y') <= Column('x')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_greater_than_true(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('y') > Column('x')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_greater_than_false(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('x') > Column('y')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_greater_than_or_equal_equal_true(self, row_factory):
        msg_value = row_factory({'x': 5, 'x2': 5})
        result = Column('x') >= Column('x2')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_greater_than_or_equal_greater_true(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('y') >= Column('x')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_greater_than_or_equal_greater_false(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = Column('x') >= Column('y')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_and_true(self, row_factory):
        msg_value = row_factory({'x': True, 'y': True, 'z': False})
        result = Column('x') & Column('y')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_and_true_multiple(self, row_factory):
        msg_value = row_factory({'x': True, 'y': True, 'z': False})
        result = Column('x') & Column('y') & Column('x')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_and_false_multiple(self, row_factory):
        msg_value = row_factory({'x': True, 'y': True, 'z': False})
        result = Column('x') & Column('y') & Column('z')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_and_false(self, row_factory):
        msg_value = row_factory({'x': True, 'y': True, 'z': False})
        result = Column('x') & Column('z')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_and_true_bool(self, row_factory):
        msg_value = row_factory({'x': True, 'y': True, 'z': False})
        result = Column('x') & True
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_and_false_bool(self, row_factory):
        msg_value = row_factory({'x': True, 'y': True, 'z': False})
        result = Column('x') & False
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_or_true(self, row_factory):
        msg_value = row_factory({'x': True, 'y': True, 'z': False})
        result = Column('x') | Column('z')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_or_false(self, row_factory):
        msg_value = row_factory({'x': True, 'y': True, 'z': False})
        result = Column('z') | Column('z')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_and_inequalities_true(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = (Column('x') <= Column('y')) & (Column('x') <= Column('z'))
        assert isinstance(result, Column)
        assert result.eval(msg_value) is True

    def test_and_inequalities_false(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = (Column('x') <= Column('y')) & (Column('x') > Column('z'))
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_invert_int(self, row_factory):
        msg_value = row_factory({'x': 1})
        result = ~Column('x')
        assert isinstance(result, Column)
        assert result.eval(msg_value) == -2

    def test_invert_bool(self, row_factory):
        msg_value = row_factory({'x': True, 'y': True, 'z': False})
        result = ~Column('x')
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False

    def test_invert_bool_from_inequalities(self, row_factory):
        msg_value = row_factory({'x': 5, 'y': 20, 'z': 110})
        result = ~(Column('x') <= Column('y'))
        assert isinstance(result, Column)
        assert result.eval(msg_value) is False
