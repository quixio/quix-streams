from src.quixstreams.dataframes.sdf.pipeline import EventColumn


def test__apply(sample_message):
    result = EventColumn('x').apply(lambda v: v + 22)
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message) == 27


def test__addition(sample_message):
    result = EventColumn('x') + EventColumn('y')
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message) == 25


def test__multi_op(sample_message):
    result = EventColumn('x') + EventColumn('y') + EventColumn('z')
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message) == 135


def test__scaler_op(sample_message):
    result = EventColumn('x') + 2
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message) == 7


def test__subtraction(sample_message):
    result = EventColumn('y') - EventColumn('x')
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message) == 15


def test__multiplication(sample_message):
    result = EventColumn('x') * EventColumn('y')
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message) == 100


def test__div(sample_message):
    result = EventColumn('z') / EventColumn('y')
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message) == 5.5


def test__mod(sample_message):
    result = EventColumn('y') % EventColumn('x')
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message) == 0


def test__equality__true(sample_message):
    result = EventColumn('x') == EventColumn('x2')
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message)


def test__equality__false(sample_message):
    result = EventColumn('x') == EventColumn('y')
    assert isinstance(result, EventColumn)
    assert not result.eval(sample_message)


def test__inequality__true(sample_message):
    result = EventColumn('x') != EventColumn('y')
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message)


def test__inequality__false(sample_message):
    result = EventColumn('x') != EventColumn('x2')
    assert isinstance(result, EventColumn)
    assert not result.eval(sample_message)


def test__less_than__true(sample_message):
    result = EventColumn('x') < EventColumn('y')
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message)


def test__less_than__false(sample_message):
    result = EventColumn('y') < EventColumn('x')
    assert isinstance(result, EventColumn)
    assert not result.eval(sample_message)


def test__less_than_or_equal__equal_true(sample_message):
    result = EventColumn('x') <= EventColumn('x2')
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message)


def test__less_than_or_equal__less_true(sample_message):
    result = EventColumn('x') <= EventColumn('y')
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message)


def test__less_than_or_equal__false(sample_message):
    result = EventColumn('y') <= EventColumn('x')
    assert isinstance(result, EventColumn)
    assert not result.eval(sample_message)


def test__greater_than__true(sample_message):
    result = EventColumn('y') > EventColumn('x')
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message)


def test__greater_than__false(sample_message):
    result = EventColumn('x') > EventColumn('y')
    assert isinstance(result, EventColumn)
    assert not result.eval(sample_message)


def test__greater_than_or_equal__equal_true(sample_message):
    result = EventColumn('x') >= EventColumn('x2')
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message)


def test__greater_than_or_equal__greater_true(sample_message):
    result = EventColumn('y') >= EventColumn('x')
    assert isinstance(result, EventColumn)
    assert result.eval(sample_message)


def test__greater_than_or_equal__greater_false(sample_message):
    result = EventColumn('x') >= EventColumn('y')
    assert isinstance(result, EventColumn)
    assert not result.eval(sample_message)
