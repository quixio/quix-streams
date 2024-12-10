import pytest


@pytest.fixture()
def my_other_value(my_value):
    return my_value * 2
