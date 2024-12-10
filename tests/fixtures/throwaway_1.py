import pytest


@pytest.fixture()
def my_value(request):
    return request.param
