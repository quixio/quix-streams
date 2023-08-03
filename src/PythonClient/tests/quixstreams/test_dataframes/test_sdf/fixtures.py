import pytest


@pytest.fixture
def sample_message():
    return {
        'x': 5,
        'x2': 5,
        'y': 20,
        'z': 110
    }
