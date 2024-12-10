import pytest


@pytest.mark.parametrize("my_value", [1, 2, 3], indirect=True)
class TestMyValue:
    def test_my_other_value(self, my_other_value):
        print(my_other_value)
