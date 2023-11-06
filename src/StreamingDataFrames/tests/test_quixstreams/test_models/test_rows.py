class TestRows:
    def test_clone(self, row_factory):
        new_value = {"c": 3}
        change_dict = {
            "value": new_value,
        }
        row = row_factory(value={"a": 1, "b": 2})
        new_row = row.clone(**change_dict)
        assert new_row.value == new_value
        assert new_row.context == row.context
        assert id(new_row.value) != id(new_value)
        assert id(new_row.context) == id(row.context)
