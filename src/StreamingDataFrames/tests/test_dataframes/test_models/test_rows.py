class TestRows:
    def test_clone(self, row_factory):
        new_value = {"c": 3}
        new_topic = "woo"
        change_dict = {"value": new_value, "topic": new_topic}
        row = row_factory(value={"a": 1, "b": 2})
        new_row = row.clone(**change_dict)

        for s in row.__slots__:
            if s not in row._copy_map and s not in change_dict:
                assert row.__getattribute__(s) == new_row.__getattribute__(s)
        for s in row._copy_map:
            assert id(row.__getattribute__(s)) != id(new_row.__getattribute__(s))
        for s in change_dict:
            assert new_row.__getattribute__(s) == change_dict[s]
            assert new_row.__getattribute__(s) != row.__getattribute__(s)
        assert row.timestamp.type == new_row.timestamp.type
        assert row.timestamp.milliseconds == new_row.timestamp.milliseconds
