from quixstreams.sinks.core.list import ListSink


class TestListSink:
    def test_list_funcs(self):
        """
        Simple sanity checks to ensure the sink behaves like a list
        """
        sink = ListSink()
        assert sink == []

        # referencing/indexes
        sink.append("woo")
        assert sink == ["woo"]
        assert sink[0] == "woo"
        assert sink[:] == ["woo"]
        assert isinstance(sink[:], ListSink)

        # iterating, len
        sink.append("wee")
        assert len(sink) == 2
        for i in sink:
            assert i in ["woo", "wee"]

        # clearing the sink
        sink.clear()
        assert sink == []
        sink.append("another")
        assert sink == ["another"]
        sink[:] = []
        assert sink == []

        assert isinstance(sink, ListSink)

    def test_add(self):
        records = [
            {
                "value": {"field_a": "thing_a", "field_b": "thing_b"},
                "key": "my_key",
                "timestamp": 12345677890,
                "headers": [("h1", "v1"), ("h2", "v2")],
                "topic": "my_topic",
                "partition": 0,
                "offset": 5,
            },
            {
                "value": {"field_a": "thing_c", "field_b": "thing_d"},
                "key": "my_key",
                "timestamp": 12345677890,
                "headers": [("h1", "v1"), ("h2", "v2")],
                "topic": "my_topic",
                "partition": 0,
                "offset": 6,
            },
        ]
        sink = ListSink()
        for r in records:
            sink.add(**r)
        assert len(sink) == len(records)
        for idx, r in enumerate(records):
            assert sink[idx] == {**r["value"]}

    def test_add_metadata(self):
        record_dict = {
            "value": {"field_a": "thing_a", "field_b": "thing_b"},
            "key": "my_key",
            "timestamp": 12345677890,
            "headers": [("h1", "v1"), ("h2", "v2")],
            "topic": "my_topic",
            "partition": 0,
            "offset": 5,
        }
        sink = ListSink(metadata=True)
        sink.add(**record_dict)
        assert sink[0] == {
            **record_dict.pop("value"),
            **{f"_{k}": v for k, v in record_dict.items()},
        }
