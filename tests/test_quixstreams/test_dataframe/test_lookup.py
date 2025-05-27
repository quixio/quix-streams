from typing import Any

from quixstreams.dataframe.joins.lookups.base import BaseField, BaseLookup


class DummyField(BaseField):
    def __init__(self, values: dict[str, Any]):
        self.values = values


class DummyLookup(BaseLookup[DummyField]):
    def join(self, fields, on, value, key, timestamp, headers):
        # Simulate enrichment by adding all field values to value dict
        for k, field in fields.items():
            value[k] = field.values[on]


def test_lookup_join_enriches_value(dataframe_factory):
    sdf = dataframe_factory()
    sdf = sdf.lookup_join(
        DummyLookup(),
        {
            "foo": DummyField({"key1": "foo", "key2": "bar"}),
            "num": DummyField({"key1": 123, "key2": 456}),
        },
        on=None,
    )

    enriched, *_ = sdf.test(value={}, key="key1", timestamp=123456)[0]
    assert enriched == {"foo": "foo", "num": 123}

    enriched, *_ = sdf.test(value={}, key="key2", timestamp=123456)[0]
    assert enriched == {"foo": "bar", "num": 456}


def test_lookup_join_on_column(dataframe_factory):
    sdf = dataframe_factory()
    sdf = sdf.lookup_join(
        DummyLookup(),
        {
            "foo": DummyField({"custom_key_1": "foo", "custom_key_2": "bar"}),
            "num": DummyField({"custom_key_1": 123, "custom_key_2": 456}),
        },
        on="custom_key",
    )

    enriched, *_ = sdf.test(
        value={"custom_key": "custom_key_1"}, key="key1", timestamp=123456
    )[0]
    assert enriched == {"custom_key": "custom_key_1", "foo": "foo", "num": 123}

    enriched, *_ = sdf.test(
        value={"custom_key": "custom_key_2"}, key="key2", timestamp=123456
    )[0]
    assert enriched == {"custom_key": "custom_key_2", "foo": "bar", "num": 456}


def test_lookup_join_on_callable(dataframe_factory):
    def _on(value, key):
        if key == "key1":
            return "custom_key_1"
        elif key == "key2":
            return "custom_key_2"
        return None

    sdf = dataframe_factory()
    sdf = sdf.lookup_join(
        DummyLookup(),
        {
            "foo": DummyField({"custom_key_1": "foo", "custom_key_2": "bar"}),
            "num": DummyField({"custom_key_1": 123, "custom_key_2": 456}),
        },
        on=_on,
    )

    enriched, *_ = sdf.test(value={}, key="key1", timestamp=123456)[0]
    assert enriched == {"foo": "foo", "num": 123}

    enriched, *_ = sdf.test(value={}, key="key2", timestamp=123456)[0]
    assert enriched == {"foo": "bar", "num": 456}
