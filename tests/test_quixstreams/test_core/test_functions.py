import pytest

from quixstreams.core.stream.functions import (
    ApplyFunction,
    FilterFunction,
    UpdateFunction,
    TransformFunction,
    ApplyWithMetadataFunction,
    UpdateWithMetadataFunction,
    FilterWithMetadataFunction,
)

from tests.utils import Sink


class TestFunctions:
    def test_apply_function(self):
        func = ApplyFunction(lambda v: v)
        result = Sink()
        key, timestamp, headers = b"key", 1, []
        func.get_executor(result.append_record)(1, key, timestamp, headers)
        assert result == [(1, key, timestamp, headers)]

    def test_apply_with_metadata_function(self):
        func = ApplyWithMetadataFunction(lambda value, _key, _timestamp, headers: value)
        result = Sink()
        key, timestamp, headers = b"key", 1, []
        func.get_executor(result.append_record)(1, key, timestamp, headers)
        assert result == [(1, key, timestamp, headers)]

    def test_apply_expand_function(self):
        func = ApplyFunction(lambda v: [v, v + 1], expand=True)
        result = Sink()
        key, timestamp, headers = b"key", 1, []
        func.get_executor(result.append_record)(1, key, timestamp, headers)
        assert result == [(1, key, timestamp, headers), (2, key, timestamp, headers)]

    def test_update_function(self):
        value = [0]
        expected = [0, 1]
        func = UpdateFunction(lambda v: v.append(1))
        result = Sink()
        key, timestamp, headers = b"key", 1, []
        func.get_executor(result.append_record)(value, key, timestamp, headers)
        assert result == [(expected, key, timestamp, headers)]

    def test_update_with_metadata_function(self):
        value = [0]
        expected = [0, 1]
        func = UpdateWithMetadataFunction(
            lambda value_, _key, _timestamp, _headers: value_.append(1)
        )
        result = Sink()
        key, timestamp, headers = b"key", 1, []
        func.get_executor(result.append_record)(value, key, timestamp, headers)
        assert result == [(expected, key, timestamp, headers)]

    @pytest.mark.parametrize(
        "value, key, timestamp, headers,  expected",
        [
            (1, b"key", 1, [1], []),
            (0, b"key", 1, [1], [(0, b"key", 1, [1])]),
        ],
    )
    def test_filter_function(self, value, key, timestamp, headers, expected):
        func = FilterFunction(lambda v: v == 0)
        result = Sink()
        func.get_executor(result.append_record)(value, key, timestamp, headers)
        assert result == expected

    @pytest.mark.parametrize(
        "value, key, timestamp, headers, expected",
        [
            (1, b"key", 1, [1], []),
            (0, b"key", 1, [1], [(0, b"key", 1, [1])]),
        ],
    )
    def test_filter_with_metadata_function(
        self, value, key, timestamp, headers, expected
    ):
        func = FilterWithMetadataFunction(
            lambda value_, _key, _timestamp, _headers: value_ == 0
        )
        result = Sink()
        func.get_executor(result.append_record)(value, key, timestamp, headers)
        assert result == expected

    def test_transform_record_function(self):
        func = TransformFunction(
            lambda value, key, timestamp, headers: (
                value + 1,
                key + "1",
                timestamp + 1,
                [("header", b"value")],
            )
        )
        value_, key_, timestamp_, headers_ = 0, "key", 1, []
        expected = (1, "key1", 2, [("header", b"value")])

        result = Sink()
        func.get_executor(result.append_record)(value_, key_, timestamp_, headers_)
        assert result[0] == expected

    def test_transform_record_expand_function(self):
        func = TransformFunction(
            lambda value, key, timestamp, _headers: [
                (value + 1, key + "1", timestamp + 1, [("key", b"value")]),
                (value + 2, key + "2", timestamp + 2, [("key", b"value")]),
            ],
            expand=True,
        )
        value_, key_, timestamp_, headers_ = 0, "key", 1, []
        expected = [
            (1, "key1", 2, [("key", b"value")]),
            (2, "key2", 3, [("key", b"value")]),
        ]

        result = Sink()
        func.get_executor(result.append_record)(value_, key_, timestamp_, headers_)
        assert result == expected
