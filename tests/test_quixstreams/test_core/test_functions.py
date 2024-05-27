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
        key, timestamp = b"key", 1
        func.get_executor(result.append_record)(1, key, timestamp)
        assert result == [(1, key, timestamp)]

    def test_apply_with_metadata_function(self):
        func = ApplyWithMetadataFunction(lambda value, _key, _timestamp: value)
        result = Sink()
        key, timestamp = b"key", 1
        func.get_executor(result.append_record)(1, key, timestamp)
        assert result == [(1, key, timestamp)]

    def test_apply_expand_function(self):
        func = ApplyFunction(lambda v: [v, v + 1], expand=True)
        result = Sink()
        key, timestamp = b"key", 1
        func.get_executor(result.append_record)(1, key, timestamp)
        assert result == [(1, key, timestamp), (2, key, timestamp)]

    def test_update_function(self):
        value = [0]
        expected = [0, 1]
        func = UpdateFunction(lambda v: v.append(1))
        result = Sink()
        key, timestamp = b"key", 1
        func.get_executor(result.append_record)(value, key, timestamp)
        assert result == [(expected, key, timestamp)]

    def test_update_with_metadata_function(self):
        value = [0]
        expected = [0, 1]
        func = UpdateWithMetadataFunction(
            lambda value_, _key, _timestamp: value_.append(1)
        )
        result = Sink()
        key, timestamp = b"key", 1
        func.get_executor(result.append_record)(value, key, timestamp)
        assert result == [(expected, key, timestamp)]

    @pytest.mark.parametrize(
        "value, key, timestamp, expected",
        [
            (1, b"key", 1, []),
            (0, b"key", 1, [(0, b"key", 1)]),
        ],
    )
    def test_filter_function(self, value, key, timestamp, expected):
        func = FilterFunction(lambda v: v == 0)
        result = Sink()
        func.get_executor(result.append_record)(value, key, timestamp)
        assert result == expected

    @pytest.mark.parametrize(
        "value, key, timestamp, expected",
        [
            (1, b"key", 1, []),
            (0, b"key", 1, [(0, b"key", 1)]),
        ],
    )
    def test_filter_with_metadata_function(self, value, key, timestamp, expected):
        func = FilterWithMetadataFunction(lambda value_, _key, _metadata: value_ == 0)
        result = Sink()
        func.get_executor(result.append_record)(value, key, timestamp)
        assert result == expected

    def test_transform_record_function(self):
        func = TransformFunction(
            lambda value, key, timestamp: (value + 1, key + "1", timestamp + 1)
        )
        value_, key_, timestamp_ = 0, "key", 1
        expected = (1, "key1", 2)

        result = Sink()
        func.get_executor(result.append_record)(value_, key_, timestamp_)
        assert result[0] == expected

    def test_transform_record_expand_function(self):
        func = TransformFunction(
            lambda value, key, timestamp: [
                (value + 1, key + "1", timestamp + 1),
                (value + 2, key + "2", timestamp + 2),
            ],
            expand=True,
        )
        value_, key_, timestamp_ = 0, "key", 1
        expected = [(1, "key1", 2), (2, "key2", 3)]

        result = Sink()
        func.get_executor(result.append_record)(value_, key_, timestamp_)
        assert result == expected
