from operator import setitem

import pytest

from quixstreams.core.stream import Stream
from quixstreams.core.stream.functions import (
    ApplyFunction,
    UpdateFunction,
    FilterFunction,
    TransformFunction,
)
from .utils import Sink


class TestStream:
    def test_add_apply(self):
        stream = Stream().add_apply(lambda v: v + 1)
        sink = Sink()
        stream.compose(sink=sink.append_record)(1, "key", 0, [])
        assert sink[0] == (2, "key", 0, [])

    def test_add_update(self):
        stream = Stream().add_update(lambda v: v.append(1))
        result = Sink()
        stream.compose(sink=result.append_record)([0], "key", 0, [])
        assert result[0] == ([0, 1], "key", 0, [])

    @pytest.mark.parametrize(
        "value, key, timestamp, headers, expected",
        [
            (1, "key", 1, [], []),
            (0, "key", 1, [], [(0, "key", 1, [])]),
        ],
    )
    def test_add_filter(self, value, key, timestamp, headers, expected):
        stream = Stream().add_filter(lambda v: v == 0)
        result = Sink()
        stream.compose(sink=result.append_record)(value, key, timestamp, headers)
        assert result == expected

    def test_tree_root_path(self):
        stream = (
            Stream()
            .add_apply(lambda v: ...)
            .add_filter(lambda v: ...)
            .add_update(lambda v: ...)
            .add_transform(lambda v, k, t, h: ...)
        )
        tree = stream.tree_root_path()
        assert len(tree) == 5
        assert isinstance(tree[0].func, ApplyFunction)
        assert isinstance(tree[1].func, ApplyFunction)
        assert isinstance(tree[2].func, FilterFunction)
        assert isinstance(tree[3].func, UpdateFunction)
        assert isinstance(tree[4].func, TransformFunction)

    def test_diff_success(self):
        stream = Stream()
        stream = stream.add_apply(lambda v: v)
        stream2 = (
            stream.add_apply(lambda v: v)
            .add_update(lambda v: v)
            .add_filter(lambda v: v)
        )

        stream = stream.add_apply(lambda v: v)

        diff = stream.diff(stream2)

        diff_tree = diff.tree_root_path()
        assert len(diff_tree) == 3
        assert isinstance(diff_tree[0].func, ApplyFunction)
        assert isinstance(diff_tree[1].func, UpdateFunction)
        assert isinstance(diff_tree[2].func, FilterFunction)

    def test_diff_empty_same_stream_fails(self):
        stream = Stream()
        with pytest.raises(ValueError, match="The diff is empty"):
            stream.diff(stream)

    def test_diff_empty_stream_full_child_fails(self):
        stream = Stream()
        stream2 = stream.add_apply(lambda v: v)
        with pytest.raises(ValueError, match="The diff is empty"):
            stream2.diff(stream)

    def test_diff_no_common_parent_fails(self):
        stream = Stream()
        stream2 = Stream()
        with pytest.raises(ValueError, match="Common parent not found"):
            stream.diff(stream2)

    def test_compose_allow_filters_false(self):
        stream = Stream().add_filter(lambda v: v)
        with pytest.raises(ValueError, match="Filter functions are not allowed"):
            stream.compose(allow_filters=False)

    def test_compose_allow_updates_false(self):
        stream = Stream().add_update(lambda v: v)
        with pytest.raises(ValueError, match="Update functions are not allowed"):
            stream.compose(allow_updates=False)

    def test_compose_allow_transforms_false(self):
        stream = Stream().add_transform(lambda value, key, timestamp, headers: ...)
        with pytest.raises(ValueError, match="Transform functions are not allowed"):
            stream.compose(allow_transforms=False)

    def test_repr(self):
        stream = (
            Stream()
            .add_apply(lambda v: v)
            .add_update(lambda v: v)
            .add_filter(lambda v: v)
        )
        repr(stream)

    def test_apply_expand(self):
        stream = Stream().add_apply(lambda v: [v, v], expand=True)
        result = Sink()
        value, key, timestamp, headers = 1, "key", 1, []

        stream.compose(sink=result.append_record)(value, key, timestamp, headers)
        assert result == [
            (value, key, timestamp, headers),
            (value, key, timestamp, headers),
        ]

    def test_apply_expand_not_iterable_returned(self):
        stream = Stream().add_apply(lambda v: 1, expand=True)
        with pytest.raises(TypeError):
            stream.compose()(1, "key", 0, [])

    def test_apply_expand_multiple(self):
        stream = (
            Stream()
            .add_apply(lambda v: [v + 1, v + 1], expand=True)
            .add_apply(lambda v: [v, v + 1], expand=True)
        )
        result = Sink()
        value, key, timestamp, headers = 1, "key", 1, [("key", b"value")]
        stream.compose(sink=result.append_record)(value, key, timestamp, headers)
        assert result == [
            (value + 1, key, timestamp, headers),
            (value + 2, key, timestamp, headers),
            (value + 1, key, timestamp, headers),
            (value + 2, key, timestamp, headers),
        ]

    def test_apply_expand_filter_all_filtered(self):
        stream = (
            Stream()
            .add_apply(lambda v: [v, v], expand=True)
            .add_apply(lambda v: [v, v], expand=True)
            .add_filter(lambda v: v != 1)
        )
        result = Sink()
        stream.compose(sink=result.append_record)(1, "key", 0, [])
        assert result == []

    def test_apply_expand_filter_some_filtered(self):
        stream = (
            Stream()
            .add_apply(lambda v: [v, v + 1], expand=True)
            .add_filter(lambda v: v != 1)
            .add_apply(lambda v: [v, v], expand=True)
        )
        result = Sink()
        value, key, timestamp, headers = 1, "key", 1, None
        stream.compose(sink=result.append_record)(1, key, timestamp, headers)
        assert result == [(2, key, timestamp, headers), (2, key, timestamp, headers)]

    def test_apply_expand_update(self):
        stream = (
            Stream()
            .add_apply(lambda v: [{"x": v}, {"x": v + 1}], expand=True)
            .add_update(lambda v: setitem(v, "x", v["x"] + 1))
        )
        result = Sink()
        key, timestamp, headers = "key", 123, None
        stream.compose(sink=result.append_record)(1, key, timestamp, headers)
        assert result == [
            ({"x": 2}, key, timestamp, headers),
            ({"x": 3}, key, timestamp, headers),
        ]

    def test_apply_expand_update_filter(self):
        stream = (
            Stream()
            .add_apply(lambda v: [{"x": v}, {"x": v + 1}], expand=True)
            .add_update(lambda v: setitem(v, "x", v["x"] + 1))
            .add_filter(lambda v: v["x"] != 2)
        )
        result = Sink()
        key, timestamp, headers = "key", 123, []
        stream.compose(sink=result.append_record)(1, key, timestamp, headers)
        assert result == [({"x": 3}, key, timestamp, headers)]

    def test_compose_allow_expands_false(self):
        stream = Stream().add_apply(lambda v: [{"x": v}, {"x": v + 1}], expand=True)
        with pytest.raises(ValueError, match="Expand functions are not allowed"):
            stream.compose(allow_expands=False)

    def test_add_apply_with_metadata(self):
        stream = Stream().add_apply(
            lambda v, key, timestamp, headers: v + 1, metadata=True
        )
        sink = Sink()
        stream.compose(sink=sink.append_record)(1, "key", 0, None)
        assert sink[0] == (2, "key", 0, None)

    def test_apply_record_with_metadata_expanded(self):
        stream = Stream().add_apply(
            lambda value_, key_, timestamp_, headers_: [value_, value_],
            expand=True,
            metadata=True,
        )
        result = Sink()
        value, key, timestamp, headers = 1, "key", 1, []

        stream.compose(sink=result.append_record)(value, key, timestamp, headers)
        assert result == [
            (value, key, timestamp, headers),
            (value, key, timestamp, headers),
        ]

    def test_add_update_with_metadata(self):
        stream = Stream().add_update(
            lambda value, key, timestamp, headers: value.append(1), metadata=True
        )
        result = Sink()
        stream.compose(sink=result.append_record)([0], "key", 0, [])
        assert result[0] == ([0, 1], "key", 0, [])

    @pytest.mark.parametrize(
        "value, key, timestamp, headers , expected",
        [
            (1, "key", 1, [], []),
            (0, "key", 1, [], [(0, "key", 1, [])]),
        ],
    )
    def test_add_filter_with_metadata(self, value, key, timestamp, headers, expected):
        stream = Stream().add_filter(
            lambda value_, key_, timestamp_, headers_: value_ == 0, metadata=True
        )
        result = Sink()
        stream.compose(sink=result.append_record)(value, key, timestamp, headers)
        assert result == expected

    def test_compose_returning(self):
        stream = Stream().add_apply(lambda v: v + 1)
        assert stream.compose_returning()(1, "key", 0, []) == (2, "key", 0, [])
        assert stream.compose_returning()(2, "key", 0, []) == (3, "key", 0, [])

    def test_compose_returning_exception(self):
        """
        Check that internal buffer of the composed function is emptied correctly
        in case of error
        """

        def _fail(value):
            if value == 1:
                raise ValueError("fail")
            return value + 1

        stream = Stream().add_apply(_fail)
        assert stream.compose_returning()(2, "key", 0, None) == (3, "key", 0, None)
        with pytest.raises(ValueError):
            assert stream.compose_returning()(1, "key", 0, None) == (3, "key", 0, None)
        assert stream.compose_returning()(2, "key", 0, None) == (3, "key", 0, None)

    @pytest.mark.parametrize(
        "stream, err",
        [
            (Stream().add_update(lambda v: ...), "Update functions are not allowed"),
            (
                Stream().add_update(lambda v, k, t, h: ..., metadata=True),
                "Update functions are not allowed",
            ),
            (Stream().add_filter(lambda v: ...), "Filter functions are not allowed"),
            (
                Stream().add_filter(lambda v, k, t, h: ..., metadata=True),
                "Filter functions are not allowed",
            ),
            (
                Stream().add_apply(lambda v: ..., expand=True),
                "Expand functions are not allowed",
            ),
            (
                Stream().add_apply(lambda v, k, t, h: ..., expand=True, metadata=True),
                "Expand functions are not allowed",
            ),
            (
                Stream().add_transform(lambda v, k, t, h: ..., expand=True),
                "Transform functions are not allowed",
            ),
            (
                Stream().add_transform(lambda v, k, t, h: ...),
                "Transform functions are not allowed",
            ),
        ],
    )
    def test_compose_returning_not_allowed_operations_fails(self, stream, err):
        with pytest.raises(ValueError, match=err):
            stream.compose_returning()

    def test_transform_record(self):
        stream = Stream().add_transform(
            lambda value, key, timestamp, headers: (
                value + 1,
                key + "1",
                timestamp + 1,
                [("key", b"value")],
            )
        )
        result = Sink()
        stream.compose(sink=result.append_record)(0, "key", 0, [])
        assert result[0] == (1, "key1", 1, [("key", b"value")])

    def test_transform_record_expanded(self):
        stream = Stream().add_transform(
            lambda value, key, timestamp, headers: [
                (value + 1, key + "1", timestamp + 1, [("key", b"value")]),
                (value + 2, key + "2", timestamp + 2, [("key", b"value2")]),
            ],
            expand=True,
        )
        result = Sink()
        stream.compose(sink=result.append_record)(0, "key", 0, [])
        assert result == [
            (1, "key1", 1, [("key", b"value")]),
            (2, "key2", 2, [("key", b"value2")]),
        ]
