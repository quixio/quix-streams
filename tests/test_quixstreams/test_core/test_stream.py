from operator import setitem

import pytest

from quixstreams.core.stream import (
    Stream,
    Filtered,
)
from quixstreams.core.stream.functions import (
    ApplyFunction,
    UpdateFunction,
    FilterFunction,
)


class TestStream:
    def test_add_apply(self):
        stream = Stream().add_apply(lambda v: v + 1)
        assert stream.compose()(1) == 2

    def test_add_update(self):
        stream = Stream().add_update(lambda v: v.append(1))
        assert stream.compose()([0]) == [0, 1]

    @pytest.mark.parametrize(
        "value, filtered",
        [
            (1, True),
            (0, False),
        ],
    )
    def test_add_filter(self, value, filtered):
        stream = Stream().add_filter(lambda v: v == 0)

        if filtered:
            with pytest.raises(Filtered):
                stream.compose()(value)
        else:
            assert stream.compose()(value) == value

    def test_tree(self):
        stream = (
            Stream()
            .add_apply(lambda v: v)
            .add_filter(lambda v: v)
            .add_update(lambda v: v)
        )
        tree = stream.tree()
        assert len(tree) == 4
        assert isinstance(tree[0].func, ApplyFunction)
        assert isinstance(tree[1].func, ApplyFunction)
        assert isinstance(tree[2].func, FilterFunction)
        assert isinstance(tree[3].func, UpdateFunction)

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

        diff_tree = diff.tree()
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

    def test_repr(self):
        stream = (
            Stream()
            .add_apply(lambda v: v)
            .add_update(lambda v: v)
            .add_filter(lambda v: v)
        )
        repr(stream)

    def test_init_with_unwrapped_function(self): ...

    def test_apply_expand(self):
        stream = Stream().add_apply(lambda v: [v, v], expand=True)
        result = stream.compose()(1)
        assert result == [1, 1]

    def test_apply_expand_not_iterable_returned(self):
        stream = Stream().add_apply(lambda v: 1, expand=True)
        with pytest.raises(TypeError):
            stream.compose()(1)

    def test_apply_expand_multiple(self):
        stream = (
            Stream()
            .add_apply(lambda v: [v + 1, v + 1], expand=True)
            .add_apply(lambda v: [v, v + 1], expand=True)
        )
        assert stream.compose()(1) == [2, 3, 2, 3]

    def test_apply_expand_filter_all_filtered(self):
        stream = (
            Stream()
            .add_apply(lambda v: [v, v], expand=True)
            .add_apply(lambda v: [v, v], expand=True)
            .add_filter(lambda v: v != 1)
        )
        with pytest.raises(Filtered):
            assert stream.compose()(1)

    def test_apply_expand_filter_some_filtered(self):
        stream = (
            Stream()
            .add_apply(lambda v: [v, v + 1], expand=True)
            .add_filter(lambda v: v != 1)
            .add_apply(lambda v: [v, v], expand=True)
        )
        result = stream.compose()(1)
        assert result == [2, 2]

    def test_apply_expand_update(self):
        stream = (
            Stream()
            .add_apply(lambda v: [{"x": v}, {"x": v + 1}], expand=True)
            .add_update(lambda v: setitem(v, "x", v["x"] + 1))
        )
        assert stream.compose()(1) == [
            {"x": 2},
            {"x": 3},
        ]

    def test_apply_expand_update_filter(self):
        stream = (
            Stream()
            .add_apply(lambda v: [{"x": v}, {"x": v + 1}], expand=True)
            .add_update(lambda v: setitem(v, "x", v["x"] + 1))
            .add_filter(lambda v: v["x"] != 2)
        )
        assert stream.compose()(1) == [{"x": 3}]

    def test_compose_allow_expands_false(self):
        stream = Stream().add_apply(lambda v: [{"x": v}, {"x": v + 1}], expand=True)
        with pytest.raises(ValueError, match="Expand functions are not allowed"):
            stream.compose(allow_expands=False)
