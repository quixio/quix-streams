import pytest

from quixstreams.core.stream import (
    Stream,
    Filtered,
    is_filter_function,
    is_update_function,
    is_apply_function,
)


class TestStream:
    def test_add_apply(self):
        stream = Stream().add_apply(lambda v: v + 1)
        assert stream.compile()(1) == 2

    def test_add_update(self):
        stream = Stream().add_update(lambda v: v.append(1))
        assert stream.compile()([0]) == [0, 1]

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
                stream.compile()(value)
        else:
            assert stream.compile()(value) == value

    def test_tree(self):
        stream = (
            Stream()
            .add_apply(lambda v: v)
            .add_filter(lambda v: v)
            .add_update(lambda v: v)
        )
        tree = stream.tree()
        assert len(tree) == 4
        assert is_apply_function(tree[0].func)
        assert is_apply_function(tree[1].func)
        assert is_filter_function(tree[2].func)
        assert is_update_function(tree[3].func)

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
        assert is_apply_function(diff_tree[0].func)
        assert is_update_function(diff_tree[1].func)
        assert is_filter_function(diff_tree[2].func)

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

    def test_compile_allow_filters_false(self):
        stream = Stream().add_filter(lambda v: v)
        with pytest.raises(ValueError, match="Filter functions are not allowed"):
            stream.compile(allow_filters=False)

    def test_compile_allow_updates_false(self):
        stream = Stream().add_update(lambda v: v)
        with pytest.raises(ValueError, match="Update functions are not allowed"):
            stream.compile(allow_updates=False)

    def test_repr(self):
        stream = (
            Stream()
            .add_apply(lambda v: v)
            .add_update(lambda v: v)
            .add_filter(lambda v: v)
        )
        repr(stream)

    def test_init_with_unwrapped_function(self):
        ...
