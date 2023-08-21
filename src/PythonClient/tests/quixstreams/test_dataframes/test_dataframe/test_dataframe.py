import pytest

from src.quixstreams.dataframes.dataframe.pipeline import (
    InvalidPipelineBranching,
    Pipeline,
)


class TestDataframe:
    def test_dataframe(self, dataframe):
        assert dataframe.name == "test_dataframe"
        assert isinstance(dataframe._pipeline, Pipeline)
        assert dataframe._pipeline.name == dataframe.name

    def test__clone(self, dataframe):
        cloned_df = dataframe._clone()
        assert id(cloned_df) != id(dataframe)
        assert cloned_df._pipeline.name != dataframe._pipeline.name

    def test_apply(self, dataframe):
        def test_func(row):
            return row

        dataframe2 = dataframe.apply(test_func)
        assert id(dataframe) != id(dataframe2)
        assert dataframe2._pipeline.functions[-1].name == "apply:test_func"


class TestDataframeProcess:
    def test_row_apply(self, dataframe, row_with_ints, row_plus_n_func, row_plus_n):
        n = 1
        dataframe = dataframe.apply(row_plus_n_func(n))
        expected = row_plus_n(n, row_with_ints)
        assert dataframe.process(row_with_ints) == expected

    def test_row_apply_fluent(
        self,
        dataframe,
        row_with_ints,
        row_plus_n_func,
        row_plus_n,
        row_msg_value_factory,
    ):
        dataframe = dataframe.apply(row_plus_n_func(1)).apply(row_plus_n_func(2))
        expected = row_plus_n(3, row_with_ints)
        assert dataframe.process(row_with_ints) == expected

    def test_row_apply_sequential(
        self, dataframe, row_with_ints, row_plus_n_func, row_plus_n
    ):
        dataframe = dataframe.apply(row_plus_n_func(1))
        dataframe = dataframe.apply(row_plus_n_func(2))
        expected = row_plus_n(3, row_with_ints)
        actual = dataframe.process(row_with_ints)
        assert actual == expected

    def test_set_item_primitive(
        self, dataframe, row_with_ints, msg_value_with_ints, row_msg_value_factory
    ):
        dataframe["new_val"] = 1
        expected = row_msg_value_factory({**msg_value_with_ints, "new_val": 1})
        actual = dataframe.process(row_with_ints)
        assert actual == expected

    def test_set_item_column_only(
        self, dataframe, row_with_ints, msg_value_with_ints, row_msg_value_factory
    ):
        dataframe["new_val"] = dataframe["x"]
        expected = row_msg_value_factory(
            {**msg_value_with_ints, "new_val": msg_value_with_ints["x"]}
        )
        actual = dataframe.process(row_with_ints)
        assert actual == expected

    def test_set_item_column_with_function(
        self, dataframe, row_with_ints, msg_value_with_ints, row_msg_value_factory
    ):
        dataframe["new_val"] = dataframe["x"].apply(lambda v: v + 10)
        expected = row_msg_value_factory(
            {**msg_value_with_ints, "new_val": msg_value_with_ints["x"] + 10}
        )
        actual = dataframe.process(row_with_ints)
        assert actual == expected

    def test_set_item_column_with_operations(
        self, dataframe, row_with_ints, msg_value_with_ints, row_msg_value_factory
    ):
        dataframe["new_val"] = (
            dataframe["x"] + dataframe["y"].apply(lambda v: v + 10) + 1
        )
        expected = row_msg_value_factory(
            {
                **msg_value_with_ints,
                "new_val": msg_value_with_ints["x"]
                + (msg_value_with_ints["y"] + 10)
                + 1,
            }
        )
        actual = dataframe.process(row_with_ints)
        assert actual == expected

    def test_column_subset(
        self, dataframe, row_with_ints, msg_value_with_ints, row_msg_value_factory
    ):
        key_list = ["x", "y"]
        dataframe = dataframe[key_list]
        expected = row_msg_value_factory({k: msg_value_with_ints[k] for k in key_list})
        actual = dataframe.process(row_with_ints)
        assert actual == expected

    def test_column_subset_with_funcs(
        self,
        dataframe,
        row_with_ints,
        msg_value_with_ints,
        row_plus_n_func,
        row_msg_value_factory,
    ):
        n = 1
        key_list = ["x", "y"]
        dataframe = dataframe[key_list].apply(row_plus_n_func(n))
        expected = row_msg_value_factory(
            {k: msg_value_with_ints[k] + n for k in key_list}
        )
        actual = dataframe.process(row_with_ints)
        assert actual == expected

    def test_inequality_filtering(self, dataframe, row_with_ints):
        dataframe = dataframe[dataframe["x"] > 0]
        assert dataframe.process(row_with_ints) == row_with_ints

    def test_inequality_filtering_with_operation(self, dataframe, row_with_ints):
        dataframe = dataframe[(dataframe["x"] - 0 + dataframe["y"]) > 0]
        assert dataframe.process(row_with_ints) == row_with_ints

    def test_inequality_filtering_with_operation_filtered(
        self, dataframe, row_with_ints
    ):
        dataframe = dataframe[(dataframe["x"] - dataframe["y"]) > 0]
        assert dataframe.process(row_with_ints) is None

    def test_inequality_filtering_with_apply_filtered(self, dataframe, row_with_ints):
        dataframe = dataframe[dataframe["x"].apply(lambda v: v - 1000) >= 0]
        assert dataframe.process(row_with_ints) is None

    def test_inequality_filtering_filtered(self, dataframe, row_with_ints):
        dataframe = dataframe[dataframe["x"] >= 1000]
        assert dataframe.process(row_with_ints) is None

    def test_compound_inequality_filtering_continue(self, dataframe, row_with_ints):
        dataframe = dataframe[(dataframe["x"] >= 0) & (dataframe["y"] < 100)]
        assert dataframe.process(row_with_ints) == row_with_ints

    def test_compound_inequality_filtering_filtered(self, dataframe, row_with_ints):
        dataframe = dataframe[(dataframe["x"] >= 0) & (dataframe["y"] < 0)]
        assert dataframe.process(row_with_ints) is None

    @pytest.mark.skip("SDF-based filtering is not supported yet")
    def test_row_apply_filtering_fails(self, dataframe, row_with_ints):
        dataframe = dataframe[
            dataframe.apply(lambda row: row if row["x"] > 0 else None)
        ]
        with pytest.raises(
            ValueError,
            match="Filtering based on StreamingDataFrame is not supported yet",
        ):
            assert dataframe.process(row_with_ints) == row_with_ints

    @pytest.mark.skip("SDF-based filtering is not supported yet")
    def test_row_apply_filtering_continue(self, dataframe, row_with_ints):
        dataframe = dataframe[
            dataframe.apply(lambda row: row if row["x"] > 0 else None)
        ]
        assert dataframe.process(row_with_ints) == row_with_ints

    @pytest.mark.skip("SDF-based filtering is not supported yet")
    def test_row_apply_filtering_filter_none(self, dataframe, row_with_ints):
        dataframe = dataframe[
            dataframe.apply(lambda row: row if row["x"] < 0 else None)
        ]
        assert dataframe.process(row_with_ints) is None

    @pytest.mark.skip("SDF-based filtering is not supported yet")
    def test_row_apply_filtering_filter_false(self, dataframe, row_with_ints):
        dataframe = dataframe[dataframe.apply(lambda row: False)]
        assert dataframe.process(row_with_ints) is None

    @pytest.mark.skip("SDF-based filtering is not supported yet")
    def test_row_apply_filtering_sequential_continue(self, dataframe, row_with_ints):
        p_filter = dataframe.apply(lambda row: row if row["x"] > 0 else None)
        dataframe = dataframe[p_filter]
        assert dataframe.process(row_with_ints) == row_with_ints

    @pytest.mark.skip("Requires copying, not implemented yet")
    def test_row_apply_filtering_dataframe_uses_apply_in_filter(
        self, dataframe, row_with_ints, row_plus_n_func, row_plus_n
    ):
        n = 1
        dataframe = dataframe.apply(row_plus_n_func(n))
        dataframe = dataframe[dataframe.apply(lambda row: row)]
        assert dataframe.process(row_with_ints) == row_plus_n(n, row_with_ints)

    @pytest.mark.skip("Requires copying, not implemented yet")
    def test_row_apply_filtering_separate_filtering_dataframe(
        self, dataframe, row_with_ints, row_plus_n_func, row_plus_n
    ):
        dataframe = dataframe.apply(row_plus_n_func(1))
        # note, filter should NOT apply to final dataframe result!
        filter_dataframe = dataframe.apply(row_plus_n_func(2))
        dataframe = dataframe[filter_dataframe.apply(lambda row: row)]
        assert dataframe.process(row_with_ints) == row_plus_n(1, row_with_ints)

    @pytest.mark.skip("SDF-based filtering is not supported yet")
    def test_row_apply_filtering_parent_child_divergence_raises_invalid(
        self, dataframe, row_with_ints, row_plus_n_func
    ):
        """
        This is invalid due to the "parent" dataframe adding new functions after
        previously generating a child that is later used as the parent's filter.

        This is, admittedly, likely to be very edge-casey.
        """
        with pytest.raises(InvalidPipelineBranching):
            dataframe = dataframe.apply(row_plus_n_func(1))
            filter_dataframe = dataframe.apply(row_plus_n_func(2))
            dataframe = dataframe.apply(row_plus_n_func(3))
            dataframe = dataframe[filter_dataframe.apply(lambda row: row)]
            dataframe.process(row_with_ints)

    @pytest.mark.skip("This should fail based on our outline but currently does not")
    # TODO: make this fail correctly
    def test_non_row_apply_breaks_things(self, dataframe, row_with_ints):
        dataframe = dataframe.apply(lambda row: False)
        dataframe = dataframe.apply(lambda row: row)
        dataframe.process(row_with_ints)

    def test_multiple_row_generation(
        self,
        dataframe,
        more_rows_func,
        row_msg_value_factory,
        row_with_list,
        msg_value_with_list,
    ):
        dataframe = dataframe.apply(more_rows_func)
        expected = [
            row_msg_value_factory({**msg_value_with_list, "x_list": v})
            for v in msg_value_with_list["x_list"]
        ]
        actual = dataframe.process(row_with_list)
        assert actual == expected

    def test_multiple_row_generation_with_additional_apply(
        self,
        dataframe,
        more_rows_func,
        row_msg_value_factory,
        row_with_list,
        msg_value_with_list,
    ):
        def add_to_xlist(row):
            row["x_list"] += 1
            return row

        dataframe = dataframe.apply(more_rows_func)
        dataframe = dataframe.apply(add_to_xlist)
        expected = [
            row_msg_value_factory({**msg_value_with_list, "x_list": v + 1})
            for v in msg_value_with_list["x_list"]
        ]
        actual = dataframe.process(row_with_list)
        assert actual == expected

    def test_multiple_row_generation_with_additional_filtering(
        self,
        dataframe,
        more_rows_func,
        row_msg_value_factory,
        row_with_list,
        msg_value_with_list,
    ):
        dataframe = dataframe.apply(more_rows_func)
        dataframe = dataframe.apply(lambda row: row if row["x_list"] > 0 else None)
        expected = [
            row_msg_value_factory({**msg_value_with_list, "x_list": v})
            for v in msg_value_with_list["x_list"][1:]
        ]
        actual = dataframe.process(row_with_list)
        assert actual == expected
