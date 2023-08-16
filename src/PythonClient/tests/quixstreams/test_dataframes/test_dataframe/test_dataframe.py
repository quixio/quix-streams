import pytest

from quixstreams.dataframes.dataframe.pipeline import InvalidPipelineBranching, Pipeline
from quixstreams.dataframes.dataframe.column import Column
from quixstreams.dataframes.models.rows import Row
from copy import deepcopy


class TestDataframe:
    def test_dataframe(self, dataframe):
        assert dataframe.name == 'test_dataframe'
        assert isinstance(dataframe._pipeline, Pipeline)
        assert dataframe._pipeline.name == dataframe.name

    def test__subset(self, dataframe, row):
        keys = ['x', 'y']
        result = dataframe._subset(keys, row)
        assert isinstance(result, Row)
        assert result.value == {k: row[k] for k in keys}

    def test__setitem(self, dataframe, row):
        key = 'new_key'
        result = dataframe._setitem(key, 'new_value', row)
        assert isinstance(result, Row)
        assert result[key] == 'new_value'
        assert id(row) != id(result)

    def test__setitem_to_column(self, dataframe, row, column):
        key = 'new_key'
        result = dataframe._setitem(key, column, row)
        assert isinstance(result, Row)
        assert result[key] == row.value['x']
        assert id(row) != id(result)

    def test__convert_result_to_row_from_dict(self, dataframe, row):
        new_data = {'new_data_key': 'new_data_value'}
        result = dataframe._convert_result_to_row(row, new_data)
        row.value = new_data
        assert result == row

    def test__convert_result_to_row_from_not_dict(self, dataframe, row):
        new_data = 'i can be anything, maybe even a boat'
        result = dataframe._convert_result_to_row(row, new_data)
        assert result == new_data

    def test__clone(self, dataframe):
        cloned_df = dataframe._clone()
        assert id(cloned_df) != id(dataframe)
        assert cloned_df._pipeline.name != dataframe._pipeline.name

    def test__column_filter_keep(self, dataframe, column, row):
        assert dataframe._column_filter(column, row) == row

    def test__column_filter_filter(self, dataframe, row):
        assert dataframe._column_filter(Column('k'), row) is None

    def test__row_apply(self, dataframe, row, message_value, row_values_plus_1_func, row_with_values_plus_1):
        assert dataframe._row_apply(row_values_plus_1_func, row) == row_with_values_plus_1

    def test__row_apply_with_list(self, dataframe, row_with_list, message_value_with_list, more_rows_func):
        expected_values = [{**message_value_with_list, 'x_list': v} for v in message_value_with_list['x_list']]
        expected = [deepcopy(row_with_list) for _ in message_value_with_list['x_list']]
        for i in range(len(expected)):
            expected[i].value = expected_values[i]
        assert dataframe._row_apply(more_rows_func, row_with_list) == expected

    def test_apply(self, dataframe):
        def test_func(row):
            return row
        dataframe2 = dataframe.apply(test_func)
        assert id(dataframe) != id(dataframe2)
        assert dataframe2._pipeline.functions[-1].name == 'apply:test_func'

    def test__dataframe_filter(self, dataframe, row):
        filter_dataframe = dataframe.apply(lambda row: 'banana')  # returning anything but None is valid
        assert dataframe._dataframe_filter(filter_dataframe, row) == row

    def test__dataframe_filter_filtered(self, dataframe, row):
        filter_dataframe = dataframe.apply(lambda row: 'banana' if row['x'] > 10000 else None)
        assert dataframe._dataframe_filter(filter_dataframe, row) is None

    def test__filter_column(self, dataframe, column):
        assert dataframe._filter(column).func == dataframe._column_filter

    def test__filter_dataframe(self, dataframe):
        assert dataframe._filter(dataframe).func == dataframe._dataframe_filter


class TestDataframeProcess:
    def test_row_apply_dict_return(self, dataframe, row, row_with_values_plus_1):
        dataframe = dataframe.apply(lambda row: {k: v + 1 for k, v in row.items()})
        assert dataframe.process(row) == row_with_values_plus_1

    def test_row_apply_row_return(self, dataframe, row, message_value, row_values_plus_1_func, row_with_values_plus_1):
        dataframe = dataframe.apply(row_values_plus_1_func)
        assert dataframe.process(row) == row_with_values_plus_1

    def test_row_apply_fluent(self, dataframe, row, message_value):
        dataframe = dataframe.apply(lambda row: {k: v + 1 for k, v in row.items()}).apply(lambda row: {k: v + 2 for k, v in row.items()})
        assert dataframe.process(row).value == {k: v + 3 for k, v in message_value.items()}

    def test_row_apply_sequential(self, dataframe, row, message_value):
        dataframe = dataframe.apply(lambda row: {k: v + 1 for k, v in row.items()})
        dataframe = dataframe.apply(lambda row: {k: v + 2 for k, v in row.items()})
        assert dataframe.process(row).value == {k: v + 3 for k, v in message_value.items()}

    def test_set_item_column_only(self, dataframe, row, message_value):
        dataframe['new_val'] = dataframe['x']
        assert dataframe.process(row).value['new_val'] == message_value['x']

    def test_set_item_column_with_function(self, dataframe, row, message_value):
        dataframe['new_val'] = dataframe['x'].apply(lambda v: v + 10)
        assert dataframe.process(row).value['new_val'] == (message_value['x'] + 10)

    def test_set_item_column_with_operations(self, dataframe, row, message_value):
        dataframe['new_val'] = dataframe['x'] + dataframe['y'].apply(lambda v: v + 10) + 1
        assert dataframe.process(row).value['new_val'] == message_value['x'] + (message_value['y'] + 10) + 1

    def test_column_subset(self, dataframe, row, message_value):
        key_list = ['x', 'y']
        dataframe = dataframe[key_list]
        assert dataframe.process(row).value == {k: message_value[k] for k in key_list}

    def test_column_subset_with_funcs(self, dataframe, row, message_value):
        key_list = ['x', 'y']
        dataframe = dataframe[key_list].apply(lambda row: {k: v + 1 for k, v in row.items()})
        assert dataframe.process(row).value == {k: message_value[k] + 1 for k in key_list}

    def test_inequality_filtering_continue(self, dataframe, row, message_value):
        dataframe = dataframe[dataframe['x'] > 0]
        assert dataframe.process(row).value == message_value

    def test_inequality_filtering_with_operation_continue(self, dataframe, row, message_value):
        dataframe = dataframe[(dataframe['x'] - message_value['x'] + dataframe['y']) > 0]
        assert dataframe.process(row).value == message_value

    def test_inequality_filtering_with_operation_filtered(self, dataframe, row, message_value):
        dataframe = dataframe[(dataframe['x'] - dataframe['y']) > 0]
        assert dataframe.process(row) is None

    def test_inequality_filtering_with_apply_filtered(self, dataframe, row, message_value):
        dataframe = dataframe[dataframe['x'].apply(lambda v: v - 1000) >= 0]
        assert dataframe.process(row) is None

    def test_inequality_filtering_filtered(self, dataframe, row, message_value):
        dataframe = dataframe[dataframe['x'] >= 1000]
        assert dataframe.process(row) is None

    def test_compound_inequality_filtering_continue(self, dataframe, row, message_value):
        dataframe = dataframe[(dataframe['x'] >= 0) & (dataframe['y'] < 100)]
        assert dataframe.process(row).value == message_value

    def test_compound_inequality_filtering_filtered(self, dataframe, row, message_value):
        dataframe = dataframe[(dataframe['x'] >= 0) & (dataframe['y'] < 0)]
        assert dataframe.process(row) is None

    def test_row_apply_filtering_continue(self, dataframe, row, message_value):
        dataframe = dataframe[dataframe.apply(lambda row: row if row['x'] > 0 else None)]
        assert dataframe.process(row).value == message_value

    def test_row_apply_filtering_filter_none(self, dataframe, row, message_value):
        dataframe = dataframe[dataframe.apply(lambda row: row if row['x'] < 0 else None)]
        assert dataframe.process(row) is None

    def test_row_apply_filtering_filter_false(self, dataframe, row, message_value):
        dataframe = dataframe[dataframe.apply(lambda row: False)]
        assert dataframe.process(row) is None

    def test_row_apply_filtering_sequential_continue(self, dataframe, row, message_value):
        p_filter = dataframe.apply(lambda row: row if row['x'] > 0 else None)
        dataframe = dataframe[p_filter]
        assert dataframe.process(row).value == message_value

    def test_row_apply_filtering_dataframe_uses_apply_in_filter(self, dataframe, row, message_value):
        dataframe = dataframe.apply(lambda row: {k: v + 1 for k, v in row.items()})
        dataframe = dataframe[dataframe.apply(lambda row: row)]
        assert dataframe.process(row).value == {k: v + 1 for k, v in message_value.items()}

    def test_row_apply_filtering_separate_filtering_dataframe(self, dataframe, row, message_value):
        dataframe = dataframe.apply(lambda row: {k: v + 1 for k, v in row.items()})
        filter_dataframe = dataframe.apply(lambda row: {k: v + 2 for k, v in row.items()})  # note, will not apply to final dataframe result!
        dataframe = dataframe[filter_dataframe.apply(lambda row: row)]
        assert dataframe.process(row).value == {k: v + 1 for k, v in message_value.items()}

    def test_row_apply_filtering_parent_child_divergence_raises_invalid(self, dataframe, row):
        """
        This is invalid due to the "parent" dataframe adding new functions after previously generating a
        child that is later used as the parent's filter. This is admittedly likely to be very edge-casey.
        """
        with pytest.raises(InvalidPipelineBranching):
            dataframe = dataframe.apply(lambda row: {k: v + 1 for k, v in row.items()})
            filter_dataframe = dataframe.apply(lambda row: {k: v + 2 for k, v in row.items()})
            dataframe = dataframe.apply(lambda row: {k: v + 3 for k, v in row.items()})
            dataframe = dataframe[filter_dataframe.apply(lambda row: row)]
            dataframe.process(row)

    @pytest.mark.skip('This should fail based on our outline but currently does not')
    # TODO: make this fail correctly
    def test_non_row_apply_breaks_things(self, dataframe, row):
        dataframe = dataframe.apply(lambda row: False)
        dataframe = dataframe.apply(lambda row: row)
        dataframe.process(row)

    def test_multiple_row_generation(self, dataframe, row_with_list, message_value_with_list, more_rows_func):
        dataframe = dataframe.apply(more_rows_func)
        expected = [{**message_value_with_list, 'x_list': v} for v in message_value_with_list['x_list']]
        actual = [row.value for row in dataframe.process(row_with_list)]
        assert actual == expected

    def test_multiple_row_generation_with_additional_apply(self, dataframe, row_with_list, message_value_with_list, more_rows_func):
        dataframe = dataframe.apply(more_rows_func)
        dataframe = dataframe.apply(lambda row: {k: v+1 if k == 'x_list' else v for k, v in row.items()})
        expected = [{**message_value_with_list, 'x_list': v + 1} for v in message_value_with_list['x_list']]
        actual = [row.value for row in dataframe.process(row_with_list)]
        assert actual == expected
