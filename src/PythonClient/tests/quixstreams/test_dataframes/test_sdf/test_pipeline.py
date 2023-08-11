import pytest

from quixstreams.dataframes.sdf.pipeline import Column, InvalidFilter


class TestColumn:
    def test__apply(self, message_value):
        result = Column('x').apply(lambda v: v + 22)
        assert isinstance(result, Column)
        assert result.eval(message_value) == 27

    def test__addition(self, message_value):
        result = Column('x') + Column('y')
        assert isinstance(result, Column)
        assert result.eval(message_value) == 25

    def test__multi_op(self, message_value):
        result = Column('x') + Column('y') + Column('z')
        assert isinstance(result, Column)
        assert result.eval(message_value) == 135

    def test__scaler_op(self, message_value):
        result = Column('x') + 2
        assert isinstance(result, Column)
        assert result.eval(message_value) == 7

    def test__subtraction(self, message_value):
        result = Column('y') - Column('x')
        assert isinstance(result, Column)
        assert result.eval(message_value) == 15

    def test__multiplication(self, message_value):
        result = Column('x') * Column('y')
        assert isinstance(result, Column)
        assert result.eval(message_value) == 100

    def test__div(self, message_value):
        result = Column('z') / Column('y')
        assert isinstance(result, Column)
        assert result.eval(message_value) == 5.5

    def test__mod(self, message_value):
        result = Column('y') % Column('x')
        assert isinstance(result, Column)
        assert result.eval(message_value) == 0

    def test__equality__true(self, message_value):
        result = Column('x') == Column('x2')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test__equality__false(self, message_value):
        result = Column('x') == Column('y')
        assert isinstance(result, Column)
        assert not result.eval(message_value)

    def test__inequality__true(self, message_value):
        result = Column('x') != Column('y')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test__inequality__false(self, message_value):
        result = Column('x') != Column('x2')
        assert isinstance(result, Column)
        assert not result.eval(message_value)

    def test__less_than__true(self, message_value):
        result = Column('x') < Column('y')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test__less_than__false(self, message_value):
        result = Column('y') < Column('x')
        assert isinstance(result, Column)
        assert not result.eval(message_value)

    def test__less_than_or_equal__equal_true(self, message_value):
        result = Column('x') <= Column('x2')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test__less_than_or_equal__less_true(self, message_value):
        result = Column('x') <= Column('y')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test__less_than_or_equal__false(self, message_value):
        result = Column('y') <= Column('x')
        assert isinstance(result, Column)
        assert not result.eval(message_value)

    def test__greater_than__true(self, message_value):
        result = Column('y') > Column('x')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test__greater_than__false(self, message_value):
        result = Column('x') > Column('y')
        assert isinstance(result, Column)
        assert not result.eval(message_value)

    def test__greater_than_or_equal__equal_true(self, message_value):
        result = Column('x') >= Column('x2')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test__greater_than_or_equal__greater_true(self, message_value):
        result = Column('y') >= Column('x')
        assert isinstance(result, Column)
        assert result.eval(message_value)

    def test__greater_than_or_equal__greater_false(self, message_value):
        result = Column('x') >= Column('y')
        assert isinstance(result, Column)
        assert not result.eval(message_value)


class TestPipelineExamples:
    def test__row_apply(self, pipeline, row, message_value):
        pipeline = pipeline.apply(lambda row: {k: v + 1 for k, v in row.items()})
        assert pipeline.process(row).value == {k: v + 1 for k, v in message_value.items()}

    def test__row_apply__fluent(self, pipeline, row, message_value):
        pipeline = pipeline.apply(lambda row: {k: v + 1 for k, v in row.items()}).apply(lambda row: {k: v + 2 for k, v in row.items()})
        assert pipeline.process(row).value == {k: v + 3 for k, v in message_value.items()}

    def test__row_apply__sequential(self, pipeline, row, message_value):
        pipeline = pipeline.apply(lambda row: {k: v + 1 for k, v in row.items()})
        pipeline = pipeline.apply(lambda row: {k: v + 2 for k, v in row.items()})
        assert pipeline.process(row).value == {k: v + 3 for k, v in message_value.items()}

    def test__set_item__column_only(self, pipeline, row, message_value):
        pipeline['new_val'] = pipeline['x']
        assert pipeline.process(row).value['new_val'] == message_value['x']

    def test__set_item__column_with_function(self, pipeline, row, message_value):
        pipeline['new_val'] = pipeline['x'].apply(lambda v: v + 10)
        assert pipeline.process(row).value['new_val'] == (message_value['x'] + 10)

    def test__set_item__column_with_operations(self, pipeline, row, message_value):
        pipeline['new_val'] = pipeline['x'] + pipeline['y'].apply(lambda v: v + 10) + 1
        assert pipeline.process(row).value['new_val'] == message_value['x'] + (message_value['y'] + 10) + 1

    def test__column_subset(self, pipeline, row, message_value):
        key_list = ['x', 'y']
        pipeline = pipeline[key_list]
        assert pipeline.process(row).value == {k: message_value[k] for k in key_list}

    def test__column_subset__with_funcs(self, pipeline, row, message_value):
        key_list = ['x', 'y']
        pipeline = pipeline[key_list].apply(lambda row: {k: v + 1 for k, v in row.items()})
        assert pipeline.process(row).value == {k: message_value[k] + 1 for k in key_list}

    def test__inequality_filtering__continue(self, pipeline, row, message_value):
        pipeline = pipeline[pipeline['x'] > 0]
        assert pipeline.process(row).value == message_value

    def test__inequality_filtering_with_operation__continue(self, pipeline, row, message_value):
        pipeline = pipeline[(pipeline['x'] - message_value['x'] + pipeline['y']) > 0]
        assert pipeline.process(row).value == message_value

    def test__inequality_filtering_with_operation__filtered(self, pipeline, row, message_value):
        pipeline = pipeline[(pipeline['x'] - pipeline['y']) > 0]
        assert pipeline.process(row) is None

    def test__inequality_filtering_with_apply__filtered(self, pipeline, row, message_value):
        pipeline = pipeline[pipeline['x'].apply(lambda v: v - 1000) >= 0]
        assert pipeline.process(row) is None

    def test__inequality_filtering__filtered(self, pipeline, row, message_value):
        pipeline = pipeline[pipeline['x'] >= 1000]
        assert pipeline.process(row) is None

    def test__compound_inequality_filtering__continue(self, pipeline, row, message_value):
        pipeline = pipeline[(pipeline['x'] >= 0) & (pipeline['y'] < 100)]
        assert pipeline.process(row).value == message_value

    def test__compound_inequality_filtering__filtered(self, pipeline, row, message_value):
        pipeline = pipeline[(pipeline['x'] >= 0) & (pipeline['y'] < 0)]
        assert pipeline.process(row) is None

    def test__row_apply_filtering__continue(self, pipeline, row, message_value):
        pipeline = pipeline[pipeline.apply(lambda row: row if row['x'] > 0 else None)]
        assert pipeline.process(row).value == message_value

    def test__row_apply_filtering__filter_none(self, pipeline, row, message_value):
        pipeline = pipeline[pipeline.apply(lambda row: row if row['x'] < 0 else None)]
        assert pipeline.process(row) is None

    def test__row_apply_filtering__filter_false(self, pipeline, row, message_value):
        pipeline = pipeline[pipeline.apply(lambda row: False)]
        assert pipeline.process(row) is None

    def test__row_apply_filtering__sequential__continue(self, pipeline, row, message_value):
        p_filter = pipeline.apply(lambda row: row if row['x'] > 0 else None)
        pipeline = pipeline[p_filter]
        assert pipeline.process(row).value == message_value

    def test__row_apply_filtering__pipeline_uses_apply_in_filter(self, pipeline, row, message_value):
        pipeline = pipeline.apply(lambda row: {k: v + 1 for k, v in row.items()})
        pipeline = pipeline[pipeline.apply(lambda row: row)]
        assert pipeline.process(row).value == {k: v + 1 for k, v in message_value.items()}

    def test__row_apply_filtering__separate_filtering_pipeline(self, pipeline, row, message_value):
        pipeline = pipeline.apply(lambda row: {k: v + 1 for k, v in row.items()})
        filter_pipeline = pipeline.apply(lambda row: {k: v + 2 for k, v in row.items()})  # note, will not apply to final pipeline result!
        pipeline = pipeline[filter_pipeline.apply(lambda row: row)]
        assert pipeline.process(row).value == {k: v + 1 for k, v in message_value.items()}

    def test__row_apply_filtering__parent_child_divergence__raises_invalid(self, pipeline, row):
        """
        This is invalid due to the "parent" pipeline adding new functions after previously generating a
        child that is later used as the filter. This is admittedly likely to be very edge-casey.
        """
        with pytest.raises(InvalidFilter):
            pipeline = pipeline.apply(lambda row: {k: v + 1 for k, v in row.items()})
            filter_pipeline = pipeline.apply(lambda row: {k: v + 2 for k, v in row.items()})
            pipeline = pipeline.apply(lambda row: {k: v + 3 for k, v in row.items()})
            pipeline = pipeline[filter_pipeline.apply(lambda row: row)]
            pipeline.process(row)

    @pytest.skip('This should fail based on our documentation, but maybe it should not...')
    def test__non_row_apply_breaks_things(self, pipeline, row):
        pipeline = pipeline.apply(lambda row: False)
        pipeline = pipeline.apply(lambda row: row)
        pipeline.process(row)
