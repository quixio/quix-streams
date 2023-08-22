from src.quixstreams.dataframes.dataframe.pipeline import (
    PipelineFunction,
)


class TestPipeline:
    def test_pipeline(self, pipeline_function, pipeline):
        assert pipeline._functions == [pipeline_function]

    def test_apply(self, pipeline):
        def throwaway_func(data):
            return data
        pipeline = pipeline.apply(throwaway_func)
        assert isinstance(pipeline.functions[-1], PipelineFunction)
        assert pipeline.functions[-1]._func == throwaway_func

    def test_process(self, pipeline):
        assert pipeline.process({'a': 1, 'b': 2}) == {'a': 2, 'b': 3}

    def test_process_list(self, pipeline):
        actual = pipeline.process([{'a': 1, 'b': 2}, {'a': 10, 'b': 11}])
        expected = [{'a': 2, 'b': 3}, {'a': 11, 'b': 12}]
        assert actual == expected

    def test_process_break_and_return_none(self, pipeline):
        """
        Add a new function that returns None, then reverse the _functions order
        so that an exception would get thrown if the pipeline doesn't (as expected)
        stop processing when attempting to execute a function with a NoneType input.
        """
        pipeline = pipeline.apply(lambda d: None)
        pipeline._functions.reverse()
        assert pipeline.process({'a': 1, 'b': 2}) is None
