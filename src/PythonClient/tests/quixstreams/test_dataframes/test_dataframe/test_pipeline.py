import pytest
from quixstreams.dataframes.dataframe.pipeline import InvalidPipelineBranching, PipelineFunction


class TestPipeline:
    def test_pipeline(self, pipeline_function, pipeline):
        assert pipeline.name == 'test_pipeline'
        assert pipeline._parent == 'test_pipeline_parent'
        assert pipeline._graph == {'test_pipeline_parent': 'test_pipeline'}
        assert pipeline._functions == [pipeline_function]

    def test__clone(self, pipeline):
        clone = pipeline._clone()
        assert clone._parent == pipeline.name
        assert clone.functions == pipeline.functions
        assert id(clone._graph) != pipeline._graph

    def test_add_graph_node_with_parent(self, pipeline):
        pipeline._parent = 'throwaway'
        pipeline._add_graph_node()
        assert pipeline._graph['throwaway'] == pipeline.name

    def test__add_graph_node_no_parent(self, pipeline):
        pipeline._parent = None
        pipeline._add_graph_node()
        assert pipeline._graph[pipeline.name] is None

    def test__check_child_contains_parent(self, pipeline):
        pipeline.apply(lambda data: True)
        child_pipeline = pipeline._clone()
        child_pipeline = child_pipeline.apply(lambda data: 'banana')
        child_pipeline._check_child_contains_parent(pipeline)

    def test__check_child_contains_parent_raise_error(self, pipeline):
        with pytest.raises(InvalidPipelineBranching):
            child_pipeline = pipeline._clone()
            pipeline._graph['a_node'] = 'node_x'
            child_pipeline._graph['a_node'] = 'node_y'
            child_pipeline._check_child_contains_parent(pipeline)

    def test__remove_redundant_functions(self, pipeline):
        child_pipeline = pipeline._clone()
        pipeline._functions = ['f_0', 'f_1']
        child_pipeline._functions = ['f_0', 'f_1', 'f_2', 'f_3']
        assert child_pipeline._remove_redundant_functions(pipeline)._functions == ['f_2', 'f_3']

    def test__process_function(self, pipeline, pipeline_function, message_value):
        actual = pipeline._process_function(pipeline_function, message_value)
        expected = {k: v + 1 for k, v in message_value.items()}
        assert actual == expected

    def test__process_function_with_list(self, pipeline, pipeline_function, message_value):
        actual = pipeline._process_function(pipeline_function, [message_value, {'a': 10}])
        expected = [{k: v + 1 for k, v in message_value.items()}, {'a': 11}]
        assert actual == expected

    def test_apply(self, pipeline):
        def throwaway_func(data):
            return data
        pipeline = pipeline.apply(throwaway_func)
        assert isinstance(pipeline.functions[-1], PipelineFunction)
        assert pipeline.functions[-1]._func == throwaway_func

    def test_process(self, pipeline, message_value):
        actual = pipeline.process(message_value)
        expected = {k: v + 1 for k, v in message_value.items()}
        assert actual == expected

    def test_process_break_and_return_none(self, pipeline, message_value):
        pipeline = pipeline.apply(lambda data: None)
        # reverse so NoneType exception gets thrown if pipeline doesn't stop processing as expected
        pipeline._functions.reverse()
        assert pipeline.process(message_value) is None
