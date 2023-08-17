import pytest

from src.quixstreams.dataframes.dataframe.pipeline import (
    InvalidPipelineBranching,
    PipelineFunction
)


# TODO: change/remove graph/child-based tests based on upcoming SDF merge function


class TestPipeline:
    def test_pipeline(self, pipeline_function, pipeline):
        assert pipeline.name == 'test_pipeline'
        assert pipeline._parent == 'test_pipeline_parent'
        assert pipeline._graph == {'test_pipeline_parent': 'test_pipeline'}
        assert pipeline._functions == [pipeline_function]

    def test__clone(self, pipeline):
        clone = pipeline.clone()
        assert clone.parent == pipeline.name
        assert clone.functions == pipeline.functions
        assert id(clone.graph) != pipeline.graph

    def test_add_graph_node_with_parent(self, pipeline):
        pipeline._parent = 'throwaway'
        pipeline._add_graph_node()
        assert pipeline.graph['throwaway'] == pipeline.name

    def test__add_graph_node_no_parent(self, pipeline):
        pipeline._parent = None
        pipeline._add_graph_node()
        assert pipeline._graph[pipeline.name] is None

    def test__check_child_contains_parent(self, pipeline):
        pipeline.apply(lambda data: True)
        child_pipeline = pipeline.clone()
        child_pipeline = child_pipeline.apply(lambda data: 'banana')
        child_pipeline._check_child_contains_parent(pipeline)

    def test__check_child_contains_parent_raise_error(self, pipeline):
        with pytest.raises(InvalidPipelineBranching):
            child_pipeline = pipeline.clone()
            pipeline._graph['a_node'] = 'node_x'
            child_pipeline._graph['a_node'] = 'node_y'
            child_pipeline._check_child_contains_parent(pipeline)

    def test_remove_redundant_functions(self, pipeline):
        child_pipeline = pipeline.clone()
        pipeline._functions = ['f_0', 'f_1']
        child_pipeline._functions = ['f_0', 'f_1', 'f_2', 'f_3']
        expected = ['f_2', 'f_3']
        actual = child_pipeline.remove_redundant_functions(pipeline).functions
        assert actual == expected

    def test_apply(self, pipeline):
        def throwaway_func(data):
            return data
        pipeline = pipeline.apply(throwaway_func)
        assert isinstance(pipeline.functions[-1], PipelineFunction)
        assert pipeline.functions[-1]._func == throwaway_func

    def test_process(self, pipeline, msg_value_with_ints):
        actual = pipeline.process(msg_value_with_ints)
        expected = {k: v + 1 for k, v in msg_value_with_ints.items()}
        assert actual == expected

    def test_process_list(self, pipeline, msg_value_with_ints):
        actual = pipeline.process([msg_value_with_ints, {'a': 10}])
        expected = [{k: v + 1 for k, v in msg_value_with_ints.items()}, {'a': 11}]
        assert actual == expected

    def test_process_break_and_return_none(self, pipeline, msg_value_with_ints):
        """
        Add a new function that returns None, then reverse the _functions order
        so that an exception would get thrown if the pipeline doesn't (as expected)
        stop processing when attempting to execute a function with a NoneType input.
        """
        pipeline = pipeline.apply(lambda data: None)
        pipeline._functions.reverse()
        assert pipeline.process(msg_value_with_ints) is None
