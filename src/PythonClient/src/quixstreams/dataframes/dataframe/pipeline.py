import uuid
from typing import Self, Optional, Callable, Any, Union
from copy import deepcopy
from quixstreams.dataframes.models import Row
import logging
from functools import partial


LOGGER = logging.getLogger()


class InvalidPipelineBranching(Exception):
    pass


class PipelineFunction:
    def __init__(self, func: Union[Callable], pipeline_name: str, name: str = None):
        self._id = str(uuid.uuid4())
        self._pipeline_name = pipeline_name
        self._func = func
        self._name = name or self._get_func_name()
        LOGGER.debug(f'Generated PipelineFunction {self.full_id}')

    def __call__(self, row: Row):
        return self._func(row)

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def pipeline_name(self) -> str:
        return self._pipeline_name

    @property
    def full_id(self):
        return f"[P:{self.pipeline_name}];[F:{self.name}];[ID:{self.id}]"

    def _get_func_name(self):
        if isinstance(self._func, partial):
            return self._func.func.__name__
        return self._func.__name__


class Pipeline:
    def __init__(self, name: str = None, _functions: list[PipelineFunction] = None, _graph: dict = None, _parent: str = None):
        self._functions = _functions or []
        self.name = name or str(uuid.uuid4())
        self._graph = _graph or {}
        self._parent = _parent
        self._add_graph_node()

    @property
    def functions(self) -> list[PipelineFunction]:
        return self._functions

    def _clone(self) -> Self:
        return Pipeline(_functions=self._functions[:], _graph=deepcopy(self._graph), _parent=self.name)

    def _add_graph_node(self):
        if self._parent:
            self._graph[self._parent] = self.name
        else:
            self._graph[self.name] = None

    def _check_child_contains_parent(self, parent: Self):
        for key in parent._graph:
            if parent_child := parent._graph[key]:
                try:
                    assert self._graph[key] == parent_child
                except AssertionError:
                    raise InvalidPipelineBranching

    def _remove_redundant_functions(self, parent: Self) -> Self:
        self._check_child_contains_parent(parent)
        self._functions = self._functions[-(len(self._functions) - len(parent._functions)):]
        return self

    @staticmethod
    def _process_function(func: PipelineFunction, data: Any) -> Optional[Union[Any, list[Any]]]:
        if isinstance(data, list):
            data = [func(d) for d in data]
            return data if any(data) else None
        else:
            return func(data)

    def apply(self, func: Callable, func_name: str = None) -> Self:
        self._functions.append(PipelineFunction(func=func, pipeline_name=self.name, name=func_name))
        return self

    def process(self, data: Any) -> Optional[Any]:
        result = data
        for func in self._functions:
            LOGGER.debug(f'Pipeline {self.name} processing func {func.name} with value {result}')
            result = self._process_function(func, result)
            LOGGER.debug(f'Pipeline {self.name} result of {func.name} is: {result}')
            if result is None:
                LOGGER.info(f'Pipeline {self.name} processing step returned a None; terminating processing')
                break
        LOGGER.debug(f'Pipeline {self.name} finished; result is {result}')
        return result
